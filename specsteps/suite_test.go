// Package specsteps implements the steps the specifications under docs/specs/
// are written in.
//
// The suite runs one Temporal dev server and one worker per example for the
// whole run, started in TestMain. Scenarios are kept apart by using their own
// order id, and by the fact that saga idempotency keys are scoped to a workflow
// run.
//
// Everything here is _test.go. Nobody imports this package; it exists to be run.
package specsteps

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/approval"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/childflow"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/external"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/order"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/pipeline"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/state"
)

// uiPort is the dev server's Web UI, published by the spec-ui recipe. It is
// fixed rather than free-picked so that the URL printed below is the URL that
// docker-compose.yml publishes.
const uiPort = "8233"

// holdEnv keeps the dev server running after the run, so that the histories the
// scenarios just produced can be read in the UI. Set by `just spec-ui`; unset
// everywhere else, which is what keeps `just spec` and `just ci` unchanged.
const holdEnv = "SPEC_HOLD"

// specsDir is where the .feature files live, relative to this package.
const specsDir = "../docs/specs"

// suite is what the whole run shares. Every worker registers the same
// activities -- there is only one set of them -- and holds them for its
// lifetime, so they cannot be per-scenario. Handing them to each scenario
// through its context is what keeps them out of package-level variables.
//
// Sharing them across examples is safe because an idempotency key carries the
// workflow run id, so no two sagas can write the same one.
type suite struct {
	client client.Client
	acts   *activity.Activities
}

// suiteRun is the one piece of package-level state left: what TestMain has to
// hold on to in order to tear it down again. Steps never read it; they take
// what they need from the scenario's context.
type suiteRun struct {
	devServer *testsuite.DevServer
	workers   []worker.Worker
	suite     *suite
}

var running *suiteRun

func TestMain(m *testing.M) {
	// testing.Short() reads a flag that m.Run() has not parsed yet.
	flag.Parse()

	// Under -short there is no dev server and no worker at all, so `just test`
	// stays the fast loop it was. TestFeatures skips itself for the same reason.
	if testing.Short() {
		os.Exit(m.Run())
	}

	r, err := startSuite()
	running = r
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not start the spec suite: %v\n", err)
		stopSuite(r)
		os.Exit(1)
	}

	code := m.Run()
	stopSuite(r)
	os.Exit(code)
}

// TestFeatures runs every scenario under docs/specs/.
//
// Strict makes an undefined, pending or ambiguous step fail the suite, which is
// the only thing that ties a step sentence to a Go function: nothing in the
// compiler does. TestingT turns every scenario into a Go subtest, so
// `go test -run 'TestFeatures/<シナリオ名>'` runs one of them.
func TestFeatures(t *testing.T) {
	if testing.Short() {
		t.Skip("the specifications need a Temporal dev server; drop -short to run them")
	}

	godogSuite := godog.TestSuite{
		ScenarioInitializer: initializeScenario(running.suite),
		Options: &godog.Options{
			Format: "pretty",
			// The specifications live next to the prose documentation rather
			// than in godog's default ./features.
			Paths:    []string{specsDir},
			Strict:   true,
			TestingT: t,
			// Concurrency stays at the default 1. The scenarios share one set of
			// activity instances and one dev server, so they cannot run in parallel.
		},
	}

	if godogSuite.Run() != 0 {
		t.Fatal("the specifications did not pass")
	}
}

// initializeScenario registers every step and gives each scenario a state of
// its own. That state is a pointer, so steps mutate it without handing a new
// context back.
func initializeScenario(s *suite) func(*godog.ScenarioContext) {
	return func(sc *godog.ScenarioContext) {
		sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
			return context.WithValue(ctx, scenarioKey{}, &scenarioState{suite: s}), nil
		})

		registerRollbackSteps(sc)
		registerApprovalSteps(sc)
		registerChildflowSteps(sc)
		registerExternalSteps(sc)
		registerPipelineSteps(sc)
		registerStateSteps(sc)
	}
}

func startSuite() (*suiteRun, error) {
	r := &suiteRun{}

	// The dev server is the temporal CLI, which the dev image gets from
	// flake.nix. Failing here beats silently downloading a server binary.
	exe, err := exec.LookPath("temporal")
	if err != nil {
		return r, fmt.Errorf("temporal CLI not on PATH; run these inside the dev container with `just spec`: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	r.devServer, err = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: exe,
		LogLevel:     "error",

		// The Web UI is what a failing scenario is read in: the history shows
		// every activity the saga scheduled, in order, which is the same thing
		// the specifications assert as a string. It costs nothing when nobody
		// opens it, and `just spec-ui` keeps the server up long enough to.
		//
		// The UI binds to 127.0.0.1 by default, which is the container's own
		// loopback and so unreachable from the host.
		EnableUI:  true,
		UIPort:    uiPort,
		ExtraArgs: []string{"--ui-ip", "0.0.0.0"},

		// A saga can only flag itself if the server knows the attribute.
		SearchAttributes: temporal.NewSearchAttributes(
			order.CompensationFailedAttribute.ValueSet(false),
		),
	})
	if err != nil {
		return r, fmt.Errorf("start the dev server: %w", err)
	}

	r.suite = &suite{
		client: r.devServer.Client(),
		acts:   activity.NewActivities(),
	}

	start := func(name string, register func(w worker.Worker)) error {
		w := worker.New(r.suite.client, name, worker.Options{})
		register(w)
		if err := w.Start(); err != nil {
			return fmt.Errorf("start the %s worker: %w", name, err)
		}
		r.workers = append(r.workers, w)
		return nil
	}

	if err := start(order.TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(order.OrderWorkflow)
		w.RegisterActivity(r.suite.acts)
	}); err != nil {
		return r, err
	}

	// The approval example declares its own task queue, so it needs its own
	// worker.
	if err := start(approval.TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(approval.ApprovalWorkflow)
		w.RegisterActivity(r.suite.acts)
	}); err != nil {
		return r, err
	}

	if err := start(pipeline.TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(pipeline.PipelineWorkflow)
		w.RegisterActivity(r.suite.acts)
	}); err != nil {
		return r, err
	}

	// The packing children inherit this task queue, so one worker covers the
	// parent, both children and the activities.
	if err := start(childflow.TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(childflow.ChildflowWorkflow)
		w.RegisterWorkflow(childflow.PackWorkflow)
		w.RegisterActivity(r.suite.acts)
	}); err != nil {
		return r, err
	}

	// The state example is written twice, and both shapes run here: the
	// specification starts them over the same order and compares the receipts,
	// which is the only thing keeping the flat one from rotting.
	if err := start(state.TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(state.StateWorkflow)
		w.RegisterWorkflow(state.FlatWorkflow)
		w.RegisterActivity(r.suite.acts)
	}); err != nil {
		return r, err
	}

	if err := start(external.TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(external.ExternalWorkflow)
		w.RegisterWorkflow(external.InventoryWorkflow)
		w.RegisterActivity(r.suite.acts)
	}); err != nil {
		return r, err
	}

	return r, nil
}

func stopSuite(r *suiteRun) {
	for i := len(r.workers) - 1; i >= 0; i-- {
		r.workers[i].Stop()
	}

	// The workers are down by now, but the server still answers for history, so
	// everything the scenarios just did is readable. Blocking here is the whole
	// point: without it the server outlives the run by no time at all.
	if os.Getenv(holdEnv) != "" && r.devServer != nil {
		fmt.Fprintf(os.Stderr, "\n  履歴を読む: http://localhost:%s\n", uiPort)
		fmt.Fprintf(os.Stderr, "  WorkflowID は saga-<注文 id>（例: saga-cancelme, saga-undofails）\n")
		fmt.Fprintf(os.Stderr, "  終了するには Ctrl-C\n\n")
		select {}
	}

	if r.devServer != nil {
		_ = r.devServer.Stop()
	}
}
