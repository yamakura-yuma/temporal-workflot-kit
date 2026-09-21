// Package specsteps implements the steps the specifications under docs/specs/
// are written in.
//
// The suite runs one Temporal dev server and one worker per example for the
// whole run, started in TestMain. Scenarios are kept apart by using their own
// order id, and by the fact that saga idempotency keys are scoped to a workflow
// run.
package specsteps

import (
	"context"
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

	"github.com/yamakura-yuma/temporal-workflow-kit/example/approval"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/childflow"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/external"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/order"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/pipeline"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/state"
)

// uiPort is the dev server's Web UI, published by the spec-ui recipe. It is
// fixed rather than free-picked so that the URL printed below is the URL that
// docker-compose.yml publishes.
const uiPort = "8233"

// holdEnv keeps the dev server running after the run, so that the histories the
// scenarios just produced can be read in the UI. Set by `just spec-ui`; unset
// everywhere else, which is what keeps `just spec` and `just ci` unchanged.
const holdEnv = "SPEC_HOLD"

// specsDir is where the .feature files live. godog defaults to ./features; this
// keeps the specifications next to the prose documentation instead.
const specsDir = "../docs/specs"

// Suite-wide, because they are started once per run. Per-scenario state lives in
// the scenario's context instead, so that it cannot leak between scenarios.
var (
	devServer       *testsuite.DevServer
	temporalClient  client.Client
	temporalWorker  worker.Worker
	approvalWorker  worker.Worker
	pipelineWorker  worker.Worker
	childflowWorker worker.Worker
	stateWorker     worker.Worker
	externalWorker  worker.Worker
	ledger          *order.Ledger
	pipelineLedger  *pipeline.Ledger
	childflowLedger *childflow.Ledger
	stateLedger     *state.Ledger
)

func TestMain(m *testing.M) {
	if err := startSuite(); err != nil {
		fmt.Fprintf(os.Stderr, "could not start the spec suite: %v\n", err)
		stopSuite()
		os.Exit(1)
	}

	code := m.Run()
	stopSuite()
	os.Exit(code)
}

// TestFeatures runs every scenario under docs/specs/.
//
// Strict makes an undefined, pending or ambiguous step fail the suite, which is
// the only thing that ties a step sentence to a Go function: nothing in the
// compiler does. TestingT turns every scenario into a Go subtest, so
// `go test -run 'TestFeatures/<シナリオ名>'` runs one of them.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{specsDir},
			Strict:   true,
			TestingT: t,
		},
	}

	if suite.Run() != 0 {
		t.Fatal("the specifications under docs/specs/ did not pass")
	}
}

// InitializeScenario registers every step and gives each scenario a store of
// its own. The store is a pointer, so steps mutate it without handing a new
// context back.
func InitializeScenario(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		return context.WithValue(ctx, scenarioKey{}, &scenarioState{}), nil
	})

	registerOrderSteps(sc)
	registerApprovalSteps(sc)
	registerChildflowSteps(sc)
	registerExternalSteps(sc)
	registerPipelineSteps(sc)
	registerStateSteps(sc)
}

func startSuite() error {
	// The dev server is the temporal CLI, which the dev image gets from
	// flake.nix. Failing here beats silently downloading a server binary.
	exe, err := exec.LookPath("temporal")
	if err != nil {
		return fmt.Errorf("temporal CLI not on PATH; run these inside the dev container with `just spec`: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	devServer, err = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
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
		return fmt.Errorf("start the dev server: %w", err)
	}

	temporalClient = devServer.Client()
	ledger = order.NewLedger()

	activities := order.NewActivities(ledger)

	temporalWorker = worker.New(temporalClient, order.TaskQueue, worker.Options{})
	temporalWorker.RegisterWorkflow(order.OrderWorkflow)
	temporalWorker.RegisterActivity(activities)
	if err := temporalWorker.Start(); err != nil {
		return fmt.Errorf("start the worker: %w", err)
	}

	// The approval example declares its own task queue, so it needs its own
	// worker. The activities are the same instance, so both share one ledger.
	approvalWorker = worker.New(temporalClient, approval.TaskQueue, worker.Options{})
	approvalWorker.RegisterWorkflow(approval.ApprovalWorkflow)
	approvalWorker.RegisterActivity(activities)
	if err := approvalWorker.Start(); err != nil {
		return fmt.Errorf("start the approval worker: %w", err)
	}

	pipelineLedger = pipeline.NewLedger()
	pipelineWorker = worker.New(temporalClient, pipeline.TaskQueue, worker.Options{})
	pipelineWorker.RegisterWorkflow(pipeline.PipelineWorkflow)
	pipelineWorker.RegisterActivity(pipeline.NewActivities(pipelineLedger))
	if err := pipelineWorker.Start(); err != nil {
		return fmt.Errorf("start the pipeline worker: %w", err)
	}

	// The packing children inherit this task queue, so one worker covers the
	// parent, both children and the activities.
	childflowLedger = childflow.NewLedger()
	childflowWorker = worker.New(temporalClient, childflow.TaskQueue, worker.Options{})
	childflowWorker.RegisterWorkflow(childflow.ChildflowWorkflow)
	childflowWorker.RegisterWorkflow(childflow.PackWorkflow)
	childflowWorker.RegisterActivity(childflow.NewActivities(childflowLedger))
	if err := childflowWorker.Start(); err != nil {
		return fmt.Errorf("start the childflow worker: %w", err)
	}

	stateLedger = state.NewLedger()
	stateWorker = worker.New(temporalClient, state.TaskQueue, worker.Options{})
	stateWorker.RegisterWorkflow(state.StateWorkflow)
	stateWorker.RegisterActivity(state.NewActivities(stateLedger))
	if err := stateWorker.Start(); err != nil {
		return fmt.Errorf("start the state worker: %w", err)
	}

	externalWorker = worker.New(temporalClient, external.TaskQueue, worker.Options{})
	externalWorker.RegisterWorkflow(external.ExternalWorkflow)
	externalWorker.RegisterWorkflow(external.InventoryWorkflow)
	externalWorker.RegisterActivity(external.NewActivities(external.NewLedger()))
	if err := externalWorker.Start(); err != nil {
		return fmt.Errorf("start the external worker: %w", err)
	}

	return nil
}

func stopSuite() {
	for _, w := range []worker.Worker{
		externalWorker, stateWorker, childflowWorker,
		pipelineWorker, approvalWorker, temporalWorker,
	} {
		if w != nil {
			w.Stop()
		}
	}

	// The workers are down by now, but the server still answers for history, so
	// everything the scenarios just did is readable. Blocking here is the whole
	// point: without it the server outlives the run by no time at all.
	if os.Getenv(holdEnv) != "" && devServer != nil {
		fmt.Fprintf(os.Stderr, "\n  履歴を読む: http://localhost:%s\n", uiPort)
		fmt.Fprintf(os.Stderr, "  WorkflowID は saga-<注文 id>（例: saga-cancelme, saga-undofails）\n")
		fmt.Fprintf(os.Stderr, "  終了するには Ctrl-C\n\n")
		select {}
	}

	if devServer != nil {
		_ = devServer.Stop()
	}
}
