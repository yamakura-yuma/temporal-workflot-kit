// Package stepImpl implements the steps the specifications under docs/specs/ are
// written in.
//
// The suite runs one Temporal dev server and one worker for the whole run,
// started in a BeforeSuite hook. Scenarios are kept apart by using their own
// order id, and by the fact that saga idempotency keys are scoped to a workflow
// run.
package stepImpl

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/getgauge-contrib/gauge-go/gauge"
	m "github.com/getgauge-contrib/gauge-go/gauge_messages"
	"github.com/getgauge-contrib/gauge-go/testsuit"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/yamakura-yuma/temporal-saga/example/approval"
	"github.com/yamakura-yuma/temporal-saga/example/order"
)

// Suite-wide, because they are started once per run. Per-scenario state goes in
// Gauge's scenario store instead, so that it cannot leak between scenarios.
var (
	devServer      *testsuite.DevServer
	temporalClient client.Client
	temporalWorker worker.Worker
	approvalWorker worker.Worker
	ledger         *order.Ledger
)

var _ = gauge.BeforeSuite(func(*m.ExecutionInfo) {
	// The dev server is the temporal CLI, which the dev image gets from
	// flake.nix. Failing here beats silently downloading a server binary.
	exe, err := exec.LookPath("temporal")
	if err != nil {
		fail("temporal CLI not on PATH; run these inside the dev container with `just spec`: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	devServer, err = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: exe,
		LogLevel:     "error",
		// A saga can only flag itself if the server knows the attribute.
		SearchAttributes: temporal.NewSearchAttributes(
			order.CompensationFailedAttribute.ValueSet(false),
		),
	})
	if err != nil {
		fail("start the dev server: %v", err)
	}

	temporalClient = devServer.Client()
	ledger = order.NewLedger()

	activities := order.NewActivities(ledger)

	temporalWorker = worker.New(temporalClient, order.TaskQueue, worker.Options{})
	temporalWorker.RegisterWorkflow(order.OrderWorkflow)
	temporalWorker.RegisterActivity(activities)
	if err := temporalWorker.Start(); err != nil {
		fail("start the worker: %v", err)
	}

	// The approval example declares its own task queue, so it needs its own
	// worker. The activities are the same instance, so both share one ledger.
	approvalWorker = worker.New(temporalClient, approval.TaskQueue, worker.Options{})
	approvalWorker.RegisterWorkflow(approval.ApprovalWorkflow)
	approvalWorker.RegisterActivity(activities)
	if err := approvalWorker.Start(); err != nil {
		fail("start the approval worker: %v", err)
	}
}, []string{}, testsuit.AND)

var _ = gauge.AfterSuite(func(*m.ExecutionInfo) {
	if approvalWorker != nil {
		approvalWorker.Stop()
	}
	if temporalWorker != nil {
		temporalWorker.Stop()
	}
	if devServer != nil {
		_ = devServer.Stop()
	}
}, []string{}, testsuit.AND)

// fail ends the current step. testsuit.T.Fail panics, which the runner catches
// and reports against the step that was running.
func fail(format string, args ...any) {
	testsuit.T.Fail(fmt.Errorf(format, args...))
}
