package stepImpl

// How a line in specs/ reaches the code below
//
// The only link is the step text. Gauge takes a line like
//
//	* "charge" が実行されたら saga をキャンセルする
//
// strips the quoted arguments, and looks for a gauge.Step registered with the
// same text with <placeholders> where the arguments were:
//
//	gauge.Step("<step> が実行されたら saga をキャンセルする", func(step string) { ... })
//
// So to find what a line does, search this package for its text; to find where
// a step is used, search specs/ for it. `just spec-steps` prints the pairing.
// The arguments arrive in the order the placeholders appear in the text.
//
// Nothing in the compiler enforces the match, so `just spec-validate` does: it
// reports any step with no implementation, with its file and line, without
// starting a server. It runs as part of `just ci`.
//
// The steps below are kept in the order the scenarios use them.

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/getgauge-contrib/gauge-go/gauge"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/yamakura-yuma/temporal-saga/example/order"
	"github.com/yamakura-yuma/temporal-saga/saga"
)

// Scenario store keys.
const (
	keyRun = "run" // the running workflow
	keyErr = "err" // what it finished with
)

// --- starting a saga ---------------------------------------------------------

var _ = gauge.Step("注文 <id>", func(id string) {
	start(order.Order{ID: id, SKU: "widget", Amount: 4200})
})

var _ = gauge.Step("<step> で失敗する注文 <id>", func(step, id string) {
	start(order.Order{ID: id, SKU: "widget", Amount: 4200, FailAt: step})
})

var _ = gauge.Step("課金の後で待機する注文 <id>", func(id string) {
	start(order.Order{ID: id, SKU: "widget", Amount: 4200, HoldSeconds: 120})
})

var _ = gauge.Step("<step> で失敗し、<undoStep> を取り消せない注文 <id>",
	func(step, undoStep, id string) {
		start(order.Order{
			ID: id, SKU: "widget", Amount: 4200,
			FailAt: step, FailUndo: undoStep, MarkAttribute: true,
		})
	})

func start(in order.Order) {
	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "saga-" + in.ID, TaskQueue: order.TaskQueue},
		order.OrderWorkflow, in)
	if err != nil {
		fail("could not start the saga: %v", err)
	}
	gauge.GetScenarioStore()[keyRun] = run
}

// --- driving it --------------------------------------------------------------

var _ = gauge.Step("<step> が実行されたら saga をキャンセルする", func(step string) {
	run := currentRun()

	deadline := time.Now().Add(30 * time.Second)
	for !ledger.Held(run.GetRunID(), step) {
		if time.Now().After(deadline) {
			fail("%q never ran, so there was nothing to cancel", step)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := temporalClient.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID()); err != nil {
		fail("could not cancel the saga: %v", err)
	}
	awaitResult()
})

// --- outcomes ----------------------------------------------------------------

var _ = gauge.Step("saga は成功する", func() {
	if err := awaitResult(); err != nil {
		fail("expected the saga to succeed, got: %v", err)
	}
})

var _ = gauge.Step("saga は失敗する", func() {
	if err := awaitResult(); err == nil {
		fail("expected the saga to fail, but it succeeded")
	}
})

var _ = gauge.Step("saga は <text> で失敗する", func(text string) {
	err := awaitResult()
	if err == nil {
		fail("expected the saga to fail with %q, but it succeeded", text)
	}
	if !strings.Contains(err.Error(), text) {
		fail("expected the failure to mention %q, got: %v", text, err)
	}
})

var _ = gauge.Step("補償に失敗したステップは <steps>", func(steps string) {
	err := awaitResult()

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		fail("expected an ApplicationError, got %T: %v", err, err)
	}

	var report saga.CompensationReport
	if err := appErr.Details(&report); err != nil {
		fail("could not read the compensation report: %v", err)
	}
	if got, want := strings.Join(report.Failed, ", "), steps; got != want {
		fail("failed compensations: got %q, want %q", got, want)
	}
})

var _ = gauge.Step("saga は運用者向けにフラグが立つ", func() {
	run := currentRun()

	desc, err := temporalClient.DescribeWorkflowExecution(context.Background(), run.GetID(), run.GetRunID())
	if err != nil {
		fail("could not describe the execution: %v", err)
	}

	payload, ok := desc.GetWorkflowExecutionInfo().GetSearchAttributes().
		GetIndexedFields()[order.CompensationFailedAttribute.GetName()]
	if !ok {
		fail("the execution carries no %s attribute", order.CompensationFailedAttribute.GetName())
	}

	var flagged bool
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &flagged); err != nil {
		fail("could not decode the attribute: %v", err)
	}
	if !flagged {
		fail("the execution is not flagged")
	}
})

// --- what the history and the ledger say -------------------------------------

var _ = gauge.Step("ステップ <steps> が実行された", func(steps string) {
	awaitResult()

	if got, want := strings.Join(scheduledSteps(), ", "), steps; got != want {
		fail("steps in the history:\n  got:  %s\n  want: %s", got, want)
	}
})

var _ = gauge.Step("注文は <steps> を保持したままである", func(steps string) {
	run := currentRun()
	for _, step := range split(steps) {
		if !ledger.Held(run.GetRunID(), step) {
			fail("%q should still be held, but it is not", step)
		}
	}
})

var _ = gauge.Step("注文は <steps> を保持していない", func(steps string) {
	run := currentRun()
	for _, step := range split(steps) {
		if ledger.Held(run.GetRunID(), step) {
			fail("%q is still held, so it was not rolled back", step)
		}
	}
})

// --- helpers -----------------------------------------------------------------

func currentRun() client.WorkflowRun {
	run, ok := gauge.GetScenarioStore()[keyRun].(client.WorkflowRun)
	if !ok {
		fail("no saga has been started in this scenario")
	}
	return run
}

// awaitResult waits for the saga to finish, once per scenario, and remembers
// the outcome so that several steps can assert on it.
func awaitResult() error {
	store := gauge.GetScenarioStore()
	if result, ok := store[keyErr]; ok {
		err, _ := result.(error)
		return err
	}

	err := currentRun().Get(context.Background(), nil)
	store[keyErr] = err
	return err
}

// scheduledSteps returns the saga steps whose activities were scheduled, in
// order, as the workflow history records them. A compensation appears as
// "<step>:undo".
func scheduledSteps() []string {
	run := currentRun()
	prefix := run.GetRunID() + "/"

	iter := temporalClient.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID(),
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var steps []string
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			fail("could not read the history: %v", err)
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			continue
		}
		id := event.GetActivityTaskScheduledEventAttributes().GetActivityId()
		if !strings.HasPrefix(id, prefix) {
			fail("activity id %q is not scoped to the run", id)
		}
		steps = append(steps, strings.TrimPrefix(id, prefix))
	}
	return steps
}

func split(list string) []string {
	parts := strings.Split(list, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
