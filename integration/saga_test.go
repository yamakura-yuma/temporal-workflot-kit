//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

var (
	testClient client.Client
	testActs   *Activities
)

// TestMain starts one dev server and one worker for the whole package. Every
// test uses its own workflow id, and idempotency keys are scoped to a run, so
// the shared ledger does not couple them.
func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup failed:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	// The dev server is the temporal CLI, which the dev container gets from
	// flake.nix. Failing here beats silently downloading a server binary.
	exe, err := exec.LookPath("temporal")
	if err != nil {
		return 0, fmt.Errorf("temporal CLI not on PATH (run these inside the dev container: just test-integration): %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: exe,
		LogLevel:     "error",
		// The saga can only write this attribute if the server knows it.
		SearchAttributes: temporal.NewSearchAttributes(
			CompensationFailedAttribute.ValueSet(false),
		),
	})
	if err != nil {
		return 0, fmt.Errorf("start dev server: %w", err)
	}
	defer func() { _ = srv.Stop() }()

	testClient = srv.Client()
	testActs = NewActivities()

	w := worker.New(testClient, TaskQueue, worker.Options{})
	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterActivity(testActs)
	if err := w.Start(); err != nil {
		return 0, fmt.Errorf("start worker: %w", err)
	}
	defer w.Stop()

	return m.Run(), nil
}

func execute(t *testing.T, in Order) client.WorkflowRun {
	t.Helper()
	we, err := testClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "saga-" + in.ID, TaskQueue: TaskQueue},
		OrderWorkflow, in)
	require.NoError(t, err)
	return we
}

// scheduledSteps returns the steps whose activities were scheduled, in order,
// with the run-scoped prefix stripped. This is the sequence an operator sees in
// the UI, which is why the tests assert on it rather than on the in-memory
// ledger alone.
func scheduledSteps(t *testing.T, we client.WorkflowRun) []string {
	t.Helper()

	iter := testClient.GetWorkflowHistory(context.Background(), we.GetID(), we.GetRunID(),
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var steps []string
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			continue
		}
		id := event.GetActivityTaskScheduledEventAttributes().GetActivityId()
		require.True(t, strings.HasPrefix(id, we.GetRunID()+"/"),
			"activity id %q should be scoped to the run", id)
		steps = append(steps, strings.TrimPrefix(id, we.GetRunID()+"/"))
	}
	return steps
}

func held(t *testing.T, we client.WorkflowRun, step string) bool {
	t.Helper()
	return testActs.held(we.GetRunID() + "/" + step)
}

// A saga that succeeds keeps everything it claimed, and schedules no
// compensations.
func TestHappyPath(t *testing.T) {
	we := execute(t, Order{ID: "happy", SKU: "widget", Amount: 4200})

	var out Receipt
	require.NoError(t, we.Get(context.Background(), &out))
	require.Equal(t, Receipt{Reservation: "res-happy", Charge: "chg-happy", Shipment: "shp-happy"}, out)

	require.Equal(t, []string{"reserve", "charge", "ship"}, scheduledSteps(t, we))
	for _, step := range []string{"reserve", "charge", "ship"} {
		require.True(t, held(t, we, step), "%s should still be claimed", step)
	}
}

// A failed step rolls the saga back in reverse order -- including the step that
// failed, whose compensation was registered before the activity ran because a
// failure does not prove the work did not happen.
func TestRollbackRunsInReverseIncludingTheFailedStep(t *testing.T) {
	we := execute(t, Order{ID: "rollback", SKU: "widget", Amount: 4200, FailAt: "ship"})

	err := we.Get(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no carrier available")

	require.Equal(t,
		[]string{"reserve", "charge", "ship", "ship:undo", "charge:undo", "reserve:undo"},
		scheduledSteps(t, we))

	for _, step := range []string{"reserve", "charge", "ship"} {
		require.False(t, held(t, we, step), "%s should have been released", step)
	}
}

// The rollback has to survive cancellation, which is the case the obvious
// implementation gets wrong: a canceled workflow context fails every later
// activity immediately, so compensations only run if they are given a context
// from workflow.NewDisconnectedContext.
//
// The in-memory test environment cannot show this -- it runs activities even on
// a canceled context -- so this is the test that actually pins the behaviour.
func TestCancellationStillCompensates(t *testing.T) {
	we := execute(t, Order{ID: "cancelme", SKU: "widget", Amount: 4200, HoldSeconds: 120})

	// Wait until the saga is past the charge step and holding.
	require.Eventually(t, func() bool { return held(t, we, "charge") },
		30*time.Second, 50*time.Millisecond, "charge never ran")

	require.NoError(t, testClient.CancelWorkflow(context.Background(), we.GetID(), we.GetRunID()))

	err := we.Get(context.Background(), nil)
	require.Error(t, err)

	require.Equal(t,
		[]string{"reserve", "charge", "charge:undo", "reserve:undo"},
		scheduledSteps(t, we),
		"both compensations must be scheduled after the cancel; ship never started")

	require.False(t, held(t, we, "charge"), "the charge should have been refunded")
	require.False(t, held(t, we, "reserve"), "the reservation should have been released")
}

// When a compensation fails, the saga keeps going, reports which step failed,
// preserves the original cause, and flags the execution so an operator can find
// it. The search attribute only works against a real server, which has to know
// the key.
func TestCompensationFailureIsReportedAndFlagged(t *testing.T) {
	we := execute(t, Order{
		ID: "undofails", SKU: "widget", Amount: 4200,
		FailAt: "ship", FailUndo: "charge", MarkAttribute: true,
	})

	err := we.Get(context.Background(), nil)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "want an ApplicationError, got %T: %v", err, err)
	require.Equal(t, saga.CompensationFailedType, appErr.Type())

	var report saga.CompensationReport
	require.NoError(t, appErr.Details(&report))
	require.Equal(t, []string{"charge"}, report.Failed)
	require.Empty(t, report.Skipped)

	// The failing compensation did not stop the one queued behind it.
	require.Equal(t,
		[]string{"reserve", "charge", "ship", "ship:undo", "charge:undo", "reserve:undo"},
		scheduledSteps(t, we))
	require.False(t, held(t, we, "reserve"), "reserve should still have been rolled back")

	// The original failure is still the cause, not replaced by the rollback's.
	require.Contains(t, err.Error(), "no carrier available")

	require.True(t, compensationFailedFlag(t, we), "the execution should be flagged for a human")
}

func compensationFailedFlag(t *testing.T, we client.WorkflowRun) bool {
	t.Helper()

	desc, err := testClient.DescribeWorkflowExecution(context.Background(), we.GetID(), we.GetRunID())
	require.NoError(t, err)

	payload, ok := desc.GetWorkflowExecutionInfo().GetSearchAttributes().GetIndexedFields()[CompensationFailedAttribute.GetName()]
	if !ok {
		return false
	}

	var flagged bool
	require.NoError(t, converter.GetDefaultDataConverter().FromPayload(payload, &flagged))
	return flagged
}
