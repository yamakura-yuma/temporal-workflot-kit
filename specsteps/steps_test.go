package specsteps

// How a line in docs/specs/ reaches the code below.
//
// The only link is the step sentence. godog takes a line like
//
//	もし "charge" が実行されたら saga をキャンセルする
//
// strips the Gherkin keyword, and looks for the step registered with a regular
// expression that matches the rest:
//
//	sc.Step(`^"([^"]*)" が実行されたら saga をキャンセルする$`, func(ctx context.Context, step string) error { ... })
//
// To find what a line does, search this package for its text; to find where a
// step is used, search docs/specs/ for it. The capture groups arrive as
// arguments in the order they appear in the expression.
//
// Nothing in the compiler enforces the match, so the suite is run with
// Strict: true (see suite_test.go). A sentence with no implementation, or one
// that two expressions both match, fails the run instead of being skipped.
//
// The expressions are anchored on both ends. Without the anchors
// `^注文 "([^"]*)"$` would also match 承認待ちの注文 "approved", and godog would
// report the step as ambiguous.
//
// The outcome steps below ("saga は成功する", "ステップ ... が実行された" and so
// on) are shared with every other specification: they read the workflow from
// the scenario store and do not care which workflow put it there.
//
// The steps are kept in the order the scenarios use them.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/order"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// scenarioKey addresses the per-scenario store in the scenario's context. A
// struct type of its own cannot collide with another package's key.
type scenarioKey struct{}

// scenarioState is what one scenario remembers between its steps, over the
// suite it runs against.
type scenarioState struct {
	*suite

	run       client.WorkflowRun // the saga under test
	flat      client.WorkflowRun // the state example's second shape, when a scenario runs both
	inventory client.WorkflowRun // the external example's long-lived workflow
	result    error              // the saga's outcome, once awaited
	awaited   bool
}

// errNoSaga means the scenario asserted on a saga it never started. That is a
// mistake in the specification, not a failure of the code under test.
var errNoSaga = errors.New("no saga has been started in this scenario")

func stateOf(ctx context.Context) *scenarioState {
	return ctx.Value(scenarioKey{}).(*scenarioState)
}

func (s *scenarioState) currentRun() (client.WorkflowRun, error) {
	if s.run == nil {
		return nil, errNoSaga
	}
	return s.run, nil
}

// outcome waits for the saga to finish, once per scenario, and remembers the
// result so that later steps assert on the same thing. sagaErr is the saga's
// own outcome and may legitimately be non-nil; err means the step could not get
// that far.
func (s *scenarioState) outcome() (sagaErr error, err error) {
	if s.awaited {
		return s.result, nil
	}
	run, err := s.currentRun()
	if err != nil {
		return nil, err
	}
	s.result = run.Get(context.Background(), nil)
	s.awaited = true
	return s.result, nil
}

// sagaHistory is what the specifications ask the workflow history: which
// activities the saga scheduled, which of them completed, and what each was
// handed. It is read rather than the activities' own memory on purpose -- this
// is Temporal's record of what actually happened, and an activity cannot
// flatter itself in it.
type sagaHistory struct {
	// scheduled is the step names in the order they were scheduled. A
	// compensation appears as "<step>:undo".
	scheduled []string
	// completed reports, per step name, that its activity finished without an
	// error.
	completed map[string]bool
	// input is what each step's activity was handed.
	input map[string]*commonpb.Payloads
	// children is the ids of the child workflows the saga started, scoped to
	// the run like the step names are.
	children []string
}

// history walks the saga's history once and pulls out all of it.
func (s *scenarioState) history() (*sagaHistory, error) {
	run, err := s.currentRun()
	if err != nil {
		return nil, err
	}
	return s.historyOf(run.GetID(), run.GetRunID(), run.GetRunID()+"/")
}

// historyOf is history for any workflow, which the childflow specification
// needs: the packing activity runs inside the child, so it is the child's
// history that says what the child handed it.
func (s *scenarioState) historyOf(workflowID, runID, prefix string) (*sagaHistory, error) {
	h := &sagaHistory{
		completed: map[string]bool{},
		input:     map[string]*commonpb.Payloads{},
	}

	iter := s.client.GetWorkflowHistory(context.Background(), workflowID, runID,
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	// An ActivityTaskCompleted event names its activity by the id of the event
	// that scheduled it, so the scheduled events have to be indexed first.
	byEventID := map[int64]string{}

	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("could not read the history: %w", err)
		}

		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attr := event.GetActivityTaskScheduledEventAttributes()
			id := attr.GetActivityId()
			if !strings.HasPrefix(id, prefix) {
				return nil, fmt.Errorf("activity id %q is not scoped to the run", id)
			}
			step := strings.TrimPrefix(id, prefix)

			h.scheduled = append(h.scheduled, step)
			h.input[step] = attr.GetInput()
			byEventID[event.GetEventId()] = step

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			step, ok := byEventID[event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()]
			if ok {
				h.completed[step] = true
			}

		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			id := event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId()
			if !strings.HasPrefix(id, prefix) {
				return nil, fmt.Errorf("child workflow id %q is not scoped to the run", id)
			}
			h.children = append(h.children, strings.TrimPrefix(id, prefix))
		}
	}
	return h, nil
}

// standing reports whether the work of a step is still done: its activity
// completed and its compensation did not take it back. This is what replaced
// asking the activities to remember -- a step is undone when its "<step>:undo"
// ran, which is a fact about the saga rather than about a fake.
func (h *sagaHistory) standing(step string) bool {
	return h.completed[step] && !h.completed[step+":undo"]
}

// upstream decodes the upstream id the named step's compensation was handed.
// The compensation gets the same input its forward step got, so the id is in
// the payload the history recorded.
func (h *sagaHistory) upstream(step string) (string, error) {
	payloads, ok := h.input[step+":undo"]
	if !ok {
		return "", fmt.Errorf("補償 %q は実行されていません", step)
	}

	// Enough of ChargeReq and ShipReq to read either.
	var req struct {
		Reservation string `json:"reservation"`
		Charge      string `json:"charge"`
	}
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &req); err != nil {
		return "", fmt.Errorf("補償 %q の入力を読めません: %w", step, err)
	}

	switch step {
	case "charge":
		return req.Reservation, nil
	case "ship":
		return req.Charge, nil
	default:
		return "", fmt.Errorf("ステップ %q に前段の ID はありません", step)
	}
}

// sampleLine is the order every specification starts from: one widget, one
// price, and a step to fail at when the scenario wants one. The examples that
// need more of their own build it into their own input type.
func sampleLine(id, failAt string) activity.Order {
	return activity.Order{ID: id, SKU: "widget", Quantity: 2, Amount: 4200, FailAt: failAt}
}

func registerRollbackSteps(sc *godog.ScenarioContext) {
	// --- starting a saga -----------------------------------------------------

	sc.Step(`^注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).start(order.Request{Order: sampleLine(id, "")})
	})

	sc.Step(`^"([^"]*)" で失敗する注文 "([^"]*)"$`, func(ctx context.Context, step, id string) error {
		return stateOf(ctx).start(order.Request{Order: sampleLine(id, step)})
	})

	sc.Step(`^課金の後で待機する注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).start(order.Request{Order: sampleLine(id, ""), HoldSeconds: 120})
	})

	sc.Step(`^"([^"]*)" で失敗し、"([^"]*)" を取り消せない注文 "([^"]*)"$`,
		func(ctx context.Context, step, undoStep, id string) error {
			in := order.Request{Order: sampleLine(id, step), MarkAttribute: true}
			in.Order.FailUndo = undoStep
			return stateOf(ctx).start(in)
		})

	// --- driving a saga ------------------------------------------------------

	sc.Step(`^"([^"]*)" が実行されたら saga をキャンセルする$`, func(ctx context.Context, step string) error {
		s := stateOf(ctx)
		run, err := s.currentRun()
		if err != nil {
			return err
		}

		deadline := time.Now().Add(30 * time.Second)
		for {
			h, err := s.history()
			if err != nil {
				return err
			}
			if h.completed[step] {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%q never ran, so there is nothing to cancel", step)
			}
			time.Sleep(100 * time.Millisecond)
		}

		if err := s.client.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID()); err != nil {
			return fmt.Errorf("could not cancel the saga: %w", err)
		}
		_, err = s.outcome()
		return err
	})

	// --- outcomes ------------------------------------------------------------

	sc.Step(`^saga は成功する$`, func(ctx context.Context) error {
		sagaErr, err := stateOf(ctx).outcome()
		if err != nil {
			return err
		}
		if sagaErr != nil {
			return fmt.Errorf("expected the saga to succeed, got: %v", sagaErr)
		}
		return nil
	})

	sc.Step(`^saga は失敗する$`, func(ctx context.Context) error {
		sagaErr, err := stateOf(ctx).outcome()
		if err != nil {
			return err
		}
		if sagaErr == nil {
			return errors.New("expected the saga to fail, but it succeeded")
		}
		return nil
	})

	sc.Step(`^saga は "([^"]*)" で失敗する$`, func(ctx context.Context, text string) error {
		sagaErr, err := stateOf(ctx).outcome()
		if err != nil {
			return err
		}
		if sagaErr == nil {
			return fmt.Errorf("expected the saga to fail with %q, but it succeeded", text)
		}
		if !strings.Contains(sagaErr.Error(), text) {
			return fmt.Errorf("expected the failure to mention %q, got: %v", text, sagaErr)
		}
		return nil
	})

	sc.Step(`^補償に失敗したステップは "([^"]*)"$`, func(ctx context.Context, steps string) error {
		sagaErr, err := stateOf(ctx).outcome()
		if err != nil {
			return err
		}

		var appErr *temporal.ApplicationError
		if !errors.As(sagaErr, &appErr) {
			return fmt.Errorf("expected an ApplicationError, got %T: %v", sagaErr, sagaErr)
		}

		var report saga.CompensationReport
		if err := appErr.Details(&report); err != nil {
			return fmt.Errorf("could not read the compensation report: %w", err)
		}
		if got, want := strings.Join(report.Failed, ", "), steps; got != want {
			return fmt.Errorf("failed compensations: got %q, want %q", got, want)
		}
		return nil
	})

	sc.Step(`^saga は運用者向けにフラグが立つ$`, func(ctx context.Context) error {
		s := stateOf(ctx)
		run, err := s.currentRun()
		if err != nil {
			return err
		}

		desc, err := s.client.DescribeWorkflowExecution(context.Background(), run.GetID(), run.GetRunID())
		if err != nil {
			return fmt.Errorf("could not describe the execution: %w", err)
		}

		payload, ok := desc.GetWorkflowExecutionInfo().GetSearchAttributes().
			GetIndexedFields()[order.CompensationFailedAttribute.GetName()]
		if !ok {
			return fmt.Errorf("the execution carries no %s attribute", order.CompensationFailedAttribute.GetName())
		}

		var flagged bool
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &flagged); err != nil {
			return fmt.Errorf("could not decode the attribute: %w", err)
		}
		if !flagged {
			return errors.New("the execution is not flagged")
		}
		return nil
	})

	// --- what the history says -----------------------------------------------

	sc.Step(`^ステップ "([^"]*)" が実行された$`, func(ctx context.Context, steps string) error {
		s := stateOf(ctx)
		if _, err := s.outcome(); err != nil {
			return err
		}

		h, err := s.history()
		if err != nil {
			return err
		}
		if got, want := strings.Join(h.scheduled, ", "), steps; got != want {
			return fmt.Errorf("steps in the history:\n  got:  %s\n  want: %s", got, want)
		}
		return nil
	})

	sc.Step(`^注文は "([^"]*)" を保持したままである$`, func(ctx context.Context, steps string) error {
		s := stateOf(ctx)
		if _, err := s.outcome(); err != nil {
			return err
		}
		h, err := s.history()
		if err != nil {
			return err
		}
		for _, step := range split(steps) {
			if !h.standing(step) {
				return fmt.Errorf("%q should still be held, but it is not", step)
			}
		}
		return nil
	})

	sc.Step(`^注文は "([^"]*)" を保持していない$`, func(ctx context.Context, steps string) error {
		s := stateOf(ctx)
		if _, err := s.outcome(); err != nil {
			return err
		}
		h, err := s.history()
		if err != nil {
			return err
		}
		for _, step := range split(steps) {
			if h.standing(step) {
				return fmt.Errorf("%q is still held, so it was not rolled back", step)
			}
		}
		return nil
	})
}

func (s *scenarioState) start(in order.Request) error {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "saga-" + in.Order.ID, TaskQueue: order.TaskQueue},
		order.OrderWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the saga: %w", err)
	}
	s.run = run
	return nil
}

func split(list string) []string {
	parts := strings.Split(list, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
