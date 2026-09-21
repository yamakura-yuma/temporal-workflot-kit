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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/childflow"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/order"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/pipeline"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/state"
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

// scheduledSteps returns the saga's steps as activities were scheduled, in the
// order the workflow history records them. A compensation appears as
// "<step>:undo".
func (s *scenarioState) scheduledSteps() ([]string, error) {
	run, err := s.currentRun()
	if err != nil {
		return nil, err
	}
	prefix := run.GetRunID() + "/"

	iter := s.client.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID(),
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var steps []string
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("could not read the history: %w", err)
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			continue
		}
		id := event.GetActivityTaskScheduledEventAttributes().GetActivityId()
		if !strings.HasPrefix(id, prefix) {
			return nil, fmt.Errorf("activity id %q is not scoped to the run", id)
		}
		steps = append(steps, strings.TrimPrefix(id, prefix))
	}
	return steps, nil
}

func registerRollbackSteps(sc *godog.ScenarioContext) {
	// --- starting a saga -----------------------------------------------------

	sc.Step(`^注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).start(order.Order{ID: id, SKU: "widget", Amount: 4200})
	})

	sc.Step(`^"([^"]*)" で失敗する注文 "([^"]*)"$`, func(ctx context.Context, step, id string) error {
		return stateOf(ctx).start(order.Order{ID: id, SKU: "widget", Amount: 4200, FailAt: step})
	})

	sc.Step(`^課金の後で待機する注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).start(order.Order{ID: id, SKU: "widget", Amount: 4200, HoldSeconds: 120})
	})

	sc.Step(`^"([^"]*)" で失敗し、"([^"]*)" を取り消せない注文 "([^"]*)"$`,
		func(ctx context.Context, step, undoStep, id string) error {
			return stateOf(ctx).start(order.Order{
				ID: id, SKU: "widget", Amount: 4200,
				FailAt: step, FailUndo: undoStep, MarkAttribute: true,
			})
		})

	// --- driving a saga ------------------------------------------------------

	sc.Step(`^"([^"]*)" が実行されたら saga をキャンセルする$`, func(ctx context.Context, step string) error {
		s := stateOf(ctx)
		run, err := s.currentRun()
		if err != nil {
			return err
		}

		deadline := time.Now().Add(30 * time.Second)
		for !order.Held(s.order, run.GetRunID(), step) {
			if time.Now().After(deadline) {
				return fmt.Errorf("%q never ran, so there is nothing to cancel", step)
			}
			time.Sleep(50 * time.Millisecond)
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

	// --- what the history and the ledger say ---------------------------------

	sc.Step(`^ステップ "([^"]*)" が実行された$`, func(ctx context.Context, steps string) error {
		s := stateOf(ctx)
		if _, err := s.outcome(); err != nil {
			return err
		}

		scheduled, err := s.scheduledSteps()
		if err != nil {
			return err
		}
		if got, want := strings.Join(scheduled, ", "), steps; got != want {
			return fmt.Errorf("steps in the history:\n  got:  %s\n  want: %s", got, want)
		}
		return nil
	})

	sc.Step(`^注文は "([^"]*)" を保持したままである$`, func(ctx context.Context, steps string) error {
		s := stateOf(ctx)
		run, err := s.currentRun()
		if err != nil {
			return err
		}
		for _, step := range split(steps) {
			if !order.Held(s.order, run.GetRunID(), step) {
				return fmt.Errorf("%q should still be held, but it is not", step)
			}
		}
		return nil
	})

	sc.Step(`^注文は "([^"]*)" を保持していない$`, func(ctx context.Context, steps string) error {
		s := stateOf(ctx)
		run, err := s.currentRun()
		if err != nil {
			return err
		}
		for _, step := range split(steps) {
			if order.Held(s.order, run.GetRunID(), step) ||
				pipeline.Held(s.pipeline, run.GetRunID(), step) ||
				childflow.Held(s.childflow, run.GetRunID(), step) ||
				state.Held(s.state, run.GetRunID(), step) {
				return fmt.Errorf("%q is still held, so it was not rolled back", step)
			}
		}
		return nil
	})
}

func (s *scenarioState) start(in order.Order) error {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "saga-" + in.ID, TaskQueue: order.TaskQueue},
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
