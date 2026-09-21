package saga_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// --- test activities ---------------------------------------------------------

type req struct {
	Step string `json:"step"`
	Fail bool   `json:"fail"`
}

// recorder captures the order activities ran in. Activities run on their own
// goroutines in the test environment, so it is guarded.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func newRecorder() *recorder { return &recorder{} }

func (r *recorder) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

type acts struct{ r *recorder }

func (a *acts) Do(ctx context.Context, in req) (string, error) {
	a.r.record("do:" + in.Step)
	if in.Fail {
		return "", errors.New("forward failed: " + in.Step)
	}
	return "ok:" + in.Step, nil
}

func (a *acts) Undo(ctx context.Context, in req) error {
	a.r.record("undo:" + in.Step)
	return nil
}

func (a *acts) UndoFails(ctx context.Context, in req) error {
	a.r.record("undo:" + in.Step)
	return errors.New("compensation failed: " + in.Step)
}

// --- the workflow under test -------------------------------------------------

type stepSpec struct {
	Name      string `json:"name"`
	Fail      bool   `json:"fail"`
	UndoFails bool   `json:"undo_fails"`
	NoUndo    bool   `json:"no_undo"`
}

type plan struct {
	Steps []stepSpec `json:"steps"`
	// SleepAfter waits an hour after the step at this index, so a test can
	// cancel the workflow mid-saga. -1 disables it.
	SleepAfter int `json:"sleep_after"`
	// ReturnNil makes the body return a nil error even when a step failed,
	// i.e. the caller forgot to check.
	ReturnNil bool `json:"return_nil"`
	// Parallel and ContinueWithError go straight to saga.Options.
	Parallel          bool `json:"parallel"`
	ContinueWithError bool `json:"continue_with_error"`
}

func planWorkflow(ctx workflow.Context, p plan) ([]string, error) {
	var a *acts // nil receiver: only the method's name is used

	// One attempt per activity: these tests assert on the exact sequence of
	// calls, and the SDK's default retry policy would repeat the failures.
	once := &temporal.RetryPolicy{MaximumAttempts: 1}

	ctx = workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once})

	return saga.RunOrCompensate(ctx, saga.Options{
		ParallelCompensation: p.Parallel,
		ContinueWithError:    p.ContinueWithError,
	}, func(ctx workflow.Context, s *saga.Saga) ([]string, error) {
		var out []string
		for i, spec := range p.Steps {
			in := req{Step: spec.Name, Fail: spec.Fail}

			undoFn := a.Undo
			switch {
			case spec.NoUndo:
				undoFn = nil
			case spec.UndoFails:
				undoFn = a.UndoFails
			}

			var undo func(workflow.Context) error
			if undoFn != nil {
				undo = func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, undoFn, in).Get(ctx, nil)
				}
			}

			var v string
			s.Step(ctx, spec.Name, func(ctx workflow.Context) error {
				return workflow.ExecuteActivity(ctx, a.Do, in).Get(ctx, &v)
			}, undo)
			out = append(out, v)

			if p.SleepAfter == i {
				if err := workflow.Sleep(ctx, time.Hour); err != nil {
					return nil, err
				}
			}
		}
		if p.ReturnNil {
			return out, nil
		}
		return out, s.Err()
	})
}

func newEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, *recorder) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	r := newRecorder()
	env.RegisterWorkflow(planWorkflow)
	env.RegisterActivity(&acts{r: r})
	return env, r
}

func steps(names ...string) []stepSpec {
	out := make([]stepSpec, 0, len(names))
	for _, n := range names {
		out = append(out, stepSpec{Name: n})
	}
	return out
}

// --- tests -------------------------------------------------------------------

// A saga that succeeds must not compensate. The obvious property, and the one a
// "compensate in a defer" implementation gets wrong by default.
func TestSuccessDoesNotCompensate(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{Steps: steps("a", "b", "c"), SleepAfter: -1})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "do:c"}, calls)

	var out []string
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Equal(t, []string{"ok:a", "ok:b", "ok:c"}, out)
}

// Compensations run in reverse order, and the failing step is compensated too:
// its compensation was registered before the activity ran, because an activity
// that reports failure may still have taken effect.
func TestCompensatesInReverseIncludingTheFailedStep(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b"}, {Name: "c", Fail: true}},
		SleepAfter: -1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	calls := r.snapshot()
	require.Equal(t,
		[]string{"do:a", "do:b", "do:c", "undo:c", "undo:b", "undo:a"},
		calls)
}

// The library no longer gives the two halves of a step a shared idempotency
// key, and no longer names the activities a step starts. It does not touch
// ActivityID at all: a key belongs in the request the caller sends, and a
// readable history is the caller's to arrange. What used to be checked here now
// lives where it belongs -- docs/specs/childflow.feature checks that a workflow
// hands both halves of its packing step the same key.

// A body that returns nil after a step failed must still fail the workflow and
// compensate. Without this, forgetting one error check completes the workflow
// with its side effects half applied -- recorded as success, with no alert.
func TestNilErrorFromBodyStillCompensates(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b", Fail: true}, {Name: "c"}},
		SleepAfter: -1,
		ReturnNil:  true,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err, "a failed step must fail the workflow even if the body returned nil")
	require.Contains(t, err.Error(), "forward failed: b")

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "undo:b", "undo:a"}, calls,
		"step c must be skipped, and a and b compensated")

	// The half-filled result must not escape.
	var out []string
	require.Error(t, env.GetWorkflowResult(&out))
}

// Compensations must still run when the workflow is canceled. This is the case
// that breaks the obvious implementation: a canceled workflow context fails
// every later activity immediately, so compensation has to run on a context
// from workflow.NewDisconnectedContext.
func TestCompensatesAfterCancellation(t *testing.T) {
	env, r := newEnv(t)

	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, time.Minute)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b"}},
		SleepAfter: 0, // wait after step a, so the cancel lands mid-saga
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "undo:a"}, calls,
		"the compensation for step a must run despite the cancellation")
}

// A failing compensation must not replace the original failure, and must stay
// recognisable in the workflow history.
//
// errors.Join cannot do this: Temporal's failure converter is a type switch
// over concrete types that follows a single Unwrap() error, so a joined error
// is recorded as type "joinError" with no cause and NonRetryableErrorTypes
// stops matching it.
func TestCompensationFailureKeepsTypeAndCause(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a", UndoFails: true}, {Name: "b", Fail: true}},
		SleepAfter: -1,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "want an ApplicationError, got %T: %v", err, err)
	require.Equal(t, saga.CompensationFailedType, appErr.Type())
	require.NotEqual(t, "joinError", appErr.Type())

	var report saga.CompensationReport
	require.NoError(t, appErr.Details(&report))
	require.Equal(t, []string{"a"}, report.Failed)
	require.Empty(t, report.Skipped)

	// The original failure is still reachable and still says what went wrong.
	require.Contains(t, err.Error(), "compensation did not finish cleanly")
	require.NotNil(t, appErr.Unwrap(), "the original failure must stay in the cause chain")

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "undo:b", "undo:a"}, calls,
		"a failing compensation must not stop the ones still queued")
}

// A step with no compensation is allowed, and leaves nothing to undo.
func TestStepWithoutCompensation(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a", NoUndo: true}, {Name: "b", Fail: true}},
		SleepAfter: -1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "undo:b"}, calls)
}

// Step names identify a step's idempotency key, so a duplicate is a bug rather
// than something to paper over.
func TestDuplicateStepNameFails(t *testing.T) {
	env, _ := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "a"}},
		SleepAfter: -1,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate step name")
}

// --- child workflow steps ----------------------------------------------------

// childDo and childUndo are the two halves of a child-workflow step. They call
// the same activities the activity steps use, so the recorder sees one ordered
// list across both kinds of step.
func childDo(ctx workflow.Context, in req) (string, error) {
	var a *acts
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	var out string
	err := workflow.ExecuteActivity(ctx, a.Do, in).Get(ctx, &out)
	return out, err
}

func childUndo(ctx workflow.Context, in req) error {
	var a *acts
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteActivity(ctx, a.Undo, in).Get(ctx, nil)
}

// mixedWorkflow has an activity step, then a child workflow step, then an
// activity step that fails.
func mixedWorkflow(ctx workflow.Context) ([]string, error) {
	var a *acts
	once := &temporal.RetryPolicy{MaximumAttempts: 1}

	ctx = workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once})

	act := func(fn, in any) func(workflow.Context) error {
		return func(ctx workflow.Context) error {
			return workflow.ExecuteActivity(ctx, fn, in).Get(ctx, nil)
		}
	}
	child := func(fn, in any) func(workflow.Context) error {
		return func(ctx workflow.Context) error {
			return workflow.ExecuteChildWorkflow(ctx, fn, in).Get(ctx, nil)
		}
	}

	return saga.RunOrCompensate(ctx, saga.Options{}, func(ctx workflow.Context, s *saga.Saga) ([]string, error) {
		s.Step(ctx, "a", act(a.Do, req{Step: "a"}), act(a.Undo, req{Step: "a"}))
		s.Step(ctx, "b", child(childDo, req{Step: "b"}), child(childUndo, req{Step: "b"}))
		s.Step(ctx, "c", act(a.Do, req{Step: "c", Fail: true}), act(a.Undo, req{Step: "c"}))
		return nil, s.Err()
	})
}

// A step can be a child workflow as well as an activity, and the rollback runs
// both kinds in one reverse order. Nothing about the registry is per-executor:
// it holds compensations as plain functions.
func TestChildWorkflowCompensatesInOneOrder(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	r := newRecorder()
	env.RegisterWorkflow(mixedWorkflow)
	env.RegisterWorkflow(childDo)
	env.RegisterWorkflow(childUndo)
	env.RegisterActivity(&acts{r: r})

	env.ExecuteWorkflow(mixedWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	calls := r.snapshot()
	require.Equal(t,
		[]string{"do:a", "do:b", "do:c", "undo:c", "undo:b", "undo:a"},
		calls,
		"the child workflow step must take its place in the one reverse order")
}

// --- waiting for a signal ----------------------------------------------------

// waitStep is the wait written the way the library intends: a step of its own,
// which decides for itself what a missing signal means.
// awaitSignal is what the library used to export. It is a plain SDK idiom, so
// the tests carry their own copy rather than the package doing it for everyone.
func awaitSignal[T any](ctx workflow.Context, name string, timeout time.Duration) (T, bool) {
	var payload T
	arrived := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, name),
		func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &payload)
			arrived = true
		})
	selector.AddFuture(workflow.NewTimer(ctx, timeout), func(workflow.Future) {})
	selector.Select(ctx)

	return payload, arrived
}

func waitStep(out *string) func(workflow.Context) error {
	return func(ctx workflow.Context) error {
		payload, arrived := awaitSignal[string](ctx, "never", time.Hour)
		if !arrived {
			return temporal.NewApplicationError("nobody answered", "NoAnswer", nil)
		}
		*out = payload
		return nil
	}
}

// awaitWorkflow waits an hour for a signal that never comes. When failFirst is
// set, a step fails before the wait.
func awaitWorkflow(ctx workflow.Context, failFirst bool) (string, error) {
	var a *acts
	once := &temporal.RetryPolicy{MaximumAttempts: 1}

	ctx = workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once})

	return saga.RunOrCompensate(ctx, saga.Options{}, func(ctx workflow.Context, s *saga.Saga) (string, error) {
		if failFirst {
			s.Step(ctx, "a",
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, a.Do, req{Step: "a", Fail: true}).Get(ctx, nil)
				},
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, a.Undo, req{Step: "a"}).Get(ctx, nil)
				})
		}
		var payload string
		if err := s.Step(ctx, "wait", waitStep(&payload), nil); err != nil {
			return "", err
		}
		return payload, nil
	})
}

// With no failure, the wait runs its full course and its own verdict is what
// fails the saga.
func TestInlineStepRunsAndDecides(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(awaitWorkflow)
	env.RegisterActivity(&acts{r: newRecorder()})

	start := env.Now()
	env.ExecuteWorkflow(awaitWorkflow, false)

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "nobody answered",
		"the step decides what a missing signal means")
	require.GreaterOrEqual(t, env.Now().Sub(start), time.Hour,
		"without an earlier failure the wait should run to its timeout")
}

// After a step has failed there is nothing left to approve, so the wait is
// skipped like any other step. A saga on its way to being rolled back must not
// sit for an hour waiting for a human.
func TestInlineStepSkipsAfterAFailedStep(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	r := newRecorder()
	env.RegisterWorkflow(awaitWorkflow)
	env.RegisterActivity(&acts{r: r})

	start := env.Now()
	env.ExecuteWorkflow(awaitWorkflow, true)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Less(t, env.Now().Sub(start), time.Hour,
		"the wait should be skipped once a step has failed")
	require.ErrorContains(t, env.GetWorkflowError(), "forward failed: a",
		"and the earlier failure is what gets reported")

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "undo:a"}, calls,
		"and the rollback should still happen")
}

// --- which error is reported -------------------------------------------------

// maskWorkflow is the shape example/approval has without a guard on s.Err():
// a step fails, AwaitSignal returns at once, and the body reports the missing
// signal as the failure.
func maskWorkflow(ctx workflow.Context, clear bool) (string, error) {
	var a *acts
	once := &temporal.RetryPolicy{MaximumAttempts: 1}

	ctx = workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once})

	return saga.RunOrCompensate(ctx, saga.Options{}, func(ctx workflow.Context, s *saga.Saga) (string, error) {
		s.Step(ctx, "reserve",
			func(ctx workflow.Context) error {
				return workflow.ExecuteActivity(ctx, a.Do, req{Step: "reserve", Fail: true}).Get(ctx, nil)
			},
			func(ctx workflow.Context) error {
				return workflow.ExecuteActivity(ctx, a.Undo, req{Step: "reserve"}).Get(ctx, nil)
			})

		_, ok := awaitSignal[string](ctx, "approval", time.Second)
		if !ok {
			if clear {
				s.ClearErr() // 「握って自分のエラーを返す」と宣言する
			}
			return "", temporal.NewApplicationError("nobody reviewed the order in time", "ApprovalDenied", nil)
		}
		return "approved", nil
	})
}

// A step's failure outranks an error the body produced afterwards. Without
// this, a saga whose reservation failed reports "nobody reviewed the order in
// time", because AwaitSignal returned at once and the body drew the obvious
// conclusion from it.
func TestFirstFailureWins(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	r := newRecorder()
	env.RegisterWorkflow(maskWorkflow)
	env.RegisterActivity(&acts{r: r})

	env.ExecuteWorkflow(maskWorkflow, false)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)

	require.Contains(t, err.Error(), "forward failed: reserve",
		"the step that failed is the root cause and has to be what is reported")
	require.NotContains(t, err.Error(), "nobody reviewed",
		"the body's conclusion was drawn from a skipped wait, not from the truth")

	calls := r.snapshot()
	require.Equal(t, []string{"do:reserve", "undo:reserve"}, calls)
}

// Clear is how a caller says it handled the step failure and means to report
// its own error instead.
func TestClearLetsTheBodyReportItsOwnError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(maskWorkflow)
	env.RegisterActivity(&acts{r: newRecorder()})

	env.ExecuteWorkflow(maskWorkflow, true)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "nobody reviewed")
}

// --- the two Options ---------------------------------------------------------

// By default a failing compensation stops the ones still to run, and they are
// reported as skipped rather than dropped. This is the Java SDK's default too.
func TestCompensationStopsAtTheFirstFailure(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b", UndoFails: true}, {Name: "c", Fail: true}},
		SleepAfter: -1,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr))

	var report saga.CompensationReport
	require.NoError(t, appErr.Details(&report))
	require.Equal(t, []string{"b"}, report.Failed)
	require.Equal(t, []string{"a"}, report.Skipped, "a never ran and has to be reported")

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "do:c", "undo:c", "undo:b"}, calls,
		"a's compensation must not run")
}

// ContinueWithError runs the rest anyway.
func TestContinueWithErrorRunsTheRest(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:             []stepSpec{{Name: "a"}, {Name: "b", UndoFails: true}, {Name: "c", Fail: true}},
		SleepAfter:        -1,
		ContinueWithError: true,
	})

	require.True(t, env.IsWorkflowCompleted())

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(env.GetWorkflowError(), &appErr))

	var report saga.CompensationReport
	require.NoError(t, appErr.Details(&report))
	require.Equal(t, []string{"b"}, report.Failed)
	require.Empty(t, report.Skipped)

	calls := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "do:c", "undo:c", "undo:b", "undo:a"}, calls)
}

// ParallelCompensation dispatches every compensation before awaiting any of
// them, so one failing cannot stop another from being tried -- which is why
// ContinueWithError has no meaning alongside it. The order they finish in is
// not promised, so this pins the set rather than the sequence.
func TestParallelCompensationRunsThemAll(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b", UndoFails: true}, {Name: "c", Fail: true}},
		SleepAfter: -1,
		Parallel:   true,
	})

	require.True(t, env.IsWorkflowCompleted())

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(env.GetWorkflowError(), &appErr))

	var report saga.CompensationReport
	require.NoError(t, appErr.Details(&report))
	require.Equal(t, []string{"b"}, report.Failed)
	require.Empty(t, report.Skipped, "nothing is skipped: they were all dispatched")

	calls := r.snapshot()
	require.ElementsMatch(t,
		[]string{"do:a", "do:b", "do:c", "undo:a", "undo:b", "undo:c"}, calls,
		"every compensation runs, including the ones after the failure")
}
