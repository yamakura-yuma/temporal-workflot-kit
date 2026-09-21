package saga_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

// --- test activities ---------------------------------------------------------

type req struct {
	Step string `json:"step"`
	Fail bool   `json:"fail"`
}

// recorder captures the order activities ran in and the idempotency key each
// one saw. Activities run on their own goroutines in the test environment, so
// it is guarded.
type recorder struct {
	mu    sync.Mutex
	calls []string
	keys  map[string]string
}

func newRecorder() *recorder { return &recorder{keys: map[string]string{}} }

func (r *recorder) record(call string, key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
	r.keys[call] = key
}

func (r *recorder) snapshot() ([]string, map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := append([]string(nil), r.calls...)
	keys := map[string]string{}
	for k, v := range r.keys {
		keys[k] = v
	}
	return calls, keys
}

type acts struct{ r *recorder }

func (a *acts) Do(ctx context.Context, in req) (string, error) {
	key, _ := saga.IdempotencyKey(ctx)
	a.r.record("do:"+in.Step, key)
	if in.Fail {
		return "", errors.New("forward failed: " + in.Step)
	}
	return "ok:" + in.Step, nil
}

func (a *acts) Undo(ctx context.Context, in req) error {
	key, _ := saga.IdempotencyKey(ctx)
	a.r.record("undo:"+in.Step, key)
	return nil
}

func (a *acts) UndoFails(ctx context.Context, in req) error {
	key, _ := saga.IdempotencyKey(ctx)
	a.r.record("undo:"+in.Step, key)
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
	ReturnNil bool          `json:"return_nil"`
	Budget    time.Duration `json:"budget"`
}

func planWorkflow(ctx workflow.Context, p plan) ([]string, error) {
	var a *acts // nil receiver: only the method's name is used

	budget := p.Budget
	if budget == 0 {
		budget = 5 * time.Minute
	}

	// One attempt per activity: these tests assert on the exact sequence of
	// calls, and the SDK's default retry policy would repeat the failures.
	once := &temporal.RetryPolicy{MaximumAttempts: 1}

	return saga.Run(ctx, saga.Options{
		ActivityOptions:     workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationOptions: workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationBudget:  budget,
	}, func(ctx workflow.Context, s *saga.Saga) ([]string, error) {
		var out []string
		for i, spec := range p.Steps {
			undo := a.Undo
			switch {
			case spec.NoUndo:
				undo = nil
			case spec.UndoFails:
				undo = a.UndoFails
			}

			v, _ := saga.Step(ctx, s, spec.Name, saga.Activity(a.Do), saga.UndoActivity(undo), req{Step: spec.Name, Fail: spec.Fail})
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

	calls, _ := r.snapshot()
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

	calls, _ := r.snapshot()
	require.Equal(t,
		[]string{"do:a", "do:b", "do:c", "undo:c", "undo:b", "undo:a"},
		calls)
}

// A step's forward activity and its compensation must observe the same
// idempotency key: that is how a compensation finds the work it has to undo.
//
// The key is read through the activity's ActivityID, so this only holds when
// the real activity function runs. A mock set up with .Return(value) replaces
// the function and would never call IdempotencyKey -- use .Return(fn) or
// .Run(fn) if you need a mock here.
func TestForwardAndCompensationShareTheKey(t *testing.T) {
	env, r := newEnv(t)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b", Fail: true}},
		SleepAfter: -1,
	})
	require.True(t, env.IsWorkflowCompleted())

	_, keys := r.snapshot()
	for _, step := range []string{"a", "b"} {
		fwd, undo := keys["do:"+step], keys["undo:"+step]
		require.NotEmpty(t, fwd, "step %s: forward saw no key", step)
		require.Equal(t, fwd, undo, "step %s: forward and compensation disagree", step)
		require.True(t, strings.HasSuffix(fwd, "/"+step), "key %q should end in the step name", fwd)
	}
	require.NotEqual(t, keys["do:a"], keys["do:b"], "each step needs its own key")
}

// The key is derived from the run, not from FirstRunID.
//
// FirstRunID is preserved across ContinueAsNew, Retry, Cron and Reset, while a
// saga's step counter starts over in the new run -- so a key built on it would
// repeat the previous run's keys and every step would look like one that had
// already been applied. Keying on RunID and the step name avoids both that and
// the shifting that a positional counter causes when a step is inserted.
func TestDefaultKeyIsScopedToTheRun(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	probe := func(ctx workflow.Context) (map[string]string, error) {
		info := workflow.GetInfo(ctx)
		return map[string]string{
			"key":        saga.DefaultKey(ctx, "charge"),
			"runID":      info.WorkflowExecution.RunID,
			"firstRunID": info.FirstRunID,
		}, nil
	}
	env.RegisterWorkflow(probe)
	env.ExecuteWorkflow(probe)
	require.NoError(t, env.GetWorkflowError())

	var got map[string]string
	require.NoError(t, env.GetWorkflowResult(&got))

	require.Equal(t, got["runID"]+"/charge", got["key"])
	require.NotEmpty(t, got["runID"])
	require.Contains(t, got["key"], "/", "a purely numeric key can collide with the SDK's default ActivityID")

	// Pins the reason FirstRunID is not used here: the test environment does not
	// populate it, so a key built on it would be untestable as well as wrong.
	require.Empty(t, got["firstRunID"])
}

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

	calls, _ := r.snapshot()
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

	calls, _ := r.snapshot()
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

	calls, _ := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b", "undo:b", "undo:a"}, calls,
		"a failing compensation must not stop the ones still queued")
}

// When the compensation budget runs out, the compensations that never ran are
// reported rather than silently dropped.
func TestBudgetExhaustionReportsSkippedSteps(t *testing.T) {
	env, r := newEnv(t)

	// The compensation for "b" takes longer than the whole budget, leaving
	// nothing for "a".
	//
	// In production each compensation's ScheduleToCloseTimeout is also clamped
	// to the remaining budget, so one of them cannot run past it; the test
	// environment does not apply that timeout to a delayed mock, so what this
	// test pins is the accounting -- a compensation that never ran is reported,
	// not dropped.
	env.OnActivity("Undo", mock.Anything, mock.Anything).
		After(10 * time.Minute).
		Return(nil)

	env.ExecuteWorkflow(planWorkflow, plan{
		Steps:      []stepSpec{{Name: "a"}, {Name: "b", Fail: true}},
		SleepAfter: -1,
		Budget:     time.Minute,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "want an ApplicationError, got %T: %v", err, err)
	require.Equal(t, saga.CompensationFailedType, appErr.Type())

	var report saga.CompensationReport
	require.NoError(t, appErr.Details(&report))
	require.Equal(t, []string{"a"}, report.Skipped, "a's compensation should be reported, not dropped")
	require.Empty(t, report.Failed)

	// The failure that started the rollback is still the cause.
	require.Contains(t, err.Error(), "forward failed: b")

	calls, _ := r.snapshot()
	require.Equal(t, []string{"do:a", "do:b"}, calls, "the mocked compensation replaces the real one")
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

	calls, _ := r.snapshot()
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

// A saga needs a compensation budget: compensations run on a context nothing
// can cancel from outside, so without one a stuck compensation hangs forever.
func TestBudgetIsRequired(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	noBudget := func(ctx workflow.Context) error {
		_, err := saga.Run(ctx, saga.Options{
			ActivityOptions: workflow.ActivityOptions{StartToCloseTimeout: time.Minute},
		}, func(workflow.Context, *saga.Saga) (int, error) { return 0, nil })
		return err
	}
	env.RegisterWorkflow(noBudget)
	env.ExecuteWorkflow(noBudget)

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "CompensationBudget")
}

// IdempotencyKey must not panic outside an activity, or activities stop being
// unit-testable without a Temporal environment. activity.GetInfo does panic.
func TestIdempotencyKeyOutsideAnActivity(t *testing.T) {
	key, ok := saga.IdempotencyKey(context.Background())
	require.False(t, ok)
	require.Empty(t, key)
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

	return saga.Run(ctx, saga.Options{
		ActivityOptions:     workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationOptions: workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationBudget:  5 * time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) ([]string, error) {
		saga.Step(ctx, s, "a", saga.Activity(a.Do), saga.UndoActivity(a.Undo), req{Step: "a"})
		saga.Step(ctx, s, "b", saga.ChildWorkflow(childDo), saga.UndoChildWorkflow(childUndo), req{Step: "b"})
		saga.Step(ctx, s, "c", saga.Activity(a.Do), saga.UndoActivity(a.Undo), req{Step: "c", Fail: true})
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

	calls, keys := r.snapshot()
	require.Equal(t,
		[]string{"do:a", "do:b", "do:c", "undo:c", "undo:b", "undo:a"},
		calls,
		"the child workflow step must take its place in the one reverse order")

	// The child's halves run activities of their own, so the key those
	// activities see is the child's, not the saga step's. What the saga
	// guarantees is that the two children are named for the same step.
	require.NotEmpty(t, keys["do:b"])
	require.NotEmpty(t, keys["undo:b"])
}

// --- waiting for a signal ----------------------------------------------------

// waitStep is the wait written the way the library intends: a step of its own,
// which decides for itself what a missing signal means.
func waitStep(ctx workflow.Context, _ struct{}) (string, error) {
	payload, arrived := saga.AwaitSignal[string](ctx, "never", time.Hour)
	if !arrived {
		return "", temporal.NewApplicationError("nobody answered", "NoAnswer", nil)
	}
	return payload, nil
}

// awaitWorkflow waits an hour for a signal that never comes. When failFirst is
// set, a step fails before the wait.
func awaitWorkflow(ctx workflow.Context, failFirst bool) (string, error) {
	var a *acts
	once := &temporal.RetryPolicy{MaximumAttempts: 1}

	return saga.Run(ctx, saga.Options{
		ActivityOptions:     workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationOptions: workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationBudget:  5 * time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (string, error) {
		if failFirst {
			saga.Step(ctx, s, "a", saga.Activity(a.Do), saga.UndoActivity(a.Undo), req{Step: "a", Fail: true})
		}
		return saga.Step(ctx, s, "wait", saga.Func(waitStep), nil, struct{}{})
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

	calls, _ := r.snapshot()
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

	return saga.Run(ctx, saga.Options{
		ActivityOptions:     workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationOptions: workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: once},
		CompensationBudget:  5 * time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (string, error) {
		saga.Step(ctx, s, "reserve", saga.Activity(a.Do), saga.UndoActivity(a.Undo), req{Step: "reserve", Fail: true})

		_, ok := saga.AwaitSignal[string](ctx, "approval", time.Second)
		if !ok {
			if clear {
				s.Clear() // 「握って自分のエラーを返す」と宣言する
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

	calls, _ := r.snapshot()
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
