package saga

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// undoSuffix distinguishes a compensation's ActivityID from its forward step's.
// Temporal panics on a duplicate command ID within one workflow execution
// ("[TMPRL1100] adding duplicate command"), so the two cannot share an ID.
// IdempotencyKey strips this again, which is why both halves of a step observe
// the same key.
const undoSuffix = ":undo"

// Options configures a saga. The zero value is usable except for
// ActivityOptions, which must carry a timeout like any other activity call.
type Options struct {
	// ActivityOptions are the defaults for forward steps. Temporal requires at
	// least one of StartToCloseTimeout or ScheduleToCloseTimeout.
	ActivityOptions workflow.ActivityOptions

	// CompensationOptions are the defaults for compensations. Compensations
	// usually want a longer budget and more attempts than the forward step they
	// undo. When zero, ActivityOptions is used.
	CompensationOptions workflow.ActivityOptions

	// CompensationBudget bounds the whole compensation phase. It is required:
	// compensations run on a disconnected context that nothing can cancel from
	// the outside, so without a budget a stuck compensation hangs the workflow
	// forever. Each compensation's ScheduleToCloseTimeout is clamped to the
	// budget remaining when it starts, and any compensation left when the
	// budget runs out is reported as skipped rather than silently dropped.
	CompensationBudget time.Duration

	// StopOnCompensationError stops the compensation phase at the first
	// failure. The default (false) runs the remaining compensations anyway,
	// which is usually what you want: one refund failing is no reason to leave
	// the inventory reserved too.
	StopOnCompensationError bool

	// KeyFunc derives the idempotency key for a step. The default is
	// RunID + "/" + name.
	//
	// It must be deterministic: the same workflow run must produce the same key
	// for the same step name on replay. Do not key on FirstRunID -- that value
	// is preserved across ContinueAsNew, Retry, Cron and Reset, so a second run
	// would reuse the first run's keys and every step would be mistaken for one
	// that already ran.
	//
	// The key must contain a non-digit character. An activity with no explicit
	// ActivityID gets a plain decimal one from the SDK, so a purely numeric key
	// can collide with an unrelated activity.
	KeyFunc func(ctx workflow.Context, name string) string

	// CompensationFailedAttribute, when set, is flipped to true if any
	// compensation fails or is skipped, so operators can search for sagas that
	// need a human. It is opt-in because the key has to be registered on the
	// server first. Compensation failures are always logged regardless.
	CompensationFailedAttribute *temporal.SearchAttributeKeyBool
}

// Saga records the compensations for the steps that have been started, and the
// first error any of them reported. Create one with Run.
type Saga struct {
	opts  Options
	err   error
	names map[string]struct{}
	undos []undo
}

type undo struct {
	name string
	run  func(workflow.Context) error
}

// Run executes body as a saga and compensates it if body fails.
//
// The compensation phase runs on a disconnected context, so it still works when
// the workflow itself is being canceled -- which is exactly when it matters.
// Compensations run in reverse order of registration.
//
// body returning a nil error is not enough to be treated as success: if any
// step inside it failed, Run compensates and returns that error, discarding
// body's return value. That is deliberate. With the sticky-error behaviour of
// ActivityStep, a caller who forgets to check an error would otherwise return a
// half-filled result and the workflow would be recorded as completed with its
// side effects half applied.
//
// A step's failure also outranks an error the body returns on its own. Once a
// step has failed, later steps are no-ops and AwaitSignal returns at once, so
// the body tends to reach a branch that reads a zero value and reports
// something untrue -- "nobody approved this" when the truth is "the
// reservation failed". Run reports the step's failure instead. Call s.Clear()
// before returning your own error if you have handled the step failure and
// mean to replace it.
//
// Run deliberately registers no deferred function. A panic in workflow code
// fails the workflow task and the whole workflow is replayed, so there is
// nothing to compensate; and calling a blocking workflow API while a panic
// unwinds deadlocks the coroutine.
func Run[T any](ctx workflow.Context, o Options, body func(workflow.Context, *Saga) (T, error)) (T, error) {
	var zero T

	s, err := newSaga(o)
	if err != nil {
		return zero, err
	}

	out, err := body(ctx, s)

	// The first failure wins. A step that failed is the root cause; whatever
	// the body returned afterwards is fallout from it -- often a branch that
	// read a zero value and drew the wrong conclusion. Reporting the body's
	// error instead would bury the real one.
	//
	// To report an error of your own after handling a step failure, call
	// s.Clear() first. That is what it is for.
	if s.err != nil {
		err = s.err
	}
	if err == nil {
		return out, nil
	}

	// ContinueAsNew is delivered as an error but the saga is not over, so there
	// is nothing to undo. The SDK classifies it the same way.
	var continueAsNew *workflow.ContinueAsNewError
	if errors.As(err, &continueAsNew) {
		return out, err
	}

	return zero, s.compensate(ctx, err)
}

func newSaga(o Options) (*Saga, error) {
	if o.CompensationBudget <= 0 {
		return nil, fmt.Errorf("saga: Options.CompensationBudget must be positive")
	}
	if o.ActivityOptions.StartToCloseTimeout <= 0 && o.ActivityOptions.ScheduleToCloseTimeout <= 0 {
		return nil, fmt.Errorf("saga: Options.ActivityOptions needs StartToCloseTimeout or ScheduleToCloseTimeout")
	}
	if o.CompensationOptions.StartToCloseTimeout <= 0 && o.CompensationOptions.ScheduleToCloseTimeout <= 0 {
		o.CompensationOptions = o.ActivityOptions
	}
	if o.KeyFunc == nil {
		o.KeyFunc = DefaultKey
	}
	return &Saga{opts: o, names: map[string]struct{}{}}, nil
}

// DefaultKey derives a step's idempotency key from the current run and the step
// name. It is Options.KeyFunc's default.
func DefaultKey(ctx workflow.Context, name string) string {
	return workflow.GetInfo(ctx).WorkflowExecution.RunID + "/" + name
}

// Err reports the first error a step reported, if any. Once it is non-nil every
// later step is a no-op, so a linear saga can skip the error check between
// steps and let Run deal with the outcome.
func (s *Saga) Err() error { return s.err }

// Clear forgets the recorded error so that later steps run again, and so that
// an error the body returns is reported instead of the step's.
//
// Use it only when the failure was genuinely handled. It also stops Run from
// treating the saga as failed, so the compensations registered so far will not
// run unless a later step fails or the body returns an error.
func (s *Saga) Clear() { s.err = nil }

// Add registers a compensation that is not a plain activity -- a child
// workflow, a local activity, a signal to another workflow. It is the escape
// hatch from the four step constructors, for a compensation none of them fits.
//
// Register the compensation before starting the work it undoes, for the same
// reason a step does: an operation that times out may still have taken effect.
func (s *Saga) Add(name string, run func(workflow.Context) error) {
	s.undos = append(s.undos, undo{name: name, run: run})
}

func (s *Saga) fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Saga) claimName(name string) error {
	if name == "" {
		return fmt.Errorf("saga: step name must not be empty")
	}
	if _, taken := s.names[name]; taken {
		return fmt.Errorf("saga: duplicate step name %q; names identify a step's idempotency key and must be unique", name)
	}
	s.names[name] = struct{}{}
	return nil
}

// compensate runs the registered compensations in reverse order and returns the
// error the workflow should fail with.
func (s *Saga) compensate(ctx workflow.Context, cause error) error {
	if len(s.undos) == 0 {
		return cause
	}

	// A disconnected context does not inherit the parent's cancellation, so
	// compensations still run after the workflow is canceled. Nothing outside
	// can cancel it either, which is why every compensation is bounded by what
	// is left of the budget.
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	deadline := workflow.Now(dctx).Add(s.opts.CompensationBudget)
	logger := workflow.GetLogger(ctx)

	var failed, skipped []string
	var first error

	for i := len(s.undos) - 1; i >= 0; i-- {
		u := s.undos[i]

		remaining := deadline.Sub(workflow.Now(dctx))
		if remaining <= 0 {
			skipped = append(skipped, u.name)
			continue
		}

		if err := u.run(withBudget(dctx, remaining)); err != nil {
			logger.Error("saga: compensation failed", "step", u.name, "error", err)
			failed = append(failed, u.name)
			if first == nil {
				first = err
			}
			if s.opts.StopOnCompensationError {
				for j := i - 1; j >= 0; j-- {
					skipped = append(skipped, s.undos[j].name)
				}
				break
			}
		}
	}

	if len(failed) == 0 && len(skipped) == 0 {
		return cause
	}
	s.markNeedsAttention(ctx, logger)
	return newCompensationError(cause, failed, skipped, first)
}

type budgetKey struct{}

// withBudget carries the remaining compensation budget so that runUndo can clamp
// a compensation activity's ScheduleToCloseTimeout to it.
func withBudget(ctx workflow.Context, remaining time.Duration) workflow.Context {
	return workflow.WithValue(ctx, budgetKey{}, remaining)
}

// RemainingBudget reports how much of the compensation budget is left, and
// whether there is a budget at all. It returns false outside the compensation
// phase.
//
// ActivityStep and ChildWorkflowStep use it to clamp their own timeouts. Call it from a
// compensation registered with Add, which the library cannot clamp for you.
func RemainingBudget(ctx workflow.Context) (time.Duration, bool) {
	d, ok := ctx.Value(budgetKey{}).(time.Duration)
	return d, ok
}

func (s *Saga) markNeedsAttention(ctx workflow.Context, logger log.Logger) {
	if s.opts.CompensationFailedAttribute == nil {
		return
	}
	err := workflow.UpsertTypedSearchAttributes(ctx, s.opts.CompensationFailedAttribute.ValueSet(true))
	if err != nil {
		logger.Error("saga: could not set the compensation-failed search attribute", "error", err)
	}
}
