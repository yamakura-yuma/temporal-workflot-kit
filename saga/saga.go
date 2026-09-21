package saga

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// undoSuffix distinguishes a compensation's ActivityID from its forward step's.
// Temporal panics on a duplicate command ID within one workflow execution
// ("[TMPRL1100] adding duplicate command"), so the two cannot share an ID. It
// is also what makes a rollback legible in a history: "charge" and
// "charge:undo" sit next to each other.
const undoSuffix = ":undo"

// Options configures a saga.
type Options struct {
	// ParallelCompensation fires every compensation at once instead of running
	// them in reverse order. The default is reverse order, which is what a
	// rollback usually means: a step that ran later may depend on one that ran
	// earlier, and undoing them out of order can fail.
	//
	// Set it only when the steps are genuinely independent. ContinueWithError
	// has no effect here, because every compensation is dispatched before any
	// of them is awaited.
	//
	// The Java SDK's io.temporal.workflow.Saga has the same two options, under
	// the same names.
	ParallelCompensation bool

	// ContinueWithError keeps running the remaining compensations after one of
	// them fails, instead of stopping at the first failure.
	//
	// The default matches the Java SDK's: stop. Think before taking it, though
	// -- a refund failing is not much of a reason to leave the inventory
	// reserved as well, and whatever is left unrun is reported as Skipped
	// rather than attempted.
	ContinueWithError bool

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

// RunOrCompensate executes body as a saga and compensates it if body fails.
//
// The compensation phase runs on a disconnected context, so it still works when
// the workflow itself is being canceled -- which is exactly when it matters.
// Compensations run in reverse order of registration.
//
// body returning a nil error is not enough to be treated as success: if any
// step inside it failed, Run compensates and returns that error, discarding
// body's return value. That is deliberate. With the sticky-error behaviour of
// Step, a caller who forgets to check an error would otherwise return a
// half-filled result and the workflow would be recorded as completed with its
// side effects half applied.
//
// A step's failure also outranks an error the body returns on its own. Once a
// step has failed, later steps are no-ops and a wait returns at once, so
// the body tends to reach a branch that reads a zero value and reports
// something untrue -- "nobody approved this" when the truth is "the
// reservation failed". Run reports the step's failure instead. Call s.Clear()
// before returning your own error if you have handled the step failure and
// mean to replace it.
//
// It compensates on the way out of body rather than from a deferred function.
// A panic in workflow code fails the workflow task and the whole workflow is
// replayed, so a panic is not a saga failure and there is nothing to undo: a
// deferred rollback would undo a workflow that is about to run again.
func RunOrCompensate[T any](ctx workflow.Context, o Options, body func(workflow.Context, *Saga) (T, error)) (T, error) {
	var zero T

	s := newSaga(o)

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

func newSaga(o Options) *Saga {
	return &Saga{opts: o, names: map[string]struct{}{}}
}

// stepKey names a step within one workflow run. Step puts it on the activities
// the step starts, so that a history reads the way the saga was written.
//
// It is also the shape an idempotency key wants, which is why
// docs/activity-contract.md tells callers to build one like this. The library
// does not build it for them: the key belongs in the request they send, and
// nothing here would know where to put it.
func stepKey(ctx workflow.Context, name string) string {
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

// addCompensation records a compensation, the way the Java SDK's Saga does.
// Unlike that one it is not exported: only Step calls it, and it does so before
// running the forward half, which is the ordering the whole package exists to
// guarantee. It is not exported for that reason: a caller who could register a
// compensation directly could register it too late, or after the saga has
// already failed, and nothing would say so.
func (s *Saga) addCompensation(name string, run func(workflow.Context) error) {
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
	// can cancel it either, which is why a compensation needs a
	// ScheduleToCloseTimeout of its own.
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	logger := workflow.GetLogger(ctx)

	var failed, skipped []string
	var first error

	record := func(name string, err error) {
		logger.Error("saga: compensation failed", "step", name, "error", err)
		failed = append(failed, name)
		if first == nil {
			first = err
		}
	}

	if s.opts.ParallelCompensation {
		// Every compensation is dispatched before any of them is awaited, so
		// one failing cannot stop another from being tried. Failures are
		// collected in registration order to keep the report deterministic.
		errs := make([]error, len(s.undos))

		wg := workflow.NewWaitGroup(dctx)
		for i := range s.undos {
			wg.Add(1)
			workflow.Go(dctx, func(gctx workflow.Context) {
				defer wg.Done()
				errs[i] = s.undos[i].run(gctx)
			})
		}
		wg.Wait(dctx)

		for i, err := range errs {
			if err != nil {
				record(s.undos[i].name, err)
			}
		}
	} else {
		for i := len(s.undos) - 1; i >= 0; i-- {
			u := s.undos[i]

			if err := u.run(dctx); err != nil {
				record(u.name, err)

				if !s.opts.ContinueWithError {
					for j := i - 1; j >= 0; j-- {
						skipped = append(skipped, s.undos[j].name)
					}
					break
				}
			}
		}
	}

	if len(failed) == 0 && len(skipped) == 0 {
		return cause
	}
	s.markNeedsAttention(ctx, logger)
	return newCompensationError(cause, failed, skipped, first)
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
