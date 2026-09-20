package saga

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

// Step runs one forward activity and registers its compensation.
//
// fwd and undo are taken as typed functions rather than the SDK's `any`, so the
// compiler checks that they belong together: undo returns only an error, fwd
// returns a value too, and both take the same input. Swapping the two, or
// passing an argument of the wrong type, is a compile error. That check is
// worth having because the SDK does not make it -- ExecuteActivity resolves a
// function value to a name string before validating anything, and the string
// path skips argument validation entirely. A mismatch there does not surface
// until the activity runs, which for a compensation means it surfaces while the
// saga is already failing.
//
// The compensation is registered *before* the forward activity is executed. An
// activity that fails with a timeout may still have run to completion on a
// worker, so a compensation registered only on success would leave that side
// effect behind forever. The cost is that a compensation may be invoked for a
// step that never took effect: see the package documentation for the contract
// that puts on the activity.
//
// Both halves of the step are given the same idempotency key, readable inside
// the activity with IdempotencyKey.
//
// After any step fails, later Step calls return that error without running
// anything, so a linear saga can ignore the returned error and let Run decide
// the outcome. undo may be nil for a step with nothing to undo.
func Step[In, Out any](
	ctx workflow.Context,
	s *Saga,
	name string,
	fwd func(context.Context, In) (Out, error),
	undoFn func(context.Context, In) error,
	in In,
) (Out, error) {
	var zero Out

	if s.err != nil {
		return zero, s.err
	}
	if err := s.claimName(name); err != nil {
		s.fail(err)
		return zero, err
	}

	key := s.opts.KeyFunc(ctx, name)

	if undoFn != nil {
		s.Add(name, func(cctx workflow.Context) error {
			return runUndo(cctx, s.opts.CompensationOptions, key+undoSuffix, undoFn, in)
		})
	}

	opts := s.opts.ActivityOptions
	opts.ActivityID = key

	var out Out
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), fwd, in).Get(ctx, &out); err != nil {
		s.fail(err)
		return zero, err
	}
	return out, nil
}

// runUndo executes one compensation, clamped to whatever is left of the
// compensation budget so that a stuck compensation cannot outlive it.
func runUndo[In any](
	ctx workflow.Context,
	opts workflow.ActivityOptions,
	activityID string,
	undoFn func(context.Context, In) error,
	in In,
) error {
	opts.ActivityID = activityID

	if remaining, ok := budgetOf(ctx); ok {
		if opts.ScheduleToCloseTimeout <= 0 || opts.ScheduleToCloseTimeout > remaining {
			opts.ScheduleToCloseTimeout = remaining
		}
		// StartToClose may not exceed ScheduleToClose, or the server rejects the
		// activity outright.
		if opts.StartToCloseTimeout > remaining {
			opts.StartToCloseTimeout = remaining
		}
	}

	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), undoFn, in).Get(ctx, nil)
}
