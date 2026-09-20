package saga

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

// A step is a forward call paired with the call that undoes it. What the two
// calls *are* is not part of that idea: Step runs activities, ChildStep runs
// child workflows, and both go through register below.
//
// Only three things differ between them, and each is confined to the closures
// the two build:
//
//	                 executed by             key rides in                     budget clamps
//	activity         ExecuteActivity         ActivityOptions.ActivityID       ScheduleToCloseTimeout
//	child workflow   ExecuteChildWorkflow    ChildWorkflowOptions.WorkflowID  WorkflowExecutionTimeout
//
// Local activities are deliberately absent. LocalActivityOptions has no id
// field, so there is nowhere to put the idempotency key -- and a local
// activity's retries never reach the server, which makes it the wrong place
// for a side effect that needs undoing anyway.

// halves is one step's two calls, already bound to their executor. The key is
// supplied by register, which is what guarantees both halves see the same one.
type halves[In, Out any] struct {
	forward func(ctx workflow.Context, key string, in In) (Out, error)
	// undo is nil for a step with nothing to undo.
	undo func(ctx workflow.Context, key string, in In) error
}

// register is the part that is the same for every kind of step: claim the
// name, mint the key, push the compensation *before* running the forward call,
// and record the first failure.
func register[In, Out any](ctx workflow.Context, s *Saga, name string, in In, h halves[In, Out]) (Out, error) {
	var zero Out

	if s.err != nil {
		return zero, s.err
	}
	if err := s.claimName(name); err != nil {
		s.fail(err)
		return zero, err
	}

	key := s.opts.KeyFunc(ctx, name)

	if h.undo != nil {
		s.Add(name, func(cctx workflow.Context) error {
			return h.undo(cctx, key+undoSuffix, in)
		})
	}

	out, err := h.forward(ctx, key, in)
	if err != nil {
		s.fail(err)
		return zero, err
	}
	return out, nil
}

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
	h := halves[In, Out]{
		forward: func(c workflow.Context, key string, in In) (Out, error) {
			opts := s.opts.ActivityOptions
			opts.ActivityID = key

			var out Out
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(c, opts), fwd, in).Get(c, &out)
			return out, err
		},
	}

	if undoFn != nil {
		h.undo = func(c workflow.Context, key string, in In) error {
			opts := s.opts.CompensationOptions
			opts.ActivityID = key

			if remaining, ok := RemainingBudget(c); ok {
				if opts.ScheduleToCloseTimeout <= 0 || opts.ScheduleToCloseTimeout > remaining {
					opts.ScheduleToCloseTimeout = remaining
				}
				// StartToClose may not exceed ScheduleToClose, or the server
				// rejects the activity outright.
				if opts.StartToCloseTimeout > remaining {
					opts.StartToCloseTimeout = remaining
				}
			}

			return workflow.ExecuteActivity(workflow.WithActivityOptions(c, opts), undoFn, in).Get(c, nil)
		}
	}

	return register(ctx, s, name, in, h)
}

// ChildStep runs one forward child workflow and registers its compensation,
// which is also a child workflow. It is Step for work that is a workflow rather
// than an activity: a sub-saga, or anything long enough to deserve its own
// history.
//
// Everything Step promises holds here too. The compensation is registered
// before the forward child starts, both halves share an idempotency key, and
// swapping fwd and undo is a compile error -- the first argument being a
// workflow.Context rather than a context.Context is what tells the two kinds of
// step apart, which is the same distinction Temporal itself draws.
//
// The key rides in the child's WorkflowID, so the child reads it with
// IdempotencyKeyOf. Anything else about the children -- task queue, timeouts,
// retry policy, parent close policy -- comes from the context, so set it the
// ordinary way with workflow.WithChildOptions before calling. The compensation
// child's execution timeout is clamped to what is left of the compensation
// budget.
func ChildStep[In, Out any](
	ctx workflow.Context,
	s *Saga,
	name string,
	fwd func(workflow.Context, In) (Out, error),
	undoFn func(workflow.Context, In) error,
	in In,
) (Out, error) {
	h := halves[In, Out]{
		forward: func(c workflow.Context, key string, in In) (Out, error) {
			opts := workflow.GetChildWorkflowOptions(c)
			opts.WorkflowID = key

			var out Out
			err := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(c, opts), fwd, in).Get(c, &out)
			return out, err
		},
	}

	if undoFn != nil {
		h.undo = func(c workflow.Context, key string, in In) error {
			opts := workflow.GetChildWorkflowOptions(c)
			opts.WorkflowID = key

			if remaining, ok := RemainingBudget(c); ok {
				if opts.WorkflowExecutionTimeout <= 0 || opts.WorkflowExecutionTimeout > remaining {
					opts.WorkflowExecutionTimeout = remaining
				}
			}

			return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(c, opts), undoFn, in).Get(c, nil)
		}
	}

	return register(ctx, s, name, in, h)
}
