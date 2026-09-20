package saga

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

// A step is a forward call paired with the call that undoes it. What runs those
// two calls is a separate question, and it is answered by the value you hand to
// Step: Activity, ChildWorkflow or Func.
//
// Only three things differ between them, and each is confined to the closures
// those constructors build:
//
//	                 executed by             key rides in                     budget clamps
//	Activity         ExecuteActivity         ActivityOptions.ActivityID       ScheduleToCloseTimeout
//	ChildWorkflow    ExecuteChildWorkflow    ChildWorkflowOptions.WorkflowID  WorkflowExecutionTimeout
//	Func             called here and now     nothing is executed remotely     nothing to clamp
//
// Func can express the other two -- the function you give it may call
// ExecuteActivity itself -- so what justifies Activity and ChildWorkflow is not
// that those executors exist, but that they carry the idempotency key and clamp
// the budget for you. An executor that does neither is Func with extra
// vocabulary, which is why sending a signal is written with Func rather than
// having a constructor of its own.
//
// Local activities have no constructor. LocalActivityOptions has no id field,
// so there is nowhere to put the key -- and a local activity's retries never
// reach the server, which makes it the wrong place for a side effect that needs
// undoing anyway.

// Exec is a step's two calls, bound to whatever executes them. Build one with
// Activity, ChildWorkflow or Func and hand it to Step.
//
// The key the closures receive is minted by Step, which is what guarantees both
// halves of a step see the same one.
type Exec[In, Out any] struct {
	forward func(ctx workflow.Context, s *Saga, key string, in In) (Out, error)
	// undo is nil for a step with nothing to undo.
	undo func(ctx workflow.Context, s *Saga, key string, in In) error
}

// Step runs one step of a saga and registers its compensation.
//
// The compensation is registered *before* the forward call is executed. A call
// that fails with a timeout may still have run to completion, so a compensation
// registered only on success would leave that side effect behind forever. The
// cost is that a compensation may be invoked for a step that never took effect:
// see the package documentation for the contract that puts on the work being
// undone.
//
// Both halves of the step are given the same idempotency key, readable with
// IdempotencyKey inside an activity and IdempotencyKeyOf inside a child
// workflow.
//
// After any step fails, later Step calls return that error without running
// anything, so a linear saga can ignore the returned error and let Run decide
// the outcome.
//
//	res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve, a.Unreserve), ReserveReq{Order: in})
//	pk, _ := saga.Step(ctx, s, "pack", saga.ChildWorkflow(PackWorkflow, UnpackWorkflow), PackReq{Order: in})
//	saga.Step(ctx, s, "approval", saga.Func(awaitApproval, nil), ApprovalReq{Wait: wait})
func Step[In, Out any](ctx workflow.Context, s *Saga, name string, e Exec[In, Out], in In) (Out, error) {
	var zero Out

	if s.err != nil {
		return zero, s.err
	}
	if err := s.claimName(name); err != nil {
		s.fail(err)
		return zero, err
	}

	key := s.opts.KeyFunc(ctx, name)

	if e.undo != nil {
		s.Add(name, func(cctx workflow.Context) error {
			return e.undo(cctx, s, key+undoSuffix, in)
		})
	}

	out, err := e.forward(ctx, s, key, in)
	if err != nil {
		s.fail(err)
		return zero, err
	}
	return out, nil
}

// Activity runs the step as an activity, and its compensation as another.
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
// The idempotency key rides in the activity's ActivityID, readable inside the
// activity with IdempotencyKey. The compensation's ScheduleToCloseTimeout is
// clamped to what is left of the compensation budget.
//
// undo may be nil for a step with nothing to undo.
func Activity[In, Out any](
	fwd func(context.Context, In) (Out, error),
	undo func(context.Context, In) error,
) Exec[In, Out] {
	e := Exec[In, Out]{
		forward: func(ctx workflow.Context, s *Saga, key string, in In) (Out, error) {
			opts := s.opts.ActivityOptions
			opts.ActivityID = key

			var out Out
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), fwd, in).Get(ctx, &out)
			return out, err
		},
	}

	if undo != nil {
		e.undo = func(ctx workflow.Context, s *Saga, key string, in In) error {
			opts := s.opts.CompensationOptions
			opts.ActivityID = key

			if remaining, ok := RemainingBudget(ctx); ok {
				if opts.ScheduleToCloseTimeout <= 0 || opts.ScheduleToCloseTimeout > remaining {
					opts.ScheduleToCloseTimeout = remaining
				}
				// StartToClose may not exceed ScheduleToClose, or the server
				// rejects the activity outright.
				if opts.StartToCloseTimeout > remaining {
					opts.StartToCloseTimeout = remaining
				}
			}

			return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), undo, in).Get(ctx, nil)
		}
	}
	return e
}

// ChildWorkflow runs the step as a child workflow, and its compensation as
// another. It is Activity for work that is a workflow: a sub-saga, or anything
// long enough to deserve its own history.
//
// The first argument of the two functions being a workflow.Context rather than
// a context.Context is what tells this apart from Activity, which is the same
// distinction Temporal itself draws.
//
// The key rides in the child's WorkflowID, so the child reads it with
// IdempotencyKeyOf. Anything else about the children -- task queue, timeouts,
// retry policy, parent close policy -- comes from the context, so set it the
// ordinary way with workflow.WithChildOptions before calling Step. The
// compensation child's execution timeout is clamped to what is left of the
// compensation budget.
func ChildWorkflow[In, Out any](
	fwd func(workflow.Context, In) (Out, error),
	undo func(workflow.Context, In) error,
) Exec[In, Out] {
	e := Exec[In, Out]{
		forward: func(ctx workflow.Context, _ *Saga, key string, in In) (Out, error) {
			opts := workflow.GetChildWorkflowOptions(ctx)
			opts.WorkflowID = key

			var out Out
			err := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, opts), fwd, in).Get(ctx, &out)
			return out, err
		},
	}

	if undo != nil {
		e.undo = func(ctx workflow.Context, _ *Saga, key string, in In) error {
			opts := workflow.GetChildWorkflowOptions(ctx)
			opts.WorkflowID = key

			if remaining, ok := RemainingBudget(ctx); ok {
				if opts.WorkflowExecutionTimeout <= 0 || opts.WorkflowExecutionTimeout > remaining {
					opts.WorkflowExecutionTimeout = remaining
				}
			}

			return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, opts), undo, in).Get(ctx, nil)
		}
	}
	return e
}

// Func runs the step by calling the function here, in this workflow, rather
// than dispatching it anywhere.
//
// It is for work that has to happen in the workflow itself and can still fail
// the saga -- waiting for a signal and deciding what the answer means, waiting
// on a condition with workflow.Await, signalling another workflow, choosing
// between routes. Returning an error from fwd fails the step, which is what
// every other kind of step does too, so the rollback follows without the body
// having to arrange it.
//
// This is what keeps the body of a saga a list of steps. Without it, a wait has
// to be written inline and its branches leak into the body:
//
//	decision, ok := saga.AwaitSignal[Decision](ctx, "approval", wait)
//	if !ok { return Receipt{}, temporal.NewApplicationError(...) }
//	if !decision.Approved { return Receipt{}, temporal.NewApplicationError(...) }
//
// With it, that judgement lives in a function of the caller's own, next to the
// activities, and the body reads:
//
//	saga.Step(ctx, s, "approval", saga.Func(awaitApproval, nil), ApprovalReq{Wait: wait})
//
// Like every step it is skipped once an earlier step has failed, so a saga on
// its way to being rolled back does not stop to wait for a human.
//
// undo may be nil, and often is: workflow code that only decides something has
// nothing to undo. When it is not nil it runs on the disconnected compensation
// context like any other compensation, and can read what is left of the budget
// with RemainingBudget.
//
// No idempotency key is minted here, because nothing is executed anywhere that
// could run twice. The step name still has to be unique, since it names the
// step in the compensation report.
func Func[In, Out any](
	fwd func(workflow.Context, In) (Out, error),
	undo func(workflow.Context, In) error,
) Exec[In, Out] {
	e := Exec[In, Out]{
		forward: func(ctx workflow.Context, _ *Saga, _ string, in In) (Out, error) {
			return fwd(ctx, in)
		},
	}

	if undo != nil {
		e.undo = func(ctx workflow.Context, _ *Saga, _ string, in In) error {
			return undo(ctx, in)
		}
	}
	return e
}
