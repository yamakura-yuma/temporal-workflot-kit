package saga

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

// A step is a forward call paired with the call that undoes it. How each of
// those two runs is a separate question, answered by the two values handed to
// Step -- and they are answered independently, so a step can be packed by a
// child workflow and unpacked by an activity.
//
// Only three things differ between them, and each is confined to the closures
// those constructors build:
//
//	forward          compensation          executed by             key rides in
//	Activity         UndoActivity          ExecuteActivity         ActivityOptions.ActivityID
//	ChildWorkflow    UndoChildWorkflow     ExecuteChildWorkflow    ChildWorkflowOptions.WorkflowID
//	Func             UndoFunc              called here and now     nothing is executed remotely
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

// Forward is a step's forward call, bound to whatever executes it. Build one
// with Activity, ChildWorkflow or Func.
type Forward[In, Out any] struct {
	run func(ctx workflow.Context, s *Saga, key string, in In) (Out, error)
}

// Undo is a step's compensation, bound to whatever executes it. Build one with
// UndoActivity, UndoChildWorkflow or UndoFunc, or pass nil for a step with
// nothing to undo.
//
// It is a separate value from Forward because the two halves do not have to run
// the same way. Packing may deserve a child workflow of its own while undoing
// it is one activity call; a charge may be an activity while reversing it is a
// signal to a ledger workflow that keeps the running total.
type Undo[In any] struct {
	run func(ctx workflow.Context, s *Saga, key string, in In) error
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
// The two halves are given separately, and do not have to run the same way:
//
//	res, _ := saga.Step(ctx, s, "reserve",
//	    saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})
//
//	pk, _ := saga.Step(ctx, s, "pack",
//	    saga.ChildWorkflow(PackWorkflow), saga.UndoActivity(a.Unpack), PackReq{Order: in})
//
//	saga.Step(ctx, s, "approval",
//	    saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})
func Step[In, Out any](ctx workflow.Context, s *Saga, name string, fwd Forward[In, Out], undo *Undo[In], in In) (Out, error) {
	var zero Out

	if s.err != nil {
		return zero, s.err
	}
	if err := s.claimName(name); err != nil {
		s.fail(err)
		return zero, err
	}

	key := s.opts.KeyFunc(ctx, name)

	if undo != nil {
		s.Add(name, func(cctx workflow.Context) error {
			return undo.run(cctx, s, key+undoSuffix, in)
		})
	}

	out, err := fwd.run(ctx, s, key, in)
	if err != nil {
		s.fail(err)
		return zero, err
	}
	return out, nil
}

// Activity runs the forward half of a step as an activity.
//
// fwd is taken as a typed function rather than the SDK's `any`, so the compiler
// checks its shape: a forward call returns a value and an error, a compensation
// returns only an error, and giving one where the other belongs will not
// compile. That check is worth having because the SDK does not make it --
// ExecuteActivity resolves a function value to a name string before validating
// anything, and the string path skips argument validation entirely. A mismatch
// there does not surface until the activity runs, which for a compensation
// means it surfaces while the saga is already failing.
//
// The idempotency key rides in the activity's ActivityID, readable inside the
// activity with IdempotencyKey.
func Activity[In, Out any](fwd func(context.Context, In) (Out, error)) Forward[In, Out] {
	return Forward[In, Out]{
		run: func(ctx workflow.Context, s *Saga, key string, in In) (Out, error) {
			opts := s.opts.ActivityOptions
			opts.ActivityID = key

			var out Out
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), fwd, in).Get(ctx, &out)
			return out, err
		},
	}
}

// UndoActivity runs a step's compensation as an activity.
//
// Its ScheduleToCloseTimeout is clamped to what is left of the compensation
// budget, and its ActivityID carries the same idempotency key the forward half
// saw -- whatever ran that half. A child workflow that packed an order and an
// activity that unpacks it read the same key, one with IdempotencyKeyOf and one
// with IdempotencyKey.
func UndoActivity[In any](undo func(context.Context, In) error) *Undo[In] {
	return &Undo[In]{
		run: func(ctx workflow.Context, s *Saga, key string, in In) error {
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
		},
	}
}

// ChildWorkflow runs the forward half of a step as a child workflow. It is
// Activity for work that is a workflow: a sub-saga, or anything long enough to
// deserve its own history.
//
// The first argument of the function being a workflow.Context rather than a
// context.Context is what tells this apart from Activity, which is the same
// distinction Temporal itself draws.
//
// The key rides in the child's WorkflowID, so the child reads it with
// IdempotencyKeyOf. Anything else about the child -- task queue, timeouts,
// retry policy, parent close policy -- comes from the context, so set it the
// ordinary way with workflow.WithChildOptions before calling Step.
func ChildWorkflow[In, Out any](fwd func(workflow.Context, In) (Out, error)) Forward[In, Out] {
	return Forward[In, Out]{
		run: func(ctx workflow.Context, _ *Saga, key string, in In) (Out, error) {
			opts := workflow.GetChildWorkflowOptions(ctx)
			opts.WorkflowID = key

			var out Out
			err := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, opts), fwd, in).Get(ctx, &out)
			return out, err
		},
	}
}

// UndoChildWorkflow runs a step's compensation as a child workflow. Its
// execution timeout is clamped to what is left of the compensation budget.
func UndoChildWorkflow[In any](undo func(workflow.Context, In) error) *Undo[In] {
	return &Undo[In]{
		run: func(ctx workflow.Context, _ *Saga, key string, in In) error {
			opts := workflow.GetChildWorkflowOptions(ctx)
			opts.WorkflowID = key

			if remaining, ok := RemainingBudget(ctx); ok {
				if opts.WorkflowExecutionTimeout <= 0 || opts.WorkflowExecutionTimeout > remaining {
					opts.WorkflowExecutionTimeout = remaining
				}
			}

			return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, opts), undo, in).Get(ctx, nil)
		},
	}
}

// Func runs the forward half of a step by calling the function here, in this
// workflow, rather than dispatching it anywhere.
//
// It is for work that has to happen in the workflow itself and can still fail
// the saga -- waiting for a signal and deciding what the answer means, waiting
// on a condition with workflow.Await, signalling another workflow, choosing
// between routes. Returning an error fails the step, which is what every other
// kind of step does too, so the rollback follows without the body having to
// arrange it.
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
//	saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})
//
// Like every step it is skipped once an earlier step has failed, so a saga on
// its way to being rolled back does not stop to wait for a human.
//
// No idempotency key is minted, because nothing is executed anywhere that could
// run twice. The step name still has to be unique, since it names the step in
// the compensation report.
func Func[In, Out any](fwd func(workflow.Context, In) (Out, error)) Forward[In, Out] {
	return Forward[In, Out]{
		run: func(ctx workflow.Context, _ *Saga, _ string, in In) (Out, error) {
			return fwd(ctx, in)
		},
	}
}

// UndoFunc runs a step's compensation by calling the function here. It runs on
// the disconnected compensation context like any other compensation, and can
// read what is left of the budget with RemainingBudget.
func UndoFunc[In any](undo func(workflow.Context, In) error) *Undo[In] {
	return &Undo[In]{
		run: func(ctx workflow.Context, _ *Saga, _ string, in In) error {
			return undo(ctx, in)
		},
	}
}
