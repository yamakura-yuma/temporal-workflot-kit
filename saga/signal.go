package saga

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// AwaitSignal waits for a signal and decodes its payload. The second result
// reports whether one arrived before the timeout.
//
// It is a plain helper, not a step: waiting has no side effect, so there is
// nothing to register and nothing to undo. Wrap it in a Func step, which is
// what turns "nobody answered" into a failure of the saga and makes the wait
// skippable once an earlier step has failed:
//
//	func awaitApproval(ctx workflow.Context, req ApprovalReq) (Decision, error) {
//	    decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, req.Wait)
//	    if !ok {
//	        return Decision{}, temporal.NewApplicationError("nobody reviewed it", DeniedType, nil)
//	    }
//	    if !decision.Approved {
//	        return Decision{}, temporal.NewApplicationError("rejected", DeniedType, nil)
//	    }
//	    return decision, nil
//	}
//
//	saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})
//
// Called directly in the body of a saga it will wait its full timeout even
// after a step has failed, which is the reason to wrap it.
//
// A cancellation while waiting ends the wait with ok false.
func AwaitSignal[T any](ctx workflow.Context, signalName string, timeout time.Duration) (T, bool) {
	var payload T

	arrived := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, signalName),
		func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &payload)
			arrived = true
		})
	selector.AddFuture(workflow.NewTimer(ctx, timeout), func(workflow.Future) {})

	selector.Select(ctx)

	if !arrived {
		// Distinguishes a cancellation from a plain timeout for the caller's
		// logs; either way the wait is over and nothing arrived.
		workflow.GetLogger(ctx).Info("saga: no signal arrived",
			"signal", signalName, "canceled", ctx.Err() != nil)
	}
	return payload, arrived
}
