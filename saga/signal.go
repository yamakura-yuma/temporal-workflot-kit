package saga

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Signal names a signal to send to a workflow.
//
// RunID may be empty, which targets whichever run of WorkflowID is current.
// That is usually what you want: the compensation runs later than the forward
// step, and pinning a run id would make it fail if the target continued as new
// in between.
type Signal struct {
	WorkflowID string
	// RunID is optional. Empty means the running instance of WorkflowID.
	RunID string
	// Name is the signal name the target listens on.
	Name string
}

// SignalStep sends a signal to another workflow as a saga step, and registers
// the signal that undoes it.
//
// It is Step for work that lives in a workflow somebody else is running: a
// long-lived inventory workflow you tell to hold stock, and tell to release it
// again if the saga fails. undo may be the zero Signal for a step with nothing
// to undo.
//
// Unlike Step and ChildWorkflowStep this carries no idempotency key, because
// SignalExternalWorkflow has no options struct to put one in. The target is
// identified by the workflow id in the Signal, and **making the receiving
// workflow tolerant of a repeated signal is the receiver's job**. In practice
// the pairing is what matters here: the saga guarantees the undoing signal is
// sent if the saga fails after the first one was sent.
//
// A signal is a command, so it is recorded in the workflow's history and is not
// re-sent on replay. It is also not bounded by the compensation budget -- the
// future resolves when the server accepts the signal, which does not involve
// running the target's code.
func SignalStep[In any](
	ctx workflow.Context,
	s *Saga,
	name string,
	fwd Signal,
	undo Signal,
	in In,
) error {
	h := halves[In, struct{}]{
		forward: func(c workflow.Context, _ string, in In) (struct{}, error) {
			err := workflow.SignalExternalWorkflow(c, fwd.WorkflowID, fwd.RunID, fwd.Name, in).Get(c, nil)
			return struct{}{}, err
		},
	}

	if undo.WorkflowID != "" && undo.Name != "" {
		h.undo = func(c workflow.Context, _ string, in In) error {
			return workflow.SignalExternalWorkflow(c, undo.WorkflowID, undo.RunID, undo.Name, in).Get(c, nil)
		}
	}

	_, err := register(ctx, s, name, in, h)
	return err
}

// AwaitSignal waits for a signal and decodes its payload. The second result
// reports whether one arrived before the timeout.
//
// It is a plain helper, not a step: waiting has no side effect, so there is
// nothing to register and nothing to undo. Wrap it in an FuncStep, which is
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
//	saga.FuncStep(ctx, s, "approval", awaitApproval, nil, ApprovalReq{Wait: wait})
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
