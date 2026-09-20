// Package approval is a second example: a saga that waits for a human between
// two steps.
//
// It exists to show three things the order example does not. How a signal fits
// between saga steps, what to do at a branch (look at s.Err() first), and that
// a rollback triggered by a business decision is written the same way as one
// triggered by a failure -- you return an error.
//
// The activities come from the order example, so this file is only about the
// signal.
package approval

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-saga/example/order"
	"github.com/yamakura-yuma/temporal-saga/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow.
const TaskQueue = "saga-approval"

// ApprovalSignal carries the decision. The payload is a Decision.
const ApprovalSignal = "approval"

// DeniedType is the error type the saga fails with when a human says no, so a
// caller can tell it apart from a step that broke.
const DeniedType = "ApprovalDenied"

// Decision is what a reviewer sends.
type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

// Request is the workflow input.
type Request struct {
	Order order.Order `json:"order"`
	// WaitSeconds bounds how long a reviewer has. Zero means one minute.
	WaitSeconds int `json:"wait_seconds,omitempty"`
}

// ApprovalWorkflow reserves stock, waits for a reviewer, and charges only if
// the answer is yes. The reservation is released whichever way it ends: the
// reviewer says no, nobody answers in time, or the workflow is canceled while
// waiting.
func ApprovalWorkflow(ctx workflow.Context, in Request) (order.Receipt, error) {
	var a *order.Activities

	wait := time.Duration(in.WaitSeconds) * time.Second
	if wait <= 0 {
		wait = time.Minute
	}

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (order.Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), order.ReserveReq{Order: in.Order})

		// The wait is a step like any other. What "nobody answered" means is
		// decided inside awaitApproval, the same way an activity decides what
		// its own failure means, so no branch leaks into this body.
		saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})

		chg, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund), order.ChargeReq{Order: in.Order})

		return order.Receipt{Reservation: res, Charge: chg}, nil
	})
}

// ApprovalReq is the input of the approval step.
type ApprovalReq struct {
	// Wait bounds how long a reviewer has.
	Wait time.Duration `json:"wait"`
}

// awaitApproval waits for a reviewer and turns the answer into a result or an
// error. It is ordinary workflow code, written by the caller, in the same place
// an activity would be: the saga only sequences it.
//
// Returning an error is the whole rollback trigger. Run releases the
// reservation on the way out.
func awaitApproval(ctx workflow.Context, req ApprovalReq) (Decision, error) {
	decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, req.Wait)
	if !ok {
		return Decision{}, temporal.NewApplicationError(
			"nobody reviewed the order in time", DeniedType, nil)
	}
	if !decision.Approved {
		return Decision{}, temporal.NewApplicationError(
			"the order was rejected by "+decision.By, DeniedType, nil)
	}
	return decision, nil
}
