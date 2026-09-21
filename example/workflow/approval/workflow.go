// Package approval is the example for a saga that waits for a human between two
// steps.
//
// It shows three things the order example does not. How a wait fits between
// saga steps, what to do at a branch (look at s.Err() first), and that a
// rollback triggered by a business decision is written the same way as one
// triggered by a failure -- you return an error.
//
// The wait is a step like any other, and it is why a step's halves are plain
// workflow functions: waiting runs no activity at all. Being a step is what
// makes it skippable once an earlier step has failed, so a saga on its way to a
// rollback does not sit here for an hour first.
package approval

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow.
const TaskQueue = "saga-approval"

// ApprovalSignal carries the decision. The payload is a Decision.
const ApprovalSignal = "approval"

// DeniedType is the error type the saga fails with when a human says no, so a
// caller can tell it apart from a step that broke.
const DeniedType = "ApprovalDenied"

var acts *activity.Activities

// Decision is what a reviewer sends.
type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

// Request is the workflow input.
type Request struct {
	Order activity.Order `json:"order"`
	// WaitSeconds bounds how long a reviewer has. Zero means one minute.
	WaitSeconds int `json:"wait_seconds,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
}

// ApprovalWorkflow reserves stock, waits for a reviewer, and charges only if
// the answer is yes. The reservation is released whichever way it ends: the
// reviewer says no, nobody answers in time, or the workflow is canceled while
// waiting.
func ApprovalWorkflow(ctx workflow.Context, in Request) (Receipt, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		// A compensation runs on a context nothing can cancel, so bound it here:
		// without ScheduleToCloseTimeout the default retry policy is unlimited.
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	return saga.RunOrCompensate(ctx, saga.Options{},
		func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
			w := &fulfillment{in: in}

			s.Step(ctx, "reserve", w.reserve, w.unreserve)

			// Nothing to undo about having waited, so the compensation is nil.
			s.Step(ctx, "approval", w.await, nil)

			s.Step(ctx, "charge", w.chargeCard, w.refund)

			return Receipt{Reservation: w.reservation, Charge: w.charge}, nil
		})
}

type fulfillment struct {
	in Request

	reservation string
	charge      string
	decision    Decision
}

func (w *fulfillment) reserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Reserve,
		activity.ReserveReq{Order: w.in.Order}).Get(ctx, &w.reservation)
}

func (w *fulfillment) unreserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Unreserve,
		activity.ReserveReq{Order: w.in.Order, Reservation: w.reservation}).Get(ctx, nil)
}

// await waits for a reviewer and turns the answer into a result or an error.
// Returning an error is the whole rollback trigger: RunOrCompensate releases the
// reservation on the way out.
//
// What "nobody answered" means is decided here, the same way an activity
// decides what its own failure means, so no branch leaks into the saga body.
func (w *fulfillment) await(ctx workflow.Context) error {
	wait := time.Duration(w.in.WaitSeconds) * time.Second
	if wait <= 0 {
		wait = time.Minute
	}

	decision, ok := awaitDecision(ctx, wait)
	if !ok {
		return temporal.NewApplicationError(
			"nobody reviewed the order in time", DeniedType, nil)
	}
	if !decision.Approved {
		return temporal.NewApplicationError(
			"the order was rejected by "+decision.By, DeniedType, nil)
	}

	w.decision = decision
	return nil
}

func (w *fulfillment) chargeCard(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Charge,
		activity.ChargeReq{Order: w.in.Order, Reservation: w.reservation}).Get(ctx, &w.charge)
}

func (w *fulfillment) refund(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Refund,
		activity.ChargeReq{Order: w.in.Order, Charge: w.charge}).Get(ctx, nil)
}

// awaitDecision waits for a reviewer and reports whether one answered before
// the timeout.
//
// It is ordinary workflow code -- a selector over the signal channel and a
// timer -- and the saga library has nothing to do with it. That is why it lives
// here: waiting has no side effect, carries no idempotency key and has nothing
// to undo, so there would be nothing for a saga package to add.
func awaitDecision(ctx workflow.Context, timeout time.Duration) (Decision, bool) {
	var decision Decision
	arrived := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, ApprovalSignal),
		func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &decision)
			arrived = true
		})
	selector.AddFuture(workflow.NewTimer(ctx, timeout), func(workflow.Future) {})

	selector.Select(ctx)

	if !arrived {
		// Distinguishes a cancellation from a plain timeout in the logs; either
		// way the wait is over and nothing arrived.
		workflow.GetLogger(ctx).Info("no decision arrived",
			"canceled", ctx.Err() != nil)
	}
	return decision, arrived
}
