// Package state is the example for keeping the body of a saga short.
//
// This saga has five steps and an input far wider than any one of them needs.
// Each step has to project that input down to the request its activity takes,
// and the ids the earlier steps returned have to reach the steps that need
// them.
//
// Putting the input and every result on one struct, and making each half of
// each step a method on it, is what keeps the body of saga.Run to five lines --
// one per step, in order. It is also what lets a compensation read what its
// forward half produced, with no plumbing at all.
//
// The same saga is written the other way in workflow_flat.go, with every half
// inline, so the two can be read against each other. Neither is wrong; for a
// short saga over a narrow input the inline one is fine. docs/specs/
// state.feature runs both and checks they agree.
package state

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow. Both
// shapes of this example run on it.
const TaskQueue = "saga-state"

// ApprovalSignal carries the decision the approve step waits for.
const ApprovalSignal = "approval"

// DeniedType is the error type the saga fails with when a reviewer says no.
const DeniedType = "ApprovalDenied"

var acts *activity.Activities

// Order is the workflow input. It is this wide on purpose: the activities take
// a much narrower activity.Order, and projecting one onto the other at every
// step is the problem this example is about.
type Order struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Amount   int    `json:"amount"`
	Currency string `json:"currency"`
	Address  string `json:"address"`
	Coupon   string `json:"coupon,omitempty"`

	// WaitSeconds bounds how long a reviewer has. Zero means one minute.
	WaitSeconds int `json:"wait_seconds,omitempty"`
	// FailAt names a step whose activity should fail.
	FailAt string `json:"fail_at,omitempty"`
}

// line is the part of the order the activities are given.
func (o Order) line() activity.Order {
	return activity.Order{
		ID:       o.ID,
		SKU:      o.SKU,
		Quantity: o.Quantity,
		Amount:   o.Amount,
		FailAt:   o.FailAt,
	}
}

// wait is how long the approve step gives a reviewer.
func (o Order) wait() time.Duration {
	if o.WaitSeconds <= 0 {
		return time.Minute
	}
	return time.Duration(o.WaitSeconds) * time.Second
}

// Decision is what a reviewer sends.
type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

// Receipt is the workflow output. Both shapes return the same one, which is
// what the specification compares.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	ApprovedBy  string `json:"approved_by"`
	Pack        string `json:"pack"`
	Shipment    string `json:"shipment"`
}

// activityOptions are shared with the flat shape in workflow_flat.go. The
// difference between the two is meant to be the body of the saga, not this.
func activityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
}

// StateWorkflow reserves stock, charges against that reservation, waits for a
// reviewer, packs, and ships. Compare workflow_flat.go.
func StateWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())

	return saga.Run(ctx, saga.Options{CompensationBudget: time.Minute},
		func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
			w := &fulfillment{in: in}

			saga.Step(ctx, s, "reserve", w.reserve, w.unreserve)
			saga.Step(ctx, s, "charge", w.chargeCard, w.refund)
			saga.Step(ctx, s, "approve", w.approve, nil)
			saga.Step(ctx, s, "pack", w.pack, w.unpack)
			saga.Step(ctx, s, "ship", w.ship, w.cancelShipment)

			return w.receipt(), nil
		})
}

// fulfillment is the workflow's state: the input, and what each step produced.
// It never leaves the workflow, so it does not have to be serializable.
type fulfillment struct {
	in Order

	reservation string
	charge      string
	approvedBy  string
	packing     string
	shipment    string
}

func (w *fulfillment) receipt() Receipt {
	return Receipt{
		Reservation: w.reservation,
		Charge:      w.charge,
		ApprovedBy:  w.approvedBy,
		Pack:        w.packing,
		Shipment:    w.shipment,
	}
}

func (w *fulfillment) reserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Reserve,
		activity.ReserveReq{Order: w.in.line()}).Get(ctx, &w.reservation)
}

func (w *fulfillment) unreserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Unreserve,
		activity.ReserveReq{Order: w.in.line(), Reservation: w.reservation}).Get(ctx, nil)
}

func (w *fulfillment) chargeCard(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Charge,
		activity.ChargeReq{Order: w.in.line(), Reservation: w.reservation}).Get(ctx, &w.charge)
}

func (w *fulfillment) refund(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Refund,
		activity.ChargeReq{Order: w.in.line(), Charge: w.charge}).Get(ctx, nil)
}

// approve waits for a reviewer. It runs no activity, so there is nothing in the
// history for it and nothing to undo.
func (w *fulfillment) approve(ctx workflow.Context) error {
	decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, w.in.wait())
	if !ok {
		return temporal.NewApplicationError(
			"nobody reviewed order "+w.in.ID+" in time", DeniedType, nil)
	}
	if !decision.Approved {
		return temporal.NewApplicationError(
			"the order was rejected by "+decision.By, DeniedType, nil)
	}

	w.approvedBy = decision.By
	return nil
}

func (w *fulfillment) pack(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Pack,
		activity.PackReq{Order: w.in.line()}).Get(ctx, &w.packing)
}

func (w *fulfillment) unpack(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Unpack,
		activity.PackReq{Order: w.in.line(), Pack: w.packing}).Get(ctx, nil)
}

func (w *fulfillment) ship(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Ship,
		activity.ShipReq{Order: w.in.line(), Charge: w.charge, Pack: w.packing, ApprovedBy: w.approvedBy}).Get(ctx, &w.shipment)
}

func (w *fulfillment) cancelShipment(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.CancelShipment,
		activity.ShipReq{Order: w.in.line(), Charge: w.charge, Shipment: w.shipment}).Get(ctx, nil)
}
