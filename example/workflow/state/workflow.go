// Package state is the example for keeping the body of a saga short.
//
// This saga has five steps and an input far wider than any one of them needs.
// Written inline, every saga.Step call has to project that input down to the
// request the activity takes, and the ids the earlier steps returned have to be
// carried by hand from the step that produced them to the step that needs them.
// The order of the steps -- the one thing a reader opens a saga to find -- ends
// up buried in the plumbing.
//
// Here the workflow input and everything the steps produce live in one struct,
// each step is a method on it, and the closure passed to saga.Run is two lines.
//
// The same saga is written the other way in workflow_flat.go so the two can be
// read against each other. Neither is wrong; the flat one is what every other
// example in this repository does, and for three narrow steps it is the one to
// prefer. docs/specs/state.feature runs both and checks they agree.
//
// The one rule to keep in mind is that saga.Step's input is the activity's
// argument, so it has to be serializable. The state struct itself never goes to
// an activity; the methods build the request from it.
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

// ApprovalSignal carries the decision the approve step waits for. The payload
// is a Decision.
const ApprovalSignal = "approval"

// DeniedType is the error type the saga fails with when a reviewer says no, so
// a caller can tell it apart from a step that broke.
const DeniedType = "ApprovalDenied"

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

// sagaOptions is shared with the flat shape in workflow_flat.go. The difference
// between the two is meant to be the body of the saga, not its options.
func sagaOptions() saga.Options {
	return saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}
}

// StateWorkflow reserves stock, charges against that reservation, waits for a
// reviewer, packs, and ships against that charge -- five steps, written so that
// the body of saga.Run is two lines. Compare workflow_flat.go.
func StateWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	return saga.Run(ctx, sagaOptions(), func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		w := &fulfillment{in: in}
		return w.run(ctx, s)
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

// run is the saga itself, readable top to bottom.
func (w *fulfillment) run(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
	w.reserve(ctx, s)
	w.chargeCard(ctx, s)
	w.approve(ctx, s)
	w.pack(ctx, s)
	w.ship(ctx, s)

	if err := s.Err(); err != nil {
		return Receipt{}, err
	}
	return Receipt{
		Reservation: w.reservation,
		Charge:      w.charge,
		ApprovedBy:  w.approvedBy,
		Pack:        w.packing,
		Shipment:    w.shipment,
	}, nil
}

// Each step pulls what it needs out of the state and puts its result back.
// The step errors are not returned: after the first failure every later Step is
// a no-op, and run checks s.Err() once, before building the receipt.

func (w *fulfillment) reserve(ctx workflow.Context, s *saga.Saga) {
	var a *activity.Activities

	w.reservation, _ = saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve),
		activity.ReserveReq{Order: w.in.line()})
}

func (w *fulfillment) chargeCard(ctx workflow.Context, s *saga.Saga) {
	var a *activity.Activities

	w.charge, _ = saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund),
		activity.ChargeReq{Order: w.in.line(), Reservation: w.reservation})
}

// approve waits for a reviewer. It is a Func step: the waiting is workflow
// code, so there is no activity, nothing to undo, and nothing in the workflow
// history for the specification to find.
func (w *fulfillment) approve(ctx workflow.Context, s *saga.Saga) {
	decision, _ := saga.Step(ctx, s, "approve", saga.Func(awaitApproval), nil, ApproveReq{
		Order:    w.in.ID,
		Customer: w.in.Customer,
		Amount:   w.in.Amount,
		Wait:     w.in.wait(),
	})
	w.approvedBy = decision.By
}

func (w *fulfillment) pack(ctx workflow.Context, s *saga.Saga) {
	var a *activity.Activities

	w.packing, _ = saga.Step(ctx, s, "pack", saga.Activity(a.Pack), saga.UndoActivity(a.Unpack),
		activity.PackReq{Order: w.in.line()})
}

func (w *fulfillment) ship(ctx workflow.Context, s *saga.Saga) {
	var a *activity.Activities

	w.shipment, _ = saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment),
		activity.ShipReq{Order: w.in.line(), Charge: w.charge, Pack: w.packing, ApprovedBy: w.approvedBy})
}

// ApproveReq is the input of the approve step. The step is a Func, so this
// never reaches an activity; it stays here with the workflow code that reads
// it.
type ApproveReq struct {
	Order    string        `json:"order"`
	Customer string        `json:"customer"`
	Amount   int           `json:"amount"`
	Wait     time.Duration `json:"wait"`
}

// awaitApproval waits for a reviewer and turns the answer into a result or an
// error. Both shapes of this example use it.
//
// Returning an error is the whole rollback trigger: Run undoes the charge and
// the reservation on the way out.
func awaitApproval(ctx workflow.Context, req ApproveReq) (Decision, error) {
	decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, req.Wait)
	if !ok {
		return Decision{}, temporal.NewApplicationError(
			"nobody reviewed order "+req.Order+" in time", DeniedType, nil)
	}
	if !decision.Approved {
		return Decision{}, temporal.NewApplicationError(
			"the order was rejected by "+decision.By, DeniedType, nil)
	}
	return decision, nil
}
