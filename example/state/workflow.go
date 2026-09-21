// Package state is the example for keeping the body of a saga short.
//
// Once the requests an activity takes have more than a couple of fields, a saga
// written inline turns into a wall: every saga.Step call carries a literal that
// restates half the workflow input. Here the workflow input and everything the
// steps produce live in one struct, each step is a method on it, and the
// closure passed to saga.Run is two lines.
//
// The one rule to keep in mind is that saga.Step's input is an activity
// argument, so it has to be serializable. The state struct itself never goes to
// an activity; the methods build a request from it.
package state

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow.
const TaskQueue = "saga-state"

// Order is the workflow input. It has enough fields that restating them at
// every step would be the problem this example is about.
type Order struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Amount   int    `json:"amount"`
	Currency string `json:"currency"`
	Address  string `json:"address"`
	Coupon   string `json:"coupon,omitempty"`
	FailAt   string `json:"fail_at,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// StateWorkflow is the same three-step saga as the other examples, written so
// that the body of saga.Run is two lines.
func StateWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	opts := saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}

	return saga.Run(ctx, opts, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		w := &fulfillment{in: in}
		return w.run(ctx, s)
	})
}

// fulfillment is the workflow's state: its input, and what each step produced.
// It never leaves the workflow, so it does not have to be serializable.
type fulfillment struct {
	in Order

	reservation string
	charge      string
	shipment    string
}

// run is the saga itself, readable top to bottom.
func (w *fulfillment) run(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
	w.reserve(ctx, s)
	w.chargeCard(ctx, s)
	w.ship(ctx, s)

	if err := s.Err(); err != nil {
		return Receipt{}, err
	}
	return Receipt{Reservation: w.reservation, Charge: w.charge, Shipment: w.shipment}, nil
}

// Each step pulls what it needs out of the state and puts its result back. The
// step error is not returned: after the first failure the later steps are
// no-ops, and run checks s.Err() once, before building the receipt.

func (w *fulfillment) reserve(ctx workflow.Context, s *saga.Saga) {
	var a *Activities

	w.reservation, _ = saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{
		Order:    w.in.ID,
		SKU:      w.in.SKU,
		Quantity: w.in.Quantity,
		Fail:     w.in.FailAt == "reserve",
	})
}

func (w *fulfillment) chargeCard(ctx workflow.Context, s *saga.Saga) {
	var a *Activities

	w.charge, _ = saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund), ChargeReq{
		Order:       w.in.ID,
		Customer:    w.in.Customer,
		Amount:      w.in.Amount,
		Currency:    w.in.Currency,
		Coupon:      w.in.Coupon,
		Reservation: w.reservation,
		Fail:        w.in.FailAt == "charge",
	})
}

func (w *fulfillment) ship(ctx workflow.Context, s *saga.Saga) {
	var a *Activities

	w.shipment, _ = saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment), ShipReq{
		Order:   w.in.ID,
		Address: w.in.Address,
		Charge:  w.charge,
		Fail:    w.in.FailAt == "ship",
	})
}
