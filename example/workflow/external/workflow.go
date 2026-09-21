// Package external is the example for a step that signals another workflow.
//
// Some state does not live in a database: it lives in a long-running workflow
// somebody else started. Here an inventory workflow holds stock per SKU, and
// the saga tells it to hold, then tells it to release again if a later step
// fails.
//
// The step's halves send the signals directly. There is nothing to wrap and
// nothing special to declare -- a step is two workflow functions, and
// SignalExternalWorkflow is what these two happen to call. What the saga
// guarantees is only the pairing: if the saga fails after the hold was sent,
// the release is sent.
//
// A signal carries no idempotency key, and the receiver is responsible for
// tolerating a repeat.
package external

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflows.
const TaskQueue = "saga-external"

// The signals the inventory workflow listens on, and the query it answers.
const (
	HoldSignal    = "hold"
	ReleaseSignal = "release"
	HeldQuery     = "held"
)

var acts *activity.Activities

// HoldReq is the payload of both signals. The compensation sends the same
// value the forward step sent, so the release names exactly what was held.
type HoldReq struct {
	// Inventory is the workflow id to signal.
	Inventory string `json:"inventory"`
	Order     string `json:"order"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
}

// Request is the saga's input.
type Request struct {
	Order activity.Order `json:"order"`
	// Inventory is the workflow id of the inventory workflow to signal.
	Inventory string `json:"inventory"`
}

// Receipt is the saga's output.
type Receipt struct {
	Charge string `json:"charge"`
}

// ExternalWorkflow holds stock in another workflow, then charges. If the charge
// fails, the hold is released by the compensation registered alongside it.
func ExternalWorkflow(ctx workflow.Context, in Request) (Receipt, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	return saga.RunOrCompensate(ctx, saga.Options{CompensationBudget: time.Minute},
		func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
			w := &fulfillment{in: in}

			s.Step(ctx, "hold", w.hold, w.release)
			s.Step(ctx, "charge", w.chargeCard, w.refund)

			return Receipt{Charge: w.charge}, nil
		})
}

type fulfillment struct {
	in Request

	charge string
}

func (w *fulfillment) req() HoldReq {
	return HoldReq{
		Inventory: w.in.Inventory,
		Order:     w.in.Order.ID,
		SKU:       w.in.Order.SKU,
		Quantity:  w.in.Order.Quantity,
	}
}

func (w *fulfillment) hold(ctx workflow.Context) error {
	return workflow.SignalExternalWorkflow(ctx, w.in.Inventory, "", HoldSignal, w.req()).Get(ctx, nil)
}

func (w *fulfillment) release(ctx workflow.Context) error {
	return workflow.SignalExternalWorkflow(ctx, w.in.Inventory, "", ReleaseSignal, w.req()).Get(ctx, nil)
}

func (w *fulfillment) chargeCard(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Charge,
		activity.ChargeReq{Order: w.in.Order}).Get(ctx, &w.charge)
}

func (w *fulfillment) refund(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Refund,
		activity.ChargeReq{Order: w.in.Order, Charge: w.charge}).Get(ctx, nil)
}

// InventoryWorkflow is the long-running workflow the saga signals. It keeps a
// count per SKU, answers a query with it, and runs until its deadline.
//
// It is an ordinary workflow: it knows nothing about sagas.
func InventoryWorkflow(ctx workflow.Context, lifetime time.Duration) error {
	held := map[string]int{}

	if err := workflow.SetQueryHandler(ctx, HeldQuery, func() (map[string]int, error) {
		return held, nil
	}); err != nil {
		return err
	}

	if lifetime <= 0 {
		lifetime = 5 * time.Minute
	}

	holds := workflow.GetSignalChannel(ctx, HoldSignal)
	releases := workflow.GetSignalChannel(ctx, ReleaseSignal)
	deadline := workflow.NewTimer(ctx, lifetime)

	done := false
	for !done {
		selector := workflow.NewSelector(ctx)

		selector.AddReceive(holds, func(c workflow.ReceiveChannel, _ bool) {
			var req HoldReq
			c.Receive(ctx, &req)
			held[req.SKU] += req.Quantity
			workflow.GetLogger(ctx).Info("held", "sku", req.SKU, "qty", req.Quantity)
		})

		selector.AddReceive(releases, func(c workflow.ReceiveChannel, _ bool) {
			var req HoldReq
			c.Receive(ctx, &req)
			held[req.SKU] -= req.Quantity
			if held[req.SKU] <= 0 {
				delete(held, req.SKU)
			}
			workflow.GetLogger(ctx).Info("released", "sku", req.SKU, "qty", req.Quantity)
		})

		selector.AddFuture(deadline, func(workflow.Future) { done = true })

		selector.Select(ctx)
	}
	return nil
}
