// Package external is the example for a step that signals another workflow.
//
// Some state does not live in a database: it lives in a long-running workflow
// somebody else started. Here an inventory workflow holds stock per SKU, and
// the saga tells it to hold, then tells it to release again if a later step
// fails.
//
// Sending a signal is written with saga.Func rather than a constructor of its
// own: SignalExternalWorkflow has no options struct, so the library has no
// idempotency key to put on it and no timeout to clamp, which is exactly what a
// dedicated constructor would have been for. What the saga still guarantees is
// the pairing -- if the saga fails after the hold was sent, the release is
// sent.
package external

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

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

// HoldReq is the payload of both signals. The compensation is handed the same
// value the forward step sent, so the release names exactly what was held.
type HoldReq struct {
	// Inventory is the workflow id to signal.
	Inventory string `json:"inventory"`
	Order     string `json:"order"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
}

// sendHold and sendRelease are the two halves of the hold step. They are
// ordinary workflow code, which is all a Func step needs.
func sendHold(ctx workflow.Context, req HoldReq) (struct{}, error) {
	err := workflow.SignalExternalWorkflow(ctx, req.Inventory, "", HoldSignal, req).Get(ctx, nil)
	return struct{}{}, err
}

func sendRelease(ctx workflow.Context, req HoldReq) error {
	return workflow.SignalExternalWorkflow(ctx, req.Inventory, "", ReleaseSignal, req).Get(ctx, nil)
}

// Order is the saga's input.
type Order struct {
	ID string `json:"id"`
	// Inventory is the workflow id of the inventory workflow to signal.
	Inventory string `json:"inventory"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	Amount    int    `json:"amount"`
	FailAt    string `json:"fail_at,omitempty"`
}

// Receipt is the saga's output.
type Receipt struct {
	Charge string `json:"charge"`
}

// ChargeReq is the input of the charge step.
type ChargeReq struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
	Fail   bool   `json:"fail,omitempty"`
}

// ExternalWorkflow holds stock in another workflow, then charges. If the charge
// fails, the hold is released by the signal registered alongside it.
func ExternalWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *Activities

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		// Sending a signal is a Func step: the library has no key to put on it
		// and no timeout to clamp, so a constructor of its own would be this
		// with extra vocabulary.
		saga.Step(ctx, s, "hold", saga.Func(sendHold), saga.UndoFunc(sendRelease), HoldReq{Inventory: in.Inventory, Order: in.ID, SKU: in.SKU, Quantity: in.Quantity})

		chg, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund), ChargeReq{Order: in.ID, Amount: in.Amount, Fail: in.FailAt == "charge"})

		return Receipt{Charge: chg}, nil
	})
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

// --- the charge step ---------------------------------------------------------

// Ledger is the demo store behind the charge step.
type Ledger struct {
	mu     sync.Mutex
	claims map[string]string
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger { return &Ledger{claims: map[string]string{}} }

func (l *Ledger) claim(key, id string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.claims[key]; ok {
		return existing, false
	}
	l.claims[key] = id
	return id, true
}

func (l *Ledger) release(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.claims[key]; !ok {
		return false
	}
	delete(l.claims, key)
	return true
}

// Activities is the saga's activity set.
type Activities struct{ ledger *Ledger }

// NewActivities returns activities backed by the given ledger.
func NewActivities(ledger *Ledger) *Activities { return &Activities{ledger: ledger} }

func key(ctx context.Context, fallback string) string {
	if k, ok := saga.IdempotencyKey(ctx); ok {
		return k
	}
	return fallback
}

// Charge takes payment.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	k := key(ctx, "charge/"+req.Order)
	id, fresh := a.ledger.claim(k, "chg-"+req.Order)
	if !fresh {
		return id, nil
	}
	if req.Fail {
		a.ledger.release(k)
		return "", fmt.Errorf("charge: card declined for %s", req.Order)
	}
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.ledger.release(key(ctx, "charge/"+req.Order))
	return nil
}
