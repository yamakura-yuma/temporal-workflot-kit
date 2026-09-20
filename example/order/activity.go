// Package order is an example saga: it reserves stock, charges a card and
// books a shipment, undoing whatever already happened if a later step fails.
//
// The activities keep their ledger in memory, which is only honest for a demo.
// What is not a shortcut is the shape of that ledger: each activity claims its
// idempotency key atomically before doing any work, and each compensation
// succeeds when it finds no claim to undo. That is the contract the saga
// package relies on, and it is the part worth copying.
package order

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

// Order is the workflow input.
type Order struct {
	ID     string `json:"id"`
	SKU    string `json:"sku"`
	Amount int    `json:"amount"`
	// FailAt names a step that should fail, so the example can demonstrate a
	// rollback. Empty means the happy path.
	FailAt string `json:"fail_at,omitempty"`
	// HoldSeconds keeps the workflow waiting after the charge step, long enough
	// to cancel it from the CLI and watch the compensations still run.
	HoldSeconds int `json:"hold_seconds,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// Reservation, Charge and Shipment are the per-step payloads.
type (
	ReserveReq struct {
		Order  Order  `json:"order"`
		FailAt string `json:"fail_at,omitempty"`
	}
	ChargeReq struct {
		Order  Order  `json:"order"`
		FailAt string `json:"fail_at,omitempty"`
	}
	ShipReq struct {
		Order  Order  `json:"order"`
		FailAt string `json:"fail_at,omitempty"`
	}
)

// Activities holds the demo ledger. Register a single instance on the worker so
// that every activity shares it.
type Activities struct {
	mu     sync.Mutex
	claims map[string]string // idempotency key -> the id handed out for it
}

// NewActivities returns an Activities with an empty ledger.
func NewActivities() *Activities {
	return &Activities{claims: map[string]string{}}
}

// claim records that key has been acted on, and returns the id assigned to it.
// The second result reports whether this call is the one that did the work; a
// retry of the same key gets the original id and false.
//
// The lock is what makes the check-and-act atomic. A real activity needs the
// same property from its store -- a unique constraint, INSERT ... ON CONFLICT,
// or the downstream API's idempotency-key header. Reading first and writing
// afterwards is not equivalent: two attempts of the same activity can be in
// flight at once after a timeout, and both would see the key as unused.
func (a *Activities) claim(key, id string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, ok := a.claims[key]; ok {
		return existing, false
	}
	a.claims[key] = id
	return id, true
}

// release removes a claim, reporting whether there was one to remove.
func (a *Activities) release(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.claims[key]; !ok {
		return false
	}
	delete(a.claims, key)
	return true
}

func key(ctx context.Context, fallback string) string {
	if k, ok := saga.IdempotencyKey(ctx); ok {
		return k
	}
	// Only reached outside a Temporal activity, i.e. in a direct unit test.
	return fallback
}

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	k := key(ctx, "reserve/"+req.Order.ID)
	id, fresh := a.claim(k, "res-"+req.Order.ID)
	if !fresh {
		return id, nil
	}
	if req.FailAt == "reserve" {
		a.release(k)
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	logf(ctx, "reserved %s for %s", id, req.Order.SKU)
	return id, nil
}

// Unreserve releases stock held by Reserve. It succeeds when there is nothing
// to release, which is what lets the saga register it before Reserve runs.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	k := key(ctx, "reserve/"+req.Order.ID)
	if !a.release(k) {
		logf(ctx, "unreserve: nothing held for %s, nothing to do", req.Order.ID)
		return nil
	}
	logf(ctx, "released the stock held for %s", req.Order.ID)
	return nil
}

// Charge takes payment for the order.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	k := key(ctx, "charge/"+req.Order.ID)
	id, fresh := a.claim(k, "chg-"+req.Order.ID)
	if !fresh {
		return id, nil
	}
	if req.FailAt == "charge" {
		a.release(k)
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	logf(ctx, "charged %d for %s as %s", req.Order.Amount, req.Order.ID, id)
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k := key(ctx, "charge/"+req.Order.ID)
	if !a.release(k) {
		logf(ctx, "refund: no charge recorded for %s, nothing to do", req.Order.ID)
		return nil
	}
	logf(ctx, "refunded %s", req.Order.ID)
	return nil
}

// Ship books a shipment for the order.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	k := key(ctx, "ship/"+req.Order.ID)
	id, fresh := a.claim(k, "shp-"+req.Order.ID)
	if !fresh {
		return id, nil
	}
	if req.FailAt == "ship" {
		a.release(k)
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	logf(ctx, "booked shipment %s", id)
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	k := key(ctx, "ship/"+req.Order.ID)
	if !a.release(k) {
		logf(ctx, "cancel-shipment: nothing booked for %s, nothing to do", req.Order.ID)
		return nil
	}
	logf(ctx, "cancelled the shipment for %s", req.Order.ID)
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }() // activity.GetLogger panics outside an activity
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}
