package order

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

// Order is the workflow input. The Fail* fields exist so a test can force a
// particular failure; everything else is what a real order would carry.
type Order struct {
	ID     string `json:"id"`
	SKU    string `json:"sku"`
	Amount int    `json:"amount"`

	// FailAt names a step whose forward activity should fail.
	FailAt string `json:"fail_at,omitempty"`
	// FailUndo names a step whose compensation should fail.
	FailUndo string `json:"fail_undo,omitempty"`
	// HoldSeconds keeps the workflow waiting after the charge step, so a test
	// can cancel it mid-saga.
	HoldSeconds int `json:"hold_seconds,omitempty"`
	// MarkAttribute asks the saga to flag a failed rollback with a search
	// attribute.
	MarkAttribute bool `json:"mark_attribute,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// The per-step payloads. Each carries the whole order, so a compensation has
// the same input its forward step had.
type (
	ReserveReq struct {
		Order Order `json:"order"`
	}
	ChargeReq struct {
		Order Order `json:"order"`
	}
	ShipReq struct {
		Order Order `json:"order"`
	}
)

// Ledger is the store the activities claim their idempotency keys in. It is in
// memory, which is only honest for an example. Its shape is not: a real one
// needs the same atomicity from its storage -- a unique constraint,
// INSERT ... ON CONFLICT, or the downstream API's own idempotency-key header.
//
// It is a type of its own rather than fields on Activities because a worker
// registers every exported method of the struct it is given as an activity, so
// a query method there would be rejected as a malformed activity.
type Ledger struct {
	mu     sync.Mutex
	claims map[string]string // idempotency key -> the id handed out for it
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger { return &Ledger{claims: map[string]string{}} }

// claim records that key has been acted on and returns the id assigned to it.
// The second result reports whether this call is the one that did the work; a
// retry of the same key gets the original id and false.
//
// Holding the lock across the read and the write is the point. Checking whether
// the key was used and then acting on it is not equivalent: two attempts of the
// same activity can be in flight at once after a timeout, and both would see
// the key as unused.
func (l *Ledger) claim(key, id string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.claims[key]; ok {
		return existing, false
	}
	l.claims[key] = id
	return id, true
}

// release removes a claim, reporting whether there was one to remove.
func (l *Ledger) release(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.claims[key]; !ok {
		return false
	}
	delete(l.claims, key)
	return true
}

// Held reports whether a step of a given workflow run still holds its claim. A
// specification uses it to check that a rollback actually undid everything.
func (l *Ledger) Held(runID, step string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.claims[runID+"/"+step]
	return ok
}

// Activities is the worked example of the contract the saga package puts on
// activities: claim the idempotency key atomically before doing any work, and
// succeed when a compensation finds nothing to undo.
type Activities struct {
	ledger *Ledger
}

// NewActivities returns activities backed by the given ledger.
func NewActivities(ledger *Ledger) *Activities {
	return &Activities{ledger: ledger}
}

func (a *Activities) claim(key, id string) (string, bool) { return a.ledger.claim(key, id) }
func (a *Activities) release(key string) bool             { return a.ledger.release(key) }

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
	if req.Order.FailAt == "reserve" {
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
	if req.Order.FailUndo == "reserve" {
		return fmt.Errorf("unreserve: warehouse unreachable for %s", req.Order.ID)
	}
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
	if req.Order.FailAt == "charge" {
		a.release(k)
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	logf(ctx, "charged %d for %s as %s", req.Order.Amount, req.Order.ID, id)
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k := key(ctx, "charge/"+req.Order.ID)
	if req.Order.FailUndo == "charge" {
		return fmt.Errorf("refund: gateway unreachable for %s", req.Order.ID)
	}
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
	if req.Order.FailAt == "ship" {
		a.release(k)
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	logf(ctx, "booked shipment %s", id)
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	k := key(ctx, "ship/"+req.Order.ID)
	if req.Order.FailUndo == "ship" {
		return fmt.Errorf("cancel-shipment: carrier unreachable for %s", req.Order.ID)
	}
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
