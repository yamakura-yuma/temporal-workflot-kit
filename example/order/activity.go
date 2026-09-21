package order

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// Why the activities below are written the way they are.
//
// Temporal's own documentation puts the rule like this: idempotency keys "are
// enforced by the service you are calling from your Activity, not by the
// Activity itself" (https://docs.temporal.io/activity-definition). The activity
// hands the key on; whatever is downstream keeps the record that stops a second
// attempt from doing the work twice.
//
// So the shape of an activity follows its downstream. Downstream here is this
// demo service's own store, and the key is the primary key of the business row,
// which is the common case and the simplest: writing the row is claiming the
// key, one operation, with no window in between. That is why a failure below
// needs no cleanup. It fails before it writes, so there is no row.
//
// The other two cases are shorter than this one. If the downstream is an
// external API that accepts an idempotency key, pass the key in its header and
// keep no store here at all. Only a downstream that is an external API and is
// not idempotent needs the careful version -- claim the key, call, release the
// key if the call failed.

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

// Store is this demo service's database: the reservations, charges and
// shipments it has actually made, each row written under the idempotency key
// of the activity that made it.
//
// In production this is one table with a UNIQUE constraint on the key column,
// and insert below is
//
//	INSERT ... ON CONFLICT (idem_key) DO NOTHING RETURNING id
//
// -- a single statement, which is the whole point: the write and the claim
// cannot come apart. Being a map in this process is only honest for an example.
// Its shape is not a simplification.
//
// It is a type of its own rather than fields on Activities because a worker
// registers every exported method of the struct it is given as an activity, so
// a query method there would be rejected as a malformed activity.
type Store struct {
	mu   sync.Mutex
	rows map[string]string // idempotency key -> the id of the row stored under it
}

// NewStore returns an empty Store.
func NewStore() *Store { return &Store{rows: map[string]string{}} }

// insert writes a row under key and returns its id. When a row is already
// there -- a retried attempt, or a second attempt still in flight after a
// timeout -- the id from the first write comes back and nothing is written.
//
// Holding the lock across the read and the write is what the UNIQUE constraint
// would do for a real store. Checking whether the key is taken and then writing
// is not equivalent: two attempts of the same activity can be in here at once.
//
// There is no second result saying which of the two happened, because no caller
// has any use for one. The id is the same either way.
func (s *Store) insert(key, id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.rows[key]; ok {
		return existing
	}
	s.rows[key] = id
	return id
}

// delete removes the row under key, reporting whether there was one. This is
// business, not bookkeeping: it is how a reservation is released and a charge
// refunded, which is why a compensation calls it and a failed forward activity
// does not.
func (s *Store) delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[key]; !ok {
		return false
	}
	delete(s.rows, key)
	return true
}

// Activities is the worked example of the contract the saga package puts on
// activities: let the downstream enforce the idempotency key, and succeed when
// a compensation finds nothing to undo.
type Activities struct {
	store *Store
}

// NewActivities returns activities backed by the given store.
func NewActivities(store *Store) *Activities {
	return &Activities{store: store}
}

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	k, _ := saga.IdempotencyKey(ctx)
	// The row carries the key, so writing the row is the claim.
	id := a.store.insert(k, "res-"+req.Order.ID)
	logf(ctx, "reserved %s for %s", id, req.Order.SKU)
	return id, nil
}

// Unreserve releases stock held by Reserve. It succeeds when there is nothing
// to release, which is what lets the saga register it before Reserve runs.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	if req.Order.FailUndo == "reserve" {
		return fmt.Errorf("unreserve: warehouse unreachable for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	if !a.store.delete(k) {
		logf(ctx, "unreserve: nothing held for %s, nothing to do", req.Order.ID)
		return nil
	}
	logf(ctx, "released the stock held for %s", req.Order.ID)
	return nil
}

// Charge takes payment for the order.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	id := a.store.insert(k, "chg-"+req.Order.ID)
	logf(ctx, "charged %d for %s as %s", req.Order.Amount, req.Order.ID, id)
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	if req.Order.FailUndo == "charge" {
		return fmt.Errorf("refund: gateway unreachable for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	if !a.store.delete(k) {
		logf(ctx, "refund: no charge recorded for %s, nothing to do", req.Order.ID)
		return nil
	}
	logf(ctx, "refunded %s", req.Order.ID)
	return nil
}

// Ship books a shipment for the order.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	id := a.store.insert(k, "shp-"+req.Order.ID)
	logf(ctx, "booked shipment %s", id)
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	if req.Order.FailUndo == "ship" {
		return fmt.Errorf("cancel-shipment: carrier unreachable for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	if !a.store.delete(k) {
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

// --- for the specifications --------------------------------------------------
//
// What follows is here so docs/specs/ can look inside the store from outside
// the workflow. A production store has no counterpart: nothing in the business
// code asks whether a given step of a given run still has its row.

// Held reports whether a step of a given workflow run still holds its row. A
// specification uses it to check that a rollback actually undid everything.
func (s *Store) Held(runID, step string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[runID+"/"+step]
	return ok
}
