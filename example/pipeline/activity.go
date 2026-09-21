package pipeline

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// The store below is the one the order example explains at the top of its
// activity.go: the downstream of these activities is this demo service's own
// database, an in-process map whose key is the idempotency key of the row. So
// writing the row is claiming the key, and an activity that fails before it
// writes leaves nothing to clean up. A downstream that takes an idempotency key
// of its own -- an external API with a header for it -- would need no store
// here at all.
//
// What this example adds to that is the ids: every row is written under an id
// built from the row the step before it wrote.

// Each request carries what the step before it produced. The compensation of a
// step receives the same struct, so it can undo the work against the upstream
// id rather than having to look it up again.
type (
	ReserveReq struct {
		Order Order `json:"order"`
	}
	ChargeReq struct {
		Order Order `json:"order"`
		// Reservation is the id the reserve step returned.
		Reservation string `json:"reservation"`
	}
	ShipReq struct {
		Order Order `json:"order"`
		// Charge is the id the charge step returned.
		Charge string `json:"charge"`
	}
)

// Store is this demo service's database: the reservations, charges and
// shipments it has made, each row under the idempotency key of the activity
// that wrote it. In production it is one table with a UNIQUE constraint on the
// key, and insert is INSERT ... ON CONFLICT (idem_key) DO NOTHING RETURNING id.
type Store struct {
	mu   sync.Mutex
	rows map[string]string // idempotency key -> the id of the row stored under it

	// saw belongs to the specifications, not to the store; see the bottom of
	// this file.
	saw map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{rows: map[string]string{}, saw: map[string]string{}}
}

// insert writes a row under key and returns its id, giving back the id of the
// row already there if the activity is being retried. The lock across the read
// and the write is what the UNIQUE constraint does for a real store.
func (s *Store) insert(key, id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.rows[key]; ok {
		return existing
	}
	s.rows[key] = id
	return id
}

// delete removes the row under key, reporting whether there was one.
func (s *Store) delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[key]; !ok {
		return false
	}
	delete(s.rows, key)
	return true
}

// Activities is the worked example. NewActivities takes the store so a test
// can look at it.
type Activities struct{ store *Store }

// NewActivities returns activities backed by the given store.
func NewActivities(store *Store) *Activities { return &Activities{store: store} }

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	k, _ := saga.IdempotencyKey(ctx)
	// The row carries the key, so writing the row is the claim.
	return a.store.insert(k, "res-"+req.Order.ID), nil
}

// Unreserve releases the stock Reserve held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	if !a.store.delete(k) {
		return nil
	}
	logf(ctx, "released the stock for %s", req.Order.ID)
	return nil
}

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	id := a.store.insert(k, "chg-"+req.Reservation)
	logf(ctx, "charged %d against reservation %s", req.Order.Amount, req.Reservation)
	return id, nil
}

// Refund reverses Charge. It knows which reservation the charge belonged to
// because the saga handed the compensation the same input the step got.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.store.record("charge", req.Reservation)

	k, _ := saga.IdempotencyKey(ctx)
	if !a.store.delete(k) {
		return nil
	}
	logf(ctx, "refunded the charge against reservation %s", req.Reservation)
	return nil
}

// Ship books a shipment against the charge.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.store.insert(k, "shp-"+req.Charge), nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.store.record("ship", req.Charge)

	k, _ := saga.IdempotencyKey(ctx)
	if !a.store.delete(k) {
		return nil
	}
	logf(ctx, "cancelled the shipment for charge %s", req.Charge)
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }()
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}

// --- for the specifications --------------------------------------------------
//
// What follows is here so docs/specs/ can look inside the store from outside
// the workflow. A production store has no counterpart: no business code asks
// which upstream id a compensation was handed, and the compensations above call
// record for this reason only.

// record notes the upstream id a compensation was handed.
func (s *Store) record(step, upstream string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saw[step] = upstream
}

// Upstream reports the id the named step's compensation received. A
// specification uses it to check that the pipeline really did feed it through.
func (s *Store) Upstream(step string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saw[step]
}

// Held reports whether a step of a given workflow run still holds its row.
func (s *Store) Held(runID, step string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[runID+"/"+step]
	return ok
}
