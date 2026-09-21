package state

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// A copy of the order example's store, widened to four steps. It is not what
// this example is about -- the two shapes of the workflow are -- and it is here
// only so there is something for those shapes to sequence.
//
// The shape is the one the order example explains at the top of its
// activity.go: the downstream is this demo service's own database, an
// in-process map whose key is the idempotency key of the row. Writing the row
// is claiming the key, so an activity that fails before it writes leaves
// nothing to clean up. A downstream that takes an idempotency key of its own
// would need no store here at all.
//
// The requests are wide on purpose. Restating this many fields at every step is
// what the state shape in workflow.go is answering.

type (
	ReserveReq struct {
		Order    string `json:"order"`
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
		Fail     bool   `json:"fail,omitempty"`
	}
	ChargeReq struct {
		Order       string `json:"order"`
		Customer    string `json:"customer"`
		Amount      int    `json:"amount"`
		Currency    string `json:"currency"`
		Coupon      string `json:"coupon,omitempty"`
		Reservation string `json:"reservation"`
		Fail        bool   `json:"fail,omitempty"`
	}
	PackReq struct {
		Order    string `json:"order"`
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
		Address  string `json:"address"`
		Fail     bool   `json:"fail,omitempty"`
	}
	ShipReq struct {
		Order      string `json:"order"`
		Address    string `json:"address"`
		Charge     string `json:"charge"`
		ApprovedBy string `json:"approved_by"`
		Fail       bool   `json:"fail,omitempty"`
	}
)

// Store is the demo database. In production it is one table with a UNIQUE
// constraint on the idempotency key, and insert is
// INSERT ... ON CONFLICT (idem_key) DO NOTHING RETURNING id.
type Store struct {
	mu   sync.Mutex
	rows map[string]string // idempotency key -> the id of the row stored under it
}

// NewStore returns an empty Store.
func NewStore() *Store { return &Store{rows: map[string]string{}} }

// insert writes a row under key and returns its id, giving back the id of the
// row already there if the activity is being retried.
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

// Activities is the worked example's activity set.
type Activities struct{ store *Store }

// NewActivities returns activities backed by the given store.
func NewActivities(store *Store) *Activities { return &Activities{store: store} }

// Reserve holds stock.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("reserve: no stock for %s", req.SKU)
	}
	k, _ := saga.IdempotencyKey(ctx)
	// The row carries the key, so writing the row is the claim.
	id := a.store.insert(k, "res-"+req.Order)
	logf(ctx, "reserved %d x %s", req.Quantity, req.SKU)
	return id, nil
}

// Unreserve releases the stock Reserve held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.store.delete(k)
	return nil
}

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("charge: card declined for %s", req.Customer)
	}
	k, _ := saga.IdempotencyKey(ctx)
	id := a.store.insert(k, "chg-"+req.Order)
	logf(ctx, "charged %d %s to %s", req.Amount, req.Currency, req.Customer)
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.store.delete(k)
	return nil
}

// Pack makes the order ready to hand to a carrier.
func (a *Activities) Pack(ctx context.Context, req PackReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("pack: nothing to pack for %s", req.Order)
	}
	k, _ := saga.IdempotencyKey(ctx)
	id := a.store.insert(k, "pk-"+req.Order)
	logf(ctx, "packed %d x %s for %s", req.Quantity, req.SKU, req.Address)
	return id, nil
}

// Unpack reverses Pack.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.store.delete(k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("ship: no carrier for %s", req.Address)
	}
	k, _ := saga.IdempotencyKey(ctx)
	id := a.store.insert(k, "shp-"+req.Order)
	logf(ctx, "booked a shipment to %s, approved by %s", req.Address, req.ApprovedBy)
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.store.delete(k)
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }()
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}

// --- for the specifications --------------------------------------------------
//
// What follows is here so docs/specs/ can look inside the store from outside
// the workflow. A production store has no counterpart: nothing in the business
// code asks whether a given step of a given run still has its row.

// Held reports whether a step of a given workflow run still holds its row.
func (s *Store) Held(runID, step string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[runID+"/"+step]
	return ok
}
