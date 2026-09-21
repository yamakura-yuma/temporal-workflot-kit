package external

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// A copy of the order example's store, narrowed to one step. It is not what
// this example is about -- the hold step above it is -- and it is here only so
// the saga has something to fail at after the hold has been sent.
//
// The shape is the one the order example explains at the top of its
// activity.go: the downstream is this demo service's own database, an
// in-process map whose key is the idempotency key of the row, so writing the
// row is claiming the key and an activity that fails before it writes leaves
// nothing to clean up.

// ChargeReq is the input of the charge step.
type ChargeReq struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
	Fail   bool   `json:"fail,omitempty"`
}

// Store is the demo database behind the charge step. In production it is one
// table with a UNIQUE constraint on the idempotency key, and insert is
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

// Activities is the saga's activity set.
type Activities struct{ store *Store }

// NewActivities returns activities backed by the given store.
func NewActivities(store *Store) *Activities { return &Activities{store: store} }

// Charge takes payment.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("charge: card declined for %s", req.Order)
	}
	k, _ := saga.IdempotencyKey(ctx)
	// The row carries the key, so writing the row is the claim.
	return a.store.insert(k, "chg-"+req.Order), nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.store.delete(k)
	return nil
}
