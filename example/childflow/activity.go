package childflow

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
// What this example adds to that is where the key comes from. The packing child
// workflow reads it with saga.IdempotencyKeyOf and passes it down in a Note, so
// Pack writes its row under the key the child was given rather than under one
// of its own.

type (
	ReserveReq struct {
		Order Order `json:"order"`
	}
	PackReq struct {
		Order Order `json:"order"`
	}
	ShipReq struct {
		Order Order  `json:"order"`
		Pack  string `json:"pack"`
	}
)

// Note is what the packing children hand their activities.
type Note struct {
	Key   string `json:"key"`
	Order string `json:"order"`
}

// Store is this demo service's database: one row per piece of work, under the
// idempotency key of whatever wrote it. In production it is one table with a
// UNIQUE constraint on the key, and insert is
// INSERT ... ON CONFLICT (idem_key) DO NOTHING RETURNING id.
type Store struct {
	mu   sync.Mutex
	rows map[string]string // idempotency key -> the id of the row stored under it

	// keys belongs to the specifications, not to the store; see the bottom of
	// this file.
	keys map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{rows: map[string]string{}, keys: map[string]string{}}
}

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

// Activities backs both the saga's activity steps and the packing children.
type Activities struct{ store *Store }

// NewActivities returns activities backed by the given store.
func NewActivities(store *Store) *Activities { return &Activities{store: store} }

// Reserve holds stock.
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
	a.store.delete(k)
	return nil
}

// Pack is what the packing child does. The key comes from the child, not from
// this activity's own id.
func (a *Activities) Pack(ctx context.Context, note Note) (string, error) {
	a.store.noteKey("pack", note.Key)

	id := a.store.insert(note.Key, "pk-"+note.Order)
	logf(ctx, "packed %s under key %s", note.Order, note.Key)
	return id, nil
}

// Unpack undoes Pack, and succeeds when there is nothing packed.
//
// It is the compensation of a step whose forward half was a child workflow, and
// it still reads the same key -- here with IdempotencyKey, because this is an
// activity, where the child used IdempotencyKeyOf.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.store.noteKey("unpack", k)

	if !a.store.delete(k) {
		logf(ctx, "unpack: nothing packed under key %s, nothing to do", k)
		return nil
	}
	logf(ctx, "unpacked %s under key %s", req.Order.ID, k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.store.insert(k, "shp-"+req.Order.ID), nil
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
// the workflow. A production store has no counterpart: no business code records
// which idempotency key a packing child read, and Pack and Unpack above call
// noteKey for this reason only.

func (s *Store) noteKey(which, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[which] = key
}

// KeySeenBy reports the idempotency key the named packing child read. A
// specification uses it to check both halves saw the same one.
func (s *Store) KeySeenBy(which string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys[which]
}

// Held reports whether a step still holds its row.
func (s *Store) Held(runID, step string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[runID+"/"+step]
	return ok
}
