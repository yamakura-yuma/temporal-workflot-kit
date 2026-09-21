package childflow

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

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

// Ledger records what happened, including the keys the packing children saw.
type Ledger struct {
	mu     sync.Mutex
	claims map[string]string
	keys   map[string]string // "pack" / "unpack" -> the key that child read
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger {
	return &Ledger{claims: map[string]string{}, keys: map[string]string{}}
}

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

func (l *Ledger) noteKey(which, key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys[which] = key
}

// KeySeenBy reports the idempotency key the named packing child read. A
// specification uses it to check both halves saw the same one.
func (l *Ledger) KeySeenBy(which string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.keys[which]
}

// Held reports whether a step still holds its claim.
func (l *Ledger) Held(runID, step string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.claims[runID+"/"+step]
	return ok
}

// Activities backs both the saga's activity steps and the packing children.
type Activities struct{ ledger *Ledger }

// NewActivities returns activities backed by the given ledger.
func NewActivities(ledger *Ledger) *Activities { return &Activities{ledger: ledger} }

func key(ctx context.Context, fallback string) string {
	if k, ok := saga.IdempotencyKey(ctx); ok {
		return k
	}
	return fallback
}

// Reserve holds stock.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	k := key(ctx, "reserve/"+req.Order.ID)
	id, fresh := a.ledger.claim(k, "res-"+req.Order.ID)
	if !fresh {
		return id, nil
	}
	if req.Order.FailAt == "reserve" {
		a.ledger.release(k)
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	return id, nil
}

// Unreserve releases the stock Reserve held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	a.ledger.release(key(ctx, "reserve/"+req.Order.ID))
	return nil
}

// Pack is what the packing child does. The key comes from the child, not from
// this activity's own id.
func (a *Activities) Pack(ctx context.Context, note Note) (string, error) {
	a.ledger.noteKey("pack", note.Key)

	id, fresh := a.ledger.claim(note.Key, "pk-"+note.Order)
	if !fresh {
		return id, nil
	}
	logf(ctx, "packed %s under key %s", note.Order, note.Key)
	return id, nil
}

// Unpack undoes Pack, and succeeds when there is nothing packed.
//
// It is the compensation of a step whose forward half was a child workflow, and
// it still reads the same key -- here with IdempotencyKey, because this is an
// activity, where the child used IdempotencyKeyOf.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k := key(ctx, "pack/"+req.Order.ID)
	a.ledger.noteKey("unpack", k)

	if !a.ledger.release(k) {
		logf(ctx, "unpack: nothing packed under key %s, nothing to do", k)
		return nil
	}
	logf(ctx, "unpacked %s under key %s", req.Order.ID, k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	k := key(ctx, "ship/"+req.Order.ID)
	id, fresh := a.ledger.claim(k, "shp-"+req.Order.ID)
	if !fresh {
		return id, nil
	}
	if req.Order.FailAt == "ship" {
		a.ledger.release(k)
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.ledger.release(key(ctx, "ship/"+req.Order.ID))
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }()
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}
