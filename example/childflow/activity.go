package childflow

import (
	"context"
	"fmt"
	"sync"

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

// Activities backs both the saga's activity steps and the packing children.
//
// done stands in for the downstream's record of what it has already done. A
// real one is a UNIQUE column on the row the activity writes, so that writing
// the row claims the key; see docs/activity-contract.md. A map in this process
// cannot show that, so it does not try.
type Activities struct {
	done sync.Map // idempotency key -> struct{}

	// keys is for docs/specs/; see KeySeenBy at the bottom of this file.
	keys sync.Map // "pack" / "unpack" -> the key that child read
}

// NewActivities returns activities with nothing done yet.
func NewActivities() *Activities { return &Activities{} }

// Reserve holds stock.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	a.mark(ctx)
	return "res-" + req.Order.ID, nil
}

// Unreserve releases the stock Reserve held, and succeeds when none was held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	a.unmark(ctx)
	return nil
}

// Pack is what the packing child does. The key comes from the child, not from
// this activity's own context, which is the one thing this example is about.
func (a *Activities) Pack(ctx context.Context, note Note) (string, error) {
	a.keys.Store("pack", note.Key)
	a.done.Store(note.Key, struct{}{})
	return "pk-" + note.Order, nil
}

// Unpack undoes Pack, and succeeds when there is nothing packed.
//
// It is the compensation of a step whose forward half was a child workflow, and
// it still reads the same key -- here with IdempotencyKey, because this is an
// activity, where the child used IdempotencyKeyOf.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.keys.Store("unpack", k)
	a.done.Delete(k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	a.mark(ctx)
	return "shp-" + req.Order.ID, nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.unmark(ctx)
	return nil
}

func (a *Activities) mark(ctx context.Context) {
	k, _ := saga.IdempotencyKey(ctx)
	a.done.Store(k, struct{}{})
}

func (a *Activities) unmark(ctx context.Context) {
	k, _ := saga.IdempotencyKey(ctx)
	a.done.Delete(k)
}

// Held reports whether a step of a given workflow run is still done. It is here
// for docs/specs/; business code has no use for it.
func Held(a *Activities, runID, step string) bool {
	_, ok := a.done.Load(runID + "/" + step)
	return ok
}

// KeySeenBy reports the idempotency key the named packing child read, which is
// what a specification checks to see that both halves saw the same one. Pack
// and Unpack record it for this reason only.
func KeySeenBy(a *Activities, which string) string {
	v, _ := a.keys.Load(which)
	s, _ := v.(string)
	return s
}
