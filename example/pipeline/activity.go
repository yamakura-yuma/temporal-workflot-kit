package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

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

// Activities is the worked example. Each step's id is built from the id of the
// step before it, which is what makes the compensations worth watching.
//
// done stands in for the downstream's record of what it has already done. A
// real one is a UNIQUE column on the row the activity writes, so that writing
// the row claims the key; see docs/activity-contract.md. A map in this process
// cannot show that, so it does not try.
type Activities struct {
	done sync.Map // idempotency key -> struct{}

	// saw is for docs/specs/; see Upstream at the bottom of this file.
	saw sync.Map // step name -> the upstream id its compensation was handed
}

// NewActivities returns activities with nothing done yet.
func NewActivities() *Activities { return &Activities{} }

// Reserve holds stock for the order.
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

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	a.mark(ctx)
	return "chg-" + req.Reservation, nil
}

// Refund reverses Charge. It knows which reservation the charge belonged to
// because the saga handed the compensation the same input the step got.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.saw.Store("charge", req.Reservation)
	a.unmark(ctx)
	return nil
}

// Ship books a shipment against the charge.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	a.mark(ctx)
	return "shp-" + req.Charge, nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.saw.Store("ship", req.Charge)
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

// Upstream reports the id the named step's compensation received, which is what
// a specification checks to see that the pipeline really did feed it through.
// The compensations above record it for this reason only.
func Upstream(a *Activities, step string) string {
	v, _ := a.saw.Load(step)
	s, _ := v.(string)
	return s
}
