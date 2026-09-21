package state

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// The activities are a copy of the order example's, widened to four steps and
// four wide requests. They are not what this example is about -- the two shapes
// of the workflow are -- and the width is the point: restating this many fields
// at every step is what the state shape in workflow.go is answering.

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

// Activities is the worked example's activity set.
//
// done stands in for the downstream's record of what it has already done. A
// real one is a UNIQUE column on the row the activity writes, so that writing
// the row claims the key; see docs/activity-contract.md. A map in this process
// cannot show that, so it does not try.
type Activities struct {
	done sync.Map // idempotency key -> struct{}
}

// NewActivities returns activities with nothing done yet.
func NewActivities() *Activities { return &Activities{} }

// Reserve holds stock.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("reserve: no stock for %s", req.SKU)
	}
	a.mark(ctx)
	return "res-" + req.Order, nil
}

// Unreserve releases the stock Reserve held, and succeeds when none was held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	a.unmark(ctx)
	return nil
}

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("charge: card declined for %s", req.Customer)
	}
	a.mark(ctx)
	return "chg-" + req.Order, nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.unmark(ctx)
	return nil
}

// Pack makes the order ready to hand to a carrier.
func (a *Activities) Pack(ctx context.Context, req PackReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("pack: nothing to pack for %s", req.Order)
	}
	a.mark(ctx)
	return "pk-" + req.Order, nil
}

// Unpack reverses Pack, and succeeds when nothing was packed.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	a.unmark(ctx)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("ship: no carrier for %s", req.Address)
	}
	a.mark(ctx)
	return "shp-" + req.Order, nil
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
