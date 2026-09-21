package order

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
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

// Activities is the worked example of the contract the saga package puts on
// activities: hand the idempotency key to the service you call, and succeed
// when a compensation finds nothing to undo.
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

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	a.mark(ctx)
	return "res-" + req.Order.ID, nil
}

// Unreserve releases stock held by Reserve. It succeeds when there is nothing
// to release, which is what lets the saga register it before Reserve runs.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	if req.Order.FailUndo == "reserve" {
		return fmt.Errorf("unreserve: warehouse unreachable for %s", req.Order.ID)
	}
	a.unmark(ctx)
	return nil
}

// Charge takes payment for the order.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	a.mark(ctx)
	return "chg-" + req.Order.ID, nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	if req.Order.FailUndo == "charge" {
		return fmt.Errorf("refund: gateway unreachable for %s", req.Order.ID)
	}
	a.unmark(ctx)
	return nil
}

// Ship books a shipment for the order.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	a.mark(ctx)
	return "shp-" + req.Order.ID, nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	if req.Order.FailUndo == "ship" {
		return fmt.Errorf("cancel-shipment: carrier unreachable for %s", req.Order.ID)
	}
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
// for docs/specs/ to check that a rollback undid everything; business code has
// no use for it.
func Held(a *Activities, runID, step string) bool {
	_, ok := a.done.Load(runID + "/" + step)
	return ok
}
