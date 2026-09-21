package order

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// Why the activities below are one line of work each.
//
// Temporal's own documentation puts the rule like this: idempotency keys "are
// enforced by the service you are calling from your Activity, not by the
// Activity itself" (https://docs.temporal.io/activity-definition).
//
// So an activity has one job: read the key, and hand it across the boundary
// with the business arguments. That is what every activity below does.
//
// What happens to the key on the other side is the service's business, and it
// has three shapes. If the service is your own database, the key is a UNIQUE
// column and one INSERT ... ON CONFLICT (idem_key) DO NOTHING RETURNING id is
// the work and the claim at once. If it is an external API with an
// idempotency-key header, you pass the key and it deduplicates. Only a
// downstream with neither leaves the activity doing the careful version --
// claim the key, call, release the key if the call failed -- and that is the
// case to design your way out of, not the one to copy.
//
// The warehouse, the payment gateway and the carrier below are those services,
// faked in this process. Read them as systems on the other side of a network
// boundary: what matters is what the activities hand them, not how they are
// implemented, so their bodies can be skipped.
//
// Be exact about what they guarantee: a call repeated under an idempotency key
// they have already seen writes no second record and returns the first id.
// That is not the same as the work happening exactly once. Here the two
// coincide, because writing the record is the work. A service that called
// something outside itself and then recorded the result could die in between
// and make that outside call twice -- the gap docs/activity-contract.md
// describes, and the reason the first case above, one statement against your
// own database, has none.
//
// Their bodies ignore most of the business arguments they are handed. A real
// one would not; the arguments are here because handing them over is the
// activity's job.

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

// Services is what a worker is given: the fake systems the activities call.
type Services struct {
	Warehouse *warehouse
	Payments  *payments
	Carrier   *carrier
}

// NewServices returns the three systems, each empty.
func NewServices() *Services {
	return &Services{
		Warehouse: &warehouse{holds: map[string]string{}},
		Payments:  &payments{charges: map[string]string{}},
		Carrier:   &carrier{bookings: map[string]string{}},
	}
}

// warehouse is the stock system. It runs in this process, but it is here to be
// read as a system across a network boundary; the body is not the point.
//
// It guarantees this much and no more: a second Hold under a key it has
// already seen holds no more stock and returns the id of the first
// reservation.
type warehouse struct {
	mu    sync.Mutex
	holds map[string]string // idempotency key -> reservation id
}

// Hold reserves stock for an order and returns the reservation id. Called
// twice with the same key it holds nothing the second time and returns the
// first reservation.
func (w *warehouse) Hold(key, order, sku string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if id, ok := w.holds[key]; ok {
		return id
	}
	id := "res-" + order
	w.holds[key] = id
	return id
}

// Release frees the stock held under key, reporting whether anything was held.
func (w *warehouse) Release(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.holds[key]; !ok {
		return false
	}
	delete(w.holds, key)
	return true
}

// payments is the payment gateway.
//
// It guarantees this much and no more: a second Charge under a key it has
// already seen takes no more money and returns the id of the first charge.
type payments struct {
	mu      sync.Mutex
	charges map[string]string // idempotency key -> charge id
}

// Charge takes money for an order and returns the charge id. The key is what
// stops a retried activity from charging the card twice.
func (p *payments) Charge(key, order string, amount int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.charges[key]; ok {
		return id
	}
	id := "chg-" + order
	p.charges[key] = id
	return id
}

// Refund reverses the charge made under key, reporting whether there was one.
func (p *payments) Refund(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.charges[key]; !ok {
		return false
	}
	delete(p.charges, key)
	return true
}

// carrier is the shipping company.
//
// It guarantees this much and no more: a second Book under a key it has
// already seen books nothing further and returns the id of the first shipment.
type carrier struct {
	mu       sync.Mutex
	bookings map[string]string // idempotency key -> shipment id
}

// Book books a shipment for an order and returns the shipment id.
func (c *carrier) Book(key, order string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id, ok := c.bookings[key]; ok {
		return id
	}
	id := "shp-" + order
	c.bookings[key] = id
	return id
}

// Cancel cancels the shipment booked under key, reporting whether there was
// one.
func (c *carrier) Cancel(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.bookings[key]; !ok {
		return false
	}
	delete(c.bookings, key)
	return true
}

// Activities is the worked example of the contract the saga package puts on
// activities: hand the idempotency key to the service that does the work, and
// succeed when a compensation finds nothing to undo.
type Activities struct {
	warehouse *warehouse
	payments  *payments
	carrier   *carrier
}

// NewActivities returns activities that call the given services.
func NewActivities(s *Services) *Activities {
	return &Activities{warehouse: s.Warehouse, payments: s.Payments, carrier: s.Carrier}
}

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.warehouse.Hold(k, req.Order.ID, req.Order.SKU), nil
}

// Unreserve releases stock held by Reserve.
//
// Release returning false -- nothing was held -- is not an error. The saga
// registers a compensation before its step runs, so a compensation can be
// asked to undo something that never happened.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	if req.Order.FailUndo == "reserve" {
		return fmt.Errorf("unreserve: warehouse unreachable for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	a.warehouse.Release(k)
	return nil
}

// Charge takes payment for the order.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.payments.Charge(k, req.Order.ID, req.Order.Amount), nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	if req.Order.FailUndo == "charge" {
		return fmt.Errorf("refund: gateway unreachable for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	a.payments.Refund(k)
	return nil
}

// Ship books a shipment for the order.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.carrier.Book(k, req.Order.ID), nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	if req.Order.FailUndo == "ship" {
		return fmt.Errorf("cancel-shipment: carrier unreachable for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	a.carrier.Cancel(k)
	return nil
}

// --- for the specifications --------------------------------------------------
//
// What follows is here so docs/specs/ can look into the services from outside
// the workflow. Real systems have no counterpart: nothing in business code asks
// whether a given step of a given run still has its record.

// Held reports whether any of the services still holds a record for a step of a
// given workflow run. A specification uses it to check that a rollback actually
// undid everything.
func (s *Services) Held(runID, step string) bool {
	key := runID + "/" + step
	return s.Warehouse.has(key) || s.Payments.has(key) || s.Carrier.has(key)
}

func (w *warehouse) has(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.holds[key]
	return ok
}

func (p *payments) has(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.charges[key]
	return ok
}

func (c *carrier) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.bookings[key]
	return ok
}
