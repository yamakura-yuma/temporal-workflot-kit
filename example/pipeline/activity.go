package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// The activities below are one line of work each, for the reason the order
// example spells out at the top of its activity.go: an idempotency key is
// "enforced by the service you are calling from your Activity, not by the
// Activity itself" (https://docs.temporal.io/activity-definition), so an
// activity's job is to read the key and hand it across the boundary with the
// business arguments.
//
// The warehouse, the payment gateway and the carrier are those services, faked
// in this process. Read them as systems across a network boundary; their
// bodies are not the point.
//
// What this example adds is what else the activity hands over: each service is
// told the id the service before it returned.
//
// Be exact about what they guarantee: a call repeated under an idempotency key
// they have already seen writes no second record and returns the first id.
// That is not the same as the work happening exactly once. Here the two
// coincide, because writing the record is the work. A service that called
// something outside itself and then recorded the result could die in between
// and make that outside call twice -- the gap docs/activity-contract.md
// describes, and the reason the first of the three cases the order example
// lists, one statement against your own database, has none.
//
// Their bodies ignore most of the business arguments they are handed. A real
// one would not; the arguments are here because handing them over is the
// activity's job.

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

// Services is what a worker is given: the fake systems the activities call.
type Services struct {
	Warehouse *warehouse
	Payments  *payments
	Carrier   *carrier

	// saw belongs to the specifications, not to any of the systems; see the
	// bottom of this file.
	mu  sync.Mutex
	saw map[string]string
}

// NewServices returns the three systems, each empty.
func NewServices() *Services {
	return &Services{
		Warehouse: &warehouse{holds: map[string]string{}},
		Payments:  &payments{charges: map[string]string{}},
		Carrier:   &carrier{bookings: map[string]string{}},
		saw:       map[string]string{},
	}
}

// warehouse is the stock system.
//
// It guarantees this much and no more: a second Hold under a key it has
// already seen holds no more stock and returns the id of the first
// reservation.
type warehouse struct {
	mu    sync.Mutex
	holds map[string]string // idempotency key -> reservation id
}

// Hold reserves stock for an order and returns the reservation id.
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

// payments is the payment gateway. It charges against a reservation, so the
// charge it returns names the reservation it was made for.
//
// It guarantees this much and no more: a second Charge under a key it has
// already seen takes no more money and returns the id of the first charge.
type payments struct {
	mu      sync.Mutex
	charges map[string]string // idempotency key -> charge id
}

// Charge takes money against a reservation and returns the charge id.
func (p *payments) Charge(key, reservation string, amount int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.charges[key]; ok {
		return id
	}
	id := "chg-" + reservation
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

// carrier is the shipping company. It ships against a charge.
//
// It guarantees this much and no more: a second Book under a key it has
// already seen books nothing further and returns the id of the first shipment.
type carrier struct {
	mu       sync.Mutex
	bookings map[string]string // idempotency key -> shipment id
}

// Book books a shipment against a charge and returns the shipment id.
func (c *carrier) Book(key, charge string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id, ok := c.bookings[key]; ok {
		return id
	}
	id := "shp-" + charge
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

// Activities is the worked example. NewActivities takes the services so a test
// can look at them.
type Activities struct {
	warehouse *warehouse
	payments  *payments
	carrier   *carrier

	// spec is the observation window the specifications read; see the bottom
	// of this file.
	spec *Services
}

// NewActivities returns activities that call the given services.
func NewActivities(s *Services) *Activities {
	return &Activities{warehouse: s.Warehouse, payments: s.Payments, carrier: s.Carrier, spec: s}
}

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.warehouse.Hold(k, req.Order.ID, req.Order.SKU), nil
}

// Unreserve releases the stock Reserve held, and succeeds when none was held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.warehouse.Release(k)
	return nil
}

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.payments.Charge(k, req.Reservation, req.Order.Amount), nil
}

// Refund reverses Charge. It knows which reservation the charge belonged to
// because the saga handed the compensation the same input the step got.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.spec.record("charge", req.Reservation)

	k, _ := saga.IdempotencyKey(ctx)
	a.payments.Refund(k)
	return nil
}

// Ship books a shipment against the charge.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.carrier.Book(k, req.Charge), nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.spec.record("ship", req.Charge)

	k, _ := saga.IdempotencyKey(ctx)
	a.carrier.Cancel(k)
	return nil
}

// --- for the specifications --------------------------------------------------
//
// What follows is here so docs/specs/ can look into the services from outside
// the workflow. Real systems have no counterpart: no business code asks which
// upstream id a compensation was handed, and the compensations above call
// record for this reason only.

// record notes the upstream id a compensation was handed.
func (s *Services) record(step, upstream string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saw[step] = upstream
}

// Upstream reports the id the named step's compensation received. A
// specification uses it to check that the pipeline really did feed it through.
func (s *Services) Upstream(step string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saw[step]
}

// Held reports whether any of the services still holds a record for a step of a
// given workflow run.
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
