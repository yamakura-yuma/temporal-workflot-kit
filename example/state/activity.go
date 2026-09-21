package state

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// A copy of the order example's services, widened to four steps. They are not
// what this example is about -- the two shapes of the workflow are -- and they
// are here only so there is something for those shapes to sequence.
//
// The shape is the one the order example explains at the top of its
// activity.go: an idempotency key is "enforced by the service you are calling
// from your Activity, not by the Activity itself"
// (https://docs.temporal.io/activity-definition), so an activity reads the key
// and hands it across the boundary with the business arguments, and that is all
// it does. The systems below are faked in this process; their bodies are not
// the point.
//
// The requests are wide on purpose. Restating this many fields at every step is
// what the state shape in workflow.go is answering.
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

// Services is what a worker is given: the fake systems the activities call.
type Services struct {
	Warehouse *warehouse
	Payments  *payments
	Packer    *packer
	Carrier   *carrier
}

// NewServices returns the four systems, each empty.
func NewServices() *Services {
	return &Services{
		Warehouse: &warehouse{holds: map[string]string{}},
		Payments:  &payments{charges: map[string]string{}},
		Packer:    &packer{parcels: map[string]string{}},
		Carrier:   &carrier{bookings: map[string]string{}},
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

// Hold reserves stock and returns the reservation id.
func (w *warehouse) Hold(key, order, sku string, quantity int) string {
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

// Charge takes money against a reservation and returns the charge id.
func (p *payments) Charge(key, order, customer string, amount int, currency string) string {
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

// packer is the packing station.
//
// It guarantees this much and no more: a second Pack under a key it has
// already seen packs nothing further and returns the id of the first parcel.
type packer struct {
	mu      sync.Mutex
	parcels map[string]string // idempotency key -> parcel id
}

// Pack packs an order into a parcel and returns the parcel id.
func (p *packer) Pack(key, order, sku string, quantity int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.parcels[key]; ok {
		return id
	}
	id := "pk-" + order
	p.parcels[key] = id
	return id
}

// Unpack undoes the packing done under key, reporting whether there was any.
func (p *packer) Unpack(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.parcels[key]; !ok {
		return false
	}
	delete(p.parcels, key)
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

// Book books a shipment to an address and returns the shipment id.
func (c *carrier) Book(key, order, address string) string {
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

// Activities is the worked example's activity set.
type Activities struct {
	warehouse *warehouse
	payments  *payments
	packer    *packer
	carrier   *carrier
}

// NewActivities returns activities that call the given services.
func NewActivities(s *Services) *Activities {
	return &Activities{warehouse: s.Warehouse, payments: s.Payments, packer: s.Packer, carrier: s.Carrier}
}

// Reserve holds stock.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("reserve: no stock for %s", req.SKU)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.warehouse.Hold(k, req.Order, req.SKU, req.Quantity), nil
}

// Unreserve releases the stock Reserve held, and succeeds when none was held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.warehouse.Release(k)
	return nil
}

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("charge: card declined for %s", req.Customer)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.payments.Charge(k, req.Order, req.Customer, req.Amount, req.Currency), nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.payments.Refund(k)
	return nil
}

// Pack makes the order ready to hand to a carrier.
func (a *Activities) Pack(ctx context.Context, req PackReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("pack: nothing to pack for %s", req.Order)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.packer.Pack(k, req.Order, req.SKU, req.Quantity), nil
}

// Unpack reverses Pack, and succeeds when nothing was packed.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.packer.Unpack(k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("ship: no carrier for %s", req.Address)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.carrier.Book(k, req.Order, req.Address), nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
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
// given workflow run.
func (s *Services) Held(runID, step string) bool {
	key := runID + "/" + step
	return s.Warehouse.has(key) || s.Payments.has(key) ||
		s.Packer.has(key) || s.Carrier.has(key)
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

func (p *packer) has(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.parcels[key]
	return ok
}

func (c *carrier) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.bookings[key]
	return ok
}
