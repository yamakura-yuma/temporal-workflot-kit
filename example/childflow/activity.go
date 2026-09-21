package childflow

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
// The warehouse, the packing station and the carrier are those services, faked
// in this process. Read them as systems across a network boundary; their
// bodies are not the point.
//
// What this example adds is where the key comes from. The packing child
// workflow reads it with saga.IdempotencyKeyOf and passes it down in a Note, so
// Pack hands the packing station the key the child was given rather than one of
// its own.
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

// Services is what a worker is given: the fake systems the activities call.
type Services struct {
	Warehouse *warehouse
	Packer    *packer
	Carrier   *carrier

	// keys belongs to the specifications, not to any of the systems; see the
	// bottom of this file.
	mu   sync.Mutex
	keys map[string]string
}

// NewServices returns the three systems, each empty.
func NewServices() *Services {
	return &Services{
		Warehouse: &warehouse{holds: map[string]string{}},
		Packer:    &packer{parcels: map[string]string{}},
		Carrier:   &carrier{bookings: map[string]string{}},
		keys:      map[string]string{},
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

// packer is the packing station.
//
// It guarantees this much and no more: a second Pack under a key it has
// already seen packs nothing further and returns the id of the first parcel.
type packer struct {
	mu      sync.Mutex
	parcels map[string]string // idempotency key -> parcel id
}

// Pack packs an order into a parcel and returns the parcel id.
func (p *packer) Pack(key, order string) string {
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

// Activities backs both the saga's activity steps and the packing children.
type Activities struct {
	warehouse *warehouse
	packer    *packer
	carrier   *carrier

	// spec is the observation window the specifications read; see the bottom
	// of this file.
	spec *Services
}

// NewActivities returns activities that call the given services.
func NewActivities(s *Services) *Activities {
	return &Activities{warehouse: s.Warehouse, packer: s.Packer, carrier: s.Carrier, spec: s}
}

// Reserve holds stock.
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

// Pack is what the packing child does. The key comes from the child, not from
// this activity's own id, so it is the one argument that is not read out of
// the context here.
func (a *Activities) Pack(ctx context.Context, note Note) (string, error) {
	a.spec.noteKey("pack", note.Key)

	return a.packer.Pack(note.Key, note.Order), nil
}

// Unpack undoes Pack, and succeeds when there is nothing packed.
//
// It is the compensation of a step whose forward half was a child workflow, and
// it still reads the same key -- here with IdempotencyKey, because this is an
// activity, where the child used IdempotencyKeyOf.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.spec.noteKey("unpack", k)

	a.packer.Unpack(k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.carrier.Book(k, req.Order.ID), nil
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
// the workflow. Real systems have no counterpart: no business code records
// which idempotency key a packing child read, and Pack and Unpack above call
// noteKey for this reason only.

func (s *Services) noteKey(which, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[which] = key
}

// KeySeenBy reports the idempotency key the named packing child read. A
// specification uses it to check both halves saw the same one.
func (s *Services) KeySeenBy(which string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys[which]
}

// Held reports whether any of the services still holds a record for a step of a
// given workflow run.
func (s *Services) Held(runID, step string) bool {
	key := runID + "/" + step
	return s.Warehouse.has(key) || s.Packer.has(key) || s.Carrier.has(key)
}

func (w *warehouse) has(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.holds[key]
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
