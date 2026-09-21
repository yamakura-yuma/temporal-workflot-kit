package state

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

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
	ShipReq struct {
		Order   string `json:"order"`
		Address string `json:"address"`
		Charge  string `json:"charge"`
		Fail    bool   `json:"fail,omitempty"`
	}
)

// Ledger is the demo store.
type Ledger struct {
	mu     sync.Mutex
	claims map[string]string
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger { return &Ledger{claims: map[string]string{}} }

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

// Held reports whether a step still holds its claim.
func (l *Ledger) Held(runID, step string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.claims[runID+"/"+step]
	return ok
}

// Activities is the worked example's activity set.
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
	k := key(ctx, "reserve/"+req.Order)
	id, fresh := a.ledger.claim(k, "res-"+req.Order)
	if !fresh {
		return id, nil
	}
	if req.Fail {
		a.ledger.release(k)
		return "", fmt.Errorf("reserve: no stock for %s", req.SKU)
	}
	logf(ctx, "reserved %d x %s", req.Quantity, req.SKU)
	return id, nil
}

// Unreserve releases the stock Reserve held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	a.ledger.release(key(ctx, "reserve/"+req.Order))
	return nil
}

// Charge takes payment.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	k := key(ctx, "charge/"+req.Order)
	id, fresh := a.ledger.claim(k, "chg-"+req.Order)
	if !fresh {
		return id, nil
	}
	if req.Fail {
		a.ledger.release(k)
		return "", fmt.Errorf("charge: card declined for %s", req.Customer)
	}
	logf(ctx, "charged %d %s to %s", req.Amount, req.Currency, req.Customer)
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.ledger.release(key(ctx, "charge/"+req.Order))
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	k := key(ctx, "ship/"+req.Order)
	id, fresh := a.ledger.claim(k, "shp-"+req.Order)
	if !fresh {
		return id, nil
	}
	if req.Fail {
		a.ledger.release(k)
		return "", fmt.Errorf("ship: no carrier for %s", req.Address)
	}
	logf(ctx, "booked a shipment to %s", req.Address)
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.ledger.release(key(ctx, "ship/"+req.Order))
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }()
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}
