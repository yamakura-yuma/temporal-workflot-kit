package external

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// ChargeReq is the input of the charge step.
type ChargeReq struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
	Fail   bool   `json:"fail,omitempty"`
}

// Ledger is the demo store behind the charge step.
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

// Activities is the saga's activity set.
type Activities struct{ ledger *Ledger }

// NewActivities returns activities backed by the given ledger.
func NewActivities(ledger *Ledger) *Activities { return &Activities{ledger: ledger} }

func key(ctx context.Context, fallback string) string {
	if k, ok := saga.IdempotencyKey(ctx); ok {
		return k
	}
	return fallback
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
		return "", fmt.Errorf("charge: card declined for %s", req.Order)
	}
	return id, nil
}

// Refund reverses Charge.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.ledger.release(key(ctx, "charge/"+req.Order))
	return nil
}
