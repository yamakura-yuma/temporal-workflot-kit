package external

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// A copy of the order example's payment gateway, and the only service this
// example calls. It is not what the example is about -- the hold step above it
// is -- and it is here only so the saga has something to fail at after the hold
// has been sent.
//
// The shape is the one the order example explains at the top of its
// activity.go: an idempotency key is "enforced by the service you are calling
// from your Activity, not by the Activity itself"
// (https://docs.temporal.io/activity-definition), so the activity reads the key
// and hands it to the gateway, and that is all it does.
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

// ChargeReq is the input of the charge step.
type ChargeReq struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
	Fail   bool   `json:"fail,omitempty"`
}

// Services is what a worker is given: the fake systems the activities call.
// There is only one of them here.
type Services struct {
	Payments *payments
}

// NewServices returns the payment gateway, empty.
func NewServices() *Services {
	return &Services{Payments: &payments{charges: map[string]string{}}}
}

// payments is the payment gateway. It runs in this process, but it is here to
// be read as a system across a network boundary; the body is not the point.
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

// Activities is the saga's activity set.
type Activities struct {
	payments *payments
}

// NewActivities returns activities that call the given services.
func NewActivities(s *Services) *Activities {
	return &Activities{payments: s.Payments}
}

// Charge takes payment.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("charge: card declined for %s", req.Order)
	}
	k, _ := saga.IdempotencyKey(ctx)
	return a.payments.Charge(k, req.Order, req.Amount), nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.payments.Refund(k)
	return nil
}
