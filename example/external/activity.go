package external

import (
	"context"
	"fmt"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// The charge step is a copy of the order example's, and not what this example
// is about; it is here so the saga has something to fail at after the hold has
// been sent.

// ChargeReq is the input of the charge step.
type ChargeReq struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
	Fail   bool   `json:"fail,omitempty"`
}

// Activities is the saga's activity set.
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

// Charge takes payment.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Fail {
		return "", fmt.Errorf("charge: card declined for %s", req.Order)
	}
	k, _ := saga.IdempotencyKey(ctx)
	a.done.Store(k, struct{}{})
	return "chg-" + req.Order, nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.done.Delete(k)
	return nil
}
