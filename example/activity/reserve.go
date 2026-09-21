package activity

import (
	"context"
	"fmt"
)

// ReserveReq is the input of the reserve step.
type ReserveReq struct {
	Order Order `json:"order"`
}

// Reserve holds stock for the order.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	if req.Order.FailAt == "reserve" {
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	a.mark(ctx)
	return "res-" + req.Order.ID, nil
}

// Unreserve releases stock held by Reserve. It succeeds when there is nothing
// to release, which is what lets a saga register it before Reserve runs.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	if req.Order.FailUndo == "reserve" {
		return fmt.Errorf("unreserve: warehouse unreachable for %s", req.Order.ID)
	}
	a.unmark(ctx)
	return nil
}
