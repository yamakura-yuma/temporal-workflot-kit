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
	return "res-" + req.Order.ID, nil
}

// Unreserve releases stock held by Reserve. It succeeds when there is nothing
// to release: a saga registers a compensation before its step runs, so this can
// be asked to undo something that never happened. A real one gets that for free
// from a DELETE that matches no row.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	if req.Order.FailUndo == "reserve" {
		return fmt.Errorf("unreserve: warehouse unreachable for %s", req.Order.ID)
	}
	return nil
}
