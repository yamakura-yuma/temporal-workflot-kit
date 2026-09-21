package activity

import (
	"context"
	"fmt"
)

// ChargeReq is the input of the charge step.
type ChargeReq struct {
	Order Order `json:"order"`

	// Reservation is the id the reserve step returned, for a saga that feeds it
	// through. example/workflow/order leaves it empty and nothing breaks;
	// example/workflow/pipeline fills it, and that is the whole of what
	// pipeline is about.
	Reservation string `json:"reservation,omitempty"`
}

// Charge takes payment for the order.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	if req.Order.FailAt == "charge" {
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	return "chg-" + req.Order.ID, nil
}

// Refund reverses Charge, and succeeds when there was no charge to reverse.
//
// It is handed the same input the forward step got, so it knows which
// reservation the charge belonged to without looking it up. A compensation
// cannot see its own step's output -- it was registered before the step ran --
// but everything upstream is right here, in Reservation.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	if req.Order.FailUndo == "charge" {
		return fmt.Errorf("refund: gateway unreachable for %s", req.Order.ID)
	}
	return nil
}
