package activity

import (
	"context"
	"fmt"
)

// ShipReq is the input of the ship step.
type ShipReq struct {
	Order Order `json:"order"`

	// Charge is the id the charge step returned, for a saga that feeds it
	// through; see example/workflow/pipeline.
	Charge string `json:"charge,omitempty"`
	// Pack is the id the pack step returned, for a saga that packs first; see
	// example/workflow/childflow.
	Pack string `json:"pack,omitempty"`
	// ApprovedBy names the reviewer, for a saga that waits for one; see
	// example/workflow/state.
	ApprovedBy string `json:"approved_by,omitempty"`
}

// Ship books a shipment for the order.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	if req.Order.FailAt == "ship" {
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	return "shp-" + req.Order.ID, nil
}

// CancelShipment reverses Ship, and succeeds when nothing was booked.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	if req.Order.FailUndo == "ship" {
		return fmt.Errorf("cancel-shipment: carrier unreachable for %s", req.Order.ID)
	}
	return nil
}
