package activity

import (
	"context"
	"fmt"
)

// PackReq is the input of the pack step.
type PackReq struct {
	Order Order `json:"order"`

	// Key is the saga's idempotency key, when the workflow chose to send one.
	// example/workflow/childflow derives it once and puts it in the request for
	// both halves of its packing step, so that the child workflow that packs
	// and the activity that unpacks act under the same key.
	//
	// The other sagas leave it empty: example/workflow/state runs Pack as an
	// ordinary step and sends no key at all. Nothing here reads it either way.
	// A real Pack would hand it to the packing system; this one has nowhere to
	// hand it, so it only has to arrive -- docs/specs/childflow.feature checks
	// that it did, out of the history.
	Key string `json:"key,omitempty"`

	// Pack is what Pack returned, filled in when Unpack is undoing a parcel
	// that was actually packed.
	Pack string `json:"pack,omitempty"`
}

// Pack makes the order ready to hand to a carrier.
func (a *Activities) Pack(ctx context.Context, req PackReq) (string, error) {
	if req.Order.FailAt == "pack" {
		return "", fmt.Errorf("pack: nothing to pack for %s", req.Order.ID)
	}
	return "pk-" + req.Order.ID, nil
}

// Unpack reverses Pack, and succeeds when there is nothing packed.
//
// It is an activity even when the forward half was a child workflow. Both get
// the same key because the workflow put the same string in both requests, not
// because anything in the saga library arranged it.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	if req.Order.FailUndo == "pack" {
		return fmt.Errorf("unpack: packing station unreachable for %s", req.Order.ID)
	}
	return nil
}
