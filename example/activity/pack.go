package activity

import (
	"context"
	"fmt"
)

// PackReq is the input of the pack step.
type PackReq struct {
	Order Order `json:"order"`

	// Key carries the saga's idempotency key across a child workflow boundary.
	// A saga that runs Pack as an ordinary activity step leaves it empty and
	// the key is on the activity's own context, where saga.IdempotencyKey finds
	// it. example/workflow/childflow runs the forward half in a child workflow,
	// which reads the key with saga.IdempotencyKeyOf and passes it down here,
	// so that both halves of the step act under the same key.
	//
	// A real Pack would hand whichever of the two it has to the packing system.
	// This one has nowhere to hand it, so it only has to arrive --
	// docs/specs/childflow.feature checks that it did, out of the history.
	Key string `json:"key,omitempty"`
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
// It is an activity even when the forward half was a child workflow, so a real
// one would read the key with saga.IdempotencyKey where the child used
// saga.IdempotencyKeyOf. Both get the same value: the key rides in the child's
// WorkflowID and in this activity's ActivityID, and the ":undo" suffix is
// stripped off before either is handed back.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	if req.Order.FailUndo == "pack" {
		return fmt.Errorf("unpack: packing station unreachable for %s", req.Order.ID)
	}
	return nil
}
