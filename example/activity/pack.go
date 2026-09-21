package activity

import (
	"context"
	"fmt"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// PackReq is the input of the pack step.
type PackReq struct {
	Order Order `json:"order"`

	// Key carries the saga's idempotency key across a child workflow boundary.
	// A saga that runs Pack as an ordinary activity step leaves it empty, and
	// Pack reads the key from its own context instead. example/workflow/
	// childflow runs the forward half in a child workflow, which reads the key
	// with saga.IdempotencyKeyOf and passes it down here, so that both halves
	// of the step act under the same key.
	Key string `json:"key,omitempty"`
}

// Pack makes the order ready to hand to a carrier.
func (a *Activities) Pack(ctx context.Context, req PackReq) (string, error) {
	if req.Order.FailAt == "pack" {
		return "", fmt.Errorf("pack: nothing to pack for %s", req.Order.ID)
	}

	key := req.Key
	if key == "" {
		key, _ = saga.IdempotencyKey(ctx)
	}
	a.keys.Store("pack", key)
	a.done.Store(key, struct{}{})

	return "pk-" + req.Order.ID, nil
}

// Unpack reverses Pack, and succeeds when there is nothing packed.
//
// It is an activity even when the forward half was a child workflow, so it
// reads the key with saga.IdempotencyKey where the child used
// saga.IdempotencyKeyOf. Both get the same value.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k, _ := saga.IdempotencyKey(ctx)
	a.keys.Store("unpack", k)

	if req.Order.FailUndo == "pack" {
		return fmt.Errorf("unpack: packing station unreachable for %s", req.Order.ID)
	}
	a.done.Delete(k)
	return nil
}
