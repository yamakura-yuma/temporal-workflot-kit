// Package activity is the activities the example workflows call.
//
// There is one set of them, not one per workflow, and that is the point of the
// directory: an activity belongs to a worker rather than to a workflow, and the
// same Reserve is called by example/workflow/order, /pipeline, /childflow and
// /state. example/workflow/approval adds no activity of its own at all -- it is
// only about the wait between two of these.
//
// One activity per file, each holding its input type, its forward and its
// compensation.
package activity

import (
	"context"
	"sync"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// Order is the order as an activity sees it. A workflow whose input needs
// nothing more takes this as its own input; the ones that need more of their
// own keep their own type and build this out of it, which is what
// example/workflow/state is about.
//
// FailAt and FailUndo are how a specification forces a particular failure. They
// name a step rather than a workflow, so the same order can be pushed through
// any of the sagas.
type Order struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity,omitempty"`
	Amount   int    `json:"amount,omitempty"`

	// FailAt names the step whose forward activity should fail.
	FailAt string `json:"fail_at,omitempty"`
	// FailUndo names the step whose compensation should fail.
	FailUndo string `json:"fail_undo,omitempty"`
}

// Activities is the worked example of the contract the saga package puts on
// activities: hand the idempotency key to the service you call, and succeed
// when a compensation finds nothing to undo.
//
// done stands in for the downstream's record of what it has already done. A
// real one is a UNIQUE column on the row the activity writes, so that writing
// the row claims the key; see docs/activity-contract.md. A map in this process
// cannot show that, so it does not try.
type Activities struct {
	done sync.Map // idempotency key -> struct{}

	// saw and keys are for docs/specs/; see the bottom of this file.
	saw  sync.Map
	keys sync.Map
}

// NewActivities returns activities with nothing done yet. One set is shared by
// every worker; the idempotency keys carry the workflow run id, so two sagas
// cannot collide in here.
func NewActivities() *Activities { return &Activities{} }

// mark records that the work for this activity's key has been done, and unmark
// takes it back. A compensation calls unmark whether or not there is anything
// to take back.
func (a *Activities) mark(ctx context.Context) {
	k, _ := saga.IdempotencyKey(ctx)
	a.done.Store(k, struct{}{})
}

func (a *Activities) unmark(ctx context.Context) {
	k, _ := saga.IdempotencyKey(ctx)
	a.done.Delete(k)
}

// --- for the specifications --------------------------------------------------
//
// What follows is here so docs/specs/ can see what the activities did. Business
// code has no use for any of it, which is why none of it is a method: reading
// the type should not turn up test scaffolding.

// Held reports whether a step of a given workflow run is still done. A
// specification uses it to check that a rollback undid everything.
func Held(a *Activities, runID, step string) bool {
	_, ok := a.done.Load(runID + "/" + step)
	return ok
}

// Upstream reports the id the named step's compensation was handed, which is
// what example/workflow/pipeline is about. The compensations record it for this
// reason only.
func Upstream(a *Activities, step string) string {
	v, _ := a.saw.Load(step)
	s, _ := v.(string)
	return s
}

// KeySeenBy reports the idempotency key the named half of the packing step
// read, which is what example/workflow/childflow is about. Pack and Unpack
// record it for this reason only.
func KeySeenBy(a *Activities, which string) string {
	v, _ := a.keys.Load(which)
	s, _ := v.(string)
	return s
}
