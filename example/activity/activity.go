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
//
// They hold no state, and each body is its business failure check and its
// result. That is not a simplification of a real activity, it is what is left
// when there is nothing downstream: a real one writes to a database or calls an
// API, and that write is the activity's work rather than a side effect of it.
// The record of what was done belongs to that downstream, not to the type
// below, which is why there is nothing here to keep it in.
//
// The consequence is that nothing here reads the idempotency key, because there
// is nothing to hand it to. docs/interface.md has the shape a real one
// takes -- the key as a UNIQUE column, or an Idempotency-Key header -- and
// saga.IdempotencyKey is how an activity reads it. What the specifications
// check instead is the workflow history, which is Temporal's own record of what
// each activity was handed and what came back.
package activity

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
// activities: hand the idempotency key to whatever does the work, and succeed
// when a compensation finds nothing to undo.
//
// It carries no fields. A worker registers the exported methods of the value it
// is given, so the type is here to group them and to be the place a real one
// would hold its handle to a database or an API client.
type Activities struct{}

// NewActivities returns the activities. One set is shared by every worker.
func NewActivities() *Activities { return &Activities{} }
