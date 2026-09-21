// Package childflow is the example for a step that is a child workflow rather
// than an activity.
//
// A saga can mix them, and so can a single step. Here packing is a child
// workflow because it is long enough to deserve its own history, while undoing
// it is one activity call, and reserving and shipping stay activities
// throughout. The rollback runs them all in one reverse order.
//
// The child reads its idempotency key with saga.IdempotencyKeyOf, which is the
// workflow-side twin of saga.IdempotencyKey. The key rides in the child's
// WorkflowID, and the compensating child gets the same key back with the
// ":undo" suffix stripped.
package childflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow. The
// children inherit it.
const TaskQueue = "saga-childflow"

// Order is the workflow input.
type Order struct {
	ID     string `json:"id"`
	SKU    string `json:"sku"`
	FailAt string `json:"fail_at,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Pack        string `json:"pack"`
	Shipment    string `json:"shipment"`
}

// ChildflowWorkflow reserves stock, packs the order in a child workflow, and
// books a shipment.
func ChildflowWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *Activities

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})

		// The two halves run differently. Packing is long enough to deserve a
		// child workflow of its own; undoing it is one activity call. Both
		// still see the same idempotency key -- the child reads it with
		// IdempotencyKeyOf, the activity with IdempotencyKey.
		pack, _ := saga.Step(ctx, s, "pack",
			saga.ChildWorkflow(PackWorkflow), saga.UndoActivity(a.Unpack),
			PackReq{Order: in})

		shp, _ := saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment), ShipReq{Order: in, Pack: pack})

		return Receipt{Reservation: res, Pack: pack, Shipment: shp}, nil
	})
}

// PackWorkflow is the forward half of the packing step.
func PackWorkflow(ctx workflow.Context, req PackReq) (string, error) {
	var a *Activities

	key, _ := saga.IdempotencyKeyOf(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	var id string
	err := workflow.ExecuteActivity(ctx, a.Pack, Note{Key: key, Order: req.Order.ID}).Get(ctx, &id)
	return id, err
}
