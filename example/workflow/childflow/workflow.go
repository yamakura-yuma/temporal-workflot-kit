// Package childflow is the example for a step whose forward half is a child
// workflow and whose compensation is an activity.
//
// A saga can mix them, and so can a single step. Here packing is a child
// workflow because it is long enough to deserve its own history, while undoing
// it is one activity call, and reserving and shipping stay activities
// throughout. The rollback runs them all in one reverse order.
//
// Nothing in the library knows about any of that. A step's halves are plain
// workflow functions, so choosing ExecuteChildWorkflow for one and
// ExecuteActivity for the other is a choice made here, in two methods.
//
// Both halves act under the same idempotency key because this workflow derives
// it once, with saga.StepKey, and passes it to both. The key travels in the
// request like any other field -- it does not ride on anything Temporal owns,
// and the activities never have to ask the library for it.
package childflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow. The
// children inherit it.
const TaskQueue = "saga-childflow"

var acts *activity.Activities

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Pack        string `json:"pack"`
	Shipment    string `json:"shipment"`
}

// ChildflowWorkflow reserves stock, packs the order in a child workflow, and
// books a shipment.
func ChildflowWorkflow(ctx workflow.Context, in activity.Order) (Receipt, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	return saga.RunOrCompensate(ctx, saga.Options{CompensationBudget: time.Minute},
		func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
			w := &fulfillment{in: in, packKey: saga.StepKey(ctx, "pack")}

			s.Step(ctx, "reserve", w.reserve, w.unreserve)
			s.Step(ctx, "pack", w.pack, w.unpack)
			s.Step(ctx, "ship", w.ship, w.cancelShipment)

			return Receipt{Reservation: w.reservation, Pack: w.packing, Shipment: w.shipment}, nil
		})
}

type fulfillment struct {
	in activity.Order

	// packKey is derived once and given to both halves of the packing step.
	packKey string

	reservation string
	packing     string
	shipment    string
}

func (w *fulfillment) reserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Reserve,
		activity.ReserveReq{Order: w.in}).Get(ctx, &w.reservation)
}

func (w *fulfillment) unreserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Unreserve,
		activity.ReserveReq{Order: w.in, Reservation: w.reservation}).Get(ctx, nil)
}

// pack runs the forward half as a child workflow. Naming the child after the
// step is this workflow's choice, made the ordinary way: it is what makes the
// parent's history readable, and it is not something the library does.
func (w *fulfillment) pack(ctx workflow.Context) error {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: w.packKey})

	return workflow.ExecuteChildWorkflow(ctx, PackWorkflow,
		activity.PackReq{Order: w.in, Key: w.packKey}).Get(ctx, &w.packing)
}

// unpack undoes it with a single activity, under the same key the child was
// given.
func (w *fulfillment) unpack(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Unpack,
		activity.PackReq{Order: w.in, Key: w.packKey, Pack: w.packing}).Get(ctx, nil)
}

func (w *fulfillment) ship(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Ship,
		activity.ShipReq{Order: w.in, Pack: w.packing}).Get(ctx, &w.shipment)
}

func (w *fulfillment) cancelShipment(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.CancelShipment,
		activity.ShipReq{Order: w.in, Pack: w.packing, Shipment: w.shipment}).Get(ctx, nil)
}

// PackWorkflow is the forward half of the packing step. It is an ordinary
// workflow: it knows nothing about sagas, and the key it hands the activity is
// the one its caller put in the request.
func PackWorkflow(ctx workflow.Context, req activity.PackReq) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	var id string
	err := workflow.ExecuteActivity(ctx, acts.Pack, req).Get(ctx, &id)
	return id, err
}
