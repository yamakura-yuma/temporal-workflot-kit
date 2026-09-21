// Package pipeline is the example for a saga whose steps feed each other: the
// reservation id goes into the charge, and the charge id goes into the
// shipment.
//
// The point is what that does to the compensations. A compensation is given the
// same input as the step it undoes, so it gets the upstream ids for free --
// which matters, because the compensation is registered before the step runs
// and therefore cannot see that step's output.
//
// The activities are the shared ones in example/activity/. Nothing about them
// is special to this example: ChargeReq and ShipReq simply have fields for the
// upstream ids, which example/workflow/order leaves empty and the body below
// fills in. The theme lives here, in two lines of a workflow.
package pipeline

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow.
const TaskQueue = "saga-pipeline"

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// PipelineWorkflow reserves stock, charges against that reservation, and ships
// against that charge.
func PipelineWorkflow(ctx workflow.Context, in activity.Order) (Receipt, error) {
	var a *activity.Activities

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), activity.ReserveReq{Order: in})

		chg, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund), activity.ChargeReq{Order: in, Reservation: res})

		shp, _ := saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment), activity.ShipReq{Order: in, Charge: chg})

		return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
	})
}
