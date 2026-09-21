// Package pipeline is the example for a saga whose steps feed each other: the
// reservation id goes into the charge, and the charge id goes into the
// shipment.
//
// The point is what that does to the compensations. Each half of a step is a
// method on one struct, so a compensation simply reads the field its forward
// half wrote -- w.charge in refund, w.shipment in cancelShipment. Nothing has
// to be threaded through the saga for it.
//
// That is also why a compensation can undo precisely. It is registered before
// its forward half runs, so at registration time there is nothing to read; by
// the time it runs, the field is filled. If the forward half never returned,
// the field is empty, and the compensation has to fall back on the idempotency
// key it sent -- see docs/interface.md.
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

var acts *activity.Activities

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// PipelineWorkflow reserves stock, charges against that reservation, and ships
// against that charge.
func PipelineWorkflow(ctx workflow.Context, in activity.Order) (Receipt, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		// A compensation runs on a context nothing can cancel, so bound it here:
		// without ScheduleToCloseTimeout the default retry policy is unlimited.
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	return saga.RunOrCompensate(ctx, saga.Options{},
		func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
			w := &fulfillment{in: in}

			s.Step(ctx, "reserve", w.reserve, w.unreserve)
			s.Step(ctx, "charge", w.chargeCard, w.refund)
			s.Step(ctx, "ship", w.ship, w.cancelShipment)

			return Receipt{Reservation: w.reservation, Charge: w.charge, Shipment: w.shipment}, nil
		})
}

type fulfillment struct {
	in activity.Order

	reservation string
	charge      string
	shipment    string
}

func (w *fulfillment) reserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Reserve,
		activity.ReserveReq{Order: w.in}).Get(ctx, &w.reservation)
}

func (w *fulfillment) unreserve(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Unreserve,
		activity.ReserveReq{Order: w.in}).Get(ctx, nil)
}

// charge takes the reservation the step before it produced.
func (w *fulfillment) chargeCard(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Charge,
		activity.ChargeReq{Order: w.in, Reservation: w.reservation}).Get(ctx, &w.charge)
}

// refund knows which reservation the charge belonged to, and which charge it is
// reversing, because both are on the struct.
func (w *fulfillment) refund(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Refund,
		activity.ChargeReq{Order: w.in, Reservation: w.reservation, Charge: w.charge}).Get(ctx, nil)
}

func (w *fulfillment) ship(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Ship,
		activity.ShipReq{Order: w.in, Charge: w.charge}).Get(ctx, &w.shipment)
}

func (w *fulfillment) cancelShipment(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.CancelShipment,
		activity.ShipReq{Order: w.in, Charge: w.charge, Shipment: w.shipment}).Get(ctx, nil)
}
