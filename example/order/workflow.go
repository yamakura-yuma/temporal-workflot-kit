package order

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

// TaskQueue is shared between the worker and workflow starters.
const TaskQueue = "saga-task-queue"

// OrderWorkflow reserves stock, charges the card and books a shipment. If any
// step fails, the steps that already ran are undone in reverse order.
//
// The step errors are ignored on purpose: after the first failure every later
// Step is a no-op, and Run fails the workflow with that error rather than
// returning the half-filled Receipt this body would otherwise produce.
func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *Activities

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
		},
		CompensationOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			// Unbounded attempts, held in check by CompensationBudget below:
			// giving up on a rollback is worse than retrying it.
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 0},
		},
		CompensationBudget: 5 * time.Minute,
	}, func(s *saga.Saga) (Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", a.Reserve, a.Unreserve,
			ReserveReq{Order: in, FailAt: in.FailAt})

		chg, _ := saga.Step(ctx, s, "charge", a.Charge, a.Refund,
			ChargeReq{Order: in, FailAt: in.FailAt})

		// Somewhere to cancel the workflow from the outside. Sleep returns a
		// cancellation error, which Run turns into a rollback -- on a
		// disconnected context, or the compensations below would all fail
		// immediately.
		if in.HoldSeconds > 0 && s.Err() == nil {
			if err := workflow.Sleep(ctx, time.Duration(in.HoldSeconds)*time.Second); err != nil {
				return Receipt{}, err
			}
		}

		shp, _ := saga.Step(ctx, s, "ship", a.Ship, a.CancelShipment,
			ShipReq{Order: in, FailAt: in.FailAt})

		return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
	})
}
