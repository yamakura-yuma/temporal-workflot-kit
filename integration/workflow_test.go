//go:build integration

package integration

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

// TaskQueue is shared between the test worker and the test client.
const TaskQueue = "saga-integration"

// CompensationFailedAttribute flags a saga whose rollback did not finish, so
// operators can search for the executions that need a human. The key has to be
// registered on the server before a workflow can write it; the test harness
// registers it on the dev server it starts.
var CompensationFailedAttribute = temporal.NewSearchAttributeKeyBool("SagaCompensationFailed")

// OrderWorkflow reserves stock, charges the card and books a shipment. If any
// step fails, the steps that already ran are undone in reverse order.
//
// The step errors are ignored on purpose: after the first failure every later
// Step is a no-op, and Run fails the workflow with that error rather than
// returning the half-filled Receipt this body would otherwise produce.
func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *Activities // nil receiver: only the methods' names are used

	opts := saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}
	if in.MarkAttribute {
		opts.CompensationFailedAttribute = &CompensationFailedAttribute
	}

	return saga.Run(ctx, opts, func(s *saga.Saga) (Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", a.Reserve, a.Unreserve, ReserveReq{Order: in})
		chg, _ := saga.Step(ctx, s, "charge", a.Charge, a.Refund, ChargeReq{Order: in})

		// Somewhere to cancel the workflow from the outside. Sleep returns a
		// cancellation error, which Run turns into a rollback -- on a
		// disconnected context, or every compensation below would fail
		// immediately instead.
		if in.HoldSeconds > 0 && s.Err() == nil {
			if err := workflow.Sleep(ctx, time.Duration(in.HoldSeconds)*time.Second); err != nil {
				return Receipt{}, err
			}
		}

		shp, _ := saga.Step(ctx, s, "ship", a.Ship, a.CancelShipment, ShipReq{Order: in})

		return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
	})
}
