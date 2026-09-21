// Package order is the basic saga: three activity steps, each with a
// compensation, undone in reverse when anything fails.
//
// Read this one first. The other workflows under example/workflow/ are variants
// of it, and they all call the same activities from example/activity/.
package order

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// TaskQueue is shared between the test worker and the test client.
const TaskQueue = "saga-integration"

// CompensationFailedAttribute flags a saga whose rollback did not finish, so
// operators can search for the executions that need a human. The key has to be
// registered on the server before a workflow can write it; the test harness
// registers it on the dev server it starts.
var CompensationFailedAttribute = temporal.NewSearchAttributeKeyBool("SagaCompensationFailed")

// Request is the workflow input: an order, plus the two knobs that belong to
// this workflow rather than to any activity.
type Request struct {
	Order activity.Order `json:"order"`

	// HoldSeconds keeps the workflow waiting after the charge step, so a test
	// can cancel it mid-saga.
	HoldSeconds int `json:"hold_seconds,omitempty"`
	// MarkAttribute asks the saga to flag a failed rollback with a search
	// attribute.
	MarkAttribute bool `json:"mark_attribute,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// OrderWorkflow reserves stock, charges the card and books a shipment. If any
// step fails, the steps that already ran are undone in reverse order.
//
// The step errors are ignored on purpose: after the first failure every later
// Step is a no-op, and Run fails the workflow with that error rather than
// returning the half-filled Receipt this body would otherwise produce.
func OrderWorkflow(ctx workflow.Context, in Request) (Receipt, error) {
	var a *activity.Activities // nil receiver: only the methods' names are used

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

	return saga.Run(ctx, opts, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		// Nothing is fed from one step to the next here: every request is built
		// from the input alone. ChargeReq and ShipReq have fields for the
		// upstream ids and this saga leaves them empty. For the shape that
		// fills them, see example/workflow/pipeline.
		res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), activity.ReserveReq{Order: in.Order})
		chg, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund), activity.ChargeReq{Order: in.Order})

		// Somewhere to cancel the workflow from the outside. Sleep returns a
		// cancellation error, which Run turns into a rollback -- on a
		// disconnected context, or every compensation below would fail
		// immediately instead.
		if in.HoldSeconds > 0 && s.Err() == nil {
			if err := workflow.Sleep(ctx, time.Duration(in.HoldSeconds)*time.Second); err != nil {
				return Receipt{}, err
			}
		}

		shp, _ := saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment), activity.ShipReq{Order: in.Order})

		return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
	})
}
