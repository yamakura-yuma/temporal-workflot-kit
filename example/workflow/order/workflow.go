// Package order is the basic saga: three steps, each with a compensation,
// undone in reverse when anything fails.
//
// Read this one first. The other workflows under example/workflow/ are variants
// of it, and they all call the same activities from example/activity/.
//
// Each half of a step is an ordinary method that runs an activity and puts the
// result on the struct. The library never wraps workflow.ExecuteActivity, so
// what you see is what Temporal does.
package order

import (
	"errors"
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

// acts is a nil receiver. Only the names of its methods are used, to tell
// ExecuteActivity which activity to run.
var acts *activity.Activities

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
func OrderWorkflow(ctx workflow.Context, in Request) (Receipt, error) {
	// Ordinary activity options, set the ordinary way. Every step below runs
	// under them; a step that wants its own says so inside its own method.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		// A compensation runs on a context nothing can cancel, so bound it here:
		// without ScheduleToCloseTimeout the default retry policy is unlimited.
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// The default is the Java SDK's: stop at the first compensation that fails.
	// This saga takes the other one. A refund failing is no reason to leave the
	// stock reserved as well, and whatever cannot be undone is reported either
	// way.
	opts := saga.Options{ContinueWithError: true}

	receipt, err := saga.RunOrCompensate(ctx, opts, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		w := &fulfillment{in: in.Order}

		// The step errors are ignored on purpose: after the first failure every
		// later Step is a no-op, and RunOrCompensate fails the workflow with that
		// rather than returning the half-filled Receipt this body would
		// otherwise produce.
		s.Step(ctx, "reserve", w.reserve, w.unreserve)
		s.Step(ctx, "charge", w.chargeCard, w.refund)

		// Somewhere to cancel the workflow from the outside. Sleep returns a
		// cancellation error, which RunOrCompensate turns into a rollback -- on a
		// disconnected context, or every compensation below would fail
		// immediately instead.
		if in.HoldSeconds > 0 && s.Err() == nil {
			if err := workflow.Sleep(ctx, time.Duration(in.HoldSeconds)*time.Second); err != nil {
				return Receipt{}, err
			}
		}

		s.Step(ctx, "ship", w.ship, w.cancelShipment)

		return Receipt{Reservation: w.reservation, Charge: w.charge, Shipment: w.shipment}, nil
	})

	// Flagging a rollback that did not finish is ordinary workflow code, which
	// is why the saga package does not do it: it hands back an error of a type
	// you can match on, and the rest is one Upsert.
	if in.MarkAttribute && rollbackFailed(err) {
		if uerr := workflow.UpsertTypedSearchAttributes(ctx,
			CompensationFailedAttribute.ValueSet(true)); uerr != nil {
			workflow.GetLogger(ctx).Error("could not flag the saga", "error", uerr)
		}
	}

	return receipt, err
}

// rollbackFailed reports whether the saga failed and its rollback did not
// finish cleanly, as opposed to failing and being undone.
func rollbackFailed(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.Type() == saga.CompensationFailedType
}

// fulfillment holds the input and what each step produced. A compensation reads
// what its forward half wrote, which is why the two are methods on one struct.
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

func (w *fulfillment) chargeCard(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Charge,
		activity.ChargeReq{Order: w.in}).Get(ctx, &w.charge)
}

func (w *fulfillment) refund(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Refund,
		activity.ChargeReq{Order: w.in, Charge: w.charge}).Get(ctx, nil)
}

func (w *fulfillment) ship(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.Ship,
		activity.ShipReq{Order: w.in}).Get(ctx, &w.shipment)
}

func (w *fulfillment) cancelShipment(ctx workflow.Context) error {
	return workflow.ExecuteActivity(ctx, acts.CancelShipment,
		activity.ShipReq{Order: w.in, Shipment: w.shipment}).Get(ctx, nil)
}
