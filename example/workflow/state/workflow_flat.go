package state

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// FlatWorkflow is the saga from workflow.go with every half written inline
// instead of as a method.
//
// It is here to be read against the other one, so the two are kept honestly
// comparable: same input, same steps, same order, same options, same
// activities. Only where the code lives differs.
//
// The length is the argument, not a mistake. The input has to be projected onto
// the activities' narrower one at every half, and reservation, charge, packing
// and the decision all have to be declared up front and carried by hand,
// because the compensations need to read them. For a short saga over a narrow
// input this shape is fine, and it is what example/workflow/order uses. This is
// what it turns into at five wide steps.
//
// An example that is only read rots, so this one is executed: the worker in
// specsteps registers it on the same task queue, and docs/specs/state.feature
// runs both shapes over the same order and checks they return the same Receipt.
func FlatWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())

	return saga.RunOrCompensate(ctx, saga.Options{CompensationBudget: time.Minute},
		func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
			var reservation, charge, approvedBy, packing, shipment string

			s.Step(ctx, "reserve",
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Reserve,
						activity.ReserveReq{Order: in.line()}).Get(ctx, &reservation)
				},
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Unreserve,
						activity.ReserveReq{Order: in.line(), Reservation: reservation}).Get(ctx, nil)
				})

			s.Step(ctx, "charge",
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Charge,
						activity.ChargeReq{Order: in.line(), Reservation: reservation}).Get(ctx, &charge)
				},
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Refund,
						activity.ChargeReq{Order: in.line(), Charge: charge}).Get(ctx, nil)
				})

			s.Step(ctx, "approve",
				func(ctx workflow.Context) error {
					decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, in.wait())
					if !ok {
						return temporal.NewApplicationError(
							"nobody reviewed order "+in.ID+" in time", DeniedType, nil)
					}
					if !decision.Approved {
						return temporal.NewApplicationError(
							"the order was rejected by "+decision.By, DeniedType, nil)
					}
					approvedBy = decision.By
					return nil
				}, nil)

			s.Step(ctx, "pack",
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Pack,
						activity.PackReq{Order: in.line()}).Get(ctx, &packing)
				},
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Unpack,
						activity.PackReq{Order: in.line(), Pack: packing}).Get(ctx, nil)
				})

			s.Step(ctx, "ship",
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.Ship,
						activity.ShipReq{Order: in.line(), Charge: charge, Pack: packing, ApprovedBy: approvedBy}).Get(ctx, &shipment)
				},
				func(ctx workflow.Context) error {
					return workflow.ExecuteActivity(ctx, acts.CancelShipment,
						activity.ShipReq{Order: in.line(), Charge: charge, Shipment: shipment}).Get(ctx, nil)
				})

			return Receipt{
				Reservation: reservation,
				Charge:      charge,
				ApprovedBy:  approvedBy,
				Pack:        packing,
				Shipment:    shipment,
			}, nil
		})
}
