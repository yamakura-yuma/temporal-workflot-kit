package state

import (
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/saga"
)

// FlatWorkflow is the saga from workflow.go written without the state struct:
// five saga.Step calls in the body of saga.Run.
//
// It is here to be read against the other one, so the two are kept honestly
// comparable. Same input, same steps, same order, same options, same
// activities -- the only difference is where the plumbing lives. Read the body
// of each and decide which one shows you the saga faster.
//
// The length is the argument, not a mistake: the input has to be projected onto
// the activities' narrower one at every step, and reservation, decision, charge
// and packing all have to be carried from the step that produced them to the
// step that needs them. At three narrow steps this shape is the one to prefer,
// which is what every other example in this repository uses. This is what it
// turns into at five wide ones.
//
// An example that is only read rots, so this one is executed: the worker in
// specsteps registers it on the same task queue, and docs/specs/state.feature
// runs both shapes over the same order and checks they return the same Receipt.
func FlatWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *activity.Activities

	return saga.Run(ctx, sagaOptions(), func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		reservation, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve),
			activity.ReserveReq{Order: in.line()})

		charge, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund),
			activity.ChargeReq{Order: in.line(), Reservation: reservation})

		decision, _ := saga.Step(ctx, s, "approve", saga.Func(awaitApproval), nil, ApproveReq{
			Order:    in.ID,
			Customer: in.Customer,
			Amount:   in.Amount,
			Wait:     in.wait(),
		})

		packing, _ := saga.Step(ctx, s, "pack", saga.Activity(a.Pack), saga.UndoActivity(a.Unpack),
			activity.PackReq{Order: in.line()})

		shipment, _ := saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment),
			activity.ShipReq{Order: in.line(), Charge: charge, Pack: packing, ApprovedBy: decision.By})

		if err := s.Err(); err != nil {
			return Receipt{}, err
		}
		return Receipt{
			Reservation: reservation,
			Charge:      charge,
			ApprovedBy:  decision.By,
			Pack:        packing,
			Shipment:    shipment,
		}, nil
	})
}
