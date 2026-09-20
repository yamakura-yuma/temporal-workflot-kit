// Package saga runs a sequence of Temporal activities that can be rolled back.
//
// Each step registers a compensation before its forward activity runs, and Run
// executes the registered compensations in reverse order if the saga fails.
//
//	func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
//	    return saga.Run(ctx, saga.Options{
//	        ActivityOptions:    workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second},
//	        CompensationBudget: 10 * time.Minute,
//	    }, func(s *saga.Saga) (Receipt, error) {
//	        id, _ := saga.Step(ctx, s, "create-order", saga.Activity(a.CreateOrder), saga.UndoActivity(a.CancelOrder), in)
//	        pay, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge), saga.UndoActivity(a.Refund), ChargeReq{Order: id})
//	        return Receipt{OrderID: id, Paid: pay.ID}, nil
//	    })
//	}
//
// # What the library takes care of
//
// Compensations run on a context obtained from workflow.NewDisconnectedContext.
// This matters more than it looks: a canceled workflow context fails every
// subsequent activity immediately, so compensation written the obvious way does
// nothing in the one situation it exists for.
//
// Compensations are registered before the forward activity runs, not after it
// succeeds, so a step that timed out -- and may well have taken effect on a
// worker that never reported back -- is still rolled back.
//
// Both halves of a step see the same idempotency key through IdempotencyKey.
//
// A step failure makes every later step a no-op, and Run returns that error
// even if the body returned nil, discarding the body's result. Forgetting an
// error check therefore fails the workflow instead of completing it with half
// its side effects applied.
//
// # What you still have to do yourself
//
// The activity contract this library relies on is not automatic, and the hard
// parts of it are yours:
//
// Forward activities must claim their idempotency key atomically. Checking
// whether the key was already used and then acting on it is not enough: a
// timeout can put two attempts in flight at once and both will see the key as
// unused. Use a uniqueness constraint (INSERT ... ON CONFLICT) or the
// downstream API's own idempotency-key header.
//
// Compensations must tolerate undoing a step that never took effect, and must
// say so by succeeding rather than failing.
//
// # What this library does not solve
//
// A compensation cannot reliably tell whether the step it undoes happened. A
// forward activity that timed out keeps running on its worker: the compensation
// can observe "nothing happened", return successfully, and the side effect can
// land afterwards. Closing that hole requires the compensation to write a
// tombstone that the forward activity checks before committing -- that is, both
// halves serialized through one store. Against a third-party payment gateway or
// carrier that is not possible, and no library can make it so.
//
// Downstream systems that do not accept an idempotency key cannot be made safe
// this way at all. Neither can side effects that cannot be taken back, such as
// a sent email.
//
// Cancel workflows, do not terminate them. Termination runs no workflow code,
// so nothing is compensated.
//
// Finally, this package issues workflow commands, so upgrading it changes the
// command sequence of every workflow that uses it and breaks the replay of runs
// that are still open. Callers cannot wrap that in workflow.GetVersion, because
// the commands are inside this package. In practice the version cannot move
// until the sagas started under the old one have drained.
package saga
