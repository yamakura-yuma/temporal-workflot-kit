// Package saga runs a sequence of steps that can be rolled back.
//
// A step is two ordinary workflow functions: the forward half, and the
// compensation that undoes it. The library registers the compensation before
// the forward half runs, and Run executes the registered compensations in
// reverse order if the saga fails.
//
//	func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
//	    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
//	        StartToCloseTimeout: 10 * time.Second,
//	    })
//
//	    return saga.RunOrCompensate(ctx, saga.Options{CompensationBudget: 10 * time.Minute},
//	        func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
//	            w := &fulfillment{in: in}
//
//	            s.Step(ctx, "reserve", w.reserve, w.unreserve)
//	            s.Step(ctx, "charge", w.charge, w.refund)
//
//	            return w.receipt(), nil
//	        })
//	}
//
//	// The halves are ordinary workflow code. Nothing here is wrapped, so the
//	// activity options set above are the ones that apply, and a compensation
//	// reads what its forward half produced.
//	func (w *fulfillment) charge(ctx workflow.Context) error {
//	    return workflow.ExecuteActivity(ctx, acts.Charge,
//	        ChargeReq{Order: w.in.ID}).Get(ctx, &w.charge)
//	}
//
//	func (w *fulfillment) refund(ctx workflow.Context) error {
//	    return workflow.ExecuteActivity(ctx, acts.Refund,
//	        ChargeReq{Charge: w.charge}).Get(ctx, nil)
//	}
//
// # What the library takes care of
//
// Compensations run on a context obtained from workflow.NewDisconnectedContext.
// This matters more than it looks: a canceled workflow context fails every
// subsequent activity immediately, so compensation written the obvious way does
// nothing in the one situation it exists for.
//
// Compensations are registered before the forward half runs, not after it
// succeeds, so a step that timed out -- and may well have taken effect on a
// worker that never reported back -- is still rolled back.
//
// A step failure makes every later step a no-op, and Run returns that error
// even if the body returned nil, discarding the body's result. Forgetting an
// error check therefore fails the workflow instead of completing it with half
// its side effects applied.
//
// Step names the activities a step starts, so a history reads the way the saga
// was written: "<RunID>/charge", and "<RunID>/charge:undo" for its
// compensation.
//
// That is the whole of it. Everything else is Temporal's, and stays yours to
// write: ExecuteActivity, ExecuteChildWorkflow, a signal, the options on the
// context, and what you do with the result.
//
// # What you still have to do yourself
//
// Idempotency keys are yours. StepKey derives a value unique to one step of one
// run; put it in the request you send, because only the service you call can
// enforce it. Claiming it has to be atomic with doing the work: checking
// whether the key was used and then acting is not enough, since a timeout can
// put two attempts in flight at once and both will see it as unused. Use a
// uniqueness constraint (INSERT ... ON CONFLICT) or the downstream API's own
// idempotency-key header.
//
// Compensations must tolerate undoing a step that never took effect, and must
// say so by succeeding rather than failing. A compensation can read what its
// forward half produced -- they are usually methods on one struct -- but that
// value is empty when the forward half never reported back, which is exactly
// when the key is all there is to go on.
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
