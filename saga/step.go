package saga

import "go.temporal.io/sdk/workflow"

// Step registers undo, then runs do.
//
// Both halves are ordinary workflow code: write workflow.ExecuteActivity,
// ExecuteChildWorkflow, a signal, or nothing at all, and read the result with
// Get the way you would anywhere else. The library does not wrap any of it, so
// the activity options you set on the context are the ones that apply.
//
//	s.Step(ctx, "reserve", w.reserve, w.unreserve)
//
//	func (w *fulfillment) reserve(ctx workflow.Context) error {
//	    return workflow.ExecuteActivity(ctx, a.Reserve, req).Get(ctx, &w.reservation)
//	}
//
//	func (w *fulfillment) unreserve(ctx workflow.Context) error {
//	    return workflow.ExecuteActivity(ctx, a.Unreserve, req).Get(ctx, nil)
//	}
//
// The result of a step goes wherever do puts it, which is why undo can use it:
// a compensation reads the variable its forward half wrote. Other saga
// libraries carry that as a compensation log; in Go an ordinary captured
// variable does the same job.
//
// # What Step guarantees
//
// undo is registered before do runs, never after it succeeds. A forward call
// that times out may still have taken effect on a worker that never reported
// back, and registering afterwards would leave that behind. The cost is that
// undo may be asked to undo something that never happened, and it has to
// succeed when it finds nothing to do.
//
// After any step fails, later Step calls return that error without running
// anything, so a linear saga can ignore the returned error and let Run decide
// the outcome.
//
// undo may be nil for a step with nothing to take back.
//
// # The activity id
//
// Step names the activities do starts: their ActivityID becomes
// "<RunID>/<name>", and a compensation's is that plus ":undo". This is for
// reading a history -- the Temporal UI lists the steps by name instead of by a
// serial number, and that is how docs/specs/ identifies them.
//
// It also means one activity per step. A do that starts two would give them the
// same id and the server would reject the second, which is loud rather than
// subtle.
//
// Set the options before calling Step. If a step needs its own -- a
// compensation usually wants more attempts than the forward half did -- read
// them, change what you want, and put them back, because replacing the struct
// wholesale drops the id along with everything else:
//
//	opts := workflow.GetActivityOptions(ctx)
//	opts.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 5}
//	ctx = workflow.WithActivityOptions(ctx, opts)
//
// An idempotency key is not this id and is not the library's business: build
// one from the run and the step name and put it in the request, where the
// service you are calling can enforce it. See docs/activity-contract.md.
func (s *Saga) Step(ctx workflow.Context, name string, do, undo func(workflow.Context) error) error {
	if s.err != nil {
		return s.err
	}
	if err := s.claimName(name); err != nil {
		s.fail(err)
		return err
	}

	id := StepKey(ctx, name)

	if undo != nil {
		s.addUndo(name, func(cctx workflow.Context) error {
			return undo(withActivityID(cctx, id+undoSuffix))
		})
	}

	if err := do(withActivityID(ctx, id)); err != nil {
		s.fail(err)
		return err
	}
	return nil
}

// withActivityID puts id on the context's activity options, leaving everything
// else the caller set alone.
func withActivityID(ctx workflow.Context, id string) workflow.Context {
	opts := workflow.GetActivityOptions(ctx)
	opts.ActivityID = id
	return workflow.WithActivityOptions(ctx, opts)
}
