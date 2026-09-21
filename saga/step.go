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
// # What name is for
//
// name identifies the step in CompensationReport, and two steps may not share
// one. It does not reach Temporal: this package does not touch ActivityID, so
// do is free to start as many activities as it likes, and the options on the
// context are yours alone.
//
// If you want a history that reads the way the saga was written, set
// ActivityID yourself in do -- it is one field on ActivityOptions.
func (s *Saga) Step(ctx workflow.Context, name string, do, undo func(workflow.Context) error) error {
	if s.err != nil {
		return s.err
	}
	if err := s.claimName(name); err != nil {
		s.fail(err)
		return err
	}

	if undo != nil {
		s.addCompensation(name, undo)
	}

	if err := do(ctx); err != nil {
		s.fail(err)
		return err
	}
	return nil
}
