package saga

// The compensation list, the reverse order and the two Options are what the
// Java SDK's io.temporal.workflow.Saga and the PHP port of it have, under the
// same names. run.go and step.go hold the rest of what this package adds.
//
// Two extensions are in here rather than there, because they are inside
// compensate() and splitting that function would hide more than it showed: the
// disconnected context, which PHP has as asyncDetached and Java does not have
// at all, and the failed/skipped accounting that fills CompensationReport.
// Step names, and the claimName that keeps them unique, are here for the same
// reason -- Java's Saga has no step names.

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// Options configures a saga.
type Options struct {
	// ParallelCompensation fires every compensation at once instead of running
	// them in reverse order. The default is reverse order, which is what a
	// rollback usually means: a step that ran later may depend on one that ran
	// earlier, and undoing them out of order can fail.
	//
	// Set it only when the steps are genuinely independent. ContinueWithError
	// has no effect here, because every compensation is dispatched before any
	// of them is awaited.
	//
	// The Java SDK's io.temporal.workflow.Saga has the same two options, under
	// the same names.
	ParallelCompensation bool

	// ContinueWithError keeps running the remaining compensations after one of
	// them fails, instead of stopping at the first failure.
	//
	// The default matches the Java SDK's: stop. Think before taking it, though
	// -- a refund failing is not much of a reason to leave the inventory
	// reserved as well, and whatever is left unrun is reported as Skipped
	// rather than attempted.
	ContinueWithError bool
}

// Saga records the compensations for the steps that have been started, and the
// first error any of them reported. RunOrCompensate creates one and hands it
// to the body; there is no other way to get one.
type Saga struct {
	opts  Options
	err   error
	names map[string]struct{}
	undos []undo
}

type undo struct {
	name string
	run  func(workflow.Context) error
}

func newSaga(o Options) *Saga {
	return &Saga{opts: o, names: map[string]struct{}{}}
}

// addCompensation records a compensation, the way the Java SDK's Saga does.
// Unlike that one it is not exported: only Step calls it, and it does so before
// running the forward half, which is the ordering the whole package exists to
// guarantee. It is not exported for that reason: a caller who could register a
// compensation directly could register it too late, or after the saga has
// already failed, and nothing would say so.
func (s *Saga) addCompensation(name string, run func(workflow.Context) error) {
	s.undos = append(s.undos, undo{name: name, run: run})
}

func (s *Saga) fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Saga) claimName(name string) error {
	if name == "" {
		return fmt.Errorf("saga: step name must not be empty")
	}
	if _, taken := s.names[name]; taken {
		return fmt.Errorf("saga: duplicate step name %q; names identify a step in CompensationReport and must be unique", name)
	}
	s.names[name] = struct{}{}
	return nil
}

// compensate runs the registered compensations in reverse order and returns the
// error the workflow should fail with.
func (s *Saga) compensate(ctx workflow.Context, cause error) error {
	if len(s.undos) == 0 {
		return cause
	}

	// A disconnected context does not inherit the parent's cancellation, so
	// compensations still run after the workflow is canceled. Nothing outside
	// can cancel it either, which is why a compensation needs a
	// ScheduleToCloseTimeout of its own.
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	logger := workflow.GetLogger(ctx)

	var failed, skipped []string
	var first error

	record := func(name string, err error) {
		logger.Error("saga: compensation failed", "step", name, "error", err)
		failed = append(failed, name)
		if first == nil {
			first = err
		}
	}

	if s.opts.ParallelCompensation {
		// Every compensation is dispatched before any of them is awaited, so
		// one failing cannot stop another from being tried. Failures are
		// collected in registration order to keep the report deterministic.
		errs := make([]error, len(s.undos))

		wg := workflow.NewWaitGroup(dctx)
		for i := range s.undos {
			wg.Add(1)
			workflow.Go(dctx, func(gctx workflow.Context) {
				defer wg.Done()
				errs[i] = s.undos[i].run(gctx)
			})
		}
		wg.Wait(dctx)

		for i, err := range errs {
			if err != nil {
				record(s.undos[i].name, err)
			}
		}
	} else {
		for i := len(s.undos) - 1; i >= 0; i-- {
			u := s.undos[i]

			if err := u.run(dctx); err != nil {
				record(u.name, err)

				if !s.opts.ContinueWithError {
					for j := i - 1; j >= 0; j-- {
						skipped = append(skipped, s.undos[j].name)
					}
					break
				}
			}
		}
	}

	if len(failed) == 0 && len(skipped) == 0 {
		return cause
	}
	return newCompensationError(cause, failed, skipped, first)
}
