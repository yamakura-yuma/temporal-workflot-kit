package saga

// This file is an extension. Neither the Java SDK's Saga nor the PHP port of it
// has an equivalent: both leave the caller to write the try/catch and to
// remember to call compensate(). RunOrCompensate owns the rollback instead, so
// the body has one way out and there is nothing to forget.

import (
	"errors"

	"go.temporal.io/sdk/workflow"
)

// RunOrCompensate executes body as a saga and compensates it if body fails.
//
// The compensation phase runs on a disconnected context, so it still works when
// the workflow itself is being canceled -- which is exactly when it matters.
// Compensations run in reverse order of registration, unless
// Options.ParallelCompensation says otherwise.
//
// body returning a nil error is not enough to be treated as success: if any
// step inside it failed, RunOrCompensate compensates and returns that error, discarding
// body's return value. That is deliberate. With the sticky-error behaviour of
// Step, a caller who forgets to check an error would otherwise return a
// half-filled result and the workflow would be recorded as completed with its
// side effects half applied.
//
// A step's failure also outranks an error the body returns on its own. Once a
// step has failed, later steps are no-ops and a wait returns at once, so
// the body tends to reach a branch that reads a zero value and reports
// something untrue -- "nobody approved this" when the truth is "the
// reservation failed". RunOrCompensate reports the step's failure instead. Call s.ClearErr()
// before returning your own error if you have handled the step failure and
// mean to replace it.
//
// It compensates on the way out of body rather than from a deferred function.
// A panic in workflow code fails the workflow task and the whole workflow is
// replayed, so a panic is not a saga failure and there is nothing to undo: a
// deferred rollback would undo a workflow that is about to run again.
func RunOrCompensate[T any](ctx workflow.Context, o Options, body func(workflow.Context, *Saga) (T, error)) (T, error) {
	var zero T

	s := newSaga(o)

	out, err := body(ctx, s)

	// The first failure wins. A step that failed is the root cause; whatever
	// the body returned afterwards is fallout from it -- often a branch that
	// read a zero value and drew the wrong conclusion. Reporting the body's
	// error instead would bury the real one.
	//
	// To report an error of your own after handling a step failure, call
	// s.ClearErr() first. That is what it is for.
	if s.err != nil {
		err = s.err
	}
	if err == nil {
		return out, nil
	}

	// ContinueAsNew is delivered as an error but the saga is not over, so there
	// is nothing to undo. The SDK classifies it the same way.
	var continueAsNew *workflow.ContinueAsNewError
	if errors.As(err, &continueAsNew) {
		return out, err
	}

	return zero, s.compensate(ctx, err)
}

// Err reports the first error a step reported, if any. Once it is non-nil every
// later step is a no-op, so a linear saga can skip the error check between
// steps and let RunOrCompensate deal with the outcome.
func (s *Saga) Err() error { return s.err }

// ClearErr forgets the error Err reports, so that later steps run again and an
// error the body returns is reported instead of the step's. It leaves the
// compensations registered so far alone -- it clears the error, nothing else.
//
// Use it only when the failure was genuinely handled. It also stops
// RunOrCompensate from treating the saga as failed, so those compensations will
// not run unless a later step fails or the body returns an error.
func (s *Saga) ClearErr() { s.err = nil }
