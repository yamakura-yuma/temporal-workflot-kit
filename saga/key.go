package saga

import (
	"context"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// IdempotencyKey returns the saga step's idempotency key, and reports whether
// there was one. Call it from inside an activity.
//
// A step's forward activity and its compensation observe the same key, which is
// what lets a compensation find out whether the work it undoes actually
// happened.
//
// It reports false rather than panicking when ctx is not an activity context,
// so that activities stay unit-testable without a Temporal environment --
// activity.GetInfo panics on a plain context.
func IdempotencyKey(ctx context.Context) (key string, ok bool) {
	defer func() {
		if recover() != nil {
			key, ok = "", false
		}
	}()

	id := activity.GetInfo(ctx).ActivityID
	if id == "" {
		return "", false
	}
	return strings.TrimSuffix(id, undoSuffix), true
}

// IdempotencyKeyOf is IdempotencyKey for a child workflow started by a step
// built with ChildWorkflow or UndoChildWorkflow.
// The key rides in the child's WorkflowID.
//
// A forward child and the child that compensates it see the same key, which is
// how the compensation finds out whether the work happened.
func IdempotencyKeyOf(ctx workflow.Context) (string, bool) {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	if id == "" {
		return "", false
	}
	return strings.TrimSuffix(id, undoSuffix), true
}
