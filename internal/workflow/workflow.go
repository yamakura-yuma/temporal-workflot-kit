package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-saga/internal/activity"
)

// TaskQueue is shared between the worker and workflow starters.
const TaskQueue = "saga-task-queue"

// SagaWorkflow is a placeholder workflow that proves the worker/workflow/activity
// wiring compiles and runs end-to-end against a real Temporal server. Real saga
// steps and compensations will replace this.
func SagaWorkflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, activity.Greet, name).Get(ctx, &result)
	if err != nil {
		return "", err
	}
	return result, nil
}
