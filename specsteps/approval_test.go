package specsteps

// The steps for docs/specs/approval.feature. The outcome steps live in steps_test.go.

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cucumber/godog"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/approval"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/order"
)

func registerApprovalSteps(sc *godog.ScenarioContext) {
	sc.Step(`^承認待ちの注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startApproval(id, 0)
	})

	sc.Step(`^承認を "([^"]*)" 秒だけ待つ注文 "([^"]*)"$`, func(ctx context.Context, seconds, id string) error {
		n, err := strconv.Atoi(seconds)
		if err != nil {
			return fmt.Errorf("%q は秒数として読めません: %w", seconds, err)
		}
		return stateOf(ctx).startApproval(id, n)
	})

	sc.Step(`^承認を送る$`, func(ctx context.Context) error {
		return stateOf(ctx).signal(approval.Decision{Approved: true, By: "reviewer"})
	})

	sc.Step(`^却下を送る$`, func(ctx context.Context) error {
		return stateOf(ctx).signal(approval.Decision{Approved: false, By: "reviewer"})
	})
}

func (s *scenarioState) startApproval(id string, waitSeconds int) error {
	in := approval.Request{
		Order:       order.Order{ID: id, SKU: "widget", Amount: 4200},
		WaitSeconds: waitSeconds,
	}

	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "approval-" + id, TaskQueue: approval.TaskQueue},
		approval.ApprovalWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the approval saga: %w", err)
	}
	s.run = run
	return nil
}

// signal sends the decision. Temporal buffers a signal that arrives before the
// workflow reaches its selector, so there is nothing to wait for here.
func (s *scenarioState) signal(d approval.Decision) error {
	run, err := s.currentRun()
	if err != nil {
		return err
	}
	if err := s.client.SignalWorkflow(context.Background(),
		run.GetID(), run.GetRunID(), approval.ApprovalSignal, d); err != nil {
		return fmt.Errorf("could not signal the saga: %w", err)
	}
	return nil
}
