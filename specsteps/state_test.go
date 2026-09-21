package specsteps

// The steps for docs/specs/state.feature.

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/state"
)

func registerStateSteps(sc *godog.ScenarioContext) {
	sc.Step(`^状態を持つ注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startState(sampleOrder(id, ""))
	})

	sc.Step(`^状態を持つ注文 "([^"]*)" を "([^"]*)" で失敗させる$`,
		func(ctx context.Context, id, step string) error {
			return stateOf(ctx).startState(sampleOrder(id, step))
		})
}

func sampleOrder(id, failAt string) state.Order {
	return state.Order{
		ID:       id,
		Customer: "c-" + id,
		SKU:      "widget",
		Quantity: 2,
		Amount:   4200,
		Currency: "JPY",
		Address:  "Tokyo",
		FailAt:   failAt,
	}
}

func (s *scenarioState) startState(in state.Order) error {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "state-" + in.ID, TaskQueue: state.TaskQueue},
		state.StateWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the state saga: %w", err)
	}
	s.run = run
	return nil
}
