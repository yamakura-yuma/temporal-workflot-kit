package specsteps

// The steps for docs/specs/pipeline.feature.

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/pipeline"
)

func registerPipelineSteps(sc *godog.ScenarioContext) {
	sc.Step(`^連鎖する注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startPipeline(pipeline.Order{ID: id, SKU: "widget", Amount: 4200})
	})

	sc.Step(`^連鎖する注文 "([^"]*)" を "([^"]*)" で失敗させる$`,
		func(ctx context.Context, id, step string) error {
			return stateOf(ctx).startPipeline(pipeline.Order{ID: id, SKU: "widget", Amount: 4200, FailAt: step})
		})

	sc.Step(`^補償 "([^"]*)" が受け取った前段の ID は "([^"]*)"$`,
		func(ctx context.Context, step, upstream string) error {
			s := stateOf(ctx)
			if _, err := s.outcome(); err != nil {
				return err
			}
			if got := s.pipeline.Upstream(step); got != upstream {
				return fmt.Errorf("補償 %q が受け取った前段の ID: got %q, want %q", step, got, upstream)
			}
			return nil
		})
}

func (s *scenarioState) startPipeline(in pipeline.Order) error {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "pipeline-" + in.ID, TaskQueue: pipeline.TaskQueue},
		pipeline.PipelineWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the pipeline saga: %w", err)
	}
	s.run = run
	return nil
}
