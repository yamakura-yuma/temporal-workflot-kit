package specsteps

// The steps for docs/specs/childflow.feature.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cucumber/godog"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/childflow"
)

func registerChildflowSteps(sc *godog.ScenarioContext) {
	sc.Step(`^子ワークフローを含む注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startChildflow(childflow.Order{ID: id, SKU: "widget"})
	})

	sc.Step(`^子ワークフローを含む注文 "([^"]*)" を "([^"]*)" で失敗させる$`,
		func(ctx context.Context, id, step string) error {
			return stateOf(ctx).startChildflow(childflow.Order{ID: id, SKU: "widget", FailAt: step})
		})

	// The child workflows appear in the parent's history as
	// StartChildWorkflowExecutionInitiated, not as activity events, so this reads a
	// different event type from "ステップ ... が実行された".
	sc.Step(`^子ワークフロー "([^"]*)" が起動された$`, func(ctx context.Context, names string) error {
		s := stateOf(ctx)
		if _, err := s.outcome(); err != nil {
			return err
		}

		run, err := s.currentRun()
		if err != nil {
			return err
		}
		prefix := run.GetRunID() + "/"

		iter := s.client.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID(),
			false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

		var started []string
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				return fmt.Errorf("could not read the history: %w", err)
			}
			if event.GetEventType() != enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
				continue
			}
			id := event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId()
			if !strings.HasPrefix(id, prefix) {
				return fmt.Errorf("child workflow id %q is not scoped to the run", id)
			}
			started = append(started, strings.TrimPrefix(id, prefix))
		}

		if got, want := strings.Join(started, ", "), names; got != want {
			return fmt.Errorf("started child workflows:\n  got:  %s\n  want: %s", got, want)
		}
		return nil
	})

	sc.Step(`^梱包と取り消しが見た冪等キーは一致する$`, func(ctx context.Context) error {
		s := stateOf(ctx)
		if _, err := s.outcome(); err != nil {
			return err
		}

		pack := s.childflow.KeySeenBy("pack")
		unpack := s.childflow.KeySeenBy("unpack")

		if pack == "" {
			return errors.New("梱包の子が冪等キーを読めていません")
		}
		if pack != unpack {
			return fmt.Errorf("冪等キーが一致しません: pack=%q unpack=%q", pack, unpack)
		}
		return nil
	})
}

func (s *scenarioState) startChildflow(in childflow.Order) error {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "childflow-" + in.ID, TaskQueue: childflow.TaskQueue},
		childflow.ChildflowWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the childflow saga: %w", err)
	}
	s.run = run
	return nil
}
