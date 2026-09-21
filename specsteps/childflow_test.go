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
	"go.temporal.io/sdk/converter"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/activity"
	"github.com/yamakura-yuma/temporal-workflow-kit/example/workflow/childflow"
)

func registerChildflowSteps(sc *godog.ScenarioContext) {
	sc.Step(`^子ワークフローを含む注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startChildflow(sampleLine(id, ""))
	})

	sc.Step(`^子ワークフローを含む注文 "([^"]*)" を "([^"]*)" で失敗させる$`,
		func(ctx context.Context, id, step string) error {
			return stateOf(ctx).startChildflow(sampleLine(id, step))
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

		run, err := s.currentRun()
		if err != nil {
			return err
		}

		// What the compensation acted under. Its ActivityID is the key with
		// ":undo" on the end, which is what saga.IdempotencyKey strips off.
		undone := run.GetRunID() + "/pack"

		// What the child passed down. The packing activity runs inside the
		// child, so the child's own history is what records it -- and it
		// records the value the child read with saga.IdempotencyKeyOf, not a
		// value any activity reported about itself.
		child, err := s.historyOf(undone, "", "")
		if err != nil {
			return fmt.Errorf("梱包の子の履歴を読めません: %w", err)
		}
		if len(child.scheduled) == 0 {
			return errors.New("梱包の子はアクティビティを実行していません")
		}

		var packed struct {
			Key string `json:"key"`
		}
		if err := converter.GetDefaultDataConverter().
			FromPayloads(child.input[child.scheduled[0]], &packed); err != nil {
			return fmt.Errorf("梱包の子が渡した入力を読めません: %w", err)
		}

		if packed.Key == "" {
			return errors.New("梱包の子が冪等キーを読めていません")
		}
		if packed.Key != undone {
			return fmt.Errorf("冪等キーが一致しません: pack=%q unpack=%q", packed.Key, undone)
		}
		return nil
	})
}

func (s *scenarioState) startChildflow(in activity.Order) error {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "childflow-" + in.ID, TaskQueue: childflow.TaskQueue},
		childflow.ChildflowWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the childflow saga: %w", err)
	}
	s.run = run
	return nil
}
