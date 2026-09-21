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

		// The key the compensation was handed, out of the parent's history.
		parent, err := s.history()
		if err != nil {
			return err
		}
		undone, err := packKeyIn(parent, "Unpack")
		if err != nil {
			return fmt.Errorf("取り消し側: %w", err)
		}

		// The key the packing child handed its activity, out of the child's own
		// history. The child runs the activity, so only the child's history
		// records what it passed down.
		child, err := s.historyOf(run.GetRunID()+"/pack", "", "")
		if err != nil {
			return fmt.Errorf("梱包の子の履歴を読めません: %w", err)
		}
		if len(child.scheduled) == 0 {
			return errors.New("梱包の子はアクティビティを実行していません")
		}
		packed, err := packKeyIn(child, child.scheduled[0])
		if err != nil {
			return fmt.Errorf("梱包側: %w", err)
		}

		if packed == "" {
			return errors.New("梱包の子が冪等キーを渡していません")
		}
		if packed != undone {
			return fmt.Errorf("冪等キーが一致しません: pack=%q unpack=%q", packed, undone)
		}
		return nil
	})
}

// packKeyIn decodes the idempotency key out of the PackReq an activity was
// handed.
func packKeyIn(h *sagaHistory, name string) (string, error) {
	payloads, ok := h.input[name]
	if !ok {
		return "", fmt.Errorf("アクティビティ %q は実行されていません", name)
	}

	var req struct {
		Key string `json:"key"`
	}
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &req); err != nil {
		return "", fmt.Errorf("入力を読めません: %w", err)
	}
	return req.Key, nil
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
