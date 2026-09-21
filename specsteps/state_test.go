package specsteps

// The steps for docs/specs/state.feature. The outcome steps live in
// steps_test.go.

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

	sc.Step(`^状態を持つ注文の承認を送る$`, func(ctx context.Context) error {
		return stateOf(ctx).approveState(true)
	})

	sc.Step(`^状態を持つ注文の却下を送る$`, func(ctx context.Context) error {
		return stateOf(ctx).approveState(false)
	})

	sc.Step(`^同じ注文 "([^"]*)" を2つの書き方で動かす$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startBothShapes(sampleOrder(id, ""))
	})

	sc.Step(`^どちらの書き方も同じ Receipt を返す$`, func(ctx context.Context) error {
		return stateOf(ctx).compareShapes()
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
	run, err := s.startShape("state-"+in.ID, state.StateWorkflow, in)
	if err != nil {
		return err
	}
	s.run = run
	return nil
}

// startBothShapes runs the same order through the state shape and the flat one.
// They are separate workflows, so their steps claim separate idempotency keys
// and neither sees the other's rows.
func (s *scenarioState) startBothShapes(in state.Order) error {
	run, err := s.startShape("state-"+in.ID, state.StateWorkflow, in)
	if err != nil {
		return err
	}
	flat, err := s.startShape("flat-"+in.ID, state.FlatWorkflow, in)
	if err != nil {
		return err
	}
	s.run, s.flat = run, flat
	return nil
}

func (s *scenarioState) startShape(id string, wf any, in state.Order) (client.WorkflowRun, error) {
	run, err := s.client.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: id, TaskQueue: state.TaskQueue}, wf, in)
	if err != nil {
		return nil, fmt.Errorf("could not start the state saga %q: %w", id, err)
	}
	return run, nil
}

// approveState answers every saga the scenario started. Temporal buffers a
// signal that arrives before the workflow reaches its selector, so there is
// nothing to wait for here.
func (s *scenarioState) approveState(approved bool) error {
	if _, err := s.currentRun(); err != nil {
		return err
	}

	decision := state.Decision{Approved: approved, By: "reviewer"}
	for _, run := range []client.WorkflowRun{s.run, s.flat} {
		if run == nil {
			continue
		}
		if err := s.client.SignalWorkflow(context.Background(),
			run.GetID(), run.GetRunID(), state.ApprovalSignal, decision); err != nil {
			return fmt.Errorf("could not signal %q: %w", run.GetID(), err)
		}
	}
	return nil
}

// compareShapes is what keeps workflow_flat.go honest: the two shapes are only
// worth reading against each other if they really are the same saga.
func (s *scenarioState) compareShapes() error {
	if s.run == nil || s.flat == nil {
		return fmt.Errorf("both shapes have to be started first")
	}

	var fromState, fromFlat state.Receipt
	if err := s.run.Get(context.Background(), &fromState); err != nil {
		return fmt.Errorf("the state shape failed: %w", err)
	}
	if err := s.flat.Get(context.Background(), &fromFlat); err != nil {
		return fmt.Errorf("the flat shape failed: %w", err)
	}

	if fromState != fromFlat {
		return fmt.Errorf("the two shapes disagree:\n  state: %+v\n  flat:  %+v", fromState, fromFlat)
	}
	return nil
}
