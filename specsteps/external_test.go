package specsteps

// The steps for docs/specs/external.feature.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cucumber/godog"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/external"
)

func registerExternalSteps(sc *godog.ScenarioContext) {
	sc.Step(`^在庫ワークフロー "([^"]*)" を起動する$`, func(ctx context.Context, id string) error {
		run, err := temporalClient.ExecuteWorkflow(context.Background(),
			client.StartWorkflowOptions{ID: id, TaskQueue: external.TaskQueue},
			external.InventoryWorkflow, 2*time.Minute)
		if err != nil {
			return fmt.Errorf("could not start the inventory workflow: %w", err)
		}
		stateOf(ctx).inventory = run
		return nil
	})

	sc.Step(`^在庫を押さえる注文 "([^"]*)"$`, func(ctx context.Context, id string) error {
		return stateOf(ctx).startExternal(id, "")
	})

	sc.Step(`^在庫を押さえる注文 "([^"]*)" を "([^"]*)" で失敗させる$`,
		func(ctx context.Context, id, step string) error {
			return stateOf(ctx).startExternal(id, step)
		})

	sc.Step(`^在庫ワークフロー "([^"]*)" の "([^"]*)" の確保数は "([^"]*)"$`,
		func(ctx context.Context, id, sku, want string) error {
			if _, err := stateOf(ctx).outcome(); err != nil {
				return err
			}

			wanted, err := strconv.Atoi(want)
			if err != nil {
				return fmt.Errorf("%q は数として読めません: %w", want, err)
			}

			// The saga has finished, but the signal it sent during compensation is
			// delivered asynchronously, so give the inventory workflow a moment.
			deadline := time.Now().Add(20 * time.Second)
			for {
				got, err := heldQuantity(id, sku)
				if err != nil {
					return err
				}
				if got == wanted {
					return nil
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("在庫 %q の %q: got %d, want %d", id, sku, got, wanted)
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
}

func heldQuantity(workflowID, sku string) (int, error) {
	value, err := temporalClient.QueryWorkflow(context.Background(), workflowID, "", external.HeldQuery)
	if err != nil {
		return 0, fmt.Errorf("could not query the inventory workflow: %w", err)
	}

	held := map[string]int{}
	if err := value.Get(&held); err != nil {
		return 0, fmt.Errorf("could not decode the inventory: %w", err)
	}
	return held[sku], nil
}

func (s *scenarioState) startExternal(id, failAt string) error {
	if s.inventory == nil {
		return errors.New("在庫ワークフローが起動されていません")
	}

	in := external.Order{
		ID:        id,
		Inventory: s.inventory.GetID(),
		SKU:       "widget",
		Quantity:  2,
		Amount:    4200,
		FailAt:    failAt,
	}

	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "external-" + id, TaskQueue: external.TaskQueue},
		external.ExternalWorkflow, in)
	if err != nil {
		return fmt.Errorf("could not start the external saga: %w", err)
	}
	s.run = run
	return nil
}
