package stepImpl

// The steps for docs/specs/external.spec.

import (
	"context"
	"strconv"
	"time"

	"github.com/getgauge-contrib/gauge-go/gauge"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/external"
)

// keyInventory holds the inventory workflow's run for the scenario.
const keyInventory = "inventory"

var _ = gauge.Step("在庫ワークフロー <id> を起動する", func(id string) {
	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: id, TaskQueue: external.TaskQueue},
		external.InventoryWorkflow, 2*time.Minute)
	if err != nil {
		fail("could not start the inventory workflow: %v", err)
	}
	gauge.GetScenarioStore()[keyInventory] = run
})

var _ = gauge.Step("在庫を押さえる注文 <id>", func(id string) {
	startExternal(id, "")
})

var _ = gauge.Step("在庫を押さえる注文 <id> を <step> で失敗させる", func(id, step string) {
	startExternal(id, step)
})

var _ = gauge.Step("在庫ワークフロー <id> の <sku> の確保数は <want>", func(id, sku, want string) {
	awaitResult()

	wanted, err := strconv.Atoi(want)
	if err != nil {
		fail("%q は数として読めません: %v", want, err)
	}

	// The saga has finished, but the signal it sent during compensation is
	// delivered asynchronously, so give the inventory workflow a moment.
	deadline := time.Now().Add(20 * time.Second)
	for {
		got := heldQuantity(id, sku)
		if got == wanted {
			return
		}
		if time.Now().After(deadline) {
			fail("在庫 %q の %q: got %d, want %d", id, sku, got, wanted)
		}
		time.Sleep(50 * time.Millisecond)
	}
})

func heldQuantity(workflowID, sku string) int {
	value, err := temporalClient.QueryWorkflow(context.Background(), workflowID, "", external.HeldQuery)
	if err != nil {
		fail("could not query the inventory workflow: %v", err)
	}

	held := map[string]int{}
	if err := value.Get(&held); err != nil {
		fail("could not decode the inventory: %v", err)
	}
	return held[sku]
}

func startExternal(id, failAt string) {
	inventory, ok := gauge.GetScenarioStore()[keyInventory].(client.WorkflowRun)
	if !ok {
		fail("在庫ワークフローが起動されていません")
	}

	in := external.Order{
		ID:        id,
		Inventory: inventory.GetID(),
		SKU:       "widget",
		Quantity:  2,
		Amount:    4200,
		FailAt:    failAt,
	}

	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "external-" + id, TaskQueue: external.TaskQueue},
		external.ExternalWorkflow, in)
	if err != nil {
		fail("could not start the external saga: %v", err)
	}
	gauge.GetScenarioStore()[keyRun] = run
}
