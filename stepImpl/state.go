package stepImpl

// The steps for docs/specs/state.spec.

import (
	"context"

	"github.com/getgauge-contrib/gauge-go/gauge"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-saga/example/state"
)

var _ = gauge.Step("状態を持つ注文 <id>", func(id string) {
	startState(sampleOrder(id, ""))
})

var _ = gauge.Step("状態を持つ注文 <id> を <step> で失敗させる", func(id, step string) {
	startState(sampleOrder(id, step))
})

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

func startState(in state.Order) {
	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "state-" + in.ID, TaskQueue: state.TaskQueue},
		state.StateWorkflow, in)
	if err != nil {
		fail("could not start the state saga: %v", err)
	}
	gauge.GetScenarioStore()[keyRun] = run
}
