package stepImpl

// The steps for docs/specs/approval.spec. The outcome steps ("saga は失敗する",
// "ステップ ... が実行された" and so on) are shared with the rollback
// specification: they read the workflow from the scenario store and do not
// care which workflow put it there.

import (
	"context"
	"strconv"

	"github.com/getgauge-contrib/gauge-go/gauge"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-saga/example/approval"
	"github.com/yamakura-yuma/temporal-saga/example/order"
)

var _ = gauge.Step("承認待ちの注文 <id>", func(id string) {
	startApproval(id, 0)
})

var _ = gauge.Step("承認を <seconds> 秒だけ待つ注文 <id>", func(seconds, id string) {
	n, err := strconv.Atoi(seconds)
	if err != nil {
		fail("%q は秒数として読めません: %v", seconds, err)
	}
	startApproval(id, n)
})

var _ = gauge.Step("承認を送る", func() {
	signal(approval.Decision{Approved: true, By: "reviewer"})
})

var _ = gauge.Step("却下を送る", func() {
	signal(approval.Decision{Approved: false, By: "reviewer"})
})

func startApproval(id string, waitSeconds int) {
	in := approval.Request{
		Order:       order.Order{ID: id, SKU: "widget", Amount: 4200},
		WaitSeconds: waitSeconds,
	}

	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "approval-" + id, TaskQueue: approval.TaskQueue},
		approval.ApprovalWorkflow, in)
	if err != nil {
		fail("could not start the approval saga: %v", err)
	}
	gauge.GetScenarioStore()[keyRun] = run
}

// signal sends the decision. Temporal buffers a signal that arrives before the
// workflow reaches its selector, so there is nothing to wait for here.
func signal(d approval.Decision) {
	run := currentRun()
	err := temporalClient.SignalWorkflow(context.Background(),
		run.GetID(), run.GetRunID(), approval.ApprovalSignal, d)
	if err != nil {
		fail("could not signal the saga: %v", err)
	}
}
