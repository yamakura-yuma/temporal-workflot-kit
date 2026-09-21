package stepImpl

// The steps for docs/specs/childflow.spec.

import (
	"context"
	"strings"

	"github.com/getgauge-contrib/gauge-go/gauge"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/childflow"
)

var _ = gauge.Step("子ワークフローを含む注文 <id>", func(id string) {
	startChildflow(childflow.Order{ID: id, SKU: "widget"})
})

var _ = gauge.Step("子ワークフローを含む注文 <id> を <step> で失敗させる", func(id, step string) {
	startChildflow(childflow.Order{ID: id, SKU: "widget", FailAt: step})
})

// The child workflows appear in the parent's history as
// StartChildWorkflowExecutionInitiated, not as activity events, so this reads a
// different event type from "ステップ ... が実行された".
var _ = gauge.Step("子ワークフロー <names> が起動された", func(names string) {
	awaitResult()

	run := currentRun()
	prefix := run.GetRunID() + "/"

	iter := temporalClient.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID(),
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var started []string
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			fail("could not read the history: %v", err)
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			continue
		}
		id := event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId()
		if !strings.HasPrefix(id, prefix) {
			fail("child workflow id %q is not scoped to the run", id)
		}
		started = append(started, strings.TrimPrefix(id, prefix))
	}

	if got, want := strings.Join(started, ", "), names; got != want {
		fail("started child workflows:\n  got:  %s\n  want: %s", got, want)
	}
})

var _ = gauge.Step("梱包と取り消しが見た冪等キーは一致する", func() {
	awaitResult()

	pack := childflowLedger.KeySeenBy("pack")
	unpack := childflowLedger.KeySeenBy("unpack")

	if pack == "" {
		fail("梱包の子が冪等キーを読めていません")
	}
	if pack != unpack {
		fail("冪等キーが一致しません: pack=%q unpack=%q", pack, unpack)
	}
})

func startChildflow(in childflow.Order) {
	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "childflow-" + in.ID, TaskQueue: childflow.TaskQueue},
		childflow.ChildflowWorkflow, in)
	if err != nil {
		fail("could not start the childflow saga: %v", err)
	}
	gauge.GetScenarioStore()[keyRun] = run
}
