package stepImpl

// The steps for docs/specs/pipeline.spec.

import (
	"context"

	"github.com/getgauge-contrib/gauge-go/gauge"
	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-workflow-kit/example/pipeline"
)

var _ = gauge.Step("連鎖する注文 <id>", func(id string) {
	startPipeline(pipeline.Order{ID: id, SKU: "widget", Amount: 4200})
})

var _ = gauge.Step("連鎖する注文 <id> を <step> で失敗させる", func(id, step string) {
	startPipeline(pipeline.Order{ID: id, SKU: "widget", Amount: 4200, FailAt: step})
})

var _ = gauge.Step("補償 <step> が受け取った前段の ID は <upstream>", func(step, upstream string) {
	awaitResult()
	if got := pipelineLedger.Upstream(step); got != upstream {
		fail("補償 %q が受け取った前段の ID: got %q, want %q", step, got, upstream)
	}
})

func startPipeline(in pipeline.Order) {
	run, err := temporalClient.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "pipeline-" + in.ID, TaskQueue: pipeline.TaskQueue},
		pipeline.PipelineWorkflow, in)
	if err != nil {
		fail("could not start the pipeline saga: %v", err)
	}
	gauge.GetScenarioStore()[keyRun] = run
}
