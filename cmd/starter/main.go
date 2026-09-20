package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-saga/internal/workflow"
)

func main() {
	hostPort := os.Getenv("TEMPORAL_ADDRESS")
	if hostPort == "" {
		hostPort = client.DefaultHostPort
	}

	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	options := client.StartWorkflowOptions{
		ID:        "saga-workflow",
		TaskQueue: workflow.TaskQueue,
	}

	we, err := c.ExecuteWorkflow(context.Background(), options, workflow.SagaWorkflow, "temporal-saga")
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}
	log.Printf("started workflow, WorkflowID: %s RunID: %s", we.GetID(), we.GetRunID())

	var result string
	if err := we.Get(context.Background(), &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	log.Printf("workflow result: %s", result)
}
