package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/yamakura-yuma/temporal-saga/internal/activity"
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

	w := worker.New(c, workflow.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflow.SagaWorkflow)
	w.RegisterActivity(activity.Greet)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}
