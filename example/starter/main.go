package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/yamakura-yuma/temporal-saga/example/order"
)

func main() {
	failAt := flag.String("fail", "", "step to fail at: reserve, charge or ship. Empty runs the happy path")
	orderID := flag.String("order", "order-1", "order id, which also seeds the workflow id")
	hold := flag.Int("hold", 0, "seconds to wait after the charge step, so the workflow can be canceled mid-saga")
	flag.Parse()

	hostPort := os.Getenv("TEMPORAL_ADDRESS")
	if hostPort == "" {
		hostPort = client.DefaultHostPort
	}

	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	in := order.Order{ID: *orderID, SKU: "widget", Amount: 4200, FailAt: *failAt, HoldSeconds: *hold}

	options := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("saga-%s", *orderID),
		TaskQueue: order.TaskQueue,
	}

	we, err := c.ExecuteWorkflow(context.Background(), options, order.OrderWorkflow, in)
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}
	log.Printf("started workflow, WorkflowID: %s RunID: %s", we.GetID(), we.GetRunID())

	var out order.Receipt
	if err := we.Get(context.Background(), &out); err != nil {
		// Expected when -fail is set: the saga rolled back and reported why.
		log.Fatalf("workflow failed: %v", err)
	}
	log.Printf("workflow result: %+v", out)
}
