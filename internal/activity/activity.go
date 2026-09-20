package activity

import "context"

// Greet is a placeholder activity that proves the worker/workflow/activity
// wiring compiles and runs end-to-end against a real Temporal server.
func Greet(ctx context.Context, name string) (string, error) {
	return "Hello, " + name + "!", nil
}
