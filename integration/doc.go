// Package integration exercises the saga package against a real Temporal
// server rather than the in-memory test environment.
//
// The tests are behind a build tag because they start a server process:
//
//	just test-integration      # or: go test -tags=integration ./integration/
//
// They cover what the unit tests in saga/ cannot. The test environment runs
// activities even on a canceled context and does not enforce activity
// timeouts, so "the rollback still happens after a cancel" and "the compensated
// activity IDs appear in this order in the history" are only really answered by
// a server. So is the search attribute a saga sets when a compensation fails,
// which has to be registered on the server before it can be written.
//
// order_test.go is also the worked example: activities that claim their
// idempotency key atomically and compensations that succeed when they find
// nothing to undo.
package integration
