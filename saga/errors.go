package saga

import (
	"fmt"
	"strings"

	"go.temporal.io/sdk/temporal"
)

// CompensationFailedType is the error type a saga fails with when the
// compensation phase did not finish cleanly. Match on it in a parent workflow,
// or list it in a RetryPolicy.
const CompensationFailedType = "CompensationFailed"

// CompensationReport names the steps whose compensation did not succeed. It
// travels as the error's details, so it is readable from the workflow history
// and from a caller that does errors.As on *temporal.ApplicationError.
type CompensationReport struct {
	// Failed lists steps whose compensation ran and returned an error.
	Failed []string `json:"failed"`
	// Skipped lists steps whose compensation never ran because
	// StopOnCompensationError was set and an earlier one failed.
	Skipped []string `json:"skipped"`
}

// newCompensationError builds the error a saga fails with when compensation did
// not finish cleanly.
//
// It deliberately does not use errors.Join. Temporal's failure converter is a
// type switch over concrete types and follows a single Unwrap() error chain, so
// a joined error lands in the default branch: the type name becomes
// "joinError", the cause chain is dropped, and NonRetryableErrorTypes stops
// matching. A single-cause ApplicationError keeps the original failure intact
// in the history.
//
// The error is non-retryable so that a workflow-level retry policy cannot start
// the saga over on top of a half-completed rollback. That case needs a human.
func newCompensationError(cause error, failed, skipped []string, first error) error {
	var b strings.Builder
	b.WriteString("saga: compensation did not finish cleanly")
	if len(failed) > 0 {
		fmt.Fprintf(&b, "; failed: %s", strings.Join(failed, ", "))
	}
	if len(skipped) > 0 {
		fmt.Fprintf(&b, "; skipped: %s", strings.Join(skipped, ", "))
	}
	if first != nil {
		fmt.Fprintf(&b, "; first failure: %v", first)
	}

	return temporal.NewApplicationErrorWithOptions(
		b.String(),
		CompensationFailedType,
		temporal.ApplicationErrorOptions{
			Cause:        cause,
			NonRetryable: true,
			Details:      []any{CompensationReport{Failed: failed, Skipped: skipped}},
		},
	)
}
