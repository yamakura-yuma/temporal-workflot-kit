---
name: go-temporal-review
description: >-
  Use when reviewing or writing Go code in this repo that touches a Temporal
  workflow or activity — workflow functions, activity functions, worker
  registration, or anything under internal/workflow or internal/activity.
  Checks workflow determinism, activity idempotency, and retry/error-handling
  conventions before the change ships.
---

# Go + Temporal review

Load `references/checklist.md` and walk every changed file under
`internal/workflow/` or `internal/activity/` against it before approving.

The three failure classes that matter most here:

1. **Non-determinism in workflow code** — anything that can produce a
   different result on replay (time, randomness, goroutines, map iteration,
   direct I/O) breaks Temporal's replay model. Workflow code may only reach
   these through the SDK's deterministic equivalents (`workflow.Now`,
   `workflow.SideEffect`, `workflow.Go`, `workflow.NewTimer`, etc.).
2. **Non-idempotent activities** — activities can be retried and can execute
   more than once for the same logical attempt. An activity that isn't safe
   to run twice (e.g. a payment charge with no idempotency key) is a bug, not
   a style issue.
3. **Missing or unbounded retry policy** — every `workflow.ActivityOptions`
   needs an explicit `StartToCloseTimeout` and a considered `RetryPolicy`
   (don't rely on SDK defaults for anything that isn't a placeholder).

See `references/checklist.md` for the full item-by-item list and rationale.
