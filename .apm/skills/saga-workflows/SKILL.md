---
name: saga-workflows
description: >-
  Use when working on the saga library in this repo — the saga/ package, the
  example workflow and activities under example/, or any change to how a step
  and its compensation are wired. Covers the invariants the library depends on,
  the contract it puts on activities, and the determinism, idempotency and
  retry rules a change has to satisfy before it ships.
---

# The saga library in this repo

This repo publishes `saga`, a package for running a sequence of Temporal
activities that can be rolled back. It is a library, not an application: the
workflow under `example/` exists to exercise it.

## Where things live

| | |
| --- | --- |
| `saga/` | The library. `Run` owns the rollback, `Step` runs one forward activity and registers its compensation. |
| `example/order/` | An example saga: reserve, charge, ship, each with a compensation. |
| `example/worker/` | Registers the workflow and one `Activities` instance, polls the task queue. |
| `example/starter/` | Starts one execution. `-fail=<step>` forces a rollback. |

Nothing belongs under `internal/`: a library that lives there cannot be
imported by anyone outside this module.

## Invariants the library depends on

Each of these exists because the obvious alternative is broken. Do not
"simplify" one without reading the test that covers it.

- **Compensations run on a disconnected context.** A canceled workflow context
  fails every later activity immediately, so compensation written the obvious
  way does nothing in the one case it exists for.
- **The compensation is registered before the forward activity runs**, not
  after it succeeds. An activity that reports a timeout may still have taken
  effect on a worker that never reported back.
- **The idempotency key is `RunID + "/" + step name`.** Not `FirstRunID`: that
  is preserved across ContinueAsNew, Retry, Cron and Reset, so a second run
  would reuse the first run's keys and every step would look like one that had
  already been applied. Not a positional counter either: inserting a step would
  shift every key after it.
- **The compensation's `ActivityID` carries a `:undo` suffix.** A duplicate
  command ID panics the workflow task. `IdempotencyKey` strips it again so both
  halves of a step observe the same key.
- **`Run` fails the workflow when a step failed, even if the body returned
  nil**, and discards the body's result. Otherwise one missing error check
  completes a workflow whose side effects are half applied.
- **Compensation failures are not joined with `errors.Join`.** Temporal's
  failure converter is a type switch that follows a single `Unwrap() error`; a
  joined error is recorded as type `joinError` with no cause, and
  `NonRetryableErrorTypes` stops matching.
- **The compensation budget is required**, because nothing outside can cancel a
  disconnected context.

## Adding a saga step

1. Write the forward activity and its compensation as methods on the same
   struct, so they can share the ledger that makes them idempotent.
2. The forward activity must **claim its idempotency key atomically** —
   `INSERT ... ON CONFLICT`, a unique constraint, or the downstream API's own
   idempotency-key header. Reading the key and then acting on it is not enough:
   two attempts can be in flight at once after a timeout.
3. The compensation must **succeed when it finds nothing to undo**.
4. Call `saga.Step(ctx, s, "<name>", fwd, undo, in)` with a name unique within
   the saga. The step error can be ignored in a linear saga; `Run` handles it.
5. Register the activities on the worker. An activity the workflow reaches but
   the worker never registered fails at run time, not compile time.

## Before the change ships

Walk every changed file under `saga/` or `example/` against
`references/checklist.md`. The three failure classes that matter most:

1. **Non-determinism in workflow code** — anything that can produce a
   different result on replay (time, randomness, goroutines, map iteration,
   direct I/O) breaks Temporal's replay model. Workflow code reaches these only
   through the SDK's deterministic equivalents (`workflow.Now`,
   `workflow.SideEffect`, `workflow.Go`, `workflow.NewTimer`).
2. **Non-idempotent activities** — an activity that isn't safe to run twice is
   a bug, not a style issue.
3. **Missing or unbounded retry policy** — every `workflow.ActivityOptions`
   needs an explicit `StartToCloseTimeout` and a considered `RetryPolicy`.

Changing `saga/` changes the command sequence of every workflow that uses it,
which breaks the replay of runs that are still open. Treat any edit that adds,
removes or reorders a workflow command as a breaking change.

Run `just ci` (fmt-check, vet, build, test, all inside the dev container). When
the change touches the wiring, run it for real: `just up`, then
`just starter --fail=charge` to watch the rollback.
