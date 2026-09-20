---
name: saga-workflows
description: >-
  Use when working on the saga library in this repo — the saga/ package, the
  specifications under specs/ and their steps in stepImpl/, the example saga in
  example/order/, or any change to how a step and its compensation are wired. Covers the invariants the library depends on,
  the contract it puts on activities, and the determinism, idempotency and
  retry rules a change has to satisfy before it ships.
---

# The saga library in this repo

This repo publishes `saga`, a package for running a sequence of Temporal
activities that can be rolled back. It is a library, not an application: the
saga under `example/order/` exists to exercise it.

## Where things live

| | |
| --- | --- |
| `saga/` | The library. `Run` owns the rollback, `Step` runs one forward activity and registers its compensation. |
| `saga/saga_test.go` | Unit tests against the in-memory test environment. |
| `specs/` | Executable specifications in Gauge's markdown, written in Japanese, run against a real dev server the suite starts. |
| `stepImpl/` | The Go implementations of those steps, and the suite hooks that start the server and worker. |
| `example/order/` | The saga the specifications drive, and the worked example of the activity contract. A normal package: Gauge builds the module, not a test binary, so it cannot live in `_test.go`. |

Nothing belongs under `internal/`, and there is no application to run by hand:
a library under `internal/` cannot be imported from outside this module, and
what the old worker and starter binaries demonstrated is now asserted by the
integration tests.

## Which suite a behaviour belongs in

Temporal's in-memory test environment runs activities even on a canceled
context and does not enforce activity timeouts. Anything that depends on either
-- the rollback surviving a cancel, the compensated activity IDs appearing in
the history in order, a search attribute being written -- has to be a
specification under `specs/`, or the test will pass while the behaviour is
broken. Everything else belongs in `saga/`, where it runs in milliseconds.

A specification is prose that executes, so write the scenario as the behaviour
an operator would describe, and keep the Temporal vocabulary in `stepImpl/`.
Specifications are written in Japanese; the step text and the string literal in
`gauge.Step(...)` have to agree exactly, so both sides are Japanese even though
the rest of the repo is English.

**That text is the only link between a specification and its code.** Nothing in
the compiler checks it. To find what a line does, search `stepImpl/` for its
text, or run `just spec-steps` for the whole pairing. `just spec-validate`,
part of `just ci`, reports a step with no implementation, with its file and
line, without starting a server.

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

Walk every changed file under `saga/`, `stepImpl/` or `example/` against
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

Run `just ci` (fmt-check, vet, build, unit tests, spec-validate and the
specifications, all inside the dev container). `just spec` alone is the one to
reach for when the change touches cancellation, activity IDs, or anything else
the in-memory environment cannot show.
