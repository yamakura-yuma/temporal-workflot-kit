---
name: saga-workflows
description: >-
  Use when writing or reviewing this repo's saga — workflow functions under
  internal/workflow, activity functions under internal/activity, or the worker
  and starter wiring in cmd/. Covers how a saga step and its compensation are
  built here, and the determinism, idempotency and retry rules a change has to
  satisfy before it ships.
---

# Saga workflows in this repo

This repo orchestrates a saga with Temporal: a forward sequence of activities
where every step with a side effect owns a compensating step, run in reverse
order when a later step fails. The workflow function is the only place that
decides what runs; activities are the only place that touches the outside
world.

## Where things live

| | |
| --- | --- |
| `internal/workflow/` | Workflow functions. Deterministic, replayed from history. |
| `internal/activity/` | Activity functions. At-least-once, may run twice. |
| `cmd/worker/` | Registers every workflow and activity, polls the task queue. |
| `cmd/starter/` | Starts one execution; useful as a smoke test. |

`workflow.TaskQueue` is the single definition of the queue name — the worker
and the starter both read it, so a new entry point imports the constant rather
than repeating the string. Both binaries read `TEMPORAL_ADDRESS` and fall back
to `client.DefaultHostPort`, which is what lets the same code run against the
compose stack and a host-local dev server unchanged.

`SagaWorkflow` and the `Greet` activity are still scaffolding: they exist to
prove the wiring runs end to end, and real steps replace them.

## Adding a saga step

1. Write the activity in `internal/activity/`, and make it safe to run twice —
   an at-least-once retry must not double-apply the effect.
2. Write its compensation as a second activity, unless the step has no side
   effect to undo.
3. Call it from the workflow with explicit `ActivityOptions`, and record the
   compensation so the failure path can run it in reverse order.
4. Register both on the worker in `cmd/worker/main.go`. An activity the
   workflow reaches but the worker never registered fails at run time, not
   compile time.

## Before the change ships

Walk every changed file under `internal/workflow/` or `internal/activity/`
against `references/checklist.md`. The three failure classes that matter most:

1. **Non-determinism in workflow code** — anything that can produce a
   different result on replay (time, randomness, goroutines, map iteration,
   direct I/O) breaks Temporal's replay model. Workflow code reaches these only
   through the SDK's deterministic equivalents (`workflow.Now`,
   `workflow.SideEffect`, `workflow.Go`, `workflow.NewTimer`).
2. **Non-idempotent activities** — an activity that isn't safe to run twice
   (a charge with no idempotency key, say) is a bug, not a style issue.
3. **Missing or unbounded retry policy** — every `workflow.ActivityOptions`
   needs an explicit `StartToCloseTimeout` and a considered `RetryPolicy`.
   SDK zero-values are fine only for placeholders.

Run `just ci` (fmt-check, vet, build, test, all inside the dev container) to
confirm the change still builds, and `just up` + `just starter` to watch a real
execution when the change touches the wiring.
