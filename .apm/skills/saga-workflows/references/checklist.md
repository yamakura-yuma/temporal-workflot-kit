# Saga review checklist

## Workflow code (`internal/workflow/`)

- [ ] No direct calls to `time.Now()`, `time.Sleep()`, `rand`, goroutines, channels,
      or network/filesystem I/O. Use `workflow.Now`, `workflow.NewTimer`,
      `workflow.SideEffect`, `workflow.Go` instead.
- [ ] No iteration over a Go map when the result affects workflow decisions —
      map iteration order is non-deterministic. Sort keys first if needed.
- [ ] Every `workflow.ExecuteActivity` call sets `ActivityOptions` with an
      explicit `StartToCloseTimeout` (or `ScheduleToCloseTimeout`) and a
      `RetryPolicy` — don't rely on SDK zero-values.
- [ ] Workflow signature changes are backward compatible with in-flight
      workflow histories, or the change is paired with a versioning strategy
      (`workflow.GetVersion`) if behavior changes mid-flight.
- [ ] Long-running workflows call `workflow.NewContinueAsNewError` before
      history grows unbounded (large loops, polling).
- [ ] Saga-style workflows define explicit compensation steps for every
      forward step that has a side effect, and run compensations in reverse
      order on failure.

## Activity code (`internal/activity/`)

- [ ] Activities that cause an external side effect (write, charge, send) are
      idempotent, or take/generate an idempotency key so an at-least-once
      retry doesn't double-apply the effect.
- [ ] Activities return typed errors (or use `temporal.NewApplicationError`)
      so the workflow can distinguish retryable from terminal failures.
- [ ] Activities respect `ctx` cancellation/deadline rather than running
      unbounded.
- [ ] Activity inputs/outputs are serializable (exported fields, no channels/
      funcs/unexported-only structs).

## Worker/registration (`cmd/worker`)

- [ ] Every workflow and activity used by a started workflow is registered
      on the worker before it's needed.
- [ ] Task queue names are defined once (a shared constant) and reused by
      worker and starter, not duplicated as string literals.
