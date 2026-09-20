# temporal-saga

A Go library for the Saga pattern on [Temporal](https://temporal.io): a
sequence of activities that can be rolled back, with the rollback wired up for
the cases that are easy to get wrong.

```go
import "github.com/yamakura-yuma/temporal-saga/saga"

func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
    var a *Activities

    return saga.Run(ctx, saga.Options{
        ActivityOptions:    workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second},
        CompensationBudget: 5 * time.Minute,
    }, func(s *saga.Saga) (Receipt, error) {
        res, _ := saga.Step(ctx, s, "reserve", a.Reserve, a.Unreserve, ReserveReq{Order: in})
        chg, _ := saga.Step(ctx, s, "charge", a.Charge, a.Refund, ChargeReq{Order: in})
        shp, _ := saga.Step(ctx, s, "ship", a.Ship, a.CancelShipment, ShipReq{Order: in})

        return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
    })
}
```

If `charge` fails, `reserve` and `charge` are both undone, in reverse order,
and the workflow fails with the original error.

## What it takes care of

Temporal's own SDK ships no saga helper, and the obvious 30-line version gets
four things wrong. Those four are the reason this package exists.

**Compensations run on a disconnected context.** A canceled workflow context
fails every subsequent activity immediately, so compensation written the
obvious way does nothing in the one situation it exists for. `Run` obtains a
context from `workflow.NewDisconnectedContext` and bounds the whole phase with
`CompensationBudget` — required, because nothing outside can cancel that
context.

**The compensation is registered before its forward activity runs**, not after
it succeeds. An activity that fails with a timeout may still have run to
completion on a worker that never reported back; a compensation registered only
on success would leave that side effect behind forever.

**Both halves of a step see the same idempotency key**, derived from the run id
and the step name and readable inside an activity with `saga.IdempotencyKey`.

**A body that returns nil after a step failed still fails the workflow**, and
its result is discarded. Without that, one missing error check records the
workflow as completed with its side effects half applied — green in the UI, no
alert, nothing rolled back.

`fwd` and `undo` are taken as typed functions rather than the SDK's `any`, so
swapping them or passing the wrong argument is a compile error. The SDK does
not catch it: `ExecuteActivity` resolves a function value to a name string
before validating anything, and the string path skips argument validation, so a
mix-up surfaces only when the activity runs — for a compensation, while the
saga is already failing.

## The contract on your activities

Two things, and the first one is the hard one:

1. **Forward activities must claim their idempotency key atomically.** Checking
   whether the key was used and then acting on it is not enough: a timeout can
   put two attempts in flight at once and both will see it as unused. Use a
   uniqueness constraint (`INSERT ... ON CONFLICT`), or the downstream API's own
   idempotency-key header.
2. **Compensations must succeed when there is nothing to undo.** They are
   registered before the work happens, so they will sometimes be called for a
   step that never took effect.

`example/order/activity.go` is the worked example.

## What it does not solve

A compensation cannot reliably tell whether the step it undoes happened. A
forward activity that timed out keeps running on its worker: the compensation
can observe "nothing happened", return successfully, and the side effect can
land afterwards. Closing that requires the compensation to write a tombstone
that the forward activity checks before committing — both halves serialized
through one store. Against a third-party payment gateway or carrier that is not
possible, and no library can make it so.

Downstream systems that do not accept an idempotency key cannot be made safe
this way at all. Neither can side effects that cannot be taken back, such as a
sent email.

Cancel workflows, do not terminate them: termination runs no workflow code, so
nothing is compensated.

And this package issues workflow commands, so upgrading it changes the command
sequence of every workflow that uses it and breaks the replay of runs that are
still open — callers cannot wrap that in `workflow.GetVersion`, because the
commands are inside the package. In practice the version cannot move until the
sagas started under the old one have drained. That is the price of taking this
as a dependency rather than copying it.

## Development

Everything runs inside the container built from `Dockerfile` (Go +
`temporal-cli` via Nix), driven from the host with `just`. No host-level Go
install is needed.

```bash
just ci      # fmt-check, vet, build, unit tests, and the specifications
just test    # unit tests, against the in-memory test environment
just spec    # the specifications in specs/, against a real dev server
```

There are two suites, and the split is not about speed. Temporal's test
environment runs activities even on a canceled context and does not enforce
activity timeouts, so the behaviour this library exists for -- the rollback
that still happens after a cancel -- passes there whether or not it works.

`saga/*_test.go` covers the API invariants in milliseconds. `specs/` covers
what is visible from outside, written as prose that executes:

```
## キャンセルされた saga もロールバックされる

* 課金の後で待機する注文 "cancelme"
* "charge" が実行されたら saga をキャンセルする
* saga は失敗する
* ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge" を保持していない
```

Those steps are implemented in `stepImpl/`, matched by their text alone, and
the suite starts a real Temporal dev server with `testsuite.StartDevServer`
using the `temporal` CLI the dev image already has. `just spec-validate`
reports a step with no implementation, and `just spec-steps` prints the
pairing.

See `CLAUDE.md` for the full command list.
