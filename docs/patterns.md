# よくある形

`example/` の各パッケージは、1つのテーマにつき1つ。どれも `docs/specs/` の仕様から実際に
動かしているので、ここに書いてあることは全部動きます。

各 example には `diagram.html` が置いてあります。正常系と失敗時の経路、補償がどの順で
走るかを見るならそちらが早いです。

| 知りたいこと | 答え | 実物 | 図 |
| --- | --- | --- | --- |
| ステップはアクティビティに限るのか | 限らない。`saga.Step` に渡す値で決まる | [`example/workflow/childflow/`](../example/workflow/childflow/) | [図](../example/workflow/childflow/diagram.html) |
| アクティビティの結果を次のステップに渡せるか | 渡せる。補償も同じ入力を受け取る | [`example/workflow/pipeline/`](../example/workflow/pipeline/) | [図](../example/workflow/pipeline/diagram.html) |
| `Run` の中が長くなるのをどうするか | state 構造体とメソッドに割る。クロージャは2行。素の形との読み比べは [`workflow_flat.go`](../example/workflow/state/workflow_flat.go) | [`example/workflow/state/`](../example/workflow/state/) | [図](../example/workflow/state/diagram.html) |
| signal を待つには | `saga.Func` でステップにする。判断は自分の関数の中 | [`example/workflow/approval/`](../example/workflow/approval/) | [図](../example/workflow/approval/diagram.html) |
| signal を送るステップは書けるか | 書ける。`saga.Func` で。ただし冪等キーは載らない | [`example/workflow/external/`](../example/workflow/external/) | [図](../example/workflow/external/diagram.html) |
| 基本形 | 3ステップと補償、冪等キーを呼び先に渡すアクティビティ | [`example/workflow/order/`](../example/workflow/order/) | [図](../example/workflow/order/diagram.html) |

---

## ステップの半分は、ただのワークフローコード

`saga.Step` が取るのは、**forward と補償の2つの関数**だけです。どちらも
`func(workflow.Context) error` で、中身は普通のワークフローコードです。

```go
s.Step(ctx, "reserve", w.reserve, w.unreserve)

func (w *fulfillment) reserve(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Reserve, ReserveReq{Order: w.in}).Get(ctx, &w.reservation)
}
```

**ライブラリは `ExecuteActivity` を包みません。** だから何で実行するかは、関数の中に何を
書くかの違いでしかありません。

```go
// アクティビティ
workflow.ExecuteActivity(ctx, acts.Reserve, req).Get(ctx, &w.reservation)

// 子ワークフロー
workflow.ExecuteChildWorkflow(ctx, PackWorkflow, req).Get(ctx, &w.packing)

// 他のワークフローへの signal
workflow.SignalExternalWorkflow(ctx, id, "", HoldSignal, req).Get(ctx, nil)

// 何も実行しない。signal を待つだけ
saga.AwaitSignal[Decision](ctx, ApprovalSignal, wait)
```

**forward と補償で別々にしてかまいません。** `example/workflow/childflow/` は荷造りを子
ワークフローで実行し、荷ほどきをアクティビティ1本でやっています。補償のレジストリは
関数を持っているだけなので、巻き戻しは1つの逆順で回ります。

タイムアウトもリトライポリシーもタスクキューも、**普通に context に載せてください**。
ライブラリは奪いません。

```go
ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
    StartToCloseTimeout: 10 * time.Second,
    RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
})
```

**ローカルアクティビティも書けます。** ただし取り消しが要る副作用を置く場所ではありません。
リトライがワークフロータスク内で完結してサーバに残らないためです。

### 冪等キー

ライブラリは冪等キーを渡しません。`saga.StepKey(ctx, "charge")` が「1回の実行の1ステップ」に
固有の文字列を返すので、**自分でリクエストに入れてください**。下流がそれで重複を弾きます。

```go
req := ChargeReq{Order: w.in, IdemKey: saga.StepKey(ctx, "charge")}
```

キーを原子的に押さえるのは下流の仕事で、ライブラリにはできません。詳しくは
[activity-contract.md](activity-contract.md)。

## 前のステップの結果を次のステップや補償で使う

両半分を同じ構造体のメソッドにすると、**フィールドを読むだけ**です。

```go
type fulfillment struct {
    in          Order
    reservation string
    charge      string
}

func (w *fulfillment) chargeCard(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Charge,
        ChargeReq{Order: w.in, Reservation: w.reservation}).Get(ctx, &w.charge)   // ← 前段の出力
}

func (w *fulfillment) refund(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Refund,
        ChargeReq{Order: w.in, Charge: w.charge}).Get(ctx, nil)                   // ← 自分の forward の出力
}
```

**補償が自分の forward の出力を使えます。** 補償は forward より先に登録されますが、
*実行される*のは後なので、その時点でフィールドは埋まっています。他の saga 実装が補償ログと
して明示的に持ち回るものを、Go では普通の変数がやります。

**ただし forward が返さなかったときは空です。** 下流に書き込んだ直後にタイムアウトすると、
出力は履歴に残らず、補償は空の値を見ます。そこを埋めるのが冪等キーで、「このキーで書かれた
行を消す」という形なら、forward が成功していようと途中で落ちていようと同じ1文で足ります。

## `Run` の中を短く保つ

両半分をメソッドにすると、`Run` に渡すクロージャは**1ステップ1行**になります。

```go
return saga.RunOrCompensate(ctx, opts, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
    w := &fulfillment{in: in}

    s.Step(ctx, "reserve", w.reserve, w.unreserve)
    s.Step(ctx, "charge", w.chargeCard, w.refund)
    s.Step(ctx, "approve", w.approve, nil)
    s.Step(ctx, "pack", w.pack, w.unpack)
    s.Step(ctx, "ship", w.ship, w.cancelShipment)

    return w.receipt(), nil
})
```

ここだけ読めば、何が何の順で起きて、どれが取り消せるかが分かります。`ExecuteActivity` も
リクエストの組み立ても、各メソッドの中です。

### 半分をその場に書く形と、どちらがよいか

小さい saga なら、その場に書いても読めます。

```go
s.Step(ctx, "reserve",
    func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, acts.Reserve, req).Get(ctx, &reservation)
    },
    func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, acts.Unreserve, req).Get(ctx, nil)
    })
```

次のどれかに当たったらメソッドに割ってください。

- ステップが4つ以上ある
- 同じ入力を3つ以上のステップで使い回す
- 前段の出力を2段以上先のステップや補償へ渡す

実物が [`example/workflow/state/`](../example/workflow/state/) にあります。同じ saga が
`workflow.go`（メソッド）と [`workflow_flat.go`](../example/workflow/state/workflow_flat.go)
（その場）で書いてあり、仕様（[`state.feature`](specs/state.feature)）が両方を動かして同じ
`Receipt` が返ることを確かめています。どちらが読みやすいかは、並べて読んで決めてください。

クロージャ自体は無くせません。`Run` が最後にロールバックを判断する場所だからです。
`defer` を利用者に書かせる形は、書き忘れると補償ゼロのまま「成功」になるので採っていません
（[design.md](design.md) の項目4）。

## signal を待つ

待つ処理も**ステップにします**。半分はただの関数なので、`AwaitSignal` を呼んで判断するだけです。

```go
s.Step(ctx, "approval", w.await, nil)   // 待ったことに取り消しは無いので nil

func (w *fulfillment) await(ctx workflow.Context) error {
    decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, w.wait())
    if !ok {
        return temporal.NewApplicationError("nobody reviewed it", DeniedType, nil)
    }
    if !decision.Approved {
        return temporal.NewApplicationError("rejected by "+decision.By, DeniedType, nil)
    }
    w.approvedBy = decision.By
    return nil
}
```

判断は `await` の中で完結します。アクティビティが「自分の失敗が何を意味するか」を関数の中に
閉じ込めるのと同じ立ち位置です。

ステップなので、**先のステップが失敗していれば飛ばされます**。ロールバックに向かっている
saga が人の承認を1時間待って止まることはありません。これが「待つだけの処理をステップにする」
理由です。

**`s.Err()` のガードは要りません。** ステップが失敗していれば以降のステップは飛ばされ、
`Run` は**最初の失敗**を報告します。握って自分のエラーを返したいときだけ、先に
`s.Clear()` を呼んでください。

ただし**ライブラリが面倒を見られるのはステップの中だけ**です。`s.Err()` が立った後も、
ログ・`workflow.Sleep`・本体に直接書いた分岐は普通に実行されます。ステップの外でゼロ値を
使うなら、そこは自分で `s.Err()` を見てください。

待っている間にワークフローがキャンセルされても、補償は走ります。切り離した context で
実行されるからです。

## signal を送るステップ

他のワークフローが持っている状態を動かすステップも、同じ形です。補償は打ち消しの signal。

```go
s.Step(ctx, "hold", w.hold, w.release)

func (w *fulfillment) hold(ctx workflow.Context) error {
    return workflow.SignalExternalWorkflow(ctx, w.in.Inventory, "", HoldSignal, w.req()).Get(ctx, nil)
}

func (w *fulfillment) release(ctx workflow.Context) error {
    return workflow.SignalExternalWorkflow(ctx, w.in.Inventory, "", ReleaseSignal, w.req()).Get(ctx, nil)
}
```

補償は forward と同じ payload を送れるので、押さえた分だけを正確に戻せます。

**signal に冪等キーは載りません。** 同じ signal を2回受けても壊れないようにするのは受け手の
責任です。saga が保証するのは、対になっていることだけです。
