# よくある形

`example/` の各パッケージは、1つのテーマにつき1つ。どれも `docs/specs/` の仕様から実際に
動かしているので、ここに書いてあることは全部動きます。

各 example には `diagram.html` が置いてあります。正常系と失敗時の経路、補償がどの順で
走るかを見るならそちらが早いです。

| 知りたいこと | 答え | 実物 | 図 |
| --- | --- | --- | --- |
| ステップはアクティビティに限るのか | 限らない。`saga.Step` に渡す値で決まる | [`example/childflow/`](../example/childflow/) | [図](../example/childflow/diagram.html) |
| アクティビティの結果を次のステップに渡せるか | 渡せる。補償も同じ入力を受け取る | [`example/pipeline/`](../example/pipeline/) | [図](../example/pipeline/diagram.html) |
| `Run` の中が長くなるのをどうするか | state 構造体とメソッドに割る。クロージャは2行 | [`example/state/`](../example/state/) | [図](../example/state/diagram.html) |
| signal を待つには | `saga.Func` でステップにする。判断は自分の関数の中 | [`example/approval/`](../example/approval/) | [図](../example/approval/diagram.html) |
| signal を送るステップは書けるか | 書ける。`saga.Func` で。ただし冪等キーは載らない | [`example/external/`](../example/external/) | [図](../example/external/diagram.html) |
| 基本形 | 3ステップと補償、冪等キーを claim するアクティビティ | [`example/order/`](../example/order/) | [図](../example/order/diagram.html) |

---

## ステップの種類を選ぶ

`saga.Step` は1つだけで、**何で実行するかは渡す値が決めます**。

```go
// アクティビティのステップ
res, _ := saga.Step(ctx, s, "reserve",
    saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})

// 子ワークフローで実行し、アクティビティで取り消すステップ
pack, _ := saga.Step(ctx, s, "pack",
    saga.ChildWorkflow(PackWorkflow), saga.UndoActivity(a.Unpack), PackReq{Order: in})

// 外部ワークフローへの signal のステップ
saga.Step(ctx, s, "hold",
    saga.Func(sendHold), saga.UndoFunc(sendRelease), HoldReq{SKU: in.SKU})

// 自分のワークフローコードのステップ
saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})
```

混ぜられます。補償のレジストリは executor を区別しないので、巻き戻しは1つの逆順で回ります。

**forward と補償は別々の値なので、片方だけ別の executor にできます。** 上の例は
`example/childflow/` そのもので、荷造りは自分の履歴を持つに足る長さなので子ワークフロー、
荷ほどきはアクティビティ1回です。

子ワークフロー側は `saga.IdempotencyKeyOf` で自分の鍵を読みます。鍵は子の `WorkflowID` に
載っていて、補償側の `:undo` は剥がされた後なので、両側で同じ値になります。アクティビティ側は
`saga.IdempotencyKey` で、同じ値を読みます。

```go
func PackWorkflow(ctx workflow.Context, req PackReq) (string, error) {
    key, _ := saga.IdempotencyKeyOf(ctx)   // 子ワークフロー側
    ...
}

func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
    key, _ := saga.IdempotencyKey(ctx)     // アクティビティ側。同じ値
    ...
}
```

タスクキュー、タイムアウト、リトライポリシーといった子の設定は context から取ります。
呼ぶ前に `workflow.WithChildOptions` で普通に設定してください。補償の子の
`WorkflowExecutionTimeout` だけは、補償予算の残りに切り詰められます。

**ローカルアクティビティは対象外です。** `LocalActivityOptions` に ID フィールドが無く、
冪等キーを載せる場所がありません。そもそもローカルアクティビティはリトライがワークフロー
タスク内で完結してサーバに残らないので、取り消しが要るような副作用を置く場所ではない、
というのもあります。

## アクティビティの結果を次のステップに渡す

渡せます。前段の出力を次段のリクエストに入れるだけ。

```go
res, _ := saga.Step(ctx, s, "reserve",
    saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})
chg, _ := saga.Step(ctx, s, "charge",
    saga.Activity(a.Charge), saga.UndoActivity(a.Refund),
    ChargeReq{Order: in, Reservation: res})   // ← 前段の出力
```

**補償はそのステップ自身の出力を見られません。** 補償はステップの実行より前に登録される
ので、その時点では出力が存在しないからです。代わりに補償は forward と同じ入力を受け取り
ます。つまり前段までの出力は手元にあります。

```go
// ChargeReq を受け取るので、どの予約に対する課金かが分かる
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
    // req.Reservation が使える
}
```

自分のステップの出力が補償に必要な場合は、冪等キーで引いてください。それが
`saga.IdempotencyKey` がある理由です。

## `Run` の中を短く保つ

アクティビティの入力が増えると、ステップを並べただけのワークフローは読めなくなります。
入力と各ステップの出力を1つの構造体に持たせ、ステップをメソッドにすると、`Run` に渡す
クロージャは2行になります。

```go
return saga.Run(ctx, opts, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
    w := &fulfillment{in: in}
    return w.run(ctx, s)
})

func (w *fulfillment) run(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
    w.reserve(ctx, s)
    w.chargeCard(ctx, s)
    w.ship(ctx, s)

    if err := s.Err(); err != nil {
        return Receipt{}, err
    }
    return Receipt{Reservation: w.reservation, Charge: w.charge, Shipment: w.shipment}, nil
}
```

`saga.Step` が `ctx` と `s` を引数で取るので、ステップはメソッドでも関数でも好きに割れます。

**state 構造体そのものを `saga.Step` に渡すことはできません。** アクティビティのステップでは
`in` がそのままアクティビティの引数になるので、シリアライズ可能である必要があります。メソッドの中で state からリクエストを
組んでください。

クロージャ自体は無くせません。`Run` が最後にロールバックを判断する場所だからです。
`defer` を利用者に書かせる形は、書き忘れると補償ゼロのまま「成功」になるので採っていません
（[design.md](design.md) の項目4）。

## signal を待つ

待つ処理も**ステップにします**。`saga.Func` が、利用者の書いたワークフローコードを
その場で呼んでステップに変えます。

```go
saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})
```

判断は `awaitApproval` の中で完結します。アクティビティが「自分の失敗が何を意味するか」を
関数の中に閉じ込めるのと、同じ立ち位置です。

```go
func awaitApproval(ctx workflow.Context, req ApprovalReq) (Decision, error) {
    decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, req.Wait)
    if !ok {
        return Decision{}, temporal.NewApplicationError("nobody reviewed it", DeniedType, nil)
    }
    if !decision.Approved {
        return Decision{}, temporal.NewApplicationError("rejected by "+decision.By, DeniedType, nil)
    }
    return decision, nil
}
```

ステップなので、**先のステップが失敗していれば飛ばされます**。ロールバックに向かっている
saga が人の承認を1時間待って止まることはありません。

`saga.Func` は signal 専用ではありません。ワークフローの中で実行する必要があって、かつ
saga を失敗させうるもの全般に使えます。`workflow.Await` で条件を待つ、経路を選ぶ、など。

本体はステップの列のままになります。

```go
res, _ := saga.Step(ctx, s, "reserve",
    saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})
saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})
chg, _ := saga.Step(ctx, s, "charge",
    saga.Activity(a.Charge), saga.UndoActivity(a.Refund), ChargeReq{Order: in})
```

**`s.Err()` のガードは要りません。** ステップが失敗していれば以降のステップは飛ばされ、
`Run` は**最初の失敗**を報告します。握って自分のエラーを返したいときだけ、先に
`s.Clear()` を呼んでください。

ただし**ライブラリが面倒を見られるのはステップの中だけ**です。`s.Err()` が立った後も、
ログ・`workflow.Sleep`・本体に直接書いた分岐は普通に実行されます。ステップの外でゼロ値を
使うなら、そこは自分で `s.Err()` を見てください。

待っている間にワークフローがキャンセルされても、補償は走ります。切り離した context で
実行されるからです。

## signal を送るステップ

他のワークフローが持っている状態を動かすステップは `saga.Func` で書けます。補償は
打ち消しの signal です。

```go
saga.Step(ctx, s, "hold", saga.Func(sendHold), saga.UndoFunc(sendRelease),
    HoldReq{Inventory: in.Inventory, Order: in.ID, SKU: in.SKU, Quantity: in.Quantity})

// 2つとも普通のワークフローコード。Func / UndoFunc に渡せるのはこれで足りる
func sendHold(ctx workflow.Context, req HoldReq) (struct{}, error) {
    err := workflow.SignalExternalWorkflow(ctx, req.Inventory, "", HoldSignal, req).Get(ctx, nil)
    return struct{}{}, err
}

func sendRelease(ctx workflow.Context, req HoldReq) error {
    return workflow.SignalExternalWorkflow(ctx, req.Inventory, "", ReleaseSignal, req).Get(ctx, nil)
}
```

補償は forward と同じ payload を受け取るので、押さえた分だけを正確に戻せます。

**専用の値は用意していません。** `SignalExternalWorkflow` には options 構造体が無いので、
ライブラリが載せられる冪等キーも、切れる予算もありません。専用にしても `saga.Func` に
語彙を足すだけになります。saga が保証するのは対になっていることだけで、同じ signal を
2回受けても壊れないようにするのは受け手の責任です。
