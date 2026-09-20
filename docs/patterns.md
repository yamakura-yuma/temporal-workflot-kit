# よくある形

`example/` の各パッケージは、1つのテーマにつき1つ。どれも `docs/specs/` の仕様から実際に
動かしているので、ここに書いてあることは全部動きます。

各 example には `diagram.html` が置いてあります。正常系と失敗時の経路、補償がどの順で
走るかを見るならそちらが早いです。

| 知りたいこと | 答え | 実物 | 図 |
| --- | --- | --- | --- |
| ステップはアクティビティに限るのか | 限らない。子ワークフローも `ChildStep` でステップになる | [`example/childflow/`](../example/childflow/) | [図](../example/childflow/diagram.html) |
| アクティビティの結果を次のステップに渡せるか | 渡せる。補償も同じ入力を受け取る | [`example/pipeline/`](../example/pipeline/) | [図](../example/pipeline/diagram.html) |
| `Run` の中が長くなるのをどうするか | state 構造体とメソッドに割る。クロージャは2行 | [`example/state/`](../example/state/) | [図](../example/state/diagram.html) |
| signal を待つには | `saga.AwaitSignal`。ステップが失敗していれば待たない | [`example/approval/`](../example/approval/) | [図](../example/approval/diagram.html) |
| signal を送るステップは書けるか | 書ける。`saga.SignalStep`。ただし冪等キーは載らない | [`example/external/`](../example/external/) | [図](../example/external/diagram.html) |
| 基本形 | 3ステップと補償、冪等キーを claim するアクティビティ | [`example/order/`](../example/order/) | [図](../example/order/diagram.html) |

---

## ステップを子ワークフローにする

`Step` と `ChildStep` は形が同じで、違うのは第1引数の型だけです。`context.Context` なら
アクティビティ、`workflow.Context` なら子ワークフロー。Temporal 自身の区別と同じなので、
名前を覚えるのではなく型が教えてくれます。

```go
// アクティビティのステップ
res, _ := saga.Step(ctx, s, "reserve", a.Reserve, a.Unreserve, ReserveReq{Order: in})

// 子ワークフローのステップ
pack, _ := saga.ChildStep(ctx, s, "pack", PackWorkflow, UnpackWorkflow, PackReq{Order: in})
```

混ぜられます。補償のレジストリは executor を区別しないので、巻き戻しは1つの逆順で回ります。

子ワークフロー側は `saga.IdempotencyKeyOf` で自分の鍵を読みます。鍵は子の `WorkflowID` に
載っていて、補償側の `:undo` は剥がされた後なので、forward と補償で同じ値になります。

```go
func UnpackWorkflow(ctx workflow.Context, req PackReq) error {
    key, _ := saga.IdempotencyKeyOf(ctx)   // forward の子と同じ鍵
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
res, _ := saga.Step(ctx, s, "reserve", a.Reserve, a.Unreserve, ReserveReq{Order: in})
chg, _ := saga.Step(ctx, s, "charge", a.Charge, a.Refund,
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

`Step` と `ChildStep` は `ctx` と `s` を引数で取るので、メソッドでも関数でも好きに割れます。

**state 構造体そのものを `Step` に渡すことはできません。** `Step` の入力はアクティビティの
引数なので、シリアライズ可能である必要があります。メソッドの中で state からリクエストを
組んでください。

クロージャ自体は無くせません。`Run` が最後にロールバックを判断する場所だからです。
`defer` を利用者に書かせる形は、書き忘れると補償ゼロのまま「成功」になるので採っていません
（[design.md](design.md) の項目4）。

## signal を待つ

`saga.AwaitSignal` を使います。待つこと自体は副作用ではないのでステップではありません。
取り消すものが無いからです。

ライブラリに置いてある理由は1つだけ。**ステップが既に失敗していれば、待たずに返ります。**
ロールバックに向かっている saga が、人の承認を1時間待って止まるのを避けるためです。

```go
res, _ := saga.Step(ctx, s, "reserve", a.Reserve, a.Unreserve, ReserveReq{Order: in})

// 分岐の前に必ず s.Err() を見る。失敗していれば res はゼロ値で、
// それで分岐すると「何も無いもの」で分岐することになる
if err := s.Err(); err != nil {
    return Receipt{}, err
}

decision, ok := saga.AwaitSignal[Decision](ctx, s, "approval", time.Minute)
if !ok || !decision.Approved {
    // エラーを返すだけでロールバックが走る
    return Receipt{}, temporal.NewApplicationError("rejected", DeniedType, nil)
}
```

待っている間にワークフローがキャンセルされても、補償は走ります。切り離した context で
実行されるからです。

## signal を送るステップ

他のワークフローが持っている状態を動かすステップは `saga.SignalStep` で書けます。補償は
打ち消しの signal です。

```go
saga.SignalStep(ctx, s, "hold",
    saga.Signal{WorkflowID: in.Inventory, Name: HoldSignal},     // forward
    saga.Signal{WorkflowID: in.Inventory, Name: ReleaseSignal},  // 補償
    HoldReq{Order: in.ID, SKU: in.SKU, Quantity: in.Quantity})
```

補償は forward と同じ payload を受け取るので、押さえた分だけを正確に戻せます。

**3つの executor の中では一番弱い形です。** `SignalExternalWorkflow` には options 構造体が
無く、冪等キーを載せる場所がありません。saga が保証するのは対になっていることだけで、
同じ signal を2回受けても壊れないようにするのは受け手の責任です。
