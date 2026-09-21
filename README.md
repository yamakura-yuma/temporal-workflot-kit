# temporal-saga

Temporal で、取り消せるアクティビティの列を書くための Go ライブラリ。

在庫を押さえて、課金して、配送を手配する。途中で失敗したら、そこまでに起きたことを
逆順で取り消す。その「取り消す」側を書くのが面倒で、しかも間違えやすい。そこを引き受け
ます。

## デモ

同じ saga を、素の SDK で書いた場合とこのライブラリで書いた場合。

### 素の SDK で書くと

```go
func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
    var undo []func(workflow.Context) error

    var res string
    if err := workflow.ExecuteActivity(ctx, a.Reserve, in).Get(ctx, &res); err != nil {
        return Receipt{}, err
    }
    undo = append(undo, func(c workflow.Context) error {
        return workflow.ExecuteActivity(c, a.Unreserve, in).Get(c, nil)
    })
    //  ↑ 成功した後に積んでいる。タイムアウトしたアクティビティは
    //    ワーカー上で完走しているかもしれないのに、取り消されない

    var chg string
    if err := workflow.ExecuteActivity(ctx, a.Charge, in).Get(ctx, &chg); err != nil {
        for i := len(undo) - 1; i >= 0; i-- {
            undo[i](ctx)
            //  ↑ ctx がキャンセル済みなら、ここは全部即座に失敗する。
            //    キャンセルこそ取り消しが要る場面なのに
        }
        return Receipt{}, err
    }
    // ... ship も同じことを書く
}
```

### このライブラリで書くと

```go
func OrderWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
    var a *Activities

    return saga.Run(ctx, saga.Options{
        ActivityOptions:    workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second},
        CompensationBudget: 5 * time.Minute,
    }, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
        res, _ := saga.Step(ctx, s, "reserve",
            saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})
        chg, _ := saga.Step(ctx, s, "charge",
            saga.Activity(a.Charge), saga.UndoActivity(a.Refund), ChargeReq{Order: in})
        shp, _ := saga.Step(ctx, s, "ship",
            saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment), ShipReq{Order: in})

        return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
    })
}
```

ステップのエラーを `_` で捨てているのは手抜きではありません。最初の失敗以降、後続の
`saga.Activity` は何もせず、`Run` が元のエラーでワークフローを失敗させます。半端な `Receipt` は
外に出ません。

振る舞いは `docs/specs/` に実行できる仕様として置いてあり、`just spec` が実際の Temporal
dev server を起動して確かめます。

```
## キャンセルされた saga もロールバックされる

* 課金の後で待機する注文 "cancelme"
* "charge" が実行されたら saga をキャンセルする
* saga は失敗する
* ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge" を保持していない
```

## 機能・特徴

| 機能 | 中身 |
| --- | --- |
| ロールバック | 失敗すると、登録済みの補償を逆順で実行する |
| キャンセル対応 | ワークフローがキャンセルされても補償は実行される |
| 冪等キーの払い出し | ステップごとにキーを作り、forward と補償の両方に渡す |
| 補償の時間制限 | 補償フェーズ全体に上限を設ける。実行できなかった補償は報告する |
| 失敗の報告 | 補償が失敗したら、型付きのエラーと検索属性で残す。元のエラーは消さない |
| 型安全なステップ | `fwd` と `undo` の取り違えはコンパイルエラーになる |
| ステップの種類を選べる | アクティビティ / 子ワークフロー / 自分の関数。**forward と補償で別々に選べる**。混ざっても1つの逆順で巻き戻る |
| 逃げ道 | `Add` で任意の取り消しを登録できる |

なぜこの形なのか、素直に書くと何が壊れるのかは [docs/design.md](docs/design.md) に
コード付きで書いてあります。

## インストール・セットアップ

```bash
go get github.com/yamakura-yuma/temporal-saga/saga
```

必要なのは Go 1.26 以降と Temporal Go SDK v1.49 以降。サーバ側に特別な設定は要りません。
補償の失敗を検索属性で可視化する場合だけ、そのキーをサーバに登録しておきます。

## 使用方法

デモの後半がそのまま使い方です。`saga.Run` にワークフロー本体を渡し、各ステップを
`saga.Activity` で書きます。

アクティビティ側には2つだけ約束があります。forward は冪等キーを**原子的に** claim する
こと、補償は取り消すものが無いときに成功すること。どちらも外すとロールバックが壊れるので、
[docs/activity-contract.md](docs/activity-contract.md) を先に読んでください。塞げていない
穴も同じ文書に書いてあります。

### 他のパターン

1テーマにつき1つ。どれも `docs/specs/` の仕様から実際に動かしています。

それぞれに `diagram.html` を置いてあります。正常系と失敗時にどの順で何が走るかは、
そちらをブラウザで開くのが早いです。

| 例 | 何を見せているか | 図 |
| --- | --- | --- |
| [`example/order/`](example/order/) | 基本形。3ステップと補償、冪等キーを claim するアクティビティの書き方 | [図](example/order/diagram.html) |
| [`example/pipeline/`](example/pipeline/) | 前段の出力が次段の入力になる saga。補償が前段の ID をどう受け取るか | [図](example/pipeline/diagram.html) |
| [`example/state/`](example/state/) | 入力が多い saga を state 構造体とメソッドに割り、`Run` の中を2行に保つ | [図](example/state/diagram.html) |
| [`example/childflow/`](example/childflow/) | 子ワークフローで実行し、アクティビティで取り消すステップ。冪等キーは executor を跨いで同じ | [図](example/childflow/diagram.html) |
| [`example/approval/`](example/approval/) | signal 待ちをステップにする。判断は自分の関数の中で完結させる | [図](example/approval/diagram.html) |
| [`example/external/`](example/external/) | signal で他のワークフローを動かすステップ。失敗すると打ち消しの signal が飛ぶ | [図](example/external/diagram.html) |

形ごとの書き方は [docs/patterns.md](docs/patterns.md) に。

## API・設定

| | |
| --- | --- |
| `saga.Run(ctx, opts, body)` | saga を実行し、失敗したらロールバックする |
| `saga.Step(ctx, s, name, fwd, undo, in)` | forward を1つ実行し、その補償を登録する |
| `saga.Activity(f)` / `saga.UndoActivity(f)` | その半分をアクティビティで実行する |
| `saga.ChildWorkflow(f)` / `saga.UndoChildWorkflow(f)` | その半分を子ワークフローで実行する |
| `saga.Func(f)` / `saga.UndoFunc(f)` | その半分をこのワークフローの中で呼ぶ |
| `saga.AwaitSignal[T](ctx, name, timeout)` | signal を待つ。`saga.Func` の中で使う |
| `saga.Options` | アクティビティの既定、補償の予算、鍵の作り方 |
| `saga.IdempotencyKey(ctx)` | アクティビティ側から冪等キーを読む |
| `saga.IdempotencyKeyOf(ctx)` | 子ワークフロー側から冪等キーを読む |
| `saga.CompensationReport` | 失敗した補償とスキップされた補償の一覧 |

`CompensationBudget` は必須です。補償は外から誰もキャンセルできない context で走るので、
上限が無いと詰まったときに止める手段がありません。詳細は `go doc ./saga`。

## 貢献方法

```bash
just ci      # fmt-check, vet, build, ユニットテスト, docs-check, 仕様
```

すべてコンテナの中で動くので、ホストに Go を入れる必要はありません。どちらのスイートに
何を書くかは [docs/development.md](docs/development.md) に。

## ライセンス・作者情報

MIT License。作者は yamakura-yuma。詳細は [LICENSE](LICENSE) を参照してください。
