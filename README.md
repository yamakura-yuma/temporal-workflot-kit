# temporal-workflow-kit

Temporal のワークフローを書くための Go の部品集。今のところ中身は `saga/` の1つで、
取り消せるアクティビティの列を書くためのものです。

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
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 10 * time.Second,
    })

    return saga.Run(ctx, saga.Options{
        CompensationBudget: 5 * time.Minute,
    }, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
        w := &fulfillment{in: in}

        saga.Step(ctx, s, "reserve", w.reserve, w.unreserve)
        saga.Step(ctx, s, "charge", w.chargeCard, w.refund)
        saga.Step(ctx, s, "ship", w.ship, w.cancelShipment)

        return w.receipt(), nil
    })
}

// ステップの半分は、ただのワークフローコード。ライブラリは何も包みません
func (w *fulfillment) chargeCard(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Charge,
        ChargeReq{Order: w.in, Reservation: w.reservation}).Get(ctx, &w.charge)
}

// 補償は、forward が書いたフィールドをそのまま読めます
func (w *fulfillment) refund(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Refund,
        ChargeReq{Order: w.in, Charge: w.charge}).Get(ctx, nil)
}
```

`saga.Step` の戻り値を捨てているのは手抜きではありません。最初の失敗以降、後続の
`saga.Step` は何もせず、`Run` が元のエラーでワークフローを失敗させます。半端な `Receipt` は
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
コード付きで書いてあります。Temporal が初めてなら、その前に登場人物を図で押さえる
[docs/temporal-concepts.html](docs/temporal-concepts.html) を。

## インストール・セットアップ

```bash
go get github.com/yamakura-yuma/temporal-workflow-kit/saga
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

アクティビティは [`example/activity/`](example/activity/) に1セットだけあります。
ワークフローごとの写しは置いていません。アクティビティはワークフローではなくワーカーに
属するもので、下の6つは同じ `Reserve` や `Charge` を呼びます。

| 例 | 何を見せているか | 図 |
| --- | --- | --- |
| [`example/workflow/order/`](example/workflow/order/) | 基本形。3ステップと補償、冪等キーを呼び先に渡すアクティビティの書き方 | [図](example/workflow/order/diagram.html) |
| [`example/workflow/pipeline/`](example/workflow/pipeline/) | 前段の出力が次段の入力になる saga。補償が前段の ID をどう受け取るか | [図](example/workflow/pipeline/diagram.html) |
| [`example/workflow/state/`](example/workflow/state/) | 入力が多い5ステップの saga を state 構造体とメソッドに割り、`Run` の中を2行に保つ。同じ saga を素の形で書いた `workflow_flat.go` と読み比べられる | [図](example/workflow/state/diagram.html) |
| [`example/workflow/childflow/`](example/workflow/childflow/) | 子ワークフローで実行し、アクティビティで取り消すステップ。冪等キーは executor を跨いで同じ | [図](example/workflow/childflow/diagram.html) |
| [`example/workflow/approval/`](example/workflow/approval/) | signal 待ちをステップにする。判断は自分の関数の中で完結させる | [図](example/workflow/approval/diagram.html) |
| [`example/workflow/external/`](example/workflow/external/) | signal で他のワークフローを動かすステップ。失敗すると打ち消しの signal が飛ぶ | [図](example/workflow/external/diagram.html) |

形ごとの書き方は [docs/patterns.md](docs/patterns.md) に。

## API・設定

| | |
| --- | --- |
| `saga.Run(ctx, opts, body)` | saga を実行し、失敗したらロールバックする |
| `saga.Step(ctx, s, name, fwd, undo, in)` | forward を1つ実行し、その補償を登録する |
| `saga.StepKey(ctx, name)` | 1回の実行の1ステップに固有の文字列。冪等キーに使う |
| `saga.AwaitSignal[T](ctx, name, timeout)` | signal を待つ。`saga.Func` の中で使う |
| `saga.Options` | アクティビティの既定、補償の予算、鍵の作り方 |
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
