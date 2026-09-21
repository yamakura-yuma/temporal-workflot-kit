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
        StartToCloseTimeout:    10 * time.Second,
        ScheduleToCloseTimeout: time.Minute,   // 補償はこれが無いと無制限にリトライする
    })

    return saga.RunOrCompensate(ctx, saga.Options{
    }, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
        w := &fulfillment{in: in}

        s.Step(ctx, "reserve", w.reserve, w.unreserve)
        s.Step(ctx, "charge", w.chargeCard, w.refund)
        s.Step(ctx, "ship", w.ship, w.cancelShipment)

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
* アクティビティ "Reserve, Charge, Refund, Unreserve" が実行された
* 注文は "Reserve, Charge" を保持していない
```

## 機能・特徴

| 機能 | 中身 |
| --- | --- |
| ロールバック | 失敗すると、登録済みの補償を逆順で実行する |
| キャンセル対応 | ワークフローがキャンセルされても補償は実行される |
| 登録の順序 | 補償は forward を**実行する前**に登録される。タイムアウトした forward も取り消される |
| 失敗の報告 | 補償が失敗したら、型付きのエラーと、どのステップかの一覧で残す。元のエラーは消さない |
| 出口が1つ | エラーチェックを忘れても、ステップが失敗していればワークフローは失敗する |
| 巻き戻しの制御 | `ParallelCompensation` と `ContinueWithError`（Java SDK の `Saga` と同じ2つ） |

ステップの両半分は `func(workflow.Context) error` です。**ライブラリは
`workflow.ExecuteActivity` を包みません。** アクティビティで実行するのも、子ワークフローで
実行するのも、signal を待つのも、その関数に何を書くかの違いです。タイムアウトもリトライも
普通に context へ載せてください。

なぜこの形なのか、素直に書くと何が壊れるのかは [docs/design.md](docs/design.md) に
コード付きで書いてあります。Temporal が初めてなら、その前に登場人物を図で押さえる
[docs/temporal-concepts.html](docs/temporal-concepts.html) を。

## インストール・セットアップ

```bash
go get github.com/yamakura-yuma/temporal-workflow-kit/saga
```

必要なのは Go 1.26 以降と Temporal Go SDK v1.49 以降。サーバ側に特別な設定は要りません。
補償の失敗を検索属性で可視化するなら、そのキーをサーバに登録したうえで、ワークフロー側で
`saga.CompensationFailedType` を見て自分で立ててください（`example/workflow/order/`）。

## 使用方法

デモの後半がそのまま使い方です。`saga.RunOrCompensate` にワークフロー本体を渡し、
`s.Step` に forward と補償の2つの関数を渡します。

**このライブラリが守るのは2つだけです。** 補償を forward より先に登録すること、失敗したら
切り離した context で逆順に実行すること。残りは利用者側の契約で、守らないとロールバックが
静かに壊れます。先に [docs/interface.md](docs/interface.md) を読んでください。塞げていない
穴も同じ文書にあります。

### 他のパターン

1テーマにつき1つ。どれも `docs/specs/` の仕様から実際に動かしています。

それぞれに `diagram.html` を置いてあります。正常系と失敗時にどの順で何が走るかは、
そちらをブラウザで開くのが早いです。

アクティビティは [`example/activity/`](example/activity/) に1セットだけあります。
ワークフローごとの写しは置いていません。アクティビティはワークフローではなくワーカーに
属するもので、下の6つは同じ `Reserve` や `Charge` を呼びます。

| 例 | 何を見せているか | 図 |
| --- | --- | --- |
| [`example/workflow/order/`](example/workflow/order/) | 基本形。3ステップと補償、冪等キーを呼び先に渡すアクティビティの書き方、巻き戻し失敗の検知 | [図](example/workflow/order/diagram.html) |
| [`example/workflow/pipeline/`](example/workflow/pipeline/) | 前段の出力が次段の入力になる saga。補償が前段の ID をどう受け取るか | [図](example/workflow/pipeline/diagram.html) |
| [`example/workflow/state/`](example/workflow/state/) | 入力が多い5ステップの saga を state 構造体とメソッドに割り、`Run` の中を2行に保つ。同じ saga を素の形で書いた `workflow_flat.go` と読み比べられる | [図](example/workflow/state/diagram.html) |
| [`example/workflow/childflow/`](example/workflow/childflow/) | forward を子ワークフロー、取り消しをアクティビティで実行するステップ。冪等キーを境界の向こうへ渡す | [図](example/workflow/childflow/diagram.html) |
| [`example/workflow/approval/`](example/workflow/approval/) | signal 待ちをステップにする。判断は自分の関数の中で完結させる | [図](example/workflow/approval/diagram.html) |
| [`example/workflow/external/`](example/workflow/external/) | signal で他のワークフローを動かすステップ。失敗すると打ち消しの signal が飛ぶ | [図](example/workflow/external/diagram.html) |

形ごとの書き方は [docs/patterns.md](docs/patterns.md) に。

## API・設定

| | |
| --- | --- |
| `saga.RunOrCompensate(ctx, opts, body)` | saga を実行し、失敗したらロールバックする |
| `s.Step(ctx, name, do, undo)` | 補償を登録してから forward を実行する。両方 `func(workflow.Context) error` |
| `s.Err()` / `s.ClearErr()` | 最初に失敗したステップのエラーと、それを握るとき |
| `saga.Options` | `ParallelCompensation` と `ContinueWithError` の2つだけ。どちらも任意 |
| `saga.CompensationReport` | 失敗した補償とスキップされた補償の一覧 |

補償は外から誰もキャンセルできない context で走ります。**`ScheduleToCloseTimeout` を必ず
設定してください。** Temporal の既定のリトライは無制限で、止めるのはこれだけです。

## 貢献方法

```bash
just ci      # fmt-check, vet, build, ユニットテスト, docs-check, 仕様
```

すべてコンテナの中で動くので、ホストに Go を入れる必要はありません。どちらのスイートに
何を書くかは [docs/development.md](docs/development.md) に。

## ライセンス・作者情報

MIT License。作者は yamakura-yuma。詳細は [LICENSE](LICENSE) を参照してください。
