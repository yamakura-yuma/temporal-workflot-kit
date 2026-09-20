# temporal-saga

Temporal で、取り消せるアクティビティの列を書くための Go ライブラリ。

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

`charge` で落ちれば、`charge`、`reserve` の順に取り消され、ワークフローは元のエラーで
失敗します。

## デモ

振る舞いは `docs/specs/` に実行できる仕様として置いてあります。`just spec` が実際の
Temporal dev server を起動して、この通りに動くか確かめます。

```
## キャンセルされた saga もロールバックされる

* 課金の後で待機する注文 "cancelme"
* "charge" が実行されたら saga をキャンセルする
* saga は失敗する
* ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge" を保持していない
```

```console
$ just spec
Specifications:	1 executed	1 passed	0 failed	0 skipped
Scenarios:	4 executed	4 passed	0 failed	0 skipped
```

「ステップ ... が実行された」はワークフロー履歴を読んでいます。つまり運用が Temporal の
UI で見る順序そのもの。

## 機能・特徴

Temporal の SDK に saga のヘルパーはありません。公式サンプルの `Compensations` は
スライスと逆順ループで30行ほどで、たいていはそれで足ります。足りなくなるのは次の4点を
踏んだときで、どれも異常系でしか表に出ません。

- **キャンセルされても補償が動く。** キャンセル済みの context は以降のアクティビティを
  即座に失敗させるので、素直に書いた補償は一番必要な場面で何もしません
- **タイムアウトしたステップも取り消される。** 補償を forward の実行前に登録するため
- **forward と補償が同じ冪等キーを見る。** キーは run ID とステップ名から決まる
- **エラーチェックを1つ忘れても、壊れた成功にならない。** ステップが失敗していれば
  `Run` がワークフローを失敗させ、半端な戻り値を捨てる

`fwd` と `undo` は型付きの関数で受けるので、入れ替えはコンパイルエラーになります。
SDK はここを見ていません。

理由と実装は [docs/design.md](docs/design.md) に。

## インストール・セットアップ

```bash
go get github.com/yamakura-yuma/temporal-saga/saga
```

必要なのは Go 1.26 以降と Temporal Go SDK v1.49 以降。サーバ側に特別な設定は要りません。
補償の失敗を検索属性で可視化する場合だけ、そのキーをサーバに登録しておきます。

## 使用方法

冒頭のコードがそのまま使い方です。`saga.Run` にワークフロー本体を渡し、各ステップを
`saga.Step` で書きます。ステップのエラーは、直線的な saga なら無視して構いません。
最初の失敗以降、後続の `Step` は何もせず、`Run` がまとめて面倒を見ます。

アクティビティ側には2つだけ約束があります。forward は冪等キーを**原子的に** claim する
こと、補償は取り消すものが無いときに成功すること。どちらも外すとロールバックが壊れるので、
[docs/activity-contract.md](docs/activity-contract.md) を先に読んでください。塞げていない
穴も同じ文書に書いてあります。

## API・設定

| | |
| --- | --- |
| `saga.Run(ctx, opts, body)` | saga を実行し、失敗したらロールバックする |
| `saga.Step(ctx, s, name, fwd, undo, in)` | forward を1つ実行し、その補償を登録する |
| `saga.Options` | アクティビティの既定、補償の予算、鍵の作り方 |
| `saga.IdempotencyKey(ctx)` | アクティビティ側から冪等キーを読む |
| `saga.CompensationReport` | 失敗した補償とスキップされた補償の一覧 |

`CompensationBudget` は必須です。補償は外から誰もキャンセルできない context で走るので、
上限が無いと詰まったときに止める手段がありません。詳細は `go doc ./saga`。

## 貢献方法

```bash
just ci      # fmt-check, vet, build, ユニットテスト, 仕様
```

すべてコンテナの中で動くので、ホストに Go を入れる必要はありません。どちらのスイートに
何を書くかは [docs/development.md](docs/development.md) に。

## ライセンス・作者情報

MIT License。作者は yamakura-yuma。詳細は [LICENSE](LICENSE) を参照してください。
