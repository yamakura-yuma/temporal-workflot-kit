# temporal-saga

[Temporal](https://temporal.io) 上で Saga パターンを実装するための Go ライブラリ。
ロールバックできるアクティビティの列を書くためのもので、**間違えやすいところを
先に配線してある**のが中身です。

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

`charge` が失敗すれば、`reserve` と `charge` が逆順で取り消され、ワークフローは
元のエラーで失敗します。

## このライブラリが引き受けること

Temporal の SDK は saga のヘルパーを持っておらず、素直に書いた30行版は4つのことを
間違えます。その4つがこのパッケージの存在理由です。

**補償は disconnected context で実行する。** キャンセルされたワークフローの context
は以降のアクティビティを即座に失敗させるので、素直に書いた補償は**それが最も必要な
場面で何もしません**。`Run` は `workflow.NewDisconnectedContext` から context を取り、
`CompensationBudget` でフェーズ全体に上限をかけます。この予算は必須です
— disconnected context は外から誰もキャンセルできないからです。

**補償は forward アクティビティの実行前に登録する。** 成功した後ではありません。
タイムアウトで失敗したアクティビティは、結果を返せなかっただけでワーカー上では
完走しているかもしれません。成功時にだけ登録する実装では、その副作用が永久に
残ります。

**ステップの forward と補償は同じ冪等キーを見る。** キーは run ID とステップ名から
決まり、アクティビティの中では `saga.IdempotencyKey` で読めます。

**ステップが失敗した後に body が nil を返しても、ワークフローは失敗する。** body の
戻り値は捨てられます。そうしないと、エラーチェックを1つ忘れただけで、副作用が半分
だけ適用されたワークフローが**成功として記録されます** — UI は緑、アラートも鳴らず、
何もロールバックされません。

`fwd` と `undo` は SDK の `any` ではなく型付きの関数として受け取るので、入れ替えや
引数の型違いはコンパイルエラーになります。SDK はこれを捕まえません。
`ExecuteActivity` は関数値を検証の前に名前文字列へ変換し、文字列経路では引数の検証が
飛ばされるため、取り違えは**アクティビティが実行される時 — 補償なら saga が既に
失敗している最中 — に初めて表面化します**。

## アクティビティ側に課される契約

2つだけです。そして1つ目が難しい方です。

1. **forward アクティビティは冪等キーを原子的に claim すること。** キーが使用済みか
   確認してから処理する、では足りません。タイムアウトすると2つの試行が同時に走り、
   両方が「未使用」を見ます。一意制約（`INSERT ... ON CONFLICT`）か、下流 API 自身の
   idempotency-key ヘッダを使ってください。
2. **補償は、取り消すものが無いときに成功すること。** 補償は処理が起きる前に登録
   されるので、実際には起きなかったステップに対して呼ばれることがあります。

実装例は `example/order/activity.go` にあります。

## このライブラリが解決しないこと

**補償は、取り消す対象のステップが実際に起きたかどうかを確実には判定できません。**
タイムアウトした forward アクティビティはワーカー上で動き続けます。補償が「何も
起きていない」と観測して成功を返した**後で**、副作用が着地しうるということです。
これを塞ぐには、補償がトゥームストーンを書き、forward がそれを確認してからコミット
する必要があります。つまり両者が1つのストアで直列化されている必要があり、外部の
決済ゲートウェイや配送キャリアが相手では不可能です。どんなライブラリでも同じです。

冪等キーを受け付けない下流システムは、このやり方では安全にできません。送信済みの
メールのように**取り消せない副作用**も同様です。

ワークフローは terminate ではなく cancel してください。terminate はワークフロー
コードを一切実行しないので、何も補償されません。

そしてこのパッケージは**ワークフローコマンドを発行します**。バージョンを上げると、
それを使うすべてのワークフローのコマンド列が変わり、実行中の run のリプレイが
壊れます。コマンドがパッケージの内側にあるため、利用側は `workflow.GetVersion` で
囲えません。実際には、旧バージョンで始まった saga が尽きるまでバージョンを上げ
られません。これがコピーせず依存として取ることの代償です。

## 開発

すべて `Dockerfile` から作られるコンテナ（Nix 経由の Go + `temporal-cli`）の中で
動き、ホストからは `just` で駆動します。ホスト側に Go を入れる必要はありません。

```bash
just ci      # fmt-check, vet, build, ユニットテスト, 仕様
just test    # ユニットテスト（インメモリのテスト環境）
just spec    # specs/ の仕様（実際の dev server 相手）
```

スイートが2つあるのは速度のためではありません。Temporal のテスト環境は**キャンセル
された context でもアクティビティを実行し、アクティビティのタイムアウトも課さない**
ので、このライブラリの存在理由である「キャンセル後もロールバックが走る」は、動いて
いなくてもそこでは通ってしまいます。

`saga/*_test.go` は API の不変条件をミリ秒で押さえます。`specs/` は外から見える
振る舞いを、**実行される散文**として押さえます。

```
## キャンセルされた saga もロールバックされる

* 課金の後で待機する注文 "cancelme"
* "charge" が実行されたら saga をキャンセルする
* saga は失敗する
* ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge" を保持していない
```

これらのステップは `stepImpl/` に実装されており、**対応づけはステップ文の一致だけ**
です。スイートは `testsuite.StartDevServer` で実際の Temporal dev server を起動します
（dev イメージが持っている `temporal` CLI を使います）。実装の無いステップは
`just spec-validate` が報告し、`just spec-steps` が対応表を出します。

コマンドの一覧は `CLAUDE.md` にあります。
