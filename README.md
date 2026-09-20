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

## なぜ 30 行で済ませないのか

Temporal の SDK に saga のヘルパーはありません（`saga` や `Compensat` で全文検索して
0件）。公式サンプルの `Compensations` はスライスと逆順ループで30行ほど。たいていは
それで足ります。

足りなくなるのは次の4点を踏んだときで、どれも異常系でしか表に出ません。このライブラリ
の中身は、実質この4つです。

**キャンセルされると補償が動かない。** キャンセル済みのワークフロー context は、以降の
アクティビティをスケジュールせずに即座に失敗させます。つまり素直に書いた補償は、
一番必要な場面で何もしない。`Run` は `workflow.NewDisconnectedContext` から取った
context で補償を回します。その context は外から誰もキャンセルできないので、
`CompensationBudget` を必須にしました。詰まったときに止める手段が他にないからです。

**タイムアウトしたステップが取り消されない。** アクティビティがタイムアウトで失敗しても、
ワーカー側では完走しているかもしれません。結果を返せなかっただけ、という場合です。
成功したときだけ補償を積む実装は、この副作用を永久に取り残します。`Step` は forward を
実行する**前**に補償を登録します。

**forward と補償が別のものを見てしまう。** 両者には同じ冪等キーが渡ります。キーは
run ID とステップ名から決まり、アクティビティの中では `saga.IdempotencyKey` で読めます。

**エラーチェックを1つ忘れると、壊れた成功になる。** ステップが失敗した後に body が
nil を返しても、`Run` はワークフローを失敗させ、body の戻り値を捨てます。これが無いと、
副作用が半分だけ適用されたワークフローが完了として記録される。UI は緑、アラートも
鳴らない。

`fwd` と `undo` は SDK の `any` ではなく型付きの関数で受けるので、入れ替えや引数の型違い
はコンパイルエラーになります。SDK はここを見ていません。`ExecuteActivity` は関数値を
検証の前に名前文字列へ変換し、文字列経路では引数の照合が飛びます。取り違えが表に出るのは
アクティビティが実行される時、補償なら saga が既に失敗している最中です。

## アクティビティ側に必要なこと

2つだけ。1つ目が難しい方です。

1. forward は冪等キーを**原子的に** claim する。キーが使用済みか確認してから処理する形
   では足りません。タイムアウトすると2つの試行が同時に走り、両方が「未使用」を見ます。
   一意制約（`INSERT ... ON CONFLICT`）か、下流 API の idempotency-key ヘッダを使って
   ください。
2. 補償は、取り消すものが無いときに成功する。補償は処理が起きる前に登録されるので、
   実際には起きなかったステップに対しても呼ばれます。

書き方は `example/order/activity.go` にあります。

## 塞げていないこと

補償は、取り消す対象が実際に起きたかどうかを確実には判定できません。タイムアウトした
forward はワーカー上で動き続けます。補償が「何も起きていない」と見て成功を返した後に、
副作用が着地しうる。これを塞ぐには、補償がトゥームストーンを書いて forward がそれを
見てからコミットする、つまり両者が1つのストアで直列化されている必要があります。外部の
決済ゲートウェイや配送キャリアが相手なら無理です。ライブラリ側でできることはありません。

冪等キーを受け付けない下流も同じく安全にできません。送信済みのメールのように取り消せない
副作用も。

ワークフローは terminate ではなく cancel してください。terminate はワークフローコードを
一切実行しないので、何も補償されません。

それからこのパッケージは、ワークフローコマンドを発行します。バージョンを上げると、使って
いる全ワークフローのコマンド列が変わり、実行中の run のリプレイが壊れる。コマンドが
パッケージの内側にあるので、利用側は `workflow.GetVersion` で囲えません。実際のところ、
旧バージョンで始まった saga が尽きるまでバージョンは動かせません。コピーせず依存として
取るなら、ここは織り込んでおいてください。

## 開発

すべて `Dockerfile` のコンテナ（Nix 経由の Go と `temporal-cli`）の中で動きます。ホスト
からは `just` で叩くだけで、Go を入れる必要はありません。

```bash
just ci      # fmt-check, vet, build, ユニットテスト, 仕様
just test    # ユニットテスト（インメモリのテスト環境）
just spec    # specs/ の仕様（実際の dev server 相手）
```

スイートが2つあるのは速度のためではありません。Temporal のテスト環境はキャンセル済みの
context でもアクティビティを実行し、アクティビティのタイムアウトも課しません。だから
「キャンセル後もロールバックが走る」は、壊れていてもそこでは通ります。このライブラリの
存在理由がまさにそれなので、仕様は実サーバ相手に回します。

`saga/*_test.go` が API の不変条件をミリ秒で押さえ、`specs/` が外から見える振る舞いを
押さえる。後者は実行される散文です。

```
## キャンセルされた saga もロールバックされる

* 課金の後で待機する注文 "cancelme"
* "charge" が実行されたら saga をキャンセルする
* saga は失敗する
* ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge" を保持していない
```

ステップの実装は `stepImpl/`。仕様とコードの対応づけは、ステップ文の一致だけです。
スイートは `testsuite.StartDevServer` で実際の dev server を起動します。実装の無いステップ
は `just spec-validate` が file:line 付きで報告し、`just spec-steps` が対応表を出します。

コマンド一覧は `CLAUDE.md` に。
