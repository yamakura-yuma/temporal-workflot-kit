# 開発

すべて `Dockerfile` のコンテナ（Nix 経由の Go、`temporal-cli`、`gauge`）の中で動きます。
ホストからは `just` で叩くだけで、Go を入れる必要はありません。

```bash
just ci              # fmt-check, vet, build, ユニットテスト, docs-check, spec-validate, 仕様
just test            # ユニットテスト（インメモリのテスト環境）
just spec            # docs/specs/ の仕様（実際の dev server 相手）
just spec-ui         # 同じ実行だが dev server を残す（履歴を localhost:8233 で読む）
just spec-validate   # 全ステップに実装があるかを、実行せずに確認
just spec-steps      # どの Go 関数がどのステップを実装しているかの対応表
just docs-check      # docs のコード例と、上流由来のノートの版
just shell           # コンテナの対話シェル
```

## スイートが2つある理由

速度のためではありません。Temporal のテスト環境はキャンセル済みの context でも
アクティビティを実行し、アクティビティのタイムアウトも課しません。だから「キャンセル後も
ロールバックが走る」は、壊れていてもそこでは通ります。このライブラリの存在理由がまさに
それなので、仕様は実サーバ相手に回します。

| | 置き場所 | 何を押さえるか |
| --- | --- | --- |
| ユニット | `saga/saga_test.go` | API の不変条件。ミリ秒で回る |
| 仕様 | `docs/specs/` | 外から見える振る舞い。実サーバが要る |

実サーバでしか確かめられないものは3つあります。キャンセル後もロールバックが走ること、
補償のアクティビティ ID が履歴にこの順で現れること、補償が失敗したときに検索属性が
書かれること。後者はキーをサーバに登録しないと書けないので、スイートが起動する dev server
に登録しています。

## 仕様とコードの対応づけ

対応づけはステップ文の一致だけです。Gauge は仕様の1行から引用符の中身を引数として外し、
残りの文字列で `gauge.Step` を探します。

```
docs/specs/rollback.spec
  * "charge" が実行されたら saga をキャンセルする
            ↓
stepImpl/steps.go
  gauge.Step("<step> が実行されたら saga をキャンセルする", func(step string) { ... })
```

引数は文中にプレースホルダが現れた順に渡ります。宣言順ではありません。

コンパイラは何も確認しないので、道具で埋めています。`just spec-validate` が実装の無い
ステップを file:line 付きで、サーバを起動せずに報告します（`just ci` の一部）。
`just spec-steps` が対応表を出します。逆方向は `docs/specs/` をステップ文で検索してください。

## 仕様を足すとき

日本語で書きます。ステップ文と `gauge.Step(...)` の文字列リテラルは完全一致が必要なので、
両側とも日本語になります。シナリオは運用者が説明する言葉で書き、Temporal の語彙は
`stepImpl/` に閉じ込めてください。

## 置き場所

| パス | 中身 |
| --- | --- |
| `saga/` | ライブラリ本体とユニットテスト |
| `docs/specs/` | 実行される仕様。結合テストの本体 |
| `stepImpl/` | 仕様文と Go を繋ぐ語彙層。そのステップの実装と、サーバとワーカーを起動するスイートフック |
| `example/*/` | 仕様が動かす saga。1テーマ1 example（order / pipeline / state / childflow / approval） |
| `manifest.json`、`env/` | Gauge の設定。プロジェクトルートに置く必要がある |

結合テストは `docs/specs/` の仕様・`stepImpl/` のステップ実装・スイートが起動する実際の
dev server の3つで成り立ちます。押さえたい振る舞いは仕様の側に書き、`stepImpl/` は
その文を Go に繋ぐだけに留めてください。

`stepImpl/` という名前は gauge-go の既定で、このリポジトリの発明ではありません
（[`constants/gauge.go` の `DefaultStepImplDir`](https://github.com/getgauge-contrib/gauge-go/blob/v0.5.2/constants/gauge.go#L4)。`gauge init go` がこの名前で作ります）。
ランナーは名前を見ていないので改名しても動きます
（[`gauge/builder.go` の `LoadGaugeImpls`](https://github.com/getgauge-contrib/gauge-go/blob/v0.5.2/gauge/builder.go#L20) が
`go build ./...` と `go list ./...` でモジュールの全パッケージを生成した main に import
するため）。それでも既定のままにしてあるのは、他の Gauge プロジェクトと同じ読み方が
できるようにするためです。

`example/order/` が通常パッケージなのは、Gauge がテストバイナリではなくモジュールを
ビルドするからです。`_test.go` に置くと、ステップ実装から見えません。

`internal/` はありません。そこに置いたライブラリはモジュールの外から import できません。

## 注意

コンテナの中でリポジトリのファイルを書き換える git コマンドを走らせないでください。
コンテナは root で動くので、bind mount したファイルが root 所有になり、ホストから
編集できなくなります。`env/default/default.properties` で一度やりました。

`saga/` を変えると、それを使うすべてのワークフローのコマンド列が変わり、実行中の run の
リプレイが壊れます。ワークフローコマンドを追加・削除・並べ替える変更は、破壊的変更として
扱ってください。
