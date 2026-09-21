# 開発

すべて `Dockerfile` のコンテナ（Nix 経由の Go と `temporal-cli`）の中で動きます。
ホストからは `just` で叩くだけで、Go を入れる必要はありません。

```bash
just ci              # fmt-check, vet, build, テスト, docs-check, 仕様
just test            # ユニットテストだけ。数秒で返る
just spec            # 仕様だけ（実際の dev server 相手）
just spec-ui         # 同じ実行だが dev server を残す（履歴を localhost:8233 で読む）
just docs-check      # docs のコード例と、上流由来のノートの版
just shell           # コンテナの対話シェル
```

仕様は godog で書かれた普通の Go のテストなので、入口は `go test` だけです。何がどこに
書いてあるかは `docs/specs/README.md` が索引になっています。

## 速いループを壊さないこと

`just test` は `go test -short ./...` です。`-short` のとき仕様は自分を skip し、
dev server を起動しません（`specsteps/suite_test.go` の `TestMain` が `testing.Short()`
を見て早く返り、`TestFeatures` も先頭で `t.Skip` します）。これがないと、ユニット
テストを1本直すたびに Temporal が立ち上がり、数秒だったループが十数秒になります。

そのぶん `just ci` は `test` と `spec` の両方を並べています。`test` だけでは仕様が
走りません。

ビルドタグは使っていません。タグで切るとエディタと `go vet` からそのコードが見えなく
なるためです。`testing.Short()` なら普通にコンパイルされます。

## スイートが2つある理由

速度のためではありません。Temporal のテスト環境はキャンセル済みの context でも
アクティビティを実行し、アクティビティのタイムアウトも課しません。だから「キャンセル後も
ロールバックが走る」は、壊れていてもそこでは通ります。このライブラリの存在理由がまさに
それなので、仕様は実サーバ相手に回します。

| | 置き場所 | 何を押さえるか |
| --- | --- | --- |
| ユニット | `saga/saga_test.go` | API の不変条件。ミリ秒で回る |
| 仕様 | `docs/specs/` | 外から見える振る舞い。実サーバが要る。索引は `docs/specs/README.md` |

実サーバでしか確かめられないものは3つあります。キャンセル後もロールバックが走ること、
補償のアクティビティが履歴にこの順で現れること、補償が失敗したときに検索属性が
書かれること。後者はキーをサーバに登録しないと書けないので、スイートが起動する dev server
に登録しています。

dev server は `specsteps` の `TestMain` が1回だけ起動し、ワーカーもそこで立てます。
`-short` のときは起動しません（上の「速いループを壊さないこと」）。

## 仕様とコードの対応づけ

対応づけはステップ文の一致だけです。godog は仕様の1行から Gherkin のキーワードを外し、
残りの文字列に**正規表現が一致する**ステップを探します。引用符の中身はキャプチャグループ
として引数になります。

```
docs/specs/rollback.feature
  もし "charge" が実行されたら saga をキャンセルする
            ↓
specsteps/steps_test.go
  sc.Step(`^"([^"]*)" が実行されたら saga をキャンセルする$`, func(ctx context.Context, step string) error { ... })
```

引数はキャプチャグループが現れた順に渡ります。`ctx` を第1引数に取ると、そのシナリオ用の
保管場所（`scenarioState`）が引けます。

正規表現は**両端を `^` と `$` で留めてください**。留めないと `^注文 "([^"]*)"$` が
`承認待ちの注文 "approved"` にも当たり、godog は ambiguous として報告します。

コンパイラは何も確認しませんが、スイートは `Strict: true` で走ります
（[`internal/flags/options.go` の `Strict`](https://github.com/cucumber/godog/blob/v0.16.0/internal/flags/options.go#L42)
は "Fail suite when there are pending or undefined or ambiguous steps"）。実装の無い
ステップは飛ばされずに落ちるので、別の検査は要りません。

シナリオは Go のサブテストです
（[同 `TestingT`](https://github.com/cucumber/godog/blob/v0.16.0/internal/flags/options.go#L70)
は "TestingT runs scenarios as subtests"）。1本だけ走らせるには名前で絞ります。
空白はアンダースコアになります。

```bash
just spec -run 'TestFeatures/成功した_saga_は何も取り消さない'
```

逆方向（コードから仕様）は `docs/specs/` をステップ文で検索してください。

## 仕様を足すとき

日本語で書きます。ファイルの先頭に `# language: ja` を置くと、`機能` / `シナリオ` /
`シナリオアウトライン` / `前提` / `もし` / `ならば` / `かつ` / `例` が使えます。
ステップ文と正規表現は一致が必要なので、両側とも日本語になります。シナリオは運用者が
説明する言葉で書き、Temporal の語彙は `specsteps/` に閉じ込めてください。

パーサが拾うのは拡張子 `.feature` のファイルだけです
（[`internal/parser/parser.go`](https://github.com/cucumber/godog/blob/v0.16.0/internal/parser/parser.go#L84)）。

## 置き場所

| パス | 中身 |
| --- | --- |
| `saga/` | ライブラリ本体とユニットテスト |
| `docs/specs/` | 実行される仕様（`.feature`）と、その索引 `README.md`。結合テストの本体 |
| `specsteps/` | 仕様文と Go を繋ぐ語彙層。そのステップの実装と、サーバとワーカーを起動する `TestMain` |
| `example/activity/` | 仕様が動かすアクティビティ。ワークフローごとではなく1セットで、1アクティビティ1ファイル |
| `example/workflow/*/` | 仕様が動かす saga。1テーマ1パッケージ（order / pipeline / state / childflow / approval / external） |

結合テストは `docs/specs/` の仕様・`specsteps/` のステップ実装・スイートが起動する実際の
dev server の3つで成り立ちます。押さえたい振る舞いは仕様の側に書き、`specsteps/` は
その文を Go に繋ぐだけに留めてください。

`specsteps/` の中身はすべて `_test.go` です。仕様を走らせる以外に使う人はいないので、
通常パッケージにする理由がありません。仕様の置き場所は godog の既定（`./features`）では
なく、`specsteps/suite_test.go` の `Paths` が `../docs/specs` を指しています。仕様は
仕様書としてまとまっているべきで、`_test.go` の間に散らすと通して読めなくなるためです。

**`specsteps/` という名前と、この2分割はこのリポジトリの発明です。** Go にテスト専用
ディレクトリの規約はありません。よく引かれる
[golang-standards/project-layout](https://github.com/golang-standards/project-layout) は
README 自身が非公式だと書いています。godog の README が使う `features/` サブディレクトリも
cucumber 側の習慣で、Go の規約ではないので採っていません。既存の規約があるかのように
読まないでください。

`example/*/` が通常パッケージなのは、`specsteps/` がそれを import するからです。
`_test.go` に置いたものは他のパッケージから import できません。

`internal/` はありません。そこに置いたライブラリはモジュールの外から import できません。

## 注意

コンテナの中でリポジトリのファイルを書き換える git コマンドを走らせないでください。
コンテナは root で動くので、bind mount したファイルが root 所有になり、ホストから
編集できなくなります。以前ここに溜まっていたテストレポートの出力ディレクトリが root
所有になり、worktree を消せなくなったことがあります。

`saga/` を変えると、それを使うすべてのワークフローのコマンド列が変わり、実行中の run の
リプレイが壊れます。ワークフローコマンドを追加・削除・並べ替える変更は、破壊的変更として
扱ってください。
