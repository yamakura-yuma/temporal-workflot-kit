---
name: godog-specs
description: >-
  `docs/specs/` の実行される仕様を書く・直すとき、またはそのステップの実装を
  `specsteps/` に足すときに使う。ユニットテストと仕様のどちらに書くかの判断と、
  ステップ文とコードの対応づけを扱う。
---

# 実行される仕様

`docs/specs/` は日本語の Gherkin（`.feature`）で書かれた仕様で、スイートがプロセス内に
起動する実際の Temporal dev server に対して実行する。走らせるのは godog で、入口は
`go test` ひとつ。場所は `specsteps/suite_test.go` の `Paths` が決める（godog の既定は
`./features`）。

## どちらのスイートに書くか

速度の問題ではない。Temporal のインメモリのテスト環境には穴が2つある。

- **キャンセルされた context でもアクティビティを実行する**
- **アクティビティのタイムアウトを課さない**

どちらかに依存する振る舞い — キャンセル後もロールバックが走ること、補償のアクティビティ
ID が履歴にこの順で現れること、検索属性が書かれること — は `docs/specs/` の仕様にする。
さもないと、振る舞いが壊れていてもテストは通る。それ以外は `saga/` のユニットテストに
置く。ミリ秒で回る。

## ステップ文が唯一の接続

**仕様とコードの対応づけは、そのステップ文だけである。** コンパイラは何も確認しない。

```
docs/specs/rollback.feature
  もし "charge" が実行されたら saga をキャンセルする
            ↓
specsteps/steps_test.go
  sc.Step(`^"([^"]*)" が実行されたら saga をキャンセルする$`, func(ctx context.Context, step string) error { ... })
```

- ステップ文はキーワード（`前提` / `もし` / `ならば` / `かつ`）を外した残りが、
  登録した**正規表現に一致**しなければならない。
- 正規表現は**両端を `^` と `$` で留める**。留めないと `^注文 "([^"]*)"$` が
  `承認待ちの注文 "approved"` にも当たり、ambiguous として落ちる。
- 引数は**キャプチャグループが現れた順**に渡る。第1引数に `context.Context` を取ると、
  そのシナリオ用の `scenarioState` が `stateOf(ctx)` で引ける。
- ステップは `error` を返す。失敗は panic ではなく戻り値で伝える。
- 仕様は日本語で書く。上の理由で、リポジトリの他が英語でも両側とも日本語になる。
  ファイルの先頭に `# language: ja` が要る。
- 拡張子は `.feature` でなければ拾われない。

## 検査は Strict がやる

スイートは `Strict: true` で走る。実装の無いステップ、pending、ambiguous は飛ばされずに
**`go test` を落とす**。だから対応づけを確かめる専用のコマンドは無い。`just test` か
`just ci` を通せばよい。

## 道具

- `just spec` — 仕様だけを verbose で走らせる。godog が各シナリオと各ステップを印字する。
- `just spec -run 'TestFeatures/<シナリオ名>'` — シナリオは Go のサブテストなので1本だけ
  走らせられる。名前の空白はアンダースコアになる。
- `just spec-ui` — 同じ実行だが、終わっても dev server を残す。落ちたシナリオの
  ワークフロー履歴を `http://localhost:8233` で読むため。**シナリオが落ちた理由が
  ステップ文から読み取れないときは、まずこれを使う。** `WorkflowID` は
  `saga-<注文 id>`。Ctrl-C で終了。
- 逆引き（コードから仕様）は `docs/specs/` をステップ文で検索する。

## 書き方

仕様は実行される散文なので、シナリオは**運用者が説明する言葉**で書き、Temporal の
語彙は `specsteps/` に閉じ込める。

`specsteps/` の中身はすべて `_test.go`。仕様を走らせる以外に使う人はいないので、通常
パッケージにする理由がない。ステップを足すときは、その example に対応するファイル
（`approval_test.go` など）の `registerXxxSteps` に `sc.Step(...)` を並べる。共有の
結果判定（`saga は成功する` など）は `steps_test.go` にある。

仕様が動かす saga は `example/*/` にあり、**1テーマ1 example**。`specsteps/` がこれを
import するので通常パッケージに置く（`_test.go` に置いたものは他のパッケージから
import できない）。example を増やすときは `diagram.html` も一緒に置く。

詳しくは `docs/development.md`。
