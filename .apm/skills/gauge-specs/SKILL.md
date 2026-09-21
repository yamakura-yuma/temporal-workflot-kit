---
name: gauge-specs
description: >-
  `docs/specs/` の実行される仕様を書く・直すとき、またはそのステップの実装を
  `stepImpl/` に足すときに使う。ユニットテストと仕様のどちらに書くかの判断と、
  ステップ文とコードの対応づけを扱う。
---

# 実行される仕様

`docs/specs/` は Gauge の markdown で書かれた仕様で、スイートがプロセス内に起動する
実際の Temporal dev server に対して実行する。場所は `env/default/default.properties` の
`gauge_specs_dir` が決める（コマンドライン引数では変わらない）。

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
docs/specs/rollback.spec
  * "charge" が実行されたら saga をキャンセルする
            ↓
stepImpl/steps.go
  gauge.Step("<step> が実行されたら saga をキャンセルする", func(step string) { ... })
```

- ステップ文と `gauge.Step(...)` の文字列リテラルは**完全一致**が必要。
- 仕様は日本語で書く。上の理由で、リポジトリの他が英語でも両側とも日本語になる。
- 引数は**文中にプレースホルダが現れた順**に渡る。宣言順ではない。

## 道具

- `just spec-validate` — 実装の無いステップを file:line 付きで、サーバを起動せずに
  報告する。`just ci` の一部。
- `just spec-steps` — どの Go 関数がどのステップを実装しているかの対応表。
- `just spec-ui` — 同じ実行だが、終わっても dev server を残す。落ちたシナリオの
  ワークフロー履歴を `http://localhost:8233` で読むため。**シナリオが落ちた理由が
  ステップ文から読み取れないときは、まずこれを使う。** `WorkflowID` は
  `saga-<注文 id>`。Ctrl-C で終了。
- 逆引き（コードから仕様）は `docs/specs/` をステップ文で検索する。

## 書き方

仕様は実行される散文なので、シナリオは**運用者が説明する言葉**で書き、Temporal の
語彙は `stepImpl/` に閉じ込める。

ステップ実装を `stepImpl/` に置くのは Gauge の既定だからで、このリポジトリの発明では
ない（gauge-go の `constants/gauge.go` の `DefaultStepImplDir`。`gauge init go` が
この名前で作る）。ランナーは `go build ./...` でモジュールの全パッケージを集めるので
パッケージ名に依存しないが、他の Gauge プロジェクトと同じ読み方ができるよう既定の
ままにする。詳しくは `docs/development.md`「置き場所」。

仕様が動かす saga は `example/*/` にあり、**1テーマ1 example**。Gauge はテストバイナリ
ではなくモジュールをビルドするので、`_test.go` には置けず通常パッケージに置く。
example を増やすときは `diagram.html` も一緒に置く。

詳しくは `docs/development.md`。
