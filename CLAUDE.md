# temporal-workflow-kit

Temporal のワークフローを書くための Go の部品集。今入っているのは `saga/` の1つだけで、
ロールバックできるアクティビティの列を書くためのもの。間違えやすいところを先に配線してある。

名前が `temporal-saga` でないのは、運用向けの繰り返しワークフローのように saga ではない
部品が後から入っても嘘にならないようにするため。部品を足すときは `saga/` の横に並べる。

## 構成技術

- Go（モジュール `github.com/yamakura-yuma/temporal-workflow-kit`）
- Temporal Go SDK（`go.temporal.io/sdk`）

## レイアウト

- `saga/` — ライブラリ本体。`Run` がロールバックを所有し、`saga.Activity` が forward を1つ
  実行して補償を登録する。ユニットテストは Temporal のインメモリ環境で動く
- `docs/` — ドキュメント。`design.md`（なぜこの形か）、`patterns.md`（よくある形と
  example への索引）、`activity-contract.md`（アクティビティ側の契約と、塞げていない
  こと）、`development.md`（開発手順）、`sdk-notes.md`（Temporal SDK のソースを読んで
  得た事実の出自と、その版）
- `docs/specs/` — 日本語の Gherkin（`.feature`）で書かれた実行される仕様と、その索引
  `README.md`。**通して読める仕様書**として扱うので、まずは索引から読む。結合テストの
  本体でもあり、godog が走らせ、スイートがプロセス内に起動する実際の Temporal dev
  server に対して実行する。コードとの対応づけはステップ文が登録された正規表現に一致
  することだけで、それを検査するのはスイートの `Strict: true`。場所は
  `specsteps/suite_test.go` の `Paths` で決まる（godog の既定は `./features`）
- `specsteps/` — 仕様文と Go を繋ぐ語彙層。そのステップの Go 実装と、サーバとワーカーを
  起動する `TestMain`。中身はすべて `_test.go`。**この2分割と `specsteps` という名前は
  このリポジトリの発明で、Go の規約ではない**（Go にテスト専用ディレクトリの規約は無い）。
  理由は `docs/development.md`「置き場所」
- `example/activity/` — example のアクティビティ。**ワークフローごとではなく1セット**で、
  1アクティビティ1ファイル（`reserve.go`、`charge.go`、`ship.go`、`pack.go`）。各ファイルが
  入力型と forward と補償を持つ。`activity.go` に `Activities` 型と、仕様のための観測窓
  アクティビティはワークフローではなくワーカーに属するもので、同じ `Reserve` を
  order / pipeline / childflow / state が呼ぶ。ディレクトリの形でそれを見せている。
  **状態を持たせないこと**。下流への書き込みはアクティビティの副作用ではなく仕事
  そのもので、記録は下流のもの。仕様が見るのはワークフロー履歴であって、
  アクティビティの自己申告ではない
- `example/workflow/` — ワークフロー。テーマごとに1パッケージで、中身は `workflow.go` と
  `diagram.html` だけ。**1パッケージにまとめない**（`TaskQueue`、`Order`、`Receipt` が
  6組ぶつかって全部に接頭辞が要るため）
  - `order/` — 基本形の saga（reserve, charge, ship）。まずこれを読む
  - `approval/` — signal 待ちを `saga.Func` でステップにする例。自前のアクティビティは
    持たない
  - `pipeline/` — 前段の出力が次段の入力になる例。`ChargeReq.Reservation` を埋めるのが
    その主題で、埋めないのが `order/`
  - `state/` — 大きな入力を state 構造体とメソッドで細い入力に射影し、`Run` の中を短く
    保つ例。5ステップ（うち1つは signal 待ち）あり、同じ saga を素の形で書いた
    `workflow_flat.go` を並べて置く。仕様が両方を動かして同じ結果になることを確かめる
  - `childflow/` — ステップが子ワークフローの例（`saga.ChildWorkflow`）
  - `external/` — signal で他のワークフローを動かす例（`saga.Func`）

example は1テーマ1個。増やすときもこの単位を守り、`diagram.html` も一緒に置く。
ワークフローの入口は `workflow.go`。アクティビティを足すときは `example/activity/` に
1ファイル増やし、既存のものを写さない。入力型も example ごとに分けない
（`ChargeReq` が1つあり、使う example が `Reservation` を埋める）。

ここにアプリケーションは無く、`internal/` も無い。このリポジトリはライブラリであり、
`internal/` に置いたライブラリはモジュールの外から import できないため。

## Commands

開発は `Dockerfile` から作られるコンテナ（Nix で固定した Go + `temporal-cli`、
`flake.nix` 参照）の中で行い、ホストからは `just` で駆動する。ホスト側に Go を
入れる必要はない。リポジトリは `/workspace` に bind mount されるので、編集は
リビルド無しで反映される。

- `just` — レシピ一覧
- `just build` / `just vet` / `just test` — Go のビルド、vet、ユニットテスト。
  `just test` は `go test -short ./...` で、**仕様は skip され dev server も起動しない**。
  速いループを保つため
- `just spec` — 仕様だけを実行（`-short` 無し、実際の dev server 相手）。シナリオは Go の
  サブテストなので `just spec -run 'TestFeatures/<シナリオ名>'` で1本だけ走らせられる
- `just spec-ui` — 同じ実行だが dev server を残し、履歴を `http://localhost:8233`
  で読めるようにする。落ちたシナリオを調べるとき
- `just docs-check` — docs と README のコード例が現行 API と合っているか、上流由来の
  ノートが `go.mod` の SDK 版と合っているか
- `just ci` — fmt-check, vet, build, テスト, docs-check, 仕様。`test` が `-short` なので
  `spec` を別に並べてある。変更を出す前に通す
- `just shell` — dev コンテナの対話シェル

## スキル

エージェント設定は2箇所から来る。どちらも `just apm-install` が `./.claude/` に
展開する（`apm.yml` 参照）。

- `.apm/` — 知識の出自で4つに割ってある。編集・レビュー対象はこのディレクトリで、
  このリポジトリが自分で書いているエージェント設定はこれだけ。
  - `saga-package` — `saga/` パッケージが依存している不変条件と、ステップの足し方
  - `temporal-review` — 決定性・冪等性・リトライの、出す前のチェックリスト
  - `godog-specs` — `docs/specs/` の仕様と `specsteps/` の書き方
  - `upstream-docs` — 上流のドキュメントを指すか写すかの基準と、腐りの検出
- `core-principal` — 共有ハーネス（ルール、git のガードフック、`core-*` スキル）。
  [dotfiles](https://github.com/yamakura-yuma/dotfiles) リポジトリからコミットで
  固定して取得する。変更は向こうで行い、ここでは `apm update` で `ref` を上げる。
  ローカルでフォークしないこと。

他のリポジトリでも同じに読めるものは `.apm/` ではなく `core-principal` に属する。
`temporal-review` と `upstream-docs` はその線では共有側だが、`core-principal` は
Temporal と無関係なリポジトリも依存しているので置けない。受け皿ができるまでここに
置き、本体にはリポジトリ固有のパスを書かないでおく。

`.claude/` と `apm_modules/` は生成物で gitignore してある。

## graphify

このプロジェクトには graphify-out/ に知識グラフがある（god ノード、コミュニティ
構造、ファイル間の関係）。

規約:
- コードベースについての質問は、graphify-out/graph.json があればまず
  `graphify query "<質問>"` を実行する。関係を辿るなら `graphify path "<A>" "<B>"`、
  特定の概念に絞るなら `graphify explain "<概念>"`。いずれも範囲を絞った部分グラフを
  返すので、GRAPH_REPORT.md や生の grep 出力よりずっと小さい。
- graphify-out/wiki/index.md があれば、生のソースを辿る代わりに全体の案内として使う。
- graphify-out/GRAPH_REPORT.md を読むのは、アーキテクチャ全体を見直すときか、
  query / path / explain で十分な文脈が出てこないときだけ。
- コードを変更したら `graphify update .` を実行してグラフを最新に保つ
  （AST のみ、API 費用なし）。

この節は `graphify claude install` が生成したもの。再実行すると英語に戻ることがある。
