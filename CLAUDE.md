# temporal-saga

Temporal 上で Saga パターンを実装するための Go ライブラリ。ロールバックできる
アクティビティの列を書くためのもので、間違えやすいところを先に配線してある。

## 構成技術

- Go（モジュール `github.com/yamakura-yuma/temporal-saga`）
- Temporal Go SDK（`go.temporal.io/sdk`）

## レイアウト

- `saga/` — ライブラリ本体。`Run` がロールバックを所有し、`ActivityStep` が forward を1つ
  実行して補償を登録する。ユニットテストは Temporal のインメモリ環境で動く
- `docs/` — ドキュメント。`design.md`（なぜこの形か）、`patterns.md`（よくある形と
  example への索引）、`activity-contract.md`（アクティビティ側の契約と、塞げていない
  こと）、`development.md`（開発手順）
- `docs/specs/` — Gauge の markdown で書かれた実行される仕様。日本語。スイートが
  プロセス内に起動する実際の Temporal dev server に対して実行する。コードとの
  対応づけはステップ文が `gauge.Step(...)` の文字列と一致することだけ。場所は
  `env/default/default.properties` の `gauge_specs_dir` で決まる（コマンドライン
  引数では変わらない）
- `stepImpl/` — そのステップの Go 実装と、サーバとワーカーを起動するスイートフック
- `example/order/` — 仕様が動かす saga（reserve, charge, ship）。アクティビティ側の
  契約の実装例でもある。Gauge はテストバイナリではなくモジュールをビルドするので、
  通常パッケージに置く
- `example/approval/` — signal 待ちを `saga.FuncStep` でステップにする例。
  アクティビティは order のものを使い、待つ部分だけを見せる
- `example/pipeline/` — 前段の出力が次段の入力になる例
- `example/state/` — state 構造体とメソッドに割り、`Run` の中を短く保つ例
- `example/childflow/` — ステップが子ワークフローの例（`saga.ChildWorkflowStep`）
- `example/external/` — signal で他のワークフローを動かす例（`saga.SignalStep`）

example は1テーマ1個。増やすときもこの単位を守り、`diagram.html` も一緒に置く。

ここにアプリケーションは無く、`internal/` も無い。このリポジトリはライブラリであり、
`internal/` に置いたライブラリはモジュールの外から import できないため。

## Commands

開発は `Dockerfile` から作られるコンテナ（Nix で固定した Go + `temporal-cli`、
`flake.nix` 参照）の中で行い、ホストからは `just` で駆動する。ホスト側に Go を
入れる必要はない。リポジトリは `/workspace` に bind mount されるので、編集は
リビルド無しで反映される。

- `just` — レシピ一覧
- `just build` / `just vet` / `just test` — Go のビルド、vet、ユニットテスト
- `just spec` — `docs/specs/` の仕様を実際の dev server に対して実行
- `just spec-validate` — 全ステップに実装があるかを、実行せずに確認
- `just spec-steps` — どの Go 関数がどのステップを実装しているかの対応表
- `just ci` — fmt-check, vet, build, ユニットテスト, spec-validate, spec。変更を
  出す前に通す
- `just shell` — dev コンテナの対話シェル

## スキル

エージェント設定は2箇所から来る。どちらも `just apm-install` が `./.claude/` に
展開する（`apm.yml` 参照）。

- `.apm/` — このプロジェクト固有の知識。`saga-workflows` スキルが、ここでの saga
  ステップと補償の作り方、および変更が通すべき決定性・冪等性・リトライの
  チェックリストを扱う。編集・レビュー対象はこのディレクトリで、このリポジトリが
  自分で書いているエージェント設定はこれだけ。
- `core-principal` — 共有ハーネス（ルール、git のガードフック、`core-*` スキル）。
  [dotfiles](https://github.com/yamakura-yuma/dotfiles) リポジトリからコミットで
  固定して取得する。変更は向こうで行い、ここでは `apm update` で `ref` を上げる。
  ローカルでフォークしないこと。

他のリポジトリでも同じに読めるものは `.apm/` ではなく `core-principal` に属する。
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
