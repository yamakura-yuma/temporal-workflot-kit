---
name: temporal-review
description: >-
  Temporal のワークフロー・アクティビティの変更を出す前に確認するときに使う。
  決定性、冪等性、リトライポリシー、実行中の履歴との後方互換を扱う。
---

# Temporal の変更を出す前に

変更したワークフローコード・アクティビティコード・ワーカー登録のファイルを**すべて**
[`references/checklist.md`](references/checklist.md) に照らして確認する。1ファイルでも
通していないまま出さない。

とくに効く失敗の型は3つ。

1. **ワークフローコードの非決定性** — リプレイで違う結果になりうるもの（時刻、乱数、
   goroutine、map の走査、直接 I/O）は Temporal のリプレイモデルを壊す。ワークフロー
   コードがこれらに触れてよいのは SDK の決定的な代替（`workflow.Now`、
   `workflow.SideEffect`、`workflow.Go`、`workflow.NewTimer`）経由のときだけ。
2. **冪等でないアクティビティ** — 2回走っても安全でないアクティビティはスタイルの
   問題ではなくバグ。Temporal の配送は at-least-once で、タイムアウトしたアクティビティは
   ワーカー上で走り続けたまま再試行がかかる。
3. **リトライポリシーの欠落・無制限** — `workflow.ActivityOptions` には明示的な
   `StartToCloseTimeout` と、考えて決めた `RetryPolicy` が要る。SDK のゼロ値に頼らない。

ワークフローのコマンド列を変える変更（コマンドの追加・削除・並べ替え）は、実行中の
run のリプレイを壊す。破壊的変更として扱い、`workflow.GetVersion` と対で入れるか、
旧バージョンの run が尽きるまで待つ。
