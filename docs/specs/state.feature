# language: ja
機能: Run の中を短く保つ saga

  **`rollback.feature` の変種その1。書き方だけを変える。** 振る舞いは基本形と
  同じはずで、それをここで確かめる。

  ステップが5つある saga。reserve から始めて、charge は reserve が返した予約 id を
  入力に取り、approve は人の判断を signal で待ち、pack をはさんで、ship は charge の
  id と承認者を入力に取る。前段の出力が次段の入力になる（`pipeline.feature`）のと、
  途中で人を待つ（`approval.feature`）のが、1つの saga に同居している。入力も広い。

  この形を `example/state/workflow.go` は state 構造体とそのメソッドに割って書き、
  `example/state/workflow_flat.go` は `saga.Run` のクロージャに5つ並べて書いている。
  どちらが読みやすいかは読者が決めることで、ここで確かめるのは**2つが同じ saga で
  ある**こと。だから最後のシナリオは、同じ注文を両方に流して Receipt を比べる。
  比べていないと、片方だけ直されて静かに食い違う。

  approve のステップは下の「ステップの並び」に出てこない。アクティビティではなく
  ワークフローの中で待つ `saga.Func` なので、スケジュールされるアクティビティが
  無いから。取り消すものも無いので、巻き戻しにも出てこない。

  シナリオ: 5つのステップが最後まで通る
    前提 状態を持つ注文 "ok"
    もし 状態を持つ注文の承認を送る
    ならば saga は成功する
    かつ ステップ "reserve, charge, pack, ship" が実行された

  シナリオ: 承認されなければ、そこまでの2つが逆順で取り消される
    前提 状態を持つ注文 "denied"
    もし 状態を持つ注文の却下を送る
    ならば saga は "ApprovalDenied" で失敗する
    かつ ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
    かつ 注文は "reserve, charge" を保持していない

  シナリオ: 最後で失敗すれば、4つ分が逆順で取り消される
    前提 状態を持つ注文 "half" を "ship" で失敗させる
    もし 状態を持つ注文の承認を送る
    ならば saga は "no carrier" で失敗する
    かつ ステップ "reserve, charge, pack, ship, ship:undo, pack:undo, charge:undo, reserve:undo" が実行された
    かつ 注文は "reserve, charge, pack, ship" を保持していない

  シナリオ: 2つの書き方は同じ saga である
    前提 同じ注文 "both" を2つの書き方で動かす
    もし 状態を持つ注文の承認を送る
    ならば どちらの書き方も同じ Receipt を返す
