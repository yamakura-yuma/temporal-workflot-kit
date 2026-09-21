# language: ja
機能: 前段の出力を次段に渡す saga

  **`rollback.feature` の変種その2 — ステップの間にデータを流す。** 基本形では各
  ステップの入力が固定だった。ここでは前のステップの出力が次のステップの入力になる。

  予約 ID を課金の入力に、課金 ID を配送の入力に渡す saga。

  見たいのは補償の側。補償は**登録はステップの実行より前、実行は後**なので、走る時点では
  forward が書いたフィールドが埋まっている。だから「どの予約に対する課金を、どの課金として
  返すのか」が補償に分かる。ワークフローが持ち回る必要はない。

  forward が返さなかったとき（下流に書けた直後のタイムアウト）だけはフィールドが空になる。
  そこは冪等キーで引く領域で、`docs/interface.md` の C2・C3 にある。

  シナリオ: 前段の ID が、次段の補償にも渡っている

    配送で失敗させる。課金の補償は、課金が紐づいていた予約 ID を受け取っているはず。

    前提 連鎖する注文 "chain" を "ship" で失敗させる
    ならば saga は "no carrier available" で失敗する
    かつ アクティビティ "Reserve, Charge, Ship, CancelShipment, Refund, Unreserve" が実行された
    かつ 補償 "Refund" が受け取った前段の ID は "res-chain"
    かつ 補償 "CancelShipment" が受け取った前段の ID は "chg-chain"
    かつ 注文は "Reserve, Charge, Ship" を保持していない

  シナリオ: 正常系では最後まで連鎖する
    前提 連鎖する注文 "ok"
    ならば saga は成功する
    かつ アクティビティ "Reserve, Charge, Ship" が実行された
