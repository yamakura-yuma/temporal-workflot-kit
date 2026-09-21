# language: ja
機能: 前段の出力を次段に渡す saga

  **`rollback.feature` の変種その2 — ステップの間にデータを流す。** 基本形では各
  ステップの入力が固定だった。ここでは前のステップの出力が次のステップの入力になる。

  予約 ID を課金の入力に、課金 ID を配送の入力に渡す saga。

  見たいのは補償の側。補償はステップの実行より前に登録されるので、そのステップ自身の出力を
  見ることはできない。代わりに**forward と同じ入力**を受け取るので、前段の出力はそこに入って
  いる。だから「どの予約に対する課金を返すのか」が補償に分かる。

  シナリオ: 前段の ID が、次段の補償にも渡っている

    配送で失敗させる。課金の補償は、課金が紐づいていた予約 ID を受け取っているはず。

    前提 連鎖する注文 "chain" を "ship" で失敗させる
    ならば saga は "no carrier available" で失敗する
    かつ ステップ "reserve, charge, ship, ship:undo, charge:undo, reserve:undo" が実行された
    かつ 補償 "charge" が受け取った前段の ID は "res-chain"
    かつ 補償 "ship" が受け取った前段の ID は "chg-chain"
    かつ 注文は "reserve, charge, ship" を保持していない

  シナリオ: 正常系では最後まで連鎖する
    前提 連鎖する注文 "ok"
    ならば saga は成功する
    かつ ステップ "reserve, charge, ship" が実行された
