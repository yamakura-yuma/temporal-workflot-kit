# 前段の出力を次段に渡す saga

予約 ID を課金の入力に、課金 ID を配送の入力に渡す saga。

見たいのは補償の側。補償はステップの実行より前に登録されるので、そのステップ自身の出力を
見ることはできない。代わりに**forward と同じ入力**を受け取るので、前段の出力はそこに入って
いる。だから「どの予約に対する課金を返すのか」が補償に分かる。

## 前段の ID が、次段の補償にも渡っている

配送で失敗させる。課金の補償は、課金が紐づいていた予約 ID を受け取っているはず。

* 連鎖する注文 "chain" を "ship" で失敗させる
* saga は "no carrier available" で失敗する
* ステップ "reserve, charge, ship, ship:undo, charge:undo, reserve:undo" が実行された
* 補償 "charge" が受け取った前段の ID は "res-chain"
* 補償 "ship" が受け取った前段の ID は "chg-res-chain"
* 注文は "reserve, charge, ship" を保持していない

## 正常系では最後まで連鎖する

* 連鎖する注文 "ok"
* saga は成功する
* ステップ "reserve, charge, ship" が実行された
