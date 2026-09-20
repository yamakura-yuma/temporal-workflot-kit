# saga のロールバック

saga はステップを順に実行し、途中で失敗したら、それまでに起きたことを逆順で
取り消す。ここのシナリオは、スイートが起動する実際の Temporal サーバに対して
実行する。インメモリのテスト環境はキャンセルされた context でもアクティビティを
実行してしまい、ここで確かめたい振る舞いを示せないため。

「ステップ ... が実行された」はワークフロー履歴を読む。つまり運用が UI で見る順序
そのものを指す。"charge:undo" は "charge" の補償を表す。

## 成功した saga は何も取り消さない

* 注文 "happy"
* saga は成功する
* ステップ "reserve, charge, ship" が実行された
* 注文は "reserve, charge, ship" を保持したままである

## 失敗すると、失敗したステップを含めて逆順に取り消される

各ステップの補償は、そのステップを実行する前に登録される。だから失敗を報告した
ステップ自身も補償される。失敗は「何も起きなかった」ことの証明にならない。
アクティビティは、それを完了したワーカーの上でタイムアウトすることがある。

* "ship" で失敗する注文 "rollback"
* saga は "no carrier available" で失敗する
* ステップ "reserve, charge, ship, ship:undo, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge, ship" を保持していない

## キャンセルされた saga もロールバックされる

このライブラリの存在理由にあたるシナリオ。キャンセルされたワークフローの context
は以降のアクティビティを即座に失敗させるので、補償が動くのは
workflow.NewDisconnectedContext から得た context を渡しているからに他ならない。

* 課金の後で待機する注文 "cancelme"
* "charge" が実行されたら saga をキャンセルする
* saga は失敗する
* ステップ "reserve, charge, charge:undo, reserve:undo" が実行された
* 注文は "reserve, charge" を保持していない

## 失敗した補償は報告され、残りの補償を止めない

課金は適用されたまま残る。取り消しに失敗したのが課金だからで、これが正直な結果で
あり、実行にフラグが立つ理由でもある。ロールバックは未完了で、人が片付ける必要が
ある。

* "ship" で失敗し、"charge" を取り消せない注文 "undofails"
* saga は "CompensationFailed" で失敗する
* 補償に失敗したステップは "charge"
* saga は運用者向けにフラグが立つ
* ステップ "reserve, charge, ship, ship:undo, charge:undo, reserve:undo" が実行された
* 注文は "charge" を保持したままである
* 注文は "reserve, ship" を保持していない
