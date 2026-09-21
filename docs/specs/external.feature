# language: ja
機能: signal で他のワークフローを動かす saga

  在庫が長命のワークフローの中にある場合。saga は「押さえろ」と signal を送り、後のステップ
  が失敗したら「戻せ」と signal を送る。

  このステップには冪等キーが載りません。`SignalExternalWorkflow` に options 構造体が無く、
  `ActivityID` や `WorkflowID` に相当するフィールドが無いためです。saga が保証するのは
  **対になっていること**だけ、つまり押さえた後に失敗したら必ず戻す、という点です。

  シナリオ: 失敗すると、押さえた在庫が signal で戻される
    前提 在庫ワークフロー "inv-fail" を起動する
    かつ 在庫を押さえる注文 "ext-fail" を "charge" で失敗させる
    ならば saga は "card declined" で失敗する
    かつ 在庫ワークフロー "inv-fail" の "widget" の確保数は "0"

  シナリオ: 成功すれば在庫は押さえたまま
    前提 在庫ワークフロー "inv-ok" を起動する
    かつ 在庫を押さえる注文 "ext-ok"
    ならば saga は成功する
    かつ 在庫ワークフロー "inv-ok" の "widget" の確保数は "2"
