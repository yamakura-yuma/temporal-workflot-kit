# Temporal レビューのチェックリスト

上流の根拠は `go.temporal.io/sdk v1.49.0` の godoc に固定リンクしている。SDK を
上げたら `just docs-check` が落ちるので、そのときにリンクごと見直す。

## ワークフローコード

- [ ] `time.Now()`、`time.Sleep()`、`rand`、goroutine、channel、ネットワーク /
      ファイルシステム I/O を直接呼んでいない。
      [`workflow.Now`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#Now)、
      [`workflow.NewTimer`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#NewTimer)、
      [`workflow.SideEffect`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#SideEffect)、
      [`workflow.Go`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#Go) を使う。
- [ ] ワークフローの判断に影響する箇所で Go の map を走査していない。map の走査順は
      非決定的なので、必要ならキーをソートしてから回す。
- [ ] [`workflow.ExecuteActivity`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#ExecuteActivity)
      を呼ぶ箇所はすべて、明示的な `StartToCloseTimeout`（または
      `ScheduleToCloseTimeout`）と `RetryPolicy` を持つ
      [`ActivityOptions`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#ActivityOptions)
      を設定している。SDK のゼロ値に頼らない。
- [ ] ワークフローのシグネチャ変更が実行中の履歴と後方互換である。途中で挙動が
      変わるなら
      [`workflow.GetVersion`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#GetVersion)
      によるバージョニングと対で入れる。
- [ ] 長時間動くワークフローは、履歴が無制限に伸びる前に
      [`workflow.NewContinueAsNewError`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/workflow#NewContinueAsNewError)
      を呼ぶ（大きなループ、ポーリング）。

## アクティビティコード

- [ ] 外部に副作用を起こすアクティビティ（書き込み、課金、送信）は冪等である。
      または冪等キーを受け取る / 生成して、at-least-once のリトライで二重に
      適用されないようにしている。
- [ ] 冪等キーの claim が**原子的**である。使用済みか確認してから処理する形は不可。
      タイムアウトで2つの試行が同時に走り、両方が「未使用」を見る。
- [ ] 型付きのエラーを返す（または
      [`temporal.NewApplicationError`](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/temporal#NewApplicationError)
      を使う）。ワークフロー側がリトライ可能か終端かを区別できるようにする。
- [ ] 無制限に走り続けず、`ctx` のキャンセルとデッドラインを尊重する。
- [ ] 入出力がシリアライズ可能である（エクスポートされたフィールド、channel /
      関数 / 非公開フィールドのみの構造体を含まない）。

## ワーカーへの登録

- [ ] 開始されたワークフローが到達するワークフロー・アクティビティが、必要になる
      前にすべて
      [ワーカー](https://pkg.go.dev/go.temporal.io/sdk@v1.49.0/worker)
      へ登録されている。未登録はコンパイル時ではなく実行時に失敗する。
- [ ] ワーカーに渡す構造体の**エクスポートされたメソッドはすべてアクティビティと
      して登録される**。アクティビティでない問い合わせメソッドを同じ構造体に
      生やさない。型を分ける。
- [ ] タスクキュー名は1箇所（共有の定数）で定義し、文字列リテラルを複数箇所に
      書かない。

## このリポジトリでは

上の項目を当てる先はこの3つ。

| チェックリストの節 | 見るファイル |
| --- | --- |
| ワークフローコード | `saga/`、`example/*/` の `*Workflow` 関数 |
| アクティビティコード | `example/order/activity.go`、各 example の `Services` と `Activities` |
| ワーカーへの登録 | `specsteps/suite_test.go` |

補償そのものの規則（登録の順序、no-op で成功すること、逆順）は Temporal 一般では
なくこのリポジトリの `saga/` パッケージの不変条件なので、`saga-package` スキルにある。
