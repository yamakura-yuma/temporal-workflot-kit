# 上流由来のノート

<!-- upstream: go.temporal.io/sdk v1.49.0 -->

Temporal の公式ドキュメントが言っていない、あるいは SDK のソースを読まないと分からない
ことのうち、このリポジトリが実際に依存しているものの出自です。

**ここは出典だけを置きます。** 説明はそれぞれ使っている場所にあります。ここに説明を
コピーすると、腐る面が1つ増えるだけです。

`just docs-check` が上のスタンプと `go.mod` の `go.temporal.io/sdk` を突き合わせます。
SDK を上げると落ちるので、そのときに各行を上流と確認してください。確認のしかたは
`upstream-docs` スキルに書いてあります。

| 事実 | 上流 | このリポジトリでの使い所 |
| --- | --- | --- |
| テスト環境はアクティビティに10分の既定タイムアウトを置くだけで、指定したタイムアウトを課さない | [`internal/internal_workflow_testsuite.go#L806-L810`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_workflow_testsuite.go#L806-L810) | `docs/development.md`「スイートが2つある理由」、`docs/specs/rollback.feature` |
| failure コンバータは `Unwrap() error` を1本だけ辿る型スイッチ。`errors.Join` の返す型はどの分岐にも当たらない | [`internal/failure_converter.go#L66-L87`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/failure_converter.go#L66-L87) | `docs/design.md` 4（取り消しが失敗したとき、原因が消える）、`saga/errors.go` |
| `ExecuteActivity` は関数値を名前の文字列に解決してから検証するので、文字列経由では引数の個数も型も照合されない | [`internal/internal_activity.go#L220`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_activity.go#L220), [`#L266`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_activity.go#L266) | `docs/design.md`「`ExecuteActivity` を包まない」 |
| `RetryPolicy.MaximumAttempts` は未設定・0 のとき**無制限**で、止めるのは `ScheduleToCloseTimeout` | [`internal/error.go` の `RetryPolicy`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/error.go)（"If not set or set to 0, it means unlimited, and rely on activity ScheduleToCloseTimeout to stop"） | `saga/doc.go`、`docs/patterns.md`「補償のタイムアウト」 |
| 1つのワークフロー実行内でコマンド ID が重複すると `[TMPRL1100] adding duplicate command` で panic する | [`internal/internal_command_state_machine.go#L1082`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_command_state_machine.go#L1082) | 使っていない。`ActivityID` を設定しなくなったので当たらなくなった（`docs/design.md`「アクティビティに名前を付けない」） |
| `FirstRunID` は ContinueAsNew・Retry・Cron・Reset を跨いで保存される | [`internal/workflow.go#L1520-L1521`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/workflow.go#L1520-L1521) | `docs/interface.md` C3（利用者がキーを作るときの注意） |
| run の連鎖は1つの Workflow Execution で、WorkflowID は跨いで同じまま。ContinueAsNew・Retry・Cron の再実行は同じ WorkflowID を使い回す | [`internal/workflow.go#L1518-L1521`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/workflow.go#L1518-L1521), [`internal/internal_workflow_testsuite.go#L1256-L1259`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_workflow_testsuite.go#L1256-L1259), [`#L1280-L1284`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_workflow_testsuite.go#L1280-L1284) | `docs/design.md`「冪等キーを作らない・渡さない」の2層の表 |
| `StartWorkflowOptions.RetryPolicy` は任意で、渡さなければワークフローは retry されない | [`internal/client.go#L1227-L1229`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/client.go#L1227-L1229) | `docs/design.md`「条件は1つ。鍵の粒度が業務操作の粒度と一致すること」 |
| `WorkflowIDReusePolicy` の既定は `AllowDuplicate`。完了済みの WorkflowID でも新しい run が立つ | [`internal/client.go#L1205-L1213`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/client.go#L1205-L1213) | `docs/design.md`「条件は1つ。鍵の粒度が業務操作の粒度と一致すること」 |
| `ActivityID` を明示しないと、SDK が ScheduleID から作った純粋な10進数を振る | [`internal/internal_event_handlers.go#L808`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_event_handlers.go#L808), [`internal/internal_utils.go#L200`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_utils.go#L200) | `docs/design.md`「アクティビティに名前を付けない」（付けなくなった結果どうなるか） |
| `LocalActivityOptions` には ID フィールドが無い（タイムアウト2つ、`RetryPolicy`、`Summary` だけ） | [`internal/activity.go#L194-L216`](https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/activity.go#L194-L216) | `docs/design.md`「ローカルアクティビティは対象外」、`docs/patterns.md` |
