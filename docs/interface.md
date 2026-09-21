# 境界の契約

このライブラリが守ることは2つだけです。**補償を forward より先に登録すること**と、
**失敗したら切り離した context で逆順に実行すること**。

残りは全部、利用者側が守る契約です。破ってもコンパイルは通り、テストも普通には通り、
**本番の異常系ではじめて壊れます**。だから列挙します。

なぜライブラリ側がこれだけしかやらないのかは [design.md](design.md)、書き方は
[patterns.md](patterns.md)、実物は `example/` にあります。

---

## ライブラリが守ること

### L1. 補償は forward より先に登録される

```go
func (s *Saga) Step(ctx workflow.Context, name string, do, undo func(workflow.Context) error) error {
	if undo != nil {
		s.addCompensation(name, undo)   // 先
	}
	if err := do(ctx); err != nil {     // 後
		s.fail(err)
		return err
	}
	return nil
}
```

タイムアウトした forward は、実は成功しているかもしれません。だから**失敗したステップ自身も
補償されます**。

検査: `docs/specs/rollback.feature`「失敗すると、失敗したステップを含めて逆順に取り消される」

### L2. 巻き戻しは切り離した context で走る

```go
dctx, cancel := workflow.NewDisconnectedContext(ctx)
```

キャンセルされた context の上では、以降の `ExecuteActivity` は即座に失敗します。切り離さない
と、補償が一番必要な場面で1本も動きません。

検査: `docs/specs/rollback.feature`「キャンセルされた saga もロールバックされる」

### L3. ステップが失敗したら、ワークフローも失敗する

body が `nil` を返しても、`RunOrCompensate` はステップのエラーで失敗させます。body の戻り値は
捨てます。最初の失敗が、body が後から返すエラーより強い。

握って自分のエラーを返すときだけ `s.ClearErr()`。

---

## 利用者が守ること

### C1. 補償は「取り消すものが無い」ときに成功する

L1 の裏返しです。**起きなかったステップに対しても補償は呼ばれます。**

```go
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	// DELETE FROM reservations WHERE idem_key = $1
	// → 0 行でも成功。「予約が無い」は「解放済み」と同じ
	return nil
}
```

ここでエラーを返すと、**ロールバック全体が失敗扱い**になります。

- 破ったときに起きること: 正常な巻き戻しが `CompensationFailed` で報告され、運用が調べに来る
- 検査: `docs/specs/rollback.feature`（ship が失敗しても `Unreserve` は成功する）

### C2. forward は冪等キーを、実行と**同じ1操作で**押さえる

Temporal はアクティビティを既定でリトライします。タイムアウトで2つの試行が同時に飛ぶことも
あります。

```sql
-- 良い: 書くことと押さえることが1文
INSERT INTO charges (idem_key, ...) VALUES ($1, ...)
  ON CONFLICT (idem_key) DO NOTHING
  RETURNING id;
```

```go
// 駄目: 確認してから書く。2つの試行が両方「未使用」を見る
if !exists(key) {
    charge(...)          // ← ここで二重課金
}
```

**「確認してから実行」は冪等ではありません。** 一意制約か、下流 API の
`Idempotency-Key` ヘッダを使ってください。

- 破ったときに起きること: リトライのたびに二重課金・二重出荷
- ライブラリは助けません: 原子性は下流にしか作れません

### C3. 冪等キーは、利用者が作って渡す

ライブラリは作りません。**1回の実行の1ステップに固有**であることだけが条件です。

```go
func packKey(ctx workflow.Context) string {
	return workflow.GetInfo(ctx).WorkflowExecution.RunID + "/pack"
}
```

- `FirstRunID` は**駄目**。ContinueAsNew・Retry・Cron・Reset を跨いで保存されるので、2回目の
  run が1回目のキーを再利用し、全ステップが「適用済み」に見えます（[sdk-notes.md](sdk-notes.md)）
- 連番も**駄目**。ステップを挿入すると以降が全部ずれます
- forward と補償に**同じ値**を渡すのは書き手の仕事です（`example/workflow/childflow/`）

### C4. 補償には `ScheduleToCloseTimeout` を設定する

L2 の裏返しです。切り離した context は**外から誰も止められません**。

Temporal の既定のリトライは**無制限**で、止めるのは `ScheduleToCloseTimeout` だけです
（SDK の `RetryPolicy` がそう書いています。[sdk-notes.md](sdk-notes.md)）。
`StartToCloseTimeout` は1回の試行を縛るだけで、リトライの繰り返しは縛りません。

```go
ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
	StartToCloseTimeout:    10 * time.Second,
	ScheduleToCloseTimeout: time.Minute,      // ← 補償ではこれが要る
})
```

- 破ったときに起きること: 失敗し続ける補償が、誰も止められないままワークフローを握り続ける

### C5. forward と補償を取り違えない

**コンパイラは止めません。** 両半分とも `func(workflow.Context) error` です。

```go
s.Step(ctx, "reserve", w.unreserve, w.reserve)   // ← 通ってしまう
```

以前は型の非対称で防いでいましたが、`ExecuteActivity` を利用者に書いてもらうのと引き換えに
失いました（[design.md](design.md)「forward と補償を取り違えても、コンパイラは止めない」）。

対の名前のメソッドにしておくと、呼び出しの並びで目に見えます。

### C6. ワークフローは cancel する。terminate しない

terminate はワークフローコードを1行も実行しません。**何も補償されません。**

### C7. 1ステップの forward は、副作用を1つに保つ

`do` の中で2つの下流を叩くと、片方だけ成功した状態を1つの補償で戻すことになります。分けて
ください。ステップは安くできています。

---

## 塞げていないこと

契約を全部守っても残る穴です。**ライブラリの問題ではなく、分散システムの性質です。**

### G1. 補償は「起きたかどうか」を確実には知れない

タイムアウトした forward は、ワーカー上で**まだ動いています**。補償が「何も無い」と判断して
成功し、その後に副作用が着地することがあります。

塞ぐには、補償が墓標を書き、forward がコミット前にそれを見る必要があります。つまり**両半分を
1つのストアで直列化する**こと。第三者の決済ゲートウェイや配送業者が相手では不可能で、
どんなライブラリでもできません。

### G2. 冪等キーを受け取らない下流は、安全にできない

C2 の前提が無いので、リトライを防ぐ手段がありません。下流を変えるか、手前に自分のストアを
置いて直列化するかのどちらかです。

### G3. 取り消せない副作用は取り消せない

送信済みのメール、発火した外部イベント。補償は「打ち消しの行動」（訂正メール）にはできますが、
元に戻すことはできません。

### G4. 補償自体が失敗したら、人間が要る

`CompensationFailedType` のエラーと `CompensationReport` で報告し、**非リトライ**にしてあります。
ワークフローレベルのリトライが半分巻き戻した上から saga をやり直すのを防ぐためです。ここから
先は自動化の外です。
