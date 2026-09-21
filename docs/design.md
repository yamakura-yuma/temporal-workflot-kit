# なぜこのライブラリがあるのか

saga は「途中で失敗したら、そこまでにやったことを逆順で取り消す」だけの仕組みです。
やること自体は単純で、配列とループで書けます。

```go
var undo []func(workflow.Context) error

// 取り消し方を積んでおく
undo = append(undo, func(c workflow.Context) error { return release(c, id) })

// 失敗したら、後ろから実行する
for i := len(undo) - 1; i >= 0; i-- {
    undo[i](ctx)
}
```

Temporal の公式サンプル（[samples-go](https://github.com/temporalio/samples-go/tree/main/saga)）も
これを `defer` で書いています。たいていはこれで足ります。

ではなぜライブラリがあるのか。**この十数行が黙って壊れる場面が5つある**からです。どれも
本番の異常系でしか出ません。6つ目は壊れる話ではなく、壊れたときにどう振る舞わせるかの
選択肢です。

## 他の SDK はどうしているか

Temporal の SDK のうち、saga の補助を持っているのは2つだけです。

| SDK | 実装 |
| --- | --- |
| Java | [`io.temporal.workflow.Saga`](https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/workflow/Saga.java) |
| PHP | [`Temporal\Workflow\Saga`](https://github.com/temporalio/sdk-php/blob/master/src/Workflow/Saga.php)（Java の移植。doc コメントまで同一） |
| Go・TypeScript・Python・.NET・Ruby | 無し |

**このライブラリは Java 版の形に、Java 版が利用者に任せている3つを足したもの**です。
用語も Java に合わせてあります（`ParallelCompensation`、`ContinueWithError`、
`addCompensation`、`compensate`）。

以下、1つずつ見ます。1〜5 は「やりたいこと」「素直に書くと」「そこで何が起きるか」
「このライブラリだと」の順で、6 だけは選択肢の説明です。最後に、**意図して手放したもの**をまとめてあります。

---

## 1. キャンセルされると、取り消しが動かない

### やりたいこと

運用担当者がワークフローをキャンセルした。押さえた在庫は解放してほしい。

### 素直に書くと

```go
defer func() {
    if err != nil {
        for i := len(undo) - 1; i >= 0; i-- {
            undo[i](ctx)          // ← キャンセルされた ctx
        }
    }
}()
```

### そこで何が起きるか

**1本も動きません。** キャンセルされた context の上では、以降の `ExecuteActivity` は
実行される前に即座に失敗します。ログには補償が失敗した記録だけが並び、在庫は押さえた
ままになります。

補償が一番必要な場面が、補償が一番動かない場面でもある、という形です。

### このライブラリだと

`RunOrCompensate` が `workflow.NewDisconnectedContext` で切り離した context を作り、
その上で補償を回します。切り離した context は親のキャンセルを継承しません。

```go
dctx, cancel := workflow.NewDisconnectedContext(ctx)
defer cancel()
```

PHP SDK も `compensate()` 全体を `Workflow::asyncDetached` で包んでいて、同じ結論に
達しています。**Java SDK はこれをしていません。**

### 代わりに気をつけること

切り離した context は**外から誰も止められません**。補償が失敗し続けると、止めるものが
無くなります。Temporal の既定のリトライは無制限なので、補償には
`ScheduleToCloseTimeout` を必ず設定してください（[sdk-notes.md](sdk-notes.md)）。

ワークフローは**キャンセルしてください。terminate は駄目です。** terminate はワークフロー
コードを1行も動かさないので、何も補償されません。

---

## 2. タイムアウトしたステップが、取り消されない

### やりたいこと

課金アクティビティがタイムアウトした。実際に課金されたかどうかは分からない。分からない
なら、返金を試してほしい。

### 素直に書くと

```go
id, err := charge(ctx, req)
if err != nil {
    return err                                   // ← ここで抜ける
}
undo = append(undo, func(c workflow.Context) error { return refund(c, id) })
```

Java SDK と PHP SDK の使用例も、この順です。`addCompensation` は forward が成功した
後に呼ばれます。

### そこで何が起きるか

**取り消しが登録される前に関数を抜けます。** タイムアウトは「起きなかった」ことの証明
ではありません。ワーカーは動き続けていて、下流への課金は成立しているかもしれない。
なのに返金は永久に呼ばれません。

### このライブラリだと

`Step` が、**forward を実行する前に**補償を登録します。

```go
func (s *Saga) Step(ctx workflow.Context, name string, do, undo func(workflow.Context) error) error {
	if undo != nil {
		s.addCompensation(name, undo)      // ← 先
	}
	if err := do(ctx); err != nil {        // ← 後
		s.fail(err)
		return err
	}
	return nil
}
```

失敗したステップ自身も補償されます。`docs/specs/rollback.feature` の
「失敗すると、失敗したステップを含めて逆順に取り消される」がこれを検査しています。

### 代わりに気をつけること

**補償は「取り消すものが無い」ときに成功しなければなりません。** 起きなかったステップに
対しても呼ばれるためです。そこでエラーを返すと、ロールバック全体が失敗扱いになります。
詳しくは [interface.md](interface.md)。

---

## 3. エラーチェックを1つ忘れると、壊れた成功になる

### やりたいこと

ステップの失敗を、ワークフローの失敗にしたい。

### 素直に書くと

```go
res, _ := reserve(ctx, in)     // ← エラーを見落とした
chg, _ := charge(ctx, in)
return Receipt{res, chg}, nil  // ← nil を返してしまう
```

### そこで何が起きるか

**成功として記録されます。** 半分だけ適用された副作用が残り、アラートは鳴りません。
履歴には「Completed」とだけ書かれます。

### このライブラリだと

`RunOrCompensate` は、**body が nil を返してもステップが失敗していれば失敗させます**。
body の戻り値は捨てます。

```go
out, err := body(ctx, s)

if s.err != nil {
	err = s.err        // 最初の失敗が勝つ
}
```

**最初の失敗が、body が後から返すエラーより強い**のも同じ理由です。ステップが失敗すると
以降の `Step` は no-op になり、signal 待ちは即座に返るので、body は結果だけを見て本当では
ない結論（「誰も承認しなかった」）を出しがちです。根本原因のほうを報告します。

握って自分のエラーを返すときだけ `s.ClearErr()` を呼んでください。

### 代わりに気をつけること

**面倒を見られるのはステップの中だけです。** `s.Err()` が立った後も、ログ・
`workflow.Sleep`・body に直接書いた分岐は普通に実行されます。ステップの外でゼロ値を使う
なら、自分で `s.Err()` を見てください。

---

## 4. 取り消しが失敗したとき、原因が消える

### やりたいこと

返金が失敗した。**元の失敗（配送の失敗）と、返金の失敗の両方**を知りたい。

### 素直に書くと

```go
return errors.Join(cause, compensationErr)
```

### そこで何が起きるか

**Temporal の履歴では、両方とも読めなくなります。** failure コンバータは具象型に対する
型スイッチで、`Unwrap() error` を1本だけ辿ります。`errors.Join` が返す型はどの分岐にも
当たらないので、型名は `joinError` になり、原因の連鎖は落ち、
`NonRetryableErrorTypes` の照合も効かなくなります（[sdk-notes.md](sdk-notes.md)）。

### このライブラリだと

原因が1本の `ApplicationError` を組み立て、**どのステップが失敗したかは details に**
載せます。

```go
temporal.NewApplicationErrorWithOptions(msg, CompensationFailedType,
    temporal.ApplicationErrorOptions{
        Cause:        cause,          // 元の失敗はそのまま残る
        NonRetryable: true,
        Details:      []any{CompensationReport{Failed: failed, Skipped: skipped}},
    })
```

非リトライにしてあるのは、ワークフローレベルのリトライが**半分巻き戻した上から saga を
やり直す**のを防ぐためです。そこは人間が見る場面です。

Java と PHP は例外を1つ投げるだけで、どのステップだったかは報告しません。

---

## 5. 補償が、取り消す相手を知らない

### やりたいこと

返金したい。だが「どの課金を」返金するのかは、課金が成功して初めて分かる。

### 素直に書くと

補償の入力に、forward の出力を渡そうとします。しかし**補償は forward より先に登録**
されるので、登録の時点では出力が存在しません。

### そこで何が起きるか

型が合いません。Java SDK は `addCompensation` を forward の後に呼ぶことでこれを回避して
いますが、それは項目2の穴と引き換えです。

### このライブラリだと

両半分を**同じ構造体のメソッド**にします。補償は、forward が書いたフィールドを読むだけ
です。

```go
func (w *fulfillment) chargeCard(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Charge, req).Get(ctx, &w.charge)
}

func (w *fulfillment) refund(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Refund,
        ChargeReq{Charge: w.charge}).Get(ctx, nil)     // ← forward の出力
}
```

登録は先、**実行は後**なので、補償が動く時点でフィールドは埋まっています。他の saga 実装
（MassTransit Courier の `ICompensateActivity<TLog>` など）が「補償ログ」として明示的に
持ち回るものを、Go では普通の変数が担います。

### 代わりに気をつけること

**forward が返さなかったときは空です。** 下流に書き込んだ直後にタイムアウトすると、出力は
履歴に残りません。そこを埋めるのが冪等キーで、「このキーで書かれた行を消す」形なら、
forward が成功していようと途中で落ちていようと同じ1文で足ります。

キーはライブラリが作りません。項目「冪等キーを作らない」を参照してください。

---

## 6. 取り消しの順番と、途中で失敗したときの選択肢

### やりたいこと

補償が1本失敗した。残りをどうするか決めたい。

### このライブラリだと

Java SDK と同じ2つのオプションを、同じ名前で持っています。既定も同じです。

```go
saga.Options{
    ParallelCompensation: true,   // 逆順をやめて全部同時に投げる（既定 false）
    ContinueWithError:    true,   // 1本失敗しても残りを続ける（既定 false）
}
```

既定は**逆順**で、**最初の失敗で止めます**。止まったぶんは `CompensationReport.Skipped`
として報告されるので、落ちたのか元からやっていないのかは区別できます。

### 代わりに気をつけること

**`ContinueWithError` は入れたほうがよい場面が多い**と思います。返金が失敗したからと
いって、在庫を押さえたままにする理由はあまりありません。`example/workflow/order/` は
そうしています。

`ParallelCompensation` は**ステップが本当に独立しているときだけ**です。後のステップが前に
依存しているなら、逆順でないと取り消せません。並列のときは全部を投げてから待つので、
`ContinueWithError` は意味を持ちません（Java の doc も同じことを書いています）。

---

# 意図して手放したもの

ここから下は「やらないと決めたこと」です。どれも一度は入っていて、外しました。

## `ExecuteActivity` を包まない

ステップの両半分は `func(workflow.Context) error` です。中で何を呼ぶかは利用者が書きます。

```go
s.Step(ctx, "reserve", w.reserve, w.unreserve)

func (w *fulfillment) reserve(ctx workflow.Context) error {
    return workflow.ExecuteActivity(ctx, acts.Reserve, req).Get(ctx, &w.reservation)
}
```

以前は `saga.Activity` / `saga.ChildWorkflow` / `saga.Func` と、それぞれの `Undo*` という
6つの構成子がありました。外した理由は2つです。

**1つ目。Temporal に表現手段があるものを奪っていました。** `saga.Activity` は
`Options.ActivityOptions` で ctx の設定を丸ごと上書きしていたので、
`workflow.WithActivityOptions` で設定したタスクキューもリトライポリシーも黙って消えて
いました。しかも `saga.ChildWorkflow` のほうは ctx から読んでいて、**同じライブラリが同じ
問いに2つの答えを持っていました**。

**2つ目。Temporal を知っている人にとって、何が起きるか読めませんでした。**
`saga.Activity(a.Reserve)` を見ても、アクティビティとしてスケジュールされるのか、履歴に
残るのか、設定が効くのかが分かりません。`workflow.ExecuteActivity(ctx, ...)` なら全部
分かります。

子ワークフローで実行するのも signal を送るのも、**関数の中身の違い**になりました。

## forward と補償を取り違えても、コンパイラは止めない

**これは後退です。正直に書きます。**

以前は forward が「値とエラー」、補償が「エラーだけ」を返す非対称な型だったので、2つを
逆に書くとコンパイルが通りませんでした。今はどちらも
`func(workflow.Context) error` なので、**入れ替えてもコンパイラは何も言いません。**

```go
s.Step(ctx, "reserve", w.unreserve, w.reserve)   // ← 通ってしまう
```

引き換えに得たのが、上の「`ExecuteActivity` を包まない」です。両方は取れませんでした。

緩和になっているのは呼び出し側の見た目だけです。両半分を `reserve` / `unreserve` のような
対の名前のメソッドにしておけば、`s.Step(ctx, "reserve", w.reserve, w.unreserve)` の並びで
取り違えは目に見えます。example はすべてこの形にしてあります。

## 冪等キーを作らない・渡さない

以前は `RunID + "/" + ステップ名` を導出して `ActivityID` に載せ、アクティビティが
`saga.IdempotencyKey(ctx)` で読み戻していました。全部外しました。

理由は、**ライブラリにできることが「文字列を1つ作る」だけだった**からです。原子的に
押さえるのも、保存するのも、下流との契約も、全部利用者側です
（[interface.md](interface.md)）。そのうえ、鍵を読むために
**アクティビティが saga ライブラリを import する**必要がありました。アクティビティは
自分が saga の一部だと知る必要がありません。

今はワークフローが自分で導出し、リクエストに入れて渡します。

```go
func packKey(ctx workflow.Context) string {
    return workflow.GetInfo(ctx).WorkflowExecution.RunID + "/pack"
}
```

実物: [`example/workflow/childflow/workflow.go`](../example/workflow/childflow/workflow.go)

### 土台: Temporal の想定は2層

外したとはいえ、**何を作るべきか**は上流が決めています。重複の排除は2箇所で行う想定です。

| | どこで弾くか | 道具 |
| --- | --- | --- |
| 層1 | ワークフローが立つ前。Temporal サーバ | WorkflowID と Workflow Id Reuse / Conflict Policy |
| 層2 | アクティビティの中 | 冪等キー（RunID + ActivityID） |

層1では WorkflowID が業務識別子として扱われます。公式は WorkflowID を "meant to be a
business-process identifier"（注文番号や顧客番号のようなもの）と位置づけ、"Temporal
guarantees at most one Workflow Execution with a given Workflow Id ... at any point in
time" としています
（[Workflow Id and Run Id](https://docs.temporal.io/workflow-execution/workflowid-runid)）。

層2では、鍵の作り方まで名指しされています。"You can use a combination of the Workflow Run
ID and the Activity ID as an idempotency key" で、理由は "guaranteed to be consistent
across retry attempts but unique across Workflow Executions"
（[Activity definition](https://docs.temporal.io/activity-definition)）。

**`RunID + ステップ名` は、この層2の推奨そのものです。** このライブラリが決めたことでは
ありませんでした。だから外しても、利用者が作るべき値は変わりません。

### 条件は1つ。鍵の粒度が業務操作の粒度と一致すること

`RunID` でなく `WorkflowExecution.ID` を使うこともできます。判断の基準は1つで、
**WorkflowID に何が入っているか**です。

| WorkflowID に入っているもの | 業務操作との関係 | 鍵に使えるか |
| --- | --- | --- |
| API の Request ID（コールごとに一意） | ぴったり一致する | **使える。`RunID` より安全** |
| 注文 ID（同じ注文に複数回ワークフローを起動しうる） | 粗い | 2回目以降が全ステップ skip する |
| cron のワークフロー（WorkflowID がスケジュール単位） | 粗い | 2日目以降が全ステップ skip する |

**粗い側に外れると、処理が黙って飛びます。** エラーは出ません。全ステップが「もう済んで
いる」と判定され、何もせずに成功が返るだけです。

`RunID` は必ず「1実行」の粒度なので、業務操作と**同じか、細かい側にしか外れません**。
細かい側に外れても既定では害が出ません。Temporal のワークフローは既定では retry policy を
持たない（`StartWorkflowOptions.RetryPolicy` は任意）ので run は1本しかないからです。

逆に言うと、**workflow の retry・reset・continue-as-new を有効にすると、細かい側の外れが
実害になります。** run が変わるたびに鍵が変わるので、補償が走らないまま retry した場合に
二重実行が起きます。

```
run 1  charge で "run1/charge" を押さえる → 課金が立つ
       ワーカーが落ちる。補償は走らない
run 2  charge で "run2/charge" を押さえる → 未使用に見える → 二重課金
```

そこを塞ぐなら `WorkflowExecution.ID` に寄せます。ただし **`WorkflowIDReusePolicy` も
セットで決めてください**。既定は `AllowDuplicate` で、完了済みの WorkflowID でも新しい run
が立ちます（[sdk-notes.md](sdk-notes.md)）。

`FirstRunID` は使わないでください。ContinueAsNew・Retry・Cron・Reset を跨いで保存される
点は WorkflowID と同じですが、**値をサーバが決める**ので、意図したかどうかに関わらず2回目
の run が1回目の鍵を再利用します。意図するなら `WorkflowExecution.ID` を明示してください。

連番も駄目です。ステップを挿入すると以降が全部ずれます。

### 二重送信を止めたいだけなら、層2まで下りる必要はない

WorkflowID に API の Request ID を入れて Reuse / Conflict Policy を設定すれば、重複した
リクエストは**ワークフローが立つ前に**サーバが弾きます。それが層1の仕事です。層2まで
WorkflowID 由来にして初めて塞がるのは、**同じ WorkflowID の中で run が変わる場合**だけ
です。

## アクティビティに名前を付けない

`ActivityID` を `<RunID>/<ステップ名>` にすると Temporal UI で履歴が読みやすくなるので、
一度入れていました。外したのは、**「両半分はただのワークフローコード」という前提と
矛盾する制約**を持ち込むからです。`ActivityID` は1つのワークフロー実行内で重複できないので、
1ステップで2本のアクティビティを起動できなくなります。加えて、利用者が `do` の中で
options を丸ごと差し替えると ID が黙って消えます。

Java も PHP も `ActivityID` には触っていません。読みやすい履歴が欲しければ、利用者が
`ActivityOptions.ActivityID` を自分で設定できます。

## 補償の失敗を検索属性に立てない

`Options.CompensationFailedAttribute` がありました。巻き戻しが綺麗に終わらなかったとき、
運用が検索できるように boolean の検索属性を立てるものです。

外したのは、`workflow.UpsertTypedSearchAttributes` が素の Temporal で、しかも
**検出手段は既に公開してある**からです。`RunOrCompensate` が返すエラーの型を見れば済みます。

```go
receipt, err := saga.RunOrCompensate(ctx, opts, body)

var appErr *temporal.ApplicationError
if errors.As(err, &appErr) && appErr.Type() == saga.CompensationFailedType {
    workflow.UpsertTypedSearchAttributes(ctx, CompensationFailedAttribute.ValueSet(true))
}
```

外した結果、`Options` は **Java 版とちょうど同じ2つ**になりました。

## 補償の予算を持たない

`Options.CompensationBudget` を必須にしていた時期があります。外しました。

補償アクティビティは、ctx の `ScheduleToCloseTimeout` で**リトライ込みで**縛られます。
N ステップなら全体は N × それで有界です。予算は**同じことを別の場所でもう一度言って
いた**だけで、しかも必須だったので「忘れても守ってくれる」ものですらありませんでした。

代わりに罠を名指ししています。切り離した context は外から止められず、Temporal の既定の
リトライは無制限なので、**補償には `ScheduleToCloseTimeout` を必ず設定してください。**

## signal を待つ関数を持たない

`saga.AwaitSignal` がありました。`NewSelector` + `AddReceive` + `AddFuture(NewTimer)` の
素の SDK イディオムで、冪等キーも載せず予算も切らないので、このライブラリの基準に
合いません。`example/workflow/approval/` の `awaitDecision` に移しました。

待つこと自体をステップにするのは今も正しい形です。**ステップなら、先のステップが失敗して
いれば飛ばされます。** ロールバックに向かっている saga が人の承認を1時間待つことはあり
ません。

## ローカルアクティビティは対象外

リトライがワークフロータスク内で完結してサーバに残らないので、**取り消しが要るような
副作用を置く場所ではありません**。`LocalActivityOptions` に ID フィールドが無いのも
（[sdk-notes.md](sdk-notes.md)）、同じ方向の話です。

`do` はただの関数なので、書こうと思えば書けます。止めてはいません。

---

## まとめ

| 場面 | 素直に書くと | このライブラリだと |
| --- | --- | --- |
| キャンセルされた | 補償が1本も動かない | 切り離した context で動く |
| forward がタイムアウトした | 補償が登録されていない | 実行前に登録済み |
| エラーチェックを忘れた | 壊れた成功になる | ステップの失敗が勝つ |
| 補償が失敗した | 原因が履歴から消える | 原因1本 + どのステップかを details に |
| 補償が forward の出力を要る | 渡せない | 同じ構造体のフィールドを読む |
| 補償が途中で失敗した | 自分で決める | `ContinueWithError` |

**手放したもの**: `ExecuteActivity` のラップ、forward と補償の取り違えの検出、冪等キーの
導出と受け渡し、アクティビティの命名、補償の予算、signal を待つ関数。

どれも「Temporal に表現手段があるものは奪わない」という1つの基準で外しました。例外は
取り違えの検出で、これは基準と引き換えに失った**後退**です。
