# なぜこのライブラリがあるのか

saga は「途中で失敗したら、そこまでにやったことを逆順で取り消す」だけの仕組みです。
やること自体は単純で、Temporal の公式サンプルにも30行ほどの実装があります。

```go
var undo []func(workflow.Context) error

// 成功したら、取り消し方を積んでおく
undo = append(undo, func(c workflow.Context) error { ... })

// 失敗したら、後ろから実行する
for i := len(undo) - 1; i >= 0; i-- {
    undo[i](ctx)
}
```

たいていはこれで足ります。ではなぜライブラリがあるのか。この30行が**黙って壊れる場面が
6つある**からです。どれも本番の異常系でしか出ません。

以下、1つずつ見ます。どれも「やりたいこと」「素直に書くとこうなる」「そこで何が起きるか」
「このライブラリだとこう書く」の順です。

---

## 1. キャンセルされると、取り消しが動かない

### やりたいこと

運用担当者がワークフローをキャンセルした。押さえた在庫は解放してほしい。

### 素直に書くと

```go
if err != nil {
    for i := len(undo) - 1; i >= 0; i-- {
        undo[i](ctx)   // ← ここで使っている ctx
    }
    return Receipt{}, err
}
```

### そこで何が起きるか

`ctx` は**キャンセル済み**です。Temporal では、キャンセルされた context でアクティビティを
呼ぶと、ワーカーに届く前に即座に失敗が返ります。

店に例えると、閉店した後に返品に行くようなものです。扉が閉まっているので、何を持って
行っても受け付けてもらえない。

```
運用担当者が「キャンセル」を押す
        ↓
ワークフローの ctx が閉じる
        ↓
undo[i](ctx) を呼ぶ  →  即座に CanceledError。在庫は押さえられたまま
```

**キャンセルこそ取り消しが一番必要な場面なのに、そこだけ動きません。**

### このライブラリだと

`saga.Run` が、キャンセルの影響を受けない別の context を用意します。裏口の鍵を持っている
ようなものです。

```go
// saga.Run の中でやっていること
dctx, cancel := workflow.NewDisconnectedContext(ctx)
defer cancel()

// 取り消しは dctx で実行する。親がキャンセルされても閉じない
```

使う側は何も書きません。

```go
return saga.Run(ctx, opts, func(s *saga.Saga) (Receipt, error) {
    ...
})
```

### 代わりに気をつけること

裏口の鍵は**外から誰も閉められません**。取り消しが1つ固まると、ワークフローが永久に
終わらなくなる。なので `CompensationBudget` を必須にしました。

```go
saga.Options{
    CompensationBudget: 5 * time.Minute,   // 取り消し全体の制限時間
}
```

時間切れで実行できなかった取り消しは、消えずに `CompensationReport.Skipped` に残ります。

---

## 2. タイムアウトしたステップが、取り消されない

### やりたいこと

課金がタイムアウトで失敗した。返金してほしい。

### 素直に書くと

```go
err := workflow.ExecuteActivity(ctx, a.Charge, req).Get(ctx, &chg)
if err != nil {
    return Receipt{}, err   // 失敗したので、取り消しは積まない
}
undo = append(undo, refund)   // ← 成功したときだけ積む
```

一見これで正しく見えます。失敗したなら何も起きていないはず、だから取り消すものも無い。

### そこで何が起きるか

その「はず」が違います。**アクティビティがタイムアウトしても、ワーカー側では完走している
かもしれません。** 結果を返せなかっただけ、という場合があります。

宅配便を頼んで、追跡画面が「不明」のまま10秒経ったとします。届いていないとは限りません。
配達は済んでいて、画面の更新が遅れているだけかもしれない。

```
t=0     Charge を 10 秒の制限で開始
t=0.1   ワーカーが決済ゲートウェイに送信。応答待ち
t=10    制限時間。ワークフローは「失敗」と受け取る
        ★ ワーカーは止まらない
t=10.5  ゲートウェイから 200 が返り、課金が確定する
```

取り消しを積んでいないので、**この課金は永久に残ります。**

### このライブラリだと

`saga.Step` は、forward を投げる**前**に取り消しを積みます。

```go
chg, _ := saga.Step(ctx, s, "charge",
    saga.Activity(a.Charge), saga.UndoActivity(a.Refund), req)
//  ~~~~~~~~~~~~~~~~~~~~~~~  ~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  これを投げる前に、        これを積む
```

失敗しても、タイムアウトしても、`Refund` は必ず呼ばれます。

### 代わりに気をつけること

「実際には起きなかったステップ」に対しても取り消しが呼ばれます。なので**取り消し側は、
取り消すものが無いときに成功を返す**必要があります。

```go
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
    k, _ := saga.IdempotencyKey(ctx)
    if !a.release(k) {
        return nil   // 課金の記録が無い。返すものが無いので、これは成功
    }
    return a.gateway.Refund(k)
}
```

詳しくは [activity-contract.md](activity-contract.md) に。

---

## 3. forward と取り消しが、別のものを見てしまう

### やりたいこと

返金したい。でも「どの課金を」返すのかを、返金側が知る必要があります。

### 素直に書くと

```go
var chg Charge
err := workflow.ExecuteActivity(ctx, a.Charge, req).Get(ctx, &chg)
...
undo = append(undo, func(c workflow.Context) error {
    return workflow.ExecuteActivity(c, a.Refund, chg.ID).Get(c, nil)
    //                                           ~~~~~~
    //                                    forward が返した ID
})
```

### そこで何が起きるか

項目2で、取り消しは forward の**前**に積む必要があると分かりました。でも `chg.ID` は
forward が返す値です。積む時点ではまだ存在しません。

```
取り消しを積みたい時点  →  chg.ID はまだ空
chg.ID が手に入る時点   →  もう遅い（タイムアウトしたら手に入らない）
```

鶏と卵です。

### このライブラリだと

先に**整理券**を配ります。ステップごとに番号を作り、forward と取り消しの両方に同じ番号を
渡します。番号は forward の結果に依存しないので、実行前に決められます。

```go
// saga.Step の中でやっていること
key := ctx の RunID + "/" + ステップ名     // 例: 01a0be.../charge

opts.ActivityID = key              // forward に渡す
undoOpts.ActivityID = key + ":undo" // 取り消しに渡す
```

アクティビティ側は、どちらも同じ番号を読みます。

```go
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
    k, _ := saga.IdempotencyKey(ctx)   // "01a0be.../charge"
    ...
}

func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
    k, _ := saga.IdempotencyKey(ctx)   // 同じ "01a0be.../charge"
    ...
}
```

番号が同じなので、「この番号の課金はもう済んでいるか」「この番号の課金を返す」が書けます。

### 既定は RunID とステップ名。`KeyFunc` で変えられる

既定の `RunID + ステップ名` が言っているのは「**この試行が一度だけ**」です。run が変われば
鍵も変わります。

もう1つ、意味のある作り方があります。`WorkflowID + ステップ名` で、こちらは「**この業務
操作が一度だけ**」になります。

```go
saga.Options{
    KeyFunc: func(ctx workflow.Context, name string) string {
        return workflow.GetInfo(ctx).WorkflowExecution.ID + "/" + name
    },
}
```

**WorkflowID に API の Request ID（API コールごとに一意な ID）を入れる設計なら、これが
そのまま業務上の冪等キーになります。** 「この注文リクエストの課金は一度だけ」が、鍵の形
そのもので言えている状態です。推奨できる選択肢です。

#### 2つの差はどこで出るか

WorkflowID は run の連鎖（ContinueAsNew・Retry・Cron・Reset）を跨いで同じで、RunID だけが
変わります。なので差が出るのは、**同じワークフローが2回以上 run を作ったとき**だけです。

| | 起きるか |
| --- | --- |
| ワークフロー自身の retry | 起きる。`RetryPolicy` を渡したとき |
| reset | 起きる。運用でやり直したとき |
| cron / ContinueAsNew | リクエストごとに1本のワークフローという設計では起きない |

最後の行は**前提であって、保証ではありません**。同じステップ名を繰り返す cron や
ContinueAsNew のループでワークフローを回すなら、WorkflowID 由来の鍵は周回ごとに同じ値に
なり、2周目以降が「もう済んでいる」と判定されて何もしなくなります。WorkflowID を鍵に
できるのは、**1つの WorkflowID が1回の業務操作に対応している**設計のときだけです。繰り返す
ワークフローなら既定のままにしてください。

**既定が `RunID` で成り立っているのは、Temporal のワークフローが既定では retry policy を
持たないからです。** `StartWorkflowOptions.RetryPolicy` は任意で、渡さなければワークフロー
は1回失敗したらそこで終わります。run が1本しかないなら「この試行が一度だけ」と「この業務
操作が一度だけ」は同じことを言っています。

#### 差の向きは、既定のほうが危ない

retry が起きる設定にしたとき、既定の鍵は壊れます。

ワーカーが落ちて補償が走らないまま workflow が retry されると、新しい RunID から新しい鍵が
生まれます。前の試行が立てた課金は台帳（あるいは下流）に残ったままなのに、新しい鍵では
「まだ誰も課金していない」と見えるので、**立っている課金の上にもう一度課金します**。

```
run 1  charge で "run1/charge" を claim → 課金が立つ
       ワーカーが落ちる。補償は走らない
run 2  charge で "run2/charge" を claim → 未使用に見える → 二重課金
```

WorkflowID 由来の鍵なら、鍵は `wf/charge` のまま変わりません。run 2 は前の試行が残したもの
（業務行、下流の記録、台帳の claim のどれか）を見つけて skip します。逆に**補償が完走して
いればそれは消えている**ので、run 2 は普通に課金をやり直します。

つまり WorkflowID 由来の鍵は、「**補償できた分はやり直す、できなかった分は触らない**」に
自然に落ちます。retry を有効にするなら、こちらを選んでください。

#### WorkflowIDReusePolicy もセットで決める

WorkflowID を鍵にするなら、**同じ WorkflowID で2本目の run が立つ条件**も一緒に決める必要が
あります。`WorkflowIDReusePolicy` の既定は `AllowDuplicate` で、完了済みの WorkflowID なら
新しい run を受け付けます。

効いてくるのは、1本目が失敗して補償まで完走した後です。鍵は同じでも副作用は補償で消えて
いるので、クライアントが同じ Request ID で API を叩き直して2本目が立つと、業務操作は
やり直されます。「1回だけ」のつもりで送り直したのに2回実行された、という形になりうる。
**ステップ単位の鍵が run を跨いで同じでも、業務操作を1回に閉じるのは鍵ではなく起動側の
方針です。** `RejectDuplicate` にする、あるいは走行中の扱いを `WorkflowIDConflictPolicy` で
決める、のどちらかが要ります。

#### `KeyFunc` に載る制約

自分で書くなら3つ守ってください。

- **決定的であること。** 同じ run の同じステップ名から、リプレイ後も同じ鍵が出ること
- **`FirstRunID` を使わないこと。** これも ContinueAsNew・Retry・Cron・Reset を跨いで残る
  ので、結果は WorkflowID 由来の鍵とほぼ同じになります。違うのは**選べるかどうか**です。
  値はサーバが振るので中身に意味を持たせられず、同じ鍵がいつ生まれ直すかを
  `WorkflowIDReusePolicy` のような形で決める手段もありません。「業務操作が一度だけ」が
  欲しいなら、自分で中身を決められる WorkflowID を使ってください
- **数字だけにしないこと。** `ActivityID` を明示しないアクティビティには SDK が純粋な10進数
  を振るので、数字だけの鍵は無関係なアクティビティとぶつかります。Request ID が数字だけ
  でも `+ "/" + ステップ名` が付くので、この制約は満たします

連番（1番目、2番目...）は決定的ですが、ステップを1つ挿入すると**それ以降の番号が全部
ずれ**ます。ステップ名なら、順番を入れ替えても変わりません。

---

## 4. エラーチェックを1つ忘れると、壊れた成功になる

### やりたいこと

ステップごとに `if err != nil` を書きたくない。失敗したら、以降は黙って飛ばしてほしい。

### 素直に書くと

エラーを溜めておいて、以降のステップを何もしないようにする作りが思いつきます。

```go
res, _ := step(s, "reserve", ...)   // エラーは捨てる
chg, _ := step(s, "charge", ...)    // 前が失敗していたら、ここは何もしない
shp, _ := step(s, "ship", ...)

return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
```

### そこで何が起きるか

`charge` が失敗した場合を追います。

```
charge が失敗   →  エラーを記録し、ship は何もしない
                 →  chg と shp は空文字のまま
                 →  return Receipt{...}, nil     ← nil を返してしまった
                 →  ワークフローは「成功」として記録される
```

在庫は押さえられたまま、返金は走らず、画面は緑。アラートも鳴りません。**取り消しが1回も
実行されていないのに、誰も気づきません。**

素の `if err != nil { return }` を書いていれば、書き忘れは次の行でコンパイラかレビューに
引っかかります。**便利にした分、穴もライブラリ側で塞ぐ必要がありました。**

### このライブラリだと

`saga.Run` が、本体の戻り値を信用しません。

```go
// saga.Run の中でやっていること
out, err := body(s)

if err == nil {
    err = s.err       // 本体が nil でも、ステップが失敗していれば拾う
}
if err == nil {
    return out, nil   // 本当に成功したときだけ、戻り値を通す
}

var zero T
return zero, s.compensate(ctx, err)   // 半端な戻り値は捨てる
```

（実際にはこの後に ContinueAsNew の分岐が入ります。あれはエラーの形で返りますが、
saga が続いているだけなので取り消しません。）

チェックを忘れても、ワークフローは失敗し、取り消しは走ります。

### 最初の失敗が勝つ

もう一段あります。**ステップの失敗は、body が後から返すエラーより強い**、という規則です。

ステップが失敗すると、以降の `saga.Activity` は何もせず `AwaitSignal` は即座に返ります。すると
body は「値が空」「signal が来ない」という**結果だけを見て、本当ではない結論**を出します。

```
reserve が失敗
   ↓
AwaitSignal が即座に返る（待つ意味が無いので正しい）
   ↓
body は「誰も承認しなかった」と判断してそのエラーを返す
   ↓
ワークフローは "nobody reviewed the order in time" で失敗   ← 嘘
```

本当の原因は予約の失敗で、それがどこにも残りません。なので `Run` は `s.err` を優先します。

```go
out, err := body(ctx, s)

if s.err != nil {
    err = s.err     // 最初の失敗が根本原因。body のエラーはその後始末
}
```

握って自分のエラーを返したい場合は、先に `s.Clear()` を呼びます。`Clear` はそのために
あります。

---

## 5. forward と取り消しを逆に書いても、誰も教えてくれない

### やりたいこと

`saga.Step(..., saga.Activity(a.Charge), saga.UndoActivity(a.Refund), ...)` の2つを、
うっかり逆に書いた。気づきたい。

### 素直に書くと

SDK に合わせると、引数は `any` になります。

```go
func Step(s *Saga, name string, fwd, undo any, args ...any)
```

### そこで何が起きるか

**コンパイルも通るし、実行時のスケジュール時点でも検証されません。** SDK の
`ExecuteActivity` は関数値を名前の文字列に変換してから渡すので、引数の個数も型も照合され
ないためです。

アクセルとブレーキを逆に配線しても、走り出すまで分からないのと同じです。しかも走り出す
のは、**取り消しフェーズ、つまり saga が既に失敗している最中**です。

### このライブラリだと

型付きの関数で受けます。

```go
func Step[In, Out any](ctx workflow.Context, s *Saga, name string,
    fwd Forward[In, Out], undo *Undo[In], in In) (Out, error)

func Activity[In, Out any](fwd func(context.Context, In) (Out, error)) Forward[In, Out]

func UndoActivity[In any](undo func(context.Context, In) error) *Undo[In]
```

forward は値とエラーを返し、補償はエラーだけを返します。この非対称が効きます。逆に書くと
戻り値の形が合わず、**コンパイルエラー**になります。しかも `Activity` と `UndoActivity` の
両方で落ちるので、どちらを直せばいいかも分かります。

```go
saga.Step(ctx, s, "charge", saga.Activity(a.Refund), saga.UndoActivity(a.Charge), req)
//                                        ~~~~~~~~                    ~~~~~~~~
//                                        わざと逆に書いてみる
```

実際にコンパイラが出すのはこれです。

```
in call to saga.Activity, type func(ctx context.Context, req ChargeReq) error
of a.Refund does not match inferred type func(context.Context, ChargeReq) (Out, error)
for func(context.Context, In) (Out, error)

in call to saga.UndoActivity, type func(ctx context.Context, req ChargeReq) (string, error)
of a.Charge does not match inferred type func(context.Context, ChargeReq) error
for func(context.Context, In) error
```

走らせる前に、エディタが赤線を引きます。

---

## 6. 取り消しが失敗したとき、原因が消える

### やりたいこと

課金の取り消しに失敗した。元の失敗（配送が取れなかった）も、取り消しの失敗も、両方残したい。

### 素直に書くと

エラーを2つまとめる標準の方法があります。

```go
return errors.Join(originalErr, compensationErr)
```

### そこで何が起きるか

Temporal の履歴に残る形に変換する段階で、**中身が落ちます**。変換器は具象型の型スイッチで、
`Unwrap() error` を1本だけ辿る作りだからです。`errors.Join` が返す型はどれにも当たらず、
既定の枝に落ちます。

```
errors.Join(...)  →  型名が "joinError" になる
                  →  原因チェーンが消える
                  →  RetryPolicy の NonRetryableErrorTypes も照合されなくなる
```

履歴に残るのは、改行で連結された文字列1個だけ。

### このライブラリだと

原因を1本鎖で持つエラーを作ります。

```go
temporal.NewApplicationErrorWithOptions(
    "saga: compensation did not finish cleanly; failed: charge",
    saga.CompensationFailedType,                 // 型名で照合できる
    temporal.ApplicationErrorOptions{
        Cause:        originalErr,               // 元の失敗はここに残る
        NonRetryable: true,
        Details:      []any{report},             // 失敗したステップ名
    },
)
```

呼び出し側はこう読めます。

```go
var appErr *temporal.ApplicationError
if errors.As(err, &appErr) && appErr.Type() == saga.CompensationFailedType {
    var report saga.CompensationReport
    appErr.Details(&report)
    // report.Failed  → 取り消しに失敗したステップ
    // report.Skipped → 時間切れで実行されなかったステップ
}
```

1つ失敗しても、残りの取り消しは続けます。返金の失敗を理由に、在庫の解放までやめる理由は
ありません（`StopOnCompensationError` で変えられます）。

---

## 7. ステップがアクティビティに縛られる

### やりたいこと

梱包の工程を子ワークフローで書きたい。履歴が長くなるので独立させたい。でも saga の
ステップとして、他と同じように巻き戻したい。

### 素直に書くと

`saga.Activity` が受け取るのは `func(context.Context, In) (Out, error)` です。これは
**アクティビティ関数のシグネチャそのもの**なので、子ワークフローは渡せません。

```go
// 子ワークフローは第1引数が workflow.Context なので、Activity には入らない
saga.Step(ctx, s, "pack", saga.Activity(PackWorkflow), nil, req)   // コンパイルエラー
```

逃げ道は `s.Add` で手書きすることですが、そうすると冪等キーも予算の切り詰めも自分で
やることになります。

### そこで何が起きるか

「forward と補償を対で登録する」という中核は、**executor に依存していません**。
補償を先に積む、同じ鍵を両側に渡す、逆順で回す、予算で切る。どれもアクティビティである
必要はない。縛っていたのは引数の型だけでした。

SDK を確認すると、executor ごとに違うのは3点だけです。

| 名前 | 実行 | 鍵を載せる場所 | 予算で切る対象 |
| --- | --- | --- | --- |
| `saga.Activity` | `ExecuteActivity` | `ActivityOptions.ActivityID` | `ScheduleToCloseTimeout` |
| `saga.ChildWorkflow` | `ExecuteChildWorkflow` | `ChildWorkflowOptions.WorkflowID` | `WorkflowExecutionTimeout` |
| `saga.Func` | **その場で呼ぶ** | 遠隔実行が無いので不要 | 切るものが無い |

名前は「何を実行するか」で揃えています。`Step` は1つだけで、何で実行するかは渡す値が
決めます。`Step` という名前がアクティビティ専用だと、名前からそれが読めません。

外部ワークフローへの signal に専用の値を用意していないのもこの表のためです。
`SignalExternalWorkflow` には options 構造体が無いので、鍵を載せる場所も切る予算も
ありません。鍵も予算も扱わない executor は `saga.Func` に語彙を足しただけになります。

### このライブラリだと

中核は `Step` のまま、上の3点だけを閉じ込めたクロージャを作る値として
`saga.ChildWorkflow` と `saga.Func`、および対応する `saga.Undo*` を用意しています。

```go
pack, _ := saga.Step(ctx, s, "pack",
    saga.ChildWorkflow(PackWorkflow), saga.UndoActivity(a.Unpack), PackReq{Order: in})

saga.Step(ctx, s, "hold",
    saga.Func(sendHold), saga.UndoFunc(sendRelease), HoldReq{SKU: in.SKU})
```

形は `saga.Activity` と同じ。forward が値とエラーを返し補償がエラーだけを返す非対称も
同じなので、取り違えはやはりコンパイルエラーになります。**渡す関数の第1引数の型が
executor を選ぶ**ので、どちらを呼ぶかは型が教えてくれます。

forward と補償は別々の値なので、**片方だけ別の executor にできます**。上の `pack` が
それで、荷造りは子ワークフロー、荷ほどきはアクティビティ1回です。鍵は両側で同じなので、
子は `IdempotencyKeyOf`、アクティビティは `IdempotencyKey` で同じ値を読みます。

混在した saga は1つの逆順で巻き戻ります。補償のレジストリは元から
`func(workflow.Context) error` を持っているだけで、executor を区別していないからです。

### 待つ処理を本体から追い出す

4つ目の「ワークフロー本体」は、アクティビティだけが持っていた特権を他にも配るための
ものです。

アクティビティは「失敗が何を意味するか」を関数の中に閉じ込められます。`a.Charge` が
`error` を返せば、それがステップの失敗になり、本体には何も漏れません。ところが signal を
待つ処理は本体に書くしかなく、分岐が漏れていました。

```go
// 漏れている形
decision, ok := saga.AwaitSignal[Decision](ctx, "approval", wait)
if !ok {
    return Receipt{}, temporal.NewApplicationError("nobody reviewed it", DeniedType, nil)
}
if !decision.Approved {
    return Receipt{}, temporal.NewApplicationError("rejected", DeniedType, nil)
}
```

`saga.Func` は、利用者が書いた**ワークフローコードをその場で呼んでステップにします**。
判断はその関数の中で完結し、本体はステップの列のままになります。

```go
saga.Step(ctx, s, "approval", saga.Func(awaitApproval), nil, ApprovalReq{Wait: wait})

// 利用者が書く。アクティビティを書くのと同じ立ち位置
func awaitApproval(ctx workflow.Context, req ApprovalReq) (Decision, error) {
    decision, ok := saga.AwaitSignal[Decision](ctx, ApprovalSignal, req.Wait)
    if !ok {
        return Decision{}, temporal.NewApplicationError("nobody reviewed it", DeniedType, nil)
    }
    ...
}
```

ステップになったので、**先のステップが失敗していれば飛ばされる**のも他と同じです。
`AwaitSignal` が `*Saga` を取って自分でスキップ判定をしていた特別扱いは、これで消えました。

### どれが専用の関数に値するか

4つのうち `saga.Func` は他の3つを**全部書けてしまいます**。渡す関数の中で
`ExecuteActivity` を呼べばアクティビティのステップになるし、`SignalExternalWorkflow` を
呼べば signal のステップになる。

では何が専用の関数を正当化するのか。**冪等キーを載せるか、予算で切るか**です。

| | 鍵 | 予算 | 専用の値に値するか |
| --- | --- | --- | --- |
| `saga.Activity` | ✓ | ✓ | する |
| `saga.ChildWorkflow` | ✓ | ✓ | する |
| 外部への signal | ✗ | ✗ | **しない**。`saga.Func` で書く |
| `saga.Func` | — | — | 他が扱えないものの受け皿 |

signal を送るステップは、以前は専用の値にしていました。この基準に照らすと
`saga.Func` に語彙を足しただけだったので、落としました。

### Temporal の他のオブジェクトはどうか

副作用を持つワークフロー API を一通り当たった結果です。

| | 鍵 | 予算 | 判断 |
| --- | --- | --- | --- |
| Nexus operation（`NexusClient.ExecuteOperation`） | ✗ | ✓ `ScheduleToCloseTimeout` | **一番近い候補**。予算は切れるが鍵が載らない |
| `RequestCancelExternalWorkflow` | ✗ | ✗ | `saga.Func` で足りる。そもそも取り消しを取り消せない |
| `UpsertTypedSearchAttributes` / `UpsertMemo` | 不要 | 無い | `saga.Func` で足りる。自分の可視化情報なので遠隔実行が無い |
| `SideEffect` / `MutableSideEffect` | — | — | 値を作るだけで外に副作用が無い |
| ローカルアクティビティ | ✗ | ✓ | 鍵が載らない。リトライがサーバに残らないので置き場所としても不適 |

**Nexus だけは将来 `NexusStep` に値するかもしれません。** 他サービスへの呼び出しで、
タイムアウトを持ち、補償が要る副作用を起こしうるからです。ただし
`NexusOperationOptions` に ID フィールドが無いので、今の基準では `saga.Func` と同じ段に
なります。必要になってから足します。

### signal のステップが一番弱い理由

`saga.Func` は成立しますが、`SignalExternalWorkflow` には options 構造体が無いので
**冪等キーを載せる場所がありません**。対になっていることは保証できても、受け手が同じ
signal を2回受けたときに壊れないようにするのは受け手の責任です。形が `saga.Activity` と違うのも
そのためで、signal は関数呼び出しではないので `fwd`/`undo` に関数ではなく宛先
（`Signal`）を取ります。

### ローカルアクティビティを外した理由

`LocalActivityOptions` には ID フィールドがありません。冪等キーを載せる場所が無いので、
このライブラリの契約を満たせない。加えてローカルアクティビティはリトライがワークフロー
タスク内で完結してサーバに残らないので、取り消しが必要な副作用を置く場所としても適して
いません。

---

## まとめ

| 素直に書くと | 起きること | このライブラリ |
| --- | --- | --- |
| 取り消しを `ctx` で実行 | キャンセル時に全部失敗する | 切り離した context で実行し、制限時間を必須に |
| 成功してから取り消しを積む | タイムアウトした副作用が残る | 実行する前に積む |
| forward の戻り値を取り消しに渡す | 積む時点でまだ存在しない | 先に整理券を配り、両方に渡す |
| エラーを溜めて後で見る | 見忘れると壊れた成功になる | `Run` が本体の戻り値を信用しない |
| `any` で関数を受ける | 取り違えが取り消し中に発覚 | 型で受けてコンパイルエラーに |
| `errors.Join` で束ねる | 履歴から原因が消える | 1本鎖の `ApplicationError` |
| ステップをアクティビティに限る | 子ワークフローや外部への signal が巻き戻しに乗らない | `saga.ChildWorkflow` と `saga.Func` |

塞げていない穴は [activity-contract.md](activity-contract.md) に書いてあります。
