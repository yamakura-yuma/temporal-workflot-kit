#!/usr/bin/env bash
# docs と README のコードブロックはコンパイルされないので、腐りを止めるのはここだけ。
# 実際に一度腐らせたので機械で見ている。経緯は docs/design.md ではなく git log に。
set -euo pipefail

fail=0
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

# 1. 消した API がドキュメントに残っていないか。
#    saga.Activity / UndoActivity / ChildWorkflow / UndoChildWorkflow / Func /
#    UndoFunc と IdempotencyKey / IdempotencyKeyOf / DefaultKey は無い。ステップの
#    両半分はただの func(workflow.Context) error になった。
#
#    docs/design.md と docs/activity-contract.md は別の作業で書き換え中なので、
#    いまは対象外。書き換えが終わったらこの除外を消すこと。
gone='saga\.(Activity|UndoActivity|ChildWorkflow|UndoChildWorkflow|Func|UndoFunc)\(|\b(UndoActivity|UndoChildWorkflow|UndoFunc|IdempotencyKeyOf|IdempotencyKey|DefaultKey|KeyFunc|CompensationBudget|RemainingBudget|AwaitSignal)\b|\bsaga\.Run\(|\bsaga\.Step\('

grep -rnE "$gone" docs .apm README.md \
  | grep -v '^docs/design\.md:' > "$tmp" || true

# docs/design.md は「意図して手放したもの」の節で、消した API を名指しで説明している。
# そこは腐りではなく履歴なので、見出しより上だけを見る。
sed -n '1,/^# 意図して手放したもの/p' docs/design.md \
  | grep -nE "$gone" \
  | sed 's|^|docs/design.md:|' >> "$tmp" || true

# saga/*.go のコメントも同じ腐り方をする。ここは prefix が付かないので別の綴りで見る。
grep -rnE '\b(UndoActivity|UndoChildWorkflow|UndoFunc|IdempotencyKeyOf|IdempotencyKey|DefaultKey|runUndo|stepID)\b' \
  saga/*.go >> "$tmp" || true

if [ -s "$tmp" ]; then
  echo "消した API がドキュメントに残っている（ステップの両半分は" >&2
  echo "func(workflow.Context) error、冪等キーは saga.StepKey）:" >&2
  cat "$tmp" >&2
  fail=1
fi

# 2. 上流由来のノートのスタンプが go.mod の SDK 版と一致しているか。
sdk=$(grep -oE 'go\.temporal\.io/sdk v[0-9.]+' go.mod | awk '{print $2}')
stale=$(grep -rn 'upstream: go\.temporal\.io/sdk v' docs .apm \
        | grep -v "${sdk}\([^0-9.]\|\$\)" || true)
if [ -n "$stale" ]; then
  echo "go.mod は go.temporal.io/sdk ${sdk}。スタンプが古い:" >&2
  echo "$stale" >&2
  echo "上流を開いて各行を確認してから版を上げること（upstream-docs スキル）" >&2
  fail=1
fi

exit "$fail"
