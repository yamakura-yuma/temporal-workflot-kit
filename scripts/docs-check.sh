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
gone='saga\.(Activity|UndoActivity|ChildWorkflow|UndoChildWorkflow|Func|UndoFunc|Run|Step|StepKey|AwaitSignal|IdempotencyKey|IdempotencyKeyOf|DefaultKey)\b|\b(UndoActivity|UndoChildWorkflow|UndoFunc|IdempotencyKeyOf|IdempotencyKey|DefaultKey|KeyFunc|StepKey|CompensationBudget|RemainingBudget|AwaitSignal|StopOnCompensationError|InlineStep|undoSuffix|withActivityID|stepKey|activity-contract)\b'

# ドキュメント。design.md の「意図して手放したもの」より下だけは、消した API を名指しで
# 説明しているので別扱いにする。
grep -rnE "$gone" docs .apm README.md CLAUDE.md \
  | grep -v '^docs/design\.md:' > "$tmp" || true

sed -n '1,/^# 意図して手放したもの/p' docs/design.md \
  | grep -nE "$gone" \
  | sed 's|^|docs/design.md:|' >> "$tmp" || true

# コードとその図。ここを見ていなかったので腐りが溜まっていた。
grep -rnE "$gone" saga example specsteps --include='*.go' --include='*.html' >> "$tmp" || true

# 改名し損ねた Run。RunOrCompensate・RunID・WorkflowRun には当たらない。
# docs/temporal-concepts.html は Temporal の概念としての Run（Run ID、Run の連鎖）を
# 説明する文書なので、この検査の対象外。
grep -rnE '\bRun\b' saga example specsteps docs README.md CLAUDE.md \
     --include='*.go' --include='*.md' --include='*.html' --include='*.feature' \
  | grep -v '^docs/temporal-concepts\.html:' \
  | grep -vE 'RunOrCompensate|RunID|WorkflowRun|Run ID|Run Id|Workflow Run|\.Run\(|func Run|TestRun' >> "$tmp" || true

if [ -s "$tmp" ]; then
  echo "消した API がドキュメント・コメントに残っている。" >&2
  echo "ステップの両半分は func(workflow.Context) error、冪等キーは利用者が作る:" >&2
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
