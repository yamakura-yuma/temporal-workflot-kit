#!/usr/bin/env bash
# docs と README のコードブロックはコンパイルされないので、腐りを止めるのはここだけ。
# 実際に一度腐らせたので機械で見ている。経緯は docs/design.md ではなく git log に。
set -euo pipefail

fail=0
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

# 1. saga.Step の呼び出しが executor コンストラクタで包まれているか。
#    引数を次の行に折り返す書き方があるので、2行の窓で見る。
#    シグネチャの記載（saga.Step(ctx, s, name, fwd, undo, in)）は引数がクォート
#    されないので、ここには当たらない。
while IFS= read -r -d '' f; do
  awk -v file="$f" '
    /saga\.Step\(ctx, s, "/ { pending = $0; lineno = NR; next }
    pending {
      window = pending "\n" $0
      if (window !~ /saga\.(Activity|ChildWorkflow|Func)\(/)
        printf "%s:%d: %s\n", file, lineno, pending
      pending = ""
    }
    END {
      if (pending && pending !~ /saga\.(Activity|ChildWorkflow|Func)\(/)
        printf "%s:%d: %s\n", file, lineno, pending
    }
  ' "$f"
done < <(find docs .apm -name '*.md' -print0; printf 'README.md\0') > "$tmp"

if [ -s "$tmp" ]; then
  echo "saga.Step の forward と補償は saga.Activity / ChildWorkflow / Func と" >&2
  echo "対応する saga.Undo* で包む:" >&2
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
