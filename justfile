# Development commands for temporal-workflow-kit.
#
# Everything runs inside the container built from `Dockerfile` (Go +
# temporal-cli, pinned by `flake.nix`), so the only host-level requirements are
# docker and just. The repo is bind-mounted into /workspace, so edits on the
# host are picked up without rebuilding the image.

set shell := ["bash", "-euo", "pipefail", "-c"]

dev := "docker compose run --rm dev"

# List the available recipes.
default:
    @just --list

# --- toolchain ---------------------------------------------------------------

# Build (or rebuild) the dev image.
image:
    docker compose build

# Interactive shell inside the dev container.
shell:
    {{dev}} bash

# Run an arbitrary command inside the dev container, e.g. `just run go env`.
run +args:
    {{dev}} {{args}}

# --- Go ----------------------------------------------------------------------

# Compile every package.
build:
    {{dev}} go build ./...

# Run go vet over every package.
vet:
    {{dev}} go vet ./...

# Every test: the saga package against the in-memory test environment, and the
# specifications under docs/specs/ against a real dev server. Since godog runs
# as a plain Go test, there is only one entry point.
# Extra args go to `go test`, e.g. `just test -run TestBudget -v`.
test *args:
    {{dev}} go test ./... {{args}}

# Just the specifications, verbosely: godog prints each scenario and step, and
# the scenarios are Go subtests, so `just spec -run 'TestFeatures/成功した_saga_は何も取り消さない'`
# runs one of them. Slower than the unit tests, and the only place cancellation
# and search attributes can actually be checked.
spec *args:
    {{dev}} go test ./specsteps/ -v {{args}}

# Same run, but the dev server stays up afterwards so the histories it just
# produced can be read at http://localhost:8233. Ctrl-C to end it.
# Not {{dev}}: --service-ports is what actually publishes the port, and only
# this recipe wants it, so `just spec` and `just ci` bind nothing.
spec-ui *args:
    docker compose run --rm --service-ports -e SPEC_HOLD=1 dev go test ./specsteps/ -v {{args}}

# docs と README のコード例が現行 API と合っているか、上流由来のノートが go.mod の
# SDK 版と合っているかを確認する。docs のコードブロックはコンパイルされないので、
# 腐りを止めるのはここだけ。
docs-check:
    {{dev}} bash scripts/docs-check.sh

# Format the tree in place.
fmt:
    {{dev}} gofmt -l -w .

# Fails when anything is unformatted, which is what CI wants.
fmt-check:
    {{dev}} bash -c 'unformatted=$(gofmt -l .); if [ -n "$unformatted" ]; then echo "gofmt needed:"; echo "$unformatted"; exit 1; fi'

# Sync go.mod/go.sum with the imports in the tree.
tidy:
    {{dev}} go mod tidy

# Everything that must pass before a change ships. `test` covers the
# specifications too, so there is no separate spec recipe here.
ci: fmt-check vet build test docs-check

# --- agent config ------------------------------------------------------------

# apm and Claude Code live on the host, not in the container, so this one does
# too.
# Deploy the shared agent config from apm.yml into ./.claude/.
apm-install:
    apm install
