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

# Unit tests: the saga package against the in-memory test environment.
# Extra args go to `go test`, e.g. `just test -run TestBudget -v`.
test *args:
    {{dev}} go test ./... {{args}}

# Slower than `just test`, and the only place cancellation and search attributes
# can actually be checked.
# Run the executable specifications under docs/specs/, against a real dev server.
spec *args:
    {{dev}} gauge run {{args}}

# Same run, but the dev server stays up afterwards so the histories it just
# produced can be read at http://localhost:8233. Ctrl-C to end it.
# Not {{dev}}: --service-ports is what actually publishes the port, and only
# this recipe wants it, so `just spec` and `just ci` bind nothing.
spec-ui *args:
    docker compose run --rm --service-ports -e SPEC_HOLD=1 dev gauge run {{args}}

# Check every step in docs/specs/ has an implementation, without running anything.
spec-validate:
    {{dev}} gauge validate

# Show which Go function implements each step, since the only link between a
# line in docs/specs/ and the code is the step text.
spec-steps:
    {{dev}} grep -rn '^var _ = gauge.Step("' stepImpl/ | sed -E 's/:var _ = gauge\.Step\("/  ->  /; s/".*$//'

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

# Everything that must pass before a change ships.
ci: fmt-check vet build test docs-check spec-validate spec

# --- agent config ------------------------------------------------------------

# apm and Claude Code live on the host, not in the container, so this one does
# too.
# Deploy the shared agent config from apm.yml into ./.claude/.
apm-install:
    apm install
