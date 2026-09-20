# Development commands for temporal-saga.
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

# Run go vet over every package, including the tagged integration tests.
vet:
    {{dev}} go vet -tags=integration ./...

# Unit tests: the saga package against the in-memory test environment.
# Extra args go to `go test`, e.g. `just test -run TestBudget -v`.
test *args:
    {{dev}} go test ./... {{args}}

# Slower than `just test`, and the only place cancellation and search
# attributes can actually be checked.
# The library against a real Temporal dev server, started in-process by the test.
test-integration *args:
    {{dev}} go test -tags=integration -count=1 ./integration/ {{args}}

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
ci: fmt-check vet build test test-integration

# --- agent config ------------------------------------------------------------

# apm and Claude Code live on the host, not in the container, so this one does
# too.
# Deploy the shared agent config from apm.yml into ./.claude/.
apm-install:
    apm install
