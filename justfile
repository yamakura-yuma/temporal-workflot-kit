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

# Run go vet over every package.
vet:
    {{dev}} go vet ./...

# Extra args go to `go test`, e.g. `just test -run TestOrder -v`.
test *args:
    {{dev}} go test ./... {{args}}

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
ci: fmt-check vet build test

# --- running the service -----------------------------------------------------

# Start the Temporal dev server and the worker in the background.
up:
    docker compose up -d temporal worker

# Same, but stream the logs in the foreground (ctrl-c stops).
up-fg:
    docker compose up temporal worker

# Stop everything and remove the containers (named caches survive).
down:
    docker compose down

# Follow the logs of the running services.
logs *args:
    docker compose logs -f {{args}}

# Start one workflow execution against the running dev server.
starter:
    docker compose run --rm starter

# `temporal` CLI against the dev server, e.g. `just temporal workflow list`.
temporal +args:
    {{dev}} temporal --address temporal:7233 {{args}}

# --- agent config ------------------------------------------------------------

# apm and Claude Code live on the host, not in the container, so this one does
# too.
# Deploy the shared agent config from apm.yml into ./.claude/.
apm-install:
    apm install
