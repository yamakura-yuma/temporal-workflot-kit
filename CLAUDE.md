# temporal-saga

A Go service built on the Temporal workflow engine, implementing the Saga pattern.

## Stack

- Go (module `github.com/yamakura-yuma/temporal-saga`)
- Temporal Go SDK (`go.temporal.io/sdk`)

## Layout

- `cmd/worker/` — worker entrypoint (registers workflows/activities, polls a task queue)
- `cmd/starter/` — starts a workflow execution
- `internal/workflow/` — workflow definitions
- `internal/activity/` — activity definitions

## Commands

- Build: `go build ./...`
- Vet: `go vet ./...`
- Test: `go test ./...`
- Run locally: start a Temporal dev server (`temporal server start-dev`), then
  `go run ./cmd/worker` in one terminal and `go run ./cmd/starter` in another.
- Alternatively, no host-level install needed: `docker compose up temporal worker`
  starts the dev server and worker, then `docker compose run --rm starter` runs
  a workflow. The image (`Dockerfile`) provides Go + `temporal-cli` via Nix
  (`flake.nix`).

## Skills

Conventions, review checklists, and process (Go/Temporal review rules, how to
add or improve a skill, how to run a retrospective) live in skills under
`.claude/skills/`, managed via [`apm`](https://microsoft.github.io/apm/) with
sources in `.apm/skills/`. Extend a skill instead of adding procedural detail
here — see the `skill-authoring` skill for the flow.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
