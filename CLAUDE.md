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

Development happens inside the container built from `Dockerfile` (Go +
`temporal-cli` via Nix, pinned by `flake.nix`), driven from the host with
`just`. No host-level Go install is needed; the repo is bind-mounted into
`/workspace`, so edits are picked up without rebuilding.

- `just` — list every recipe
- `just build` / `just vet` / `just test` — Go build, vet, test
- `just ci` — fmt-check, vet, build, test; run before shipping a change
- `just up` — start the Temporal dev server and the worker in the background;
  `just starter` then runs one workflow, `just down` stops everything
- `just shell` — interactive shell in the dev container
- `just temporal <args>` — `temporal` CLI against the dev server

## Skills

Agent config comes from two places, both deployed into `./.claude/` by
`just apm-install` (see `apm.yml`):

- `.apm/` — this project's own knowledge. The `saga-workflows` skill covers how
  a saga step and its compensation are built here, plus the determinism,
  idempotency and retry checklist a change has to pass. Edit and review this
  directory; it is the only agent config this repo authors.
- `core-principal` — the shared harness (rules, git guard hooks, `core-*`
  skills) from the [dotfiles](https://github.com/yamakura-yuma/dotfiles) repo,
  pinned by commit. Change it there and bump the `ref` here with `apm update`;
  don't fork it locally.

Anything that would read the same in another repo belongs in `core-principal`,
not in `.apm/`. `.claude/` and `apm_modules/` are generated and gitignored.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
