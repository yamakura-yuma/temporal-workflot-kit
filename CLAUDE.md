# temporal-saga

A Go library for the Saga pattern on Temporal: a sequence of activities that
can be rolled back, with the rollback wired up correctly for the cases that are
easy to get wrong.

## Stack

- Go (module `github.com/yamakura-yuma/temporal-saga`)
- Temporal Go SDK (`go.temporal.io/sdk`)

## Layout

- `saga/` — the library. `Run` owns the rollback, `Step` runs one forward
  activity and registers its compensation. Unit tests run against Temporal's
  in-memory test environment
- `specs/` — executable specifications in Gauge's markdown, written in
  Japanese. Run against a real Temporal dev server the suite starts in-process.
  The only link to the code is the step text, matched against the string in
  `gauge.Step(...)`
- `stepImpl/` — the Go implementations of the steps those specifications are
  written in, plus the suite hooks that start the server and worker
- `example/order/` — the saga the specifications drive (reserve, charge, ship).
  Also the worked example of the activity contract. A normal package, because
  Gauge builds the module rather than a test binary

There is no application here and nothing under `internal/`: this repo is a
library, and a library under `internal/` cannot be imported from outside the
module.

## Commands

Development happens inside the container built from `Dockerfile` (Go +
`temporal-cli` via Nix, pinned by `flake.nix`), driven from the host with
`just`. No host-level Go install is needed; the repo is bind-mounted into
`/workspace`, so edits are picked up without rebuilding.

- `just` — list every recipe
- `just build` / `just vet` / `just test` — Go build, vet, unit tests
- `just spec` — run the specifications in `specs/` against a real dev server
- `just spec-validate` — check every step has an implementation, without running
- `just spec-steps` — which Go function implements each step
- `just ci` — fmt-check, vet, build, unit tests, spec-validate, spec; run before
  shipping a change
- `just shell` — interactive shell in the dev container

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
