---
name: skill-authoring
description: >-
  Use when adding a new skill to this repo, improving an existing one, or
  deciding whether a piece of project knowledge belongs in a skill instead of
  CLAUDE.md. Covers the apm-based authoring and deployment flow for
  .claude/skills/.
---

# Authoring and improving skills via apm

This repo manages `.claude/skills/` through `apm` rather than editing that
directory by hand. `.claude/skills/` is generated output — always edit the
source under `.apm/skills/` and redeploy.

## Add a new skill

1. Pick a lowercase-hyphenated name and create
   `.apm/skills/<name>/SKILL.md` with frontmatter:
   ```yaml
   ---
   name: <name>
   description: >-
     Imperative, intent-focused description of when to load this skill.
     ("Use when...") Under 1024 characters.
   ---
   ```
2. Write the skill body. Keep `SKILL.md` itself under ~500 lines / 5000
   tokens; push deep-dive content, checklists, or examples into
   `references/`, `scripts/`, `assets/`, or `examples/` subdirectories and
   point to them from the body (see `go-temporal-review/` for an example
   split between a short `SKILL.md` and a `references/checklist.md`).
3. Preview deployment without writing anything:
   ```
   apm install --dry-run --target claude
   ```
4. Scan for issues (hidden Unicode, etc.):
   ```
   apm audit --file .apm/skills/<name>/SKILL.md
   ```
5. Deploy:
   ```
   apm install --target claude
   ```
   This copies the skill folder to `.claude/skills/<name>/SKILL.md`.
6. Commit `.apm/skills/<name>/`, the generated `.claude/skills/<name>/`, and
   the updated `apm.lock.yaml` together.

## Improve an existing skill

Edit the files under `.apm/skills/<name>/` (never edit
`.claude/skills/<name>/` directly — it will be overwritten on the next
install), then repeat steps 3–6 above.

## Deciding where knowledge belongs

- Belongs in `CLAUDE.md`: things true of the whole project that rarely
  change — stack, build/test/run commands, directory layout.
- Belongs in a skill: anything procedural, checklist-like, or that only
  applies in a specific situation (reviewing Temporal code, writing a saga
  step, running a retro). Skills auto-activate by intent, so they scale
  better than a growing CLAUDE.md that every prompt has to carry.
- If a lesson doesn't fit an existing skill's scope, create a new one rather
  than stretching an unrelated skill's `description`.
