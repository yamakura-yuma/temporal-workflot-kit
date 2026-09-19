---
name: session-retro
description: >-
  Use at the end of a session, after a user correction, or after resolving a
  tricky bug in this repo, to decide whether a durable lesson was learned and
  where it should be recorded. Never writes procedural detail into CLAUDE.md.
---

# Session retrospective

CLAUDE.md in this repo stays intentionally short (stack, layout, commands).
It is not the place for lessons learned. This skill routes lessons to skills
instead, so the knowledge base grows without CLAUDE.md growing.

## When to trigger

- The user corrected an approach, pointed out a mistake, or stated a
  standing preference.
- A bug took real effort to track down and the root cause generalizes beyond
  this one instance.
- A review (human or `go-temporal-review`) caught something worth
  remembering for next time.

Skip it for anything one-off, already covered by an existing skill, or that
wouldn't change future behavior in this repo.

## What to do

1. State the lesson as one durable, generalizable sentence: the situation,
   what went wrong or what was preferred, and the correct behavior going
   forward.
2. Find the skill it belongs to:
   - Go/Temporal code-quality lesson → `go-temporal-review`
     (`references/checklist.md`).
   - Lesson about the skill-authoring process itself → `skill-authoring`.
   - Doesn't fit any existing skill → use the `skill-authoring` flow to
     create a new, narrowly-scoped skill for it.
3. Append the lesson to that skill's `references/` file (or `SKILL.md` body
   if there's no `references/` yet), as a short bullet with the concrete
   scenario it applies to — not a vague principle.
4. Redeploy with `apm install --target claude` and commit the change.

Do not add the lesson to `CLAUDE.md`. If you're tempted to, that's a sign it
should instead be a new skill or a `references/` entry in an existing one.
