# Domain docs

Engineering skills use these rules when reading this repo's domain documentation.

## Before exploring

Read `CONTEXT.md` at the repository root when it exists.

Read relevant ADRs under `design/adr/` when that directory exists.

Proceed silently when either location does not exist.

Skill(domain-modeling) creates these files when the project resolves domain terms or architectural decisions.

## Layout

This repo uses a single domain context.

```text
CONTEXT.md
design/adr/
```

## Use the glossary's vocabulary

Use terms as defined in `CONTEXT.md` when naming issues, proposals, hypotheses, and tests.

Do not replace defined terms with synonyms that the glossary rejects.

An undefined concept may indicate invented language or a genuine domain-model gap.

Reconsider invented language and record genuine gaps for Skill(domain-modeling).

## Flag ADR conflicts

Surface any conflict with an existing ADR instead of silently overriding it.

Name the conflicting ADR and explain why the decision may need reconsideration.
