# Research 002 — Post-Completion Operating Policy

Status: **CLOSED / REUSABLE INTELLIGENCE ASSET**

Frozen snapshot: **2026-09-23**

Research 002 is complete through Stage 19 and must remain an independent, immutable source-of-truth research asset.

## Operational placement

Research 002 is not an active always-on project and is not absorbed into ATLAS, NOUS, ARGUS, NEXUS, or a business project.

Canonical operational flow:

```text
Research 002 OpenAlex (frozen source of truth)
    -> ATLAS (primary candidate generation / filtering)
        -> NOUS (evidence, provenance, definitions, caveats)
        -> ARGUS (conditional: sensing / spatial / perception relevance)
        -> NEXUS (conditional: software / agent / orchestration relevance)
        -> Venture / Business research (conditional: commercialization question)
        -> other project/module via ATLAS project matching and explicit promotion gate
```

## Why ATLAS is the primary consumer

Research 002 contains a large candidate space. ATLAS exists to convert external technologies/research signals into evidence-backed technology candidates and to filter/project-match them before downstream action.

This prevents:
- flooding every project with raw scholarly signals;
- treating academic emergence as implementation readiness;
- duplicating the canonical Research 002 corpus;
- bypassing validation and project-promotion gates.

## Source-of-truth rule

The final Research 002 release remains canonical:
`openalex-stage19-program-complete-2026-09-23`

Downstream systems may reference, index, summarize, or cache bounded derivatives, but they must preserve:
- snapshot date;
- entity/provenance identifiers;
- evidence class;
- Stage 17 robustness limitations;
- 2026 FRONTIER_YTD status where applicable.

## Refresh strategy

Research 002 is **not continuously recomputed**.

Default operating policy:
- review refresh need quarterly;
- typically refresh every **3–6 months** only when justified;
- use event-driven refresh earlier when OpenAlex schema/taxonomy changes materially or downstream freshness becomes insufficient.

Every refresh must be a new dated snapshot/version. Never overwrite the frozen `2026-09-23` program.

## Reopen rule

Do not reopen Stages 0–19 for the frozen snapshot unless a stored validation gate is demonstrably incorrect.

Future work is:
1. downstream applied use; or
2. a new dated Research 002 snapshot refresh.
