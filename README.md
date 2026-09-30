# OpenAlex Research Runner — Research 002

Status: **PROGRAM COMPLETE**

Frozen research snapshot: **2026-09-23**

This repository contains the reproducible computation and audit chain for **Research 002 | OpenAlex Global Scholarly Graph Analysis**.

## Closed stage sequence

Stages **0–19** are complete for the frozen 2026-09-23 program.

Persistent final release:

https://github.com/lyh1703/openalex-research-runner/releases/tag/openalex-stage19-program-complete-2026-09-23

Final artifacts include:

- `FINAL_REPRODUCIBILITY_INDEX.json`
- `FINAL_ARTIFACT_MANIFEST.json`
- `FINAL_REPRODUCIBILITY_AUDIT.md`
- `EXECUTIVE_SUMMARY.md`
- `PROGRAM_COMPLETE.md`
- `STAGE19_FINAL_GATE.json`

## Reproducibility classification

- DERIVED_ARTIFACT_REPRODUCIBLE
- SOURCE_RECONSTRUCTABLE_WHILE_UPSTREAM_AVAILABLE
- NOT_BYTE_ARCHIVED
- NOT_POINT_IN_TIME_HISTORICAL

The full ~771 GB raw OpenAlex snapshot is intentionally not privately retained under the zero-cost architecture. Exact historical OpenAlex database vintages also cannot be reconstructed from a single 2026 snapshot.

Future work should be treated as either **applied downstream use** or a **new dated snapshot refresh**, not continuation of the closed 2026-09-23 stage sequence.


## Post-completion operating model

**Primary operational consumer: Project ATLAS.**

Research 002 remains the independent frozen source of truth. ATLAS consumes its scholarly-growth/convergence evidence for candidate generation and filtering, then routes selected candidates to NOUS, ARGUS, NEXUS, business research, or other projects as relevant. The full Research 002 corpus is not absorbed into those systems.

Refreshes are new dated snapshots, normally considered every 3–6 months or when a material freshness/schema/taxonomy trigger occurs.

See `POST_COMPLETION_OPERATING_POLICY.md`.
