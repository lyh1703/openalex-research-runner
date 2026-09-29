# OpenAlex Research 002 — Stage 14 In-Progress Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative baseline:
- Stages 0–13: COMPLETE
- Stage 13 release: `openalex-stage13-2026-09-23`
- Stage 14 input aggregate row count confirmed: `topic_pair_year = 53,119,699`
- Stage 14 topic hierarchy schema confirmed from the frozen Stage 8 entity layer.

Stage 14 frozen contract:
- Primary universe: Core + Tier A
- Confirmed years: 2000–2025
- 2026: frontier YTD only
- No raw 476M Works rescan
- Inputs: topic_pair_year + topic_year + base_dimensions_year + topic hierarchy dimension
- Metrics kept separate: prevalence, joint score, Jaccard, overlap coefficient, lift, PMI/NPMI, growth, lift change, first substantial year, hierarchy distance
- Novelty means novelty within the 2000+ analytic window, not proof of first-ever historical occurrence.
- No opaque composite score.

Active workflow:
- `OpenAlex Stage 14`
- Run ID: `36643308958`
- Trigger head: `66cf01ccfaa4bd70706ceccc960b51d6eec01d5d`

Expected completion artifacts:
- pair_year_primary_signals.parquet
- novel_combination_candidates.parquet
- emerging_convergence_candidates.parquet
- cross_domain_novel_candidates.parquet
- topic_convergence_year.parquet
- field_pair_coassignment_year.parquet
- domain_pair_coassignment_year.parquet
- STAGE14_VALIDATION.json
- STAGE14_FINAL_GATE.json
- OPENALEX_STAGE14_CONVERGENCE_REPORT.md

Recovery rule:
If Run 36643308958 is still queued/in_progress, do not start a duplicate run. If it fails, inspect the exact failing step and patch only Stage 14.
