# OpenAlex Research 002 — Stage 14 In-Progress Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative baseline:
- Stages 0–13: COMPLETE
- Stage 13 release: `openalex-stage13-2026-09-23`
- Stage 14 input aggregate row count confirmed: `topic_pair_year = 53,119,699`
- Topic hierarchy schema confirmed from the frozen Stage 8 entity layer.

Stage 14 frozen contract:
- Primary universe: Core + Tier A
- Confirmed years: 2000–2025
- 2026: frontier YTD only
- No raw 476M Works rescan
- Inputs: topic_pair_year + topic_year + base_dimensions_year + topic hierarchy dimension
- Metrics remain separate: prevalence, joint score, Jaccard, overlap coefficient, lift, PMI/NPMI, growth, lift change, first substantial year, hierarchy distance
- Novelty means novelty within the 2000+ analytic window, not proof of first-ever historical occurrence
- No opaque composite score

Recovery history:
1. Run `36643308958` started with a monolithic Stage 14 DuckDB table.
2. It failed in `Run Stage 14` with a DuckDB spill/temp OOM: 18.6 GiB temp limit reached while building the full pair-metric layer.
3. Stages 0–13 and all frozen inputs remain intact; no prior stage was rerun.
4. Stage 14 was restructured into **16 deterministic hash shards by canonical topic pair**, ensuring every year of the same pair remains in one shard so first-seen / lag logic remains exact.
5. Prepare, shard, and finalizer scripts were persisted before rerun.

Active workflow:
- `OpenAlex Stage 14`
- Run ID: `36643813761` (Run #2)
- Trigger head: `04cd8e7d81f33427b04955bf1976ad74c40e457e`

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
If Run 36643813761 is queued/in_progress, do not start a duplicate run. If it fails, inspect the exact failing shard/step and patch only Stage 14.
