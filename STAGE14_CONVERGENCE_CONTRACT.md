# OpenAlex Research 002 — Stage 14 Convergence / Novel Combination Contract

Frozen snapshot: **2026-09-23**

Stage 14 detects research-topic convergence and historically unusual topic combinations from the already-computed Stage 9 topic and topic-pair aggregates. It does **not** rescan 476M Works.

## Primary universe

- Primary: `corpus=core`, `tier=A`
- Historical confirmation window: **2000–2025 complete years**
- 2026: **frontier YTD**, visible but never used as a confirmed novelty/convergence event
- Expansion corpus remains a sensitivity layer, not mixed into the primary signal

## Inputs

- `topic_pair_year.parquet`
  - publication_year
  - corpus
  - tier
  - topic_id_a
  - topic_id_b
  - coassigned_works
  - joint_score_sum
- `topic_year.parquet`
  - topic marginal Works counts and impact/coverage fields
- `base_dimensions_year.parquet`
  - analytic universe denominator by year/corpus/tier
- OpenAlex topic dimension from the frozen Stage 8 entity layer when available, for Domain / Field / Subfield enrichment.

## Pair metrics

For each pair-year:

- pair prevalence = coassigned_works / total Works
- mean joint assignment score = joint_score_sum / coassigned_works
- Jaccard = pair / (topic_A + topic_B - pair)
- overlap coefficient = pair / min(topic_A, topic_B)
- lift = pair * total_Works / (topic_A * topic_B)
- PMI = log2(lift)
- NPMI = PMI / -log2(pair prevalence), where defined
- raw YoY pair growth
- 3-year pair CAGR where the 3-year base is large enough
- 3-year change in log2 lift
- first observed year in the 2000+ analytic window
- first substantial year (coassigned_works >= 25)

These measures are kept separate; Stage 14 does not collapse them into one opaque composite score.

## Novel-combination definition

A **confirmed analytic novel combination** is a pair-year satisfying all of:

1. year <= 2025
2. first year with coassigned_works >= 25
3. both component topics have >= 100 Works in that year
4. pair lift >= 1.25
5. both topic marginals exist in the same corpus/tier/year

This means "novel within the 2000+ frozen analytic window", not proof that the combination never existed before 2000 or outside OpenAlex.

## Emerging-convergence definition

A pair-year is a candidate emerging convergence when:

1. year <= 2025
2. current coassigned_works >= 50
3. 3-year lag coassigned_works >= 25
4. 3-year CAGR >= 20%
5. current lift >= 1.5
6. log2 lift is higher than 3 years earlier
7. both component topics have >= 100 Works

The feature table preserves raw values so thresholds can be re-tested in Stage 17.

## Hierarchy / distance

When topic hierarchy is available:

- same_subfield
- cross_subfield_same_field
- cross_field_same_domain
- cross_domain

Cross-field and cross-domain novel combinations are surfaced separately because they are particularly useful for Stage 15 early-signal features and the Academic Research Landscape / Lab Graph applied layer.

## Outputs

At minimum:

- `pair_year_primary_signals.parquet`
- `novel_combination_candidates.parquet`
- `emerging_convergence_candidates.parquet`
- `topic_convergence_year.parquet`
- `field_pair_year.parquet` / `domain_pair_year.parquet` when hierarchy enrichment is available
- `STAGE14_VALIDATION.json`
- `STAGE14_FINAL_GATE.json`
- `OPENALEX_STAGE14_CONVERGENCE_REPORT.md`

## QA / completion gate

Stage 14 is PASS only if:

- no duplicate pair-year keys in the primary signal table
- no self-pairs
- canonical pair order is preserved
- coassigned_works <= both topic marginals
- all denominators are positive where metrics are reported
- 2026 is excluded from confirmed events
- novel candidates satisfy their frozen thresholds
- emerging-convergence candidates satisfy their frozen thresholds
- synthetic formula checks and sampled recalculation checks pass
- provenance and input hashes/sizes are persisted

Stage 14 does not require raw multi-hundred-million Work rescanning because the required multi-topic evidence was already materialized in Stage 9.
