# OpenAlex Research 002 — Stage 15 Early-Signal Feature Engineering Contract

Frozen snapshot: **2026-09-23**

Stage 15 converts the validated growth, impact, convergence and collaboration layers from Stages 10–14 into reusable **feature matrices**. It does not decide which topic is “best”, does not create an opaque composite score, and does not define the future success label; those are Stage 16 backtesting responsibilities.

## Primary analytic universe

- Corpus: **Core**
- Tier: **A**
- Confirmed historical feature years: **2003–2025**
- 2026: **frontier YTD**, retained for current screening but excluded from Stage 16 confirmed historical labels unless explicitly handled as frontier data.
- Topic taxonomy and keyword vocabulary are the frozen 2026-09-23 OpenAlex snapshot taxonomy.

## Feature timing / anti-leakage rule

Stage 15 distinguishes **backtest-safe historical features** from **snapshot-only enrichment**.

For signal year `t`:

1. Growth / share / persistence features use only values at or before `t`.
2. Convergence features use Stage 14 pair history through `t`; no future pair-year values enter the feature.
3. Collaboration breadth features use year-`t` topic and network aggregates.
4. Mature impact features use publication cohort **`t-3`** only, and only when Stage 11 marks that cohort as a full four-year citation window and eligible for primary impact comparison.
   - Example: a 2025 signal may use the mature 2022 cohort.
   - This prevents same-year/current-snapshot citation accumulation from leaking future citation information into historical features.
5. Current-year impact is preserved only in a separate snapshot-enrichment output and is explicitly marked **not backtest-safe**.

This timing contract does not eliminate all retrospective-data caveats: OpenAlex may backfill records and the frozen 2026 taxonomy is applied retrospectively. Stage 17 will test robustness to these biases.

## Topic feature families

### A. Momentum / emergence
From Stage 10 primary-topic growth:
- primary-topic work count
- share of Core Tier-A universe
- raw YoY
- share YoY
- 3-year / 5-year count CAGR
- 3-year / 5-year share CAGR
- robust z-score of log share
- recent log-share slope
- share acceleration
- growth / decline persistence
- low-base flag
- burst / decline event state

### B. Mature impact quality
From Stage 11, publication cohort `t-3`:
- mature cohort Works
- FWCI coverage
- mean FWCI among scored Works
- normalized-percentile coverage
- mean normalized percentile
- top-1% and top-10% rates
- cited-by percentile coverage / mean
- citations per Work and uncited rate as descriptive values
- explicit impact availability / eligibility flag

No missing FWCI is converted to zero.

### C. Convergence / combinatorial novelty
From Stage 14:
- active topic partners
- topic-pair coassignment events
- novel partners
- emerging-convergence partners
- cross-field partners
- cross-domain partners
- median / maximum pair lift
- novel-partner share
- emerging-partner share
- cross-field / cross-domain partner shares
- coassignment-event intensity

### D. Collaboration breadth proxies
From Stage 9 topic-year:
- multi-label topic Works
- average institution incidences per topic Work
- average country incidences per topic Work

These are **per-Work incidence/breadth proxies**, not counts of globally unique institutions or countries.

### E. Global network context
From Stage 13:
- global average institutions per Work
- global average countries per Work
- active institutions
- active countries
- country-pair collaboration events per Work
- topic collaboration breadth relative to the same-year global baseline

### F. Cross-sectional transforms
Within each signal year, selected continuous features receive:
- percentile rank
- log1p transform where appropriate

These remain individual features. Stage 15 does not sum them into a score.

## Keyword feature family

A separate keyword-year matrix uses:
- Stage 10 keyword growth
- lagged mature Stage 11 keyword impact
- same timing and missingness rules

Keywords do not receive topic convergence or topic hierarchy fields because no validated topic↔keyword feature bridge is materialized in the current research layer.

## Stage 12 citation graph use

Stage 12 validates the global citation topology and degree census, but its compact retained topology does not provide an exact topic-keyed historical edge layer. Stage 15 therefore does **not** fabricate a direct topic citation-network feature from the Stage 12 bounded topology sample. Topic-level citation quality comes from the validated Stage 11 impact layer.

## Outputs

- `topic_early_signal_features.parquet` — backtest-safe feature matrix
- `topic_snapshot_enrichment.parquet` — current/same-cohort impact enrichment, explicitly not backtest-safe
- `keyword_early_signal_features.parquet`
- `year_context_features.parquet`
- `STAGE15_FEATURE_DICTIONARY.json`
- `STAGE15_VALIDATION.json`
- `STAGE15_FINAL_GATE.json`
- `OPENALEX_STAGE15_EARLY_SIGNAL_REPORT.md`

## Completion gate

Stage 15 is PASS only if:

- upstream Stage 10/11, 13 and 14 gates are PASS;
- topic feature keys `(year, topic_id)` are unique;
- keyword feature keys `(year, keyword_id)` are unique;
- 2026 is marked frontier YTD;
- mature impact joins always satisfy `impact_year = signal_year - 3`;
- no non-mature/current impact variable enters the backtest-safe feature matrix;
- missing impact remains missing and has an explicit availability flag;
- collaboration incidence ratios use positive denominators;
- Stage 14 convergence joins preserve topic-year uniqueness;
- cross-sectional percentiles lie in [0,1];
- no opaque composite early-signal score is emitted;
- input provenance, hashes and output hashes are persisted.

Stage 16 will define future outcomes and test which Stage 15 features actually predict subsequent growth, impact, diffusion or convergence.
