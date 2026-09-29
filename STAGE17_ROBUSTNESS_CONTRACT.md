# OpenAlex Research 002 — Stage 17 Bias / Negative Controls / Robustness Contract

Frozen snapshot: **2026-09-23**

Stage 17 attempts to falsify or weaken the Stage 16 historical associations before any transfer or operational use. A Stage 17 PASS means the robustness program executed correctly and its limitations are explicit; it does **not** mean every Stage 15 feature is robust.

## Inputs

- Stage 16 `topic_backtest_panel.parquet`
- Stage 16 `keyword_backtest_panel.parquet`
- Stage 16 `STAGE16_FINAL_GATE.json`

Stages 0–16 are not recomputed.

## Stress-test axes

### 1. Minimum-volume sensitivity

Recompute annual rank association and high/low-tail future-event lift at signal-Works thresholds:

- >=25
- >=50 (Stage 16 baseline)
- >=100
- >=250

The future event is always redefined within the filtered signal-year population.

### 2. Future-event threshold sensitivity

At the baseline >=50-Works universe, recompute future-event lift using top-tail cutoffs:

- top 20% (outcome percentile >=0.80)
- top 10% (>=0.90)
- top 5% (>=0.95)

Feature high/low tails use the same percentile threshold for symmetry.

### 3. Temporal stability

Compare baseline associations across:

- 2005–2014
- 2015 onward

This is a stability check, not a historical-vintage reconstruction.

### 4. Missingness / coverage selection

Compare the baseline universe with the subset where lagged mature-impact evidence is available at signal time.

Missing FWCI is never converted to zero.

### 5. Domain heterogeneity — topics only

Recompute topic feature/outcome relationships within each OpenAlex domain, using within-domain/year ranks and only sufficiently populated domain-year-feature cells.

This tests whether a global relationship is concentrated in only one broad scientific domain.

### 6. Deterministic random negative controls

For every target/horizon, generate 20 deterministic pseudo-random hash features from entity ID, signal year, and seed.

The same backtest procedure is applied to these synthetic features. The resulting null distribution is used to calibrate:

- absolute annual mean Spearman association
- high-tail event lift
- low-tail event lift

Real feature results are compared with the 95th-percentile null envelope. This is a procedure calibration / placebo test; it does not prove causality.

### 7. Field/domain and target heterogeneity

Preserve results separately by:

- future scholarly-share growth
- cross-domain convergence gain
- realized citation impact
- 3-year vs 5-year horizon
- topic vs keyword layer

No result is collapsed into one global score.

## Known bias that cannot be fully falsified here

A single 2026-09-23 OpenAlex snapshot cannot recreate the exact historical database vintage that would have been visible in 2008, 2012, etc. Therefore:

- retrospective record backfill,
- later metadata corrections,
- frozen-2026 topic taxonomy applied to historical Works,

cannot be fully eliminated.

Stage 17 must quantify temporal instability and document this limitation as **NOT TESTABLE FROM A SINGLE SNAPSHOT**. It must not claim strict point-in-time predictive validity.

## Robustness flags

Per feature / target / horizon, Stage 17 may emit transparent boolean flags such as:

- `sign_stable_early_vs_late`
- `sign_stable_across_volume_thresholds`
- `above_random_null_abs_rho`
- `high_tail_lift_above_random_null`

These are diagnostics only. They are not summed into a score, used to rank features, or converted into model weights.

## Outputs

- `volume_sensitivity.parquet`
- `event_threshold_sensitivity.parquet`
- `temporal_stability.parquet`
- `missingness_sensitivity.parquet`
- `topic_domain_heterogeneity.parquet`
- `negative_control_seed_results.parquet`
- `negative_control_envelope.parquet`
- `feature_robustness_flags.parquet`
- `STAGE17_LIMITATIONS.json`
- `STAGE17_VALIDATION.json`
- `STAGE17_FINAL_GATE.json`
- `OPENALEX_STAGE17_ROBUSTNESS_REPORT.md`

## Completion gate

Stage 17 is PASS only if:

- Stage 16 gate is PASS;
- every robustness axis above executes;
- no 2026 signal-year rows enter the robustness analysis;
- all future targets remain <=2025;
- impact missingness remains explicit;
- deterministic random controls contain all requested seeds;
- random-control event rates and correlations are finite or explicitly missing;
- domain tests retain domain labels and do not silently pool them;
- temporal split results exist on both sides of the split;
- volume and event-threshold sensitivity outputs contain all frozen thresholds;
- strict point-in-time limitations are explicitly recorded;
- no composite robustness score, feature ranking, or production model weight is emitted.

Stage 18 may use the Stage 17 evidence to determine where the research outputs are transferable, but must preserve these caveats.
