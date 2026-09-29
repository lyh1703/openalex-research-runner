# OpenAlex Research 002 — Stage 16 Historical Backtesting Contract

Frozen snapshot: **2026-09-23**

Stage 16 tests whether the Stage 15 early-signal features are associated with **subsequent outcomes**. It is a retrospective frozen-snapshot backtest, not a strict point-in-time production backtest, because OpenAlex can backfill records and the frozen 2026 taxonomy is applied retrospectively. Stage 17 is responsible for robustness and bias stress tests.

## Primary inputs

- `topic_early_signal_features.parquet`
- `keyword_early_signal_features.parquet`
- `STAGE15_FINAL_GATE.json`

No Stage 0–15 raw computation is rerun.

## Eligibility

Topic backtests:
- Core + Tier A Stage 15 matrix only
- signal year >= 2005
- current primary-topic Works >= 50
- 2026 is never a historical signal year

Keyword backtests:
- Core + Tier A Stage 15 matrix only
- signal year >= 2005
- current keyword Works >= 50
- 2026 is never a historical signal year

## Future outcomes

### A. Research-share growth

For horizon h in {3,5}:
- target year = t + h
- eligible only when target year <= 2025
- missing target rows are treated as zero future share/count, because the entity existed at signal year t but is absent from the target-year aggregate
- continuous target: `future_share_growth_h = future_share / current_share - 1`
- binary event: within each signal year, top decile of `future_share_growth_h`

The share target controls for growth of the overall scholarly corpus.

### B. Cross-domain convergence diffusion — topics only

For horizon h in {3,5}:
- target year = t + h <= 2025
- continuous target: `future_cross_domain_partner_delta_h = future_cross_domain_partners - current_cross_domain_partners`
- missing future convergence row is treated as zero partners
- binary event: within each signal year, top decile of the future cross-domain-partner delta

This is a diffusion/convergence outcome, not a citation outcome.

### C. Eventual citation impact of the signal-year cohort

For topics and keywords:
- only a 3-year realization horizon is used
- Stage 15 defines mature impact available at year t+3 from publication cohort t
- target is the `mature_mean_fwci` observed in the t+3 feature row
- validation requires `mature_impact_cohort_year = t`
- binary event: top decile of realized cohort FWCI within each signal year among eligible rows

Missing impact remains missing; it is never converted to zero.

## Backtest metrics

Each selected feature is evaluated separately. Stage 16 does **not** train an opaque model and does not select final weights.

For each feature / outcome / horizon / signal year:
- eligible N
- base event rate
- feature top-decile event rate and lift
- feature bottom-decile event rate and lift
- Spearman rank correlation with the continuous future outcome
- mean future outcome overall / top decile / bottom decile

Aggregate summaries report:
- years covered
- total observations
- mean / median annual Spearman rho
- positive-rho-year fraction
- pooled top-decile lift
- pooled bottom-decile lift

Both tails are retained to avoid assuming in advance that every feature should be monotonic in the same direction.

## Topic features tested

Momentum / size:
- share
- raw_yoy
- share_yoy
- count_cagr_3y
- share_cagr_3y
- count_cagr_5y
- share_cagr_5y
- share_log_robust_z
- recent_share_log_slope
- share_acceleration
- growth_persistence_years

Lagged mature impact:
- mature_mean_fwci
- mature_mean_normalized_percentile
- mature_top_10_rate
- mature_uncited_rate

Convergence:
- active_partners
- novel_partners
- emerging_partners
- cross_field_partners
- cross_domain_partners
- median_pair_lift
- novel_partner_share
- emerging_partner_share
- cross_domain_partner_share

Collaboration breadth:
- institution_breadth_relative_global
- country_breadth_relative_global

Cross-sectional transforms:
- share_percentile_year
- share_cagr_3y_percentile_year
- share_robust_z_percentile_year
- active_partners_percentile_year
- cross_domain_partners_percentile_year
- mature_fwci_percentile_year
- mature_top10_percentile_year
- institution_breadth_percentile_year
- country_breadth_percentile_year

## Keyword features tested

- share
- raw_yoy
- share_yoy
- count_cagr_3y
- share_cagr_3y
- count_cagr_5y
- share_cagr_5y
- share_log_robust_z
- recent_share_log_slope
- share_acceleration
- growth_persistence_years
- mature_mean_fwci
- mature_mean_normalized_percentile
- mature_top_10_rate
- mature_uncited_rate
- share_percentile_year
- share_cagr_3y_percentile_year
- mature_fwci_percentile_year

## Outputs

- `topic_backtest_panel.parquet`
- `keyword_backtest_panel.parquet`
- `topic_feature_backtest_year.parquet`
- `keyword_feature_backtest_year.parquet`
- `topic_feature_backtest_summary.parquet`
- `keyword_feature_backtest_summary.parquet`
- `STAGE16_TARGET_DIAGNOSTICS.json`
- `STAGE16_VALIDATION.json`
- `STAGE16_FINAL_GATE.json`
- `OPENALEX_STAGE16_BACKTEST_REPORT.md`

## Completion gate

Stage 16 is PASS only if:
- Stage 15 gate is PASS;
- no 2026 signal-year rows enter historical backtests;
- 3-year growth/convergence targets never exceed 2025;
- 5-year growth/convergence targets never exceed 2025;
- realized-impact labels satisfy `mature_impact_cohort_year = signal_year`;
- missing FWCI remains missing;
- topic and keyword panel keys are unique;
- binary event labels are derived only from future outcomes within signal year;
- feature backtest summary keys are unique;
- all Spearman coefficients and event rates/lifts are finite or explicitly missing;
- at least 10 historical signal years exist for 3-year growth;
- Stage 16 emits no final production score or model weights.

Stage 17 will stress-test threshold sensitivity, backfill/taxonomy bias, missingness, volume filters, field/domain heterogeneity, and negative controls.
