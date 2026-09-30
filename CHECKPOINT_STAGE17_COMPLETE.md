# OpenAlex Research 002 — Stage 17 Completion Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative execution:
- Workflow: `OpenAlex Stage 17`
- Run ID: `36647806257`
- Run number: **2**
- Trigger head: `09fbab1757dda95085c0cc5e682bf05d03e5a682`
- Result: **SUCCESS**
- Final QA: **PASS**

Recovery history:
1. Run `36647646909` failed only because DuckDB rejected the alias `count(*) years`.
2. The alias was minimally changed to `count(*) AS n_years`.
3. Run `36647806257` completed the full robustness program, packaging, release, and artifact publication successfully.
4. No Stage 0–16 computation was rerun.

Robustness axes executed:
- signal-volume sensitivity: 25 / 50 / 100 / 250 Works
- future-event threshold sensitivity: top 20% / 10% / 5%
- temporal stability: 2005–2014 vs 2015+
- mature-impact missingness subset
- within-domain topic heterogeneity
- deterministic random-feature negative controls
- **20 seeds per target/horizon**

Exact Stage 17 outputs:
- volume sensitivity: **916 rows**
- event-threshold sensitivity: **687 rows**
- temporal stability: **458 rows**
- missingness sensitivity: **458 rows**
- topic-domain heterogeneity: **700 rows**
- random negative-control seed results: **160 rows**
- random-control envelopes: **8 rows**
- per feature/target robustness flags: **229 rows**
- QA: **PASS**
- composite robustness score emitted: **false**
- production model weights emitted: **false**

Critical limitation:
- Strict point-in-time predictive validity was **NOT established**.
- Status: **NOT TESTABLE FROM A SINGLE SNAPSHOT**.
- A single frozen 2026-09-23 OpenAlex snapshot cannot fully remove historical record backfill, later metadata corrections, or retrospective application of the frozen 2026 topic taxonomy.
- Stage 17 supports retrospective robustness screening and procedure calibration, not causal proof or strict historical real-time validity.

Key outputs / SHA256:
- `volume_sensitivity.parquet`
  - SHA256 `33faf80925f0f629bb6c28a582dcaa28135b81b8e35f904966e417a52529e9ee`
- `event_threshold_sensitivity.parquet`
  - SHA256 `663a9fff5955a492f2ee180e7122e73f9a7c013e570174d09f8bb01c5400785d`
- `temporal_stability.parquet`
  - SHA256 `dfbb77cdb52e72bae78fba43af3d802696b76d94502389453ca77dc3fa3a606f`
- `missingness_sensitivity.parquet`
  - SHA256 `7d3b71813c7d791be6980c85c573ebc346f511769ea5204f477ee49654afb1dc`
- `topic_domain_heterogeneity.parquet`
  - SHA256 `d8b01aafde3e719c2f74f2718b1aa746e56ba260f7bb15b2c4d25d38976f3e1c`
- `negative_control_seed_results.parquet`
  - SHA256 `2185c7e840dc2992ba1f1591df0095c14b3c5c18a2133f4fe2fbbc1b08f7210c`
- `negative_control_envelope.parquet`
  - SHA256 `845be2cadb43dac703c91fb8de4170e03b0ea85fa808c6c40c9f1ec9f515d7b6`
- `feature_robustness_flags.parquet`
  - SHA256 `a94417c5888c643a85c0b370c945ae25d2eed60038938592184337cfb091ac2d`
- `STAGE17_LIMITATIONS.json`
  - SHA256 `b467c8176a09e442feaf360067d1b7a68705d7d42906ac20237fcc2d7dacd6ee`
- `STAGE17_VALIDATION.json`
  - SHA256 `e18392677d83bbccb22fc01417ead823b81e51a17c165c9333042fab3eb3547a`

Persistent release:
https://github.com/lyh1703/openalex-research-runner/releases/tag/openalex-stage17-2026-09-23

Recovery rule:
Treat Stages 0–17 as COMPLETE unless a stored gate fails or a new OpenAlex snapshot refresh is explicitly requested.
The first incomplete research artifact is **Stage 18 — TRANSFER_MATRIX**.
