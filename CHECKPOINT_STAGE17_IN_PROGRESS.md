# OpenAlex Research 002 — Stage 17 In-Progress Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative baseline:
- Stages 0–16: COMPLETE
- Stage 16 release: `openalex-stage16-2026-09-23`
- Stage 17 contract: `STAGE17_ROBUSTNESS_CONTRACT.md`

Stage 17 program:
- volume sensitivity: >=25 / 50 / 100 / 250 signal Works
- future-event threshold sensitivity: top 20% / 10% / 5%
- temporal stability: 2005–2014 vs 2015+
- mature-impact missingness subset
- within-domain topic heterogeneity
- 20 deterministic random-feature negative controls per target/horizon
- explicit single-snapshot / historical-vintage limitation
- no composite robustness score or production model weights

Recovery history:
1. Run #1 `36647646909` failed only because DuckDB rejected the alias `count(*) years` in the event-threshold summary query.
2. The alias was minimally changed to `count(*) AS n_years`.
3. No Stage 0–16 computation was rerun.

Active execution:
- Workflow: `OpenAlex Stage 17`
- Run ID: `36647806257`
- Run number: **2**
- Trigger head: `09fbab1757dda95085c0cc5e682bf05d03e5a682`

Recovery rule:
If Run 36647806257 is queued/in_progress, do not start a duplicate run.
If it fails, inspect the exact Stage 17 failing step and patch only Stage 17.
