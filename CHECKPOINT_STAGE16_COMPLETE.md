# OpenAlex Research 002 — Stage 16 Completion Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative execution:
- Workflow: `OpenAlex Stage 16`
- Run ID: `36646673466`
- Run number: **3**
- Trigger head: `82da0a3eb4b69b068a683f8bef0c5d3b5b247a46`
- Result: **SUCCESS**
- Final QA: **PASS**

Recovery history:
1. Run `36646512299` failed because the binary future-outcome label name collided with Stage 15's string `growth_event` state.
2. Outcome labels were renamed to explicit `*_breakout_event` fields.
3. Run `36646590937` completed the full backtest computation but failed only in the Markdown diagnostic query because `rows` was used as an alias.
4. That report alias was minimally fixed; no upstream or completed calculation stage was rerun.
5. Run `36646673466` completed build, packaging, release, and artifact publication successfully.

Backtest design:
- retrospective frozen-snapshot historical backtest
- signal years begin in 2005
- signal entities require >=50 Works
- 3-year and 5-year future scholarly-share growth
- 3-year and 5-year cross-domain convergence gain for topics
- 3-year realized citation impact of the signal-year publication cohort
- annual Spearman rank association
- high-decile and low-decile future-event lift
- no production score or model weights selected

Exact Stage 16 results:
- Topic backtest panel: **149,990 rows**
- Keyword backtest panel: **1,400,977 rows**
- Topic 3-year growth historical signal years: **18**
- Keyword 3-year growth historical signal years: **18**
- Topic realized-cohort-impact eligible rows: **79,411**
- Keyword realized-cohort-impact eligible rows: **743,154**
- Topic feature/year backtest rows: **3,010**
- Keyword feature/year backtest rows: **936**
- Topic feature summary rows: **525**
- Keyword feature summary rows: **162**
- QA: **PASS**
- Production model weights emitted: **false**

Key outputs / SHA256:
- `topic_backtest_panel.parquet`: 149,990 rows
  - SHA256 `c9ebc710b339d7df379e9778a6735edacdbda32e2afd0eeb40e8a8c3782c52d5`
- `keyword_backtest_panel.parquet`: 1,400,977 rows
  - SHA256 `19982284b862ccd62470ee567c0ed0b5597dfa54a2a5a37d416311ce4aa89f3a`
- `topic_feature_backtest_year.parquet`: 3,010 rows
  - SHA256 `e5d437877e4ee0a3bb9f88bce92842d9821bc279ce9dcda595a79d99128b17f0`
- `keyword_feature_backtest_year.parquet`: 936 rows
  - SHA256 `195b6dee747274a344362b70cc7e05f882527425eca2e1c4f2a99d43d2a8bbe1`
- `topic_feature_backtest_summary.parquet`: 525 rows
  - SHA256 `a6a6b97d00be58467c419e7f14b438385622d9d05c903cdef7aaa07d9ad1f88f`
- `keyword_feature_backtest_summary.parquet`: 162 rows
  - SHA256 `e025ecf40ec06d8f830a24b74debfef2d91c7b7f601e213c21b15cb4929f1bd5`
- `STAGE16_TARGET_DIAGNOSTICS.json`
  - SHA256 `c024e24742784a7f1d3dc0321d401a3fdb2ae6e96d94f7c5fe99dee2e7becaa7`
- `STAGE16_VALIDATION.json`
  - SHA256 `122c9a3056bc02454bf6d45cb7000e078f2bf1d6700d9ef5fdbed17a272a5701`

Persistent release:
https://github.com/lyh1703/openalex-research-runner/releases/tag/openalex-stage16-2026-09-23

Interpretation boundary:
This is not a strict point-in-time OpenAlex-vintage simulation. The 2026 frozen taxonomy and possible OpenAlex record backfill can create retrospective-data bias. These, plus missingness, volume filters, threshold sensitivity, field/domain heterogeneity, and negative controls, are Stage 17 responsibilities.

Recovery rule:
Treat Stages 0–16 as COMPLETE unless a stored gate fails or a new snapshot refresh is explicitly requested.
The first incomplete research artifact is **Stage 17 — Bias / Negative Controls / Robustness**.
