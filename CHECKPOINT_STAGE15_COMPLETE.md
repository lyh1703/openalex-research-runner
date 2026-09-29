# OpenAlex Research 002 — Stage 15 Completion Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative execution:
- Workflow: `OpenAlex Stage 15`
- Run ID: `36645666486`
- Run number: **2**
- Trigger head: `5923a0d3cd3c214476557c49db86e00a4e45082b`
- Result: **SUCCESS**
- Final QA: **PASS**

Recovery history:
1. Run `36645578127` failed immediately in feature construction because DuckDB 1.5.5 rejected an unqualified alias form `pub_year year`.
2. Aliases were changed to `pub_year AS year` / `publication_year AS year`.
3. Run `36645666486` completed all build, package, release and artifact steps successfully.
4. No Stage 0–14 artifact was recomputed.

Feature timing contract:
- Signal time: end of year `t`.
- Growth / convergence / collaboration features use information through `t`.
- Backtest-safe mature impact uses publication cohort **`t-3`** only, and only full 4-year / eligible Stage 11 impact rows.
- Same-year frozen-snapshot impact is isolated in `topic_snapshot_enrichment.parquet` and marked **not backtest-safe**.
- 2026 remains frontier YTD.
- No opaque composite early-signal score was emitted.

Exact Stage 15 results:
- Topic backtest-safe feature rows: **108,364**
- Keyword backtest-safe feature rows: **1,556,063**
- Topic rows with mature-impact features: **102,243**
- Keyword rows with mature-impact features: **1,184,238**
- Topic rows with convergence features: **107,948**
- Topic rows with collaboration-breadth features: **108,364**
- Year-context rows: **27**
- QA: **PASS**
- Composite score emitted: **false**

Key outputs / SHA256:
- `topic_early_signal_features.parquet`: 108,364 rows
  - SHA256 `97b941c5264531f2c0ee4cdd90f30613a5b74cf0c3a5232ee65cd37de3e923a3`
- `keyword_early_signal_features.parquet`: 1,556,063 rows
  - SHA256 `d3944687991b2bbd69cb166a0909e79d5491b1d894fc3500d07a4cfed8154c81`
- `topic_snapshot_enrichment.parquet`: 108,364 rows
  - SHA256 `24f48bbef07fe3e5cb28a61fa32ae186f6f87f51eec39918943224bf0a9dfa59`
- `year_context_features.parquet`: 27 rows
  - SHA256 `35679e3e8ce26b432b43a322742ac05c422cd70365c2135447e8d0c9e4cce8d5`
- `STAGE15_FEATURE_DICTIONARY.json`
  - SHA256 `8cef5f8adf31471d95dbb9fcb14b0ed245376700ae014c3429741a74355d6ded`
- `STAGE15_VALIDATION.json`
  - SHA256 `d1ae1108efaeb13672e770744bc75453f660a5ffaba545da1be036b7c6c09de9`

Persistent release:
https://github.com/lyh1703/openalex-research-runner/releases/tag/openalex-stage15-2026-09-23

Recovery rule:
Treat Stages 0–15 as COMPLETE unless a stored gate fails or a new OpenAlex snapshot refresh is explicitly requested.
The first incomplete research artifact is **Stage 16 — Historical Backtesting**.
