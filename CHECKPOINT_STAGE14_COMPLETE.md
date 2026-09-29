# OpenAlex Research 002 — Stage 14 Completion Checkpoint

Frozen snapshot: **2026-09-23**

Authoritative execution:
- Workflow: `OpenAlex Stage 14`
- Run ID: `36644022382`
- Run number: **3**
- Trigger head: `154b1a8a0f0c3f9666e0995af55b10bb2a91127f`
- Result: **SUCCESS**
- Prepare: PASS
- Hash shards: **16 / 16 SUCCESS**
- Finalizer: SUCCESS
- Final QA: **PASS**

Recovery history:
1. Run `36643308958`: monolithic DuckDB implementation hit an 18.6 GiB spill/temp limit.
2. Stage 14 was re-architected as 16 deterministic hash shards by canonical topic pair so all years of a pair stay together.
3. Run `36643813761`: prepare passed, but all shards exposed one expansion-sensitivity SQL binder ambiguity on `publication_year`.
4. The join was minimally qualified; Run `36644022382` then completed successfully.

Primary universe:
- Core + Tier A
- 2000–2025 eligible for confirmed historical signals
- 2026 frontier YTD only
- Novelty means first substantial emergence **within the 2000+ analytic window**, not a claim of first-ever historical occurrence.

Exact Stage 14 results:
- Primary pair-year rows evaluated: **17,341,609**
- Distinct topic pairs: **2,050,727**
- Persisted signal rows, coassigned >= 10: **3,783,903**
- Analytic novel-combination candidates: **147,898**
- Emerging-convergence candidates: **71,693**
- Cross-domain novel candidates: **43,841**
- Hierarchy-missing pair-year rows: **0**

Key output rows / SHA256:
- `pair_year_primary_signals.parquet`: 3,783,903 rows
  - SHA256 `b214a23ca5d1cdb67d94eeaf96b23dae70cfa35d82f7ad43beab826872b1a26d`
- `novel_combination_candidates.parquet`: 147,898 rows
  - SHA256 `c49e0bcf5af6b639f547598be688de94a2d02a2d36862032b58378c7bca78301`
- `emerging_convergence_candidates.parquet`: 71,693 rows
  - SHA256 `46addd562bb509b93dce9990fdaf3f8dbb35ab3249a93718078598c1a5a8ae59`
- `cross_domain_novel_candidates.parquet`: 43,841 rows
  - SHA256 `2be1d45eb752c9eb2fa7beee8784e1b78b1d2eb219a96018e99507b290c406f7`
- `topic_convergence_year.parquet`: 121,193 rows
  - SHA256 `ca50c428e7cb73bcb8e70f4216670214a895bb4099627cf1ed0d8dadad49c2c1`
- `field_pair_coassignment_year.parquet`: 8,775 rows
  - SHA256 `a35e562cd62fad99d7548b51a14663de34a401cdd8ed4411875ba28da031213e`
- `domain_pair_coassignment_year.parquet`: 162 rows
  - SHA256 `0afe4912a38db87a80381c9208b80e2096e2abe934044b86fa754ea53504c119`
- `STAGE14_VALIDATION.json`
  - SHA256 `782f4e45cc7e4df206b1eecb271c7973d95a713e7a17f7bc1f6993d07cbfed98`
- `STAGE14_THRESHOLD_SENSITIVITY.json`
  - SHA256 `a102cbd480c57ae72310a4441962372fb5f3d7dafc4189742ce80330be6e6804`

Persistent release:
https://github.com/lyh1703/openalex-research-runner/releases/tag/openalex-stage14-2026-09-23

Recovery rule:
Treat Stages 0–14 as COMPLETE unless a stored gate fails or a new OpenAlex snapshot refresh is explicitly requested.
The first incomplete research artifact is **Stage 15 — Early-Signal Feature Engineering**.
