# OpenAlex Research 002 — Stage 12 Completion Checkpoint

Frozen snapshot: **2026-09-23**  
Repository: `lyh1703/openalex-research-runner`

## Authoritative execution

- Workflow: `OpenAlex Stage 10-12`
- Run ID: `36585853580`
- Run head: `65c55e46b315e7f2783a711ee4c440fb0953861f`
- Result: **SUCCESS**
- DuckDB `year` alias fix: persisted before successful run
- Stage 10 derived post-processing: **PASS**
- Stage 11 impact/FWCI post-processing: **PASS**
- Stage 12 citation graph: **COMPLETE**
- Stage 12 shards: **8/8 SUCCESS**
- Final merge/release: **SUCCESS**

## Stage 12 census

- Works files covered: 2,040 / 2,040
- Analytic Works (2000–2026, non-retracted): 372,984,105
- Reference edges from analytic Works: 2,816,251,659
- Citation indegree sum: 2,026,992,066
- Works with >=1 reference: 106,267,838
- Works cited >=1 time: 85,081,406
- Max indegree: 801,217
- Max outdegree: 8,575
- Deterministic bounded explicit edge sample: 672,317
- Distinct sampled citing→cited pairs: 672,317
- Duplicate sampled rows: 0
- Sampled self-references: 151

## Edge handling contract

- Raw `referenced_works` evidence is preserved semantically.
- Self-reference is retained in the QA sample; later topology analyses may exclude it only when the hypothesis requires it.
- Duplicate edges are not silently destroyed; the bounded sample also has a deduplicated representation with multiplicity.
- Unresolved/deleted referenced target IDs remain provenance edges; absence from the 2000–2026 analytic node universe is not treated as proof of nonexistence.
- Full multi-billion explicit edge materialization is intentionally not retained under the zero-cost architecture.
- Exact scalar/degree census is full-snapshot for the defined analytic universe; topology evidence is a deterministic bounded sample.

## Persistent release

https://github.com/lyh1703/openalex-research-runner/releases/tag/openalex-stage10-12-2026-09-23

## Recovery rule

Do not rerun Stages 0–12 unless a stored gate fails or a source snapshot refresh is explicitly requested.  
The first incomplete research artifact is **Stage 13 — Author / Institution / Country Network**.
