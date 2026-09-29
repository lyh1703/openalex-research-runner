# OpenAlex Research 002 — Stage 13 Network Contract

Frozen snapshot: **2026-09-23**

Stage 13 builds reusable Author / Institution / Country network layers without forcing a permanent multi-billion-edge graph.

## Inputs and canonical keys

- Work: `work.id`
- Author: `work.authorships[].author.id`
- Institution: `work.institutions[].id` for work-level collaboration incidence
- Historical author→institution evidence: `work.authorships[].institutions[].id`
- Country: `work.authorships[].countries[]`
- Corpus: Core vs Expansion remains separated
- Main analytic time span: 2000–2026 non-retracted; 2026 is frontier YTD

Special authors `A9999999999` and `A5317838346` remain provenance values but are excluded from person-level network metrics.

## Materialization policy

### Full frozen-snapshot census
1. Institution activity by year/corpus/tier.
2. Country activity by year/corpus/tier.
3. Country↔country collaboration network.
4. Hyperauthorship / hyperinstitution / multicountry workload audit.

### Deterministic bounded topology
1. Institution↔institution collaboration: deterministic work sample, because global pair expansion can become very large.
2. Author↔author coauthorship: deterministic work sample with an authors-per-work guard to prevent quadratic explosion on consortium papers.
3. Author↔institution affiliation: same reproducible author sample.

The bounded layers are topology evidence, not exact global pair counts. Their sampling denominator and excluded hyperedge mass are persisted.

## Sampling / guardrails

- Institution-pair sample: `hash(work.id) % 64 == 0`; works with 2–32 distinct institutions.
- Author-pair sample: `hash(work.id) % 512 == 0`; works with 2–50 authors.
- Author-institution sample: same 1/512 work sample.
- Country pairs are full-census, but works with extreme country cardinality are audited separately and can be guarded if a runtime explosion is observed.
- No destructive author deduplication.
- Self-pairs are never generated.
- Pair endpoints are canonically ordered to avoid directional duplicates.

## Intended downstream use

- Academic Research Landscape / Lab Graph
- institution collaboration discovery
- country diffusion / internationalization
- probable-lab candidate generation from bounded coauthor + affiliation topology
- Stage 14 convergence and Stage 15 early-signal features

## Completion gate

Stage 13 is complete only when:
- all 2,040 Works Parquet files are covered exactly once,
- shard manifests cover the frozen Works manifest without gaps or duplicates,
- full-census node/country outputs and bounded topology outputs are merged,
- sample denominators and hyperedge exclusions are explicit,
- QA validates no duplicate canonical pair keys within final aggregates,
- a persistent release and checkpoint are published.
