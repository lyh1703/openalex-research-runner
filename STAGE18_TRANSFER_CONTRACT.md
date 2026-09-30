# OpenAlex Research 002 — Stage 18 TRANSFER_MATRIX Contract

Frozen snapshot: **2026-09-23**

Stage 18 converts the validated Research 002 outputs into an explicit transfer contract for downstream systems. It does **not** create new scientific claims, retrain a model, or collapse the evidence into a single score.

## Authoritative upstream state

Stage 18 requires PASS/COMPLETE gates for Stages 13–17 and preserves all Stage 17 limitations.

Primary downstream targets:

1. **Academic Research Landscape**
   - human-facing topic/field/keyword exploration
   - growth, normalized impact, convergence, collaboration breadth, historical robustness evidence
2. **Lab Graph**
   - Work ↔ Author ↔ Institution ↔ Topic navigation
   - collaboration clusters and probable-lab candidate generation
   - never represent an inferred cluster as a confirmed laboratory
3. **ATLAS**
   - research/technology scouting
   - emerging-topic and novel-combination candidate generation
4. **NOUS**
   - evidence cards, provenance, definitions, caveats, reusable research context
5. **ChemE Research Intelligence**
   - domain/subfield-filtered research landscape and signal exploration
   - no generic global association is automatically promoted to a ChemE-specific prediction
6. **Venture / Business Scouting**
   - research-intelligence context only
   - academic emergence is not equivalent to commercial success, investability, market size, or product-market fit

## Evidence classes

Each Stage 17 feature/target/horizon row is carried into the transfer matrix with transparent flags.

A feature association is marked `ROBUST_RETROSPECTIVE_ASSOCIATION` only when:

- above_random_null_abs_rho = true
- sign_stable_across_volume_thresholds = true
- sign_stable_early_vs_late = true

A stronger `TAIL_ALERT_EVIDENCE` flag additionally requires:

- high_tail_lift_above_random_null = true

These labels are **retrospective evidence classes**, not predictions, causal claims, feature rankings, model weights, or recommendations.

For mature-impact features, missingness stability is preserved as a separate condition. It is never silently assumed.

## Transfer modes

- `DIRECT_DESCRIPTIVE`
  - validated descriptive metrics/topology may be displayed or queried directly with provenance.
- `CONDITIONAL_SIGNAL`
  - a retrospective signal may be surfaced as decision-support evidence only when its explicit robustness conditions are visible.
- `CONTEXT_ONLY`
  - feature may provide context but should not drive an alert or automated decision.
- `KNOWLEDGE_CARD`
  - transferable into NOUS as structured evidence/provenance.
- `SCOUTING_ONLY`
  - may support research/venture scouting, but must not be interpreted as commercial-success prediction.
- `HOLD_FOR_DOMAIN_VALIDATION`
  - global evidence exists, but domain/subfield-specific validation is required before predictive use.

No mode authorizes autonomous high-stakes decisions.

## Target-specific policy

### Academic Research Landscape
- Topic and keyword descriptive layers: DIRECT_DESCRIPTIVE.
- Stage 16/17 associations: CONDITIONAL_SIGNAL only when robust-retrospective criteria pass.
- 2026: always FRONTIER_YTD.

### Lab Graph
- Stage 13 network layers: DIRECT_DESCRIPTIVE.
- Stage 14 convergence layers: DIRECT_DESCRIPTIVE.
- Stage 17 predictive associations may be attached only to topic-level convergence/breadth features and must retain the retrospective label.
- Probable-lab inference remains separate from Stage 17 feature robustness.

### ATLAS
- Momentum, convergence, mature-impact and collaboration-breadth evidence may enter technology scouting.
- Alerts require ROBUST_RETROSPECTIVE_ASSOCIATION; tail-based alerts additionally require TAIL_ALERT_EVIDENCE.
- ATLAS must preserve source snapshot, entity ID, signal year, target, horizon and Stage 17 limitations.

### NOUS
- Store research definitions, feature dictionaries, stage gates, transfer rules and caveats as knowledge evidence.
- Do not convert Stage 18 transfer classes into user preference or factual certainty.
- Preserve Research 002 provenance and snapshot date.

### ChemE Research Intelligence
- Descriptive OpenAlex topic/field/subfield evidence may be filtered to ChemE-relevant taxonomies.
- Global robustness evidence is CONTEXT_ONLY unless the same relation is validated inside the chosen ChemE domain/subfield slice.
- Therefore Stage 18 does not claim a generic OpenAlex signal is ChemE-specific.

### Venture / Business Scouting
- Academic growth/convergence may be SCOUTING_ONLY context.
- It must be combined later with market, company, patent, regulatory, customer and cost evidence.
- No investment, company-quality or commercial-success prediction is transferred from OpenAlex alone.

## Required artifacts

- `TRANSFER_MATRIX.parquet`
- `TRANSFER_TARGET_SUMMARY.json`
- `TRANSFER_ARTIFACT_REGISTRY.json`
- `APPLIED_INTERFACE_SPEC.md`
- `NOUS_KNOWLEDGE_CARD_OPENALEX_002.json`
- `STAGE18_VALIDATION.json`
- `STAGE18_FINAL_GATE.json`
- `OPENALEX_STAGE18_TRANSFER_REPORT.md`

## Completion gate

Stage 18 is PASS only if:

- Stage 13–17 upstream gates are COMPLETE/PASS;
- all six target systems exist in the transfer matrix/summary;
- all 229 Stage 17 feature/target/horizon evidence rows are preserved before target expansion;
- no robustness flag is converted into a numeric composite score;
- NOUS knowledge card contains snapshot/provenance/limitations;
- Lab Graph policy preserves PROBABLE_LAB vs CONFIRMED_LAB separation;
- ChemE transfer requires additional domain/subfield validation before predictive use;
- Venture transfer explicitly prohibits treating scholarly emergence as commercial-success prediction;
- Stage 17 point-in-time limitation is present in every predictive/scouting transfer policy;
- 2026 remains FRONTIER_YTD;
- no production model weights or autonomous decision rule is emitted.

Stage 19 will perform the final reproducibility audit, executive summary and PROGRAM COMPLETE closure.
