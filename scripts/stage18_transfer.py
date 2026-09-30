#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from collections import Counter, defaultdict
from pathlib import Path
import duckdb

RELEASE="2026-09-23"
TARGETS=[
    "Academic Research Landscape",
    "Lab Graph",
    "ATLAS",
    "NOUS",
    "ChemE Research Intelligence",
    "Venture / Business Scouting",
]

MOMENTUM={
 "share","raw_yoy","share_yoy","count_cagr_3y","share_cagr_3y","count_cagr_5y","share_cagr_5y",
 "share_log_robust_z","recent_share_log_slope","share_acceleration","growth_persistence_years",
 "share_percentile_year","share_cagr_3y_percentile_year","share_robust_z_percentile_year",
}
IMPACT={
 "mature_mean_fwci","mature_mean_normalized_percentile","mature_top_10_rate","mature_uncited_rate",
 "mature_fwci_percentile_year","mature_top10_percentile_year",
}
CONVERGENCE={
 "active_partners","novel_partners","emerging_partners","cross_field_partners","cross_domain_partners",
 "median_pair_lift","novel_partner_share","emerging_partner_share","cross_domain_partner_share",
 "active_partners_percentile_year","cross_domain_partners_percentile_year",
}
BREADTH={
 "institution_breadth_relative_global","country_breadth_relative_global",
 "institution_breadth_percentile_year","country_breadth_percentile_year",
}

def sha256_file(p:Path):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def write_json(p,obj):
    Path(p).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

def family(feature):
    if feature in IMPACT or feature.startswith("mature_"): return "mature_impact"
    if feature in CONVERGENCE or "partner" in feature or "pair_lift" in feature: return "convergence"
    if feature in BREADTH or "breadth" in feature: return "collaboration_breadth"
    if feature in MOMENTUM: return "momentum"
    return "other"

def rowdicts(con,path):
    cur=con.execute(f"SELECT * FROM read_parquet('{str(path).replace(chr(39),chr(39)*2)}') ORDER BY entity_type,target,horizon,feature")
    cols=[d[0] for d in cur.description]
    return [dict(zip(cols,r)) for r in cur.fetchall()]

def truth(v): return bool(v) if v is not None else False

def robust_core(r):
    return truth(r.get("above_random_null_abs_rho")) and truth(r.get("sign_stable_across_volume_thresholds")) and truth(r.get("sign_stable_early_vs_late"))

def tail_evidence(r):
    return robust_core(r) and truth(r.get("high_tail_lift_above_random_null"))

def applicable(target,r,fam):
    et=r["entity_type"]
    if target=="Academic Research Landscape": return True
    if target=="ATLAS": return True
    if target=="NOUS": return True
    if target=="Venture / Business Scouting": return True
    if target=="ChemE Research Intelligence": return et=="topic"
    if target=="Lab Graph": return et=="topic" and fam in {"convergence","collaboration_breadth"}
    return False

def transfer_policy(target,r,fam):
    rc=robust_core(r)
    impact_missing_ok = fam!="mature_impact" or truth(r.get("missingness_subset_sign_stable"))
    signal_ok=rc and impact_missing_ok

    if target=="NOUS":
        return "KNOWLEDGE_CARD",False,False,True,"Store evidence, provenance, definitions and caveats; do not convert to certainty."
    if target=="Venture / Business Scouting":
        return "SCOUTING_ONLY",False,False,True,"Academic emergence is scouting context only; commercial success requires external market/company/patent/customer evidence."
    if target=="ChemE Research Intelligence":
        return "HOLD_FOR_DOMAIN_VALIDATION",False,False,True,"Global evidence may be displayed, but predictive use requires ChemE domain/subfield-specific validation."
    if target=="Lab Graph":
        if signal_ok:
            return "CONDITIONAL_SIGNAL",True,tail_evidence(r),True,"Attach only to topic-level convergence/breadth context; probable-lab inference remains separately validated."
        return "CONTEXT_ONLY",False,False,True,"Use as descriptive topic context; do not drive lab inference."
    if target in {"Academic Research Landscape","ATLAS"}:
        if signal_ok:
            return "CONDITIONAL_SIGNAL",True,tail_evidence(r),True,"Retrospective decision-support evidence only; retain Stage 17 single-snapshot limitation."
        return "CONTEXT_ONLY",False,False,True,"Display as context/descriptive evidence; insufficient robustness for signal alerting."
    raise ValueError(target)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--gates",required=True)
    ap.add_argument("--stage17",required=True)
    ap.add_argument("--output",required=True)
    args=ap.parse_args()
    gates=Path(args.gates); s17=Path(args.stage17); out=Path(args.output); out.mkdir(parents=True,exist_ok=True)

    gate_docs={}
    for st in [13,14,15,16,17]:
        p=gates/f"STAGE{st}_FINAL_GATE.json"
        if not p.exists(): raise FileNotFoundError(p)
        gate_docs[st]=json.loads(p.read_text())
    upstream={f"stage{st}_complete":bool(gate_docs[st].get("complete") and gate_docs[st].get("qa_pass",True)) for st in [13,14,15,16,17]}
    if not all(upstream.values()): raise RuntimeError(f"Upstream gate failure: {upstream}")

    flags_path=s17/"feature_robustness_flags.parquet"
    limitations_path=s17/"STAGE17_LIMITATIONS.json"
    if not flags_path.exists() or not limitations_path.exists(): raise FileNotFoundError("Stage 17 transfer inputs missing")
    limitations=json.loads(limitations_path.read_text())

    con=duckdb.connect()
    flags=rowdicts(con,flags_path)
    if len(flags)!=229: raise RuntimeError(f"Expected 229 Stage17 evidence rows, got {len(flags)}")

    matrix=[]
    for r in flags:
        fam=family(r["feature"])
        evidence_class="ROBUST_RETROSPECTIVE_ASSOCIATION" if robust_core(r) else "RETROSPECTIVE_CONTEXT"
        if fam=="mature_impact" and robust_core(r) and not truth(r.get("missingness_subset_sign_stable")):
            evidence_class="ROBUST_RETROSPECTIVE_ASSOCIATION_WITH_MISSINGNESS_CAVEAT"
        for target in TARGETS:
            if not applicable(target,r,fam): continue
            mode,signal_allowed,tail_allowed,descriptive_allowed,note=transfer_policy(target,r,fam)
            matrix.append({
              "target_system":target,
              "entity_type":r["entity_type"],
              "research_target":r["target"],
              "horizon_years":int(r["horizon"]),
              "feature":r["feature"],
              "feature_family":fam,
              "evidence_class":evidence_class,
              "transfer_mode":mode,
              "descriptive_use_allowed":descriptive_allowed,
              "retrospective_signal_use_allowed":signal_allowed,
              "tail_alert_evidence":tail_allowed,
              "above_random_null_abs_rho":truth(r.get("above_random_null_abs_rho")),
              "sign_stable_volume":truth(r.get("sign_stable_across_volume_thresholds")),
              "sign_stable_time":truth(r.get("sign_stable_early_vs_late")),
              "missingness_subset_sign_stable":truth(r.get("missingness_subset_sign_stable")),
              "domain_same_sign_fraction":r.get("domain_same_sign_fraction"),
              "baseline_mean_annual_spearman_rho":r.get("baseline_mean_annual_spearman_rho"),
              "baseline_high_tail_lift":r.get("baseline_high_tail_lift"),
              "strict_point_in_time_validity":False,
              "snapshot_date":RELEASE,
              "policy_note":note,
            })

    # Persist matrix through JSON -> DuckDB to avoid pandas dependency.
    jsonl=out/"TRANSFER_MATRIX.jsonl"
    with jsonl.open("w",encoding="utf-8") as f:
        for r in matrix: f.write(json.dumps(r,ensure_ascii=False,default=str)+"\n")
    con.execute(f"COPY (SELECT * FROM read_json_auto('{str(jsonl).replace(chr(39),chr(39)*2)}')) TO '{str(out/'TRANSFER_MATRIX.parquet').replace(chr(39),chr(39)*2)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
    jsonl.unlink()

    summary={}
    for target in TARGETS:
        rows=[r for r in matrix if r["target_system"]==target]
        modes=Counter(r["transfer_mode"] for r in rows)
        fams=Counter(r["feature_family"] for r in rows)
        summary[target]={
          "rows":len(rows),
          "transfer_modes":dict(sorted(modes.items())),
          "feature_families":dict(sorted(fams.items())),
          "retrospective_signal_allowed_rows":sum(r["retrospective_signal_use_allowed"] for r in rows),
          "tail_alert_evidence_rows":sum(r["tail_alert_evidence"] for r in rows),
          "strict_point_in_time_validity":False,
        }
    write_json(out/"TRANSFER_TARGET_SUMMARY.json",summary)

    registry={
      "snapshot_date":RELEASE,
      "repository":"lyh1703/openalex-research-runner",
      "sources":[
        {"stage":13,"release":"openalex-stage13-2026-09-23","role":"author/institution/country network","direct_consumers":["Lab Graph","Academic Research Landscape"]},
        {"stage":14,"release":"openalex-stage14-2026-09-23","role":"topic convergence / novel combinations","direct_consumers":["Academic Research Landscape","Lab Graph","ATLAS","ChemE Research Intelligence","Venture / Business Scouting"]},
        {"stage":15,"release":"openalex-stage15-2026-09-23","role":"topic/keyword early-signal feature matrices","direct_consumers":["Academic Research Landscape","ATLAS","ChemE Research Intelligence"]},
        {"stage":16,"release":"openalex-stage16-2026-09-23","role":"retrospective historical backtest","direct_consumers":["Academic Research Landscape","ATLAS","NOUS"]},
        {"stage":17,"release":"openalex-stage17-2026-09-23","role":"robustness / negative controls / limitations","direct_consumers":TARGETS},
      ],
      "direct_artifact_interfaces":{
        "Academic Research Landscape":["topic_early_signal_features.parquet","keyword_early_signal_features.parquet","feature_robustness_flags.parquet","novel_combination_candidates.parquet"],
        "Lab Graph":["institution_activity_year.parquet","country_pair_year.parquet","institution_pair_sample_year.parquet","author_pair_sample_year.parquet","author_institution_sample_year.parquet","topic_convergence_year.parquet"],
        "ATLAS":["novel_combination_candidates.parquet","emerging_convergence_candidates.parquet","topic_early_signal_features.parquet","feature_robustness_flags.parquet"],
        "NOUS":["STAGE15_FEATURE_DICTIONARY.json","STAGE16_TARGET_DIAGNOSTICS.json","STAGE17_LIMITATIONS.json","TRANSFER_MATRIX.parquet"],
        "ChemE Research Intelligence":["topic_early_signal_features.parquet","pair_year_primary_signals.parquet","feature_robustness_flags.parquet"],
        "Venture / Business Scouting":["novel_combination_candidates.parquet","emerging_convergence_candidates.parquet","feature_robustness_flags.parquet"],
      },
      "global_caveat":"Strict historical point-in-time predictive validity is NOT TESTABLE FROM A SINGLE SNAPSHOT."
    }
    write_json(out/"TRANSFER_ARTIFACT_REGISTRY.json",registry)

    card={
      "card_id":"OPENALEX_RESEARCH_002_2026-09-23",
      "title":"OpenAlex Global Scholarly Graph — Research 002 Evidence Card",
      "snapshot_date":RELEASE,
      "status":"Stages 0-18 complete after Stage 18 gate",
      "knowledge_type":"research_evidence",
      "claims":[
        "OpenAlex Research 002 provides normalized scholarly growth, impact, citation-graph, collaboration-network, convergence and early-signal evidence over the frozen 2026-09-23 snapshot.",
        "Stage 16 historical testing is retrospective frozen-snapshot backtesting, not a strict historical database-vintage simulation.",
        "Stage 17 robustness tests include volume, event-threshold, temporal, missingness, domain heterogeneity and deterministic random negative controls.",
        "No composite predictive score or production model weights have been validated or emitted.",
      ],
      "limitations":limitations.get("unresolved_biases",[]),
      "strict_point_in_time_status":limitations.get("status"),
      "transfer_targets":TARGETS,
      "required_labels":["OBSERVED","RETROSPECTIVE_ASSOCIATION","FRONTIER_YTD","PROBABLE_LAB","CONFIRMED_LAB"],
      "provenance":{
        "repository":"lyh1703/openalex-research-runner",
        "stage17_release":"openalex-stage17-2026-09-23",
        "stage18_contract":"STAGE18_TRANSFER_CONTRACT.md",
      }
    }
    write_json(out/"NOUS_KNOWLEDGE_CARD_OPENALEX_002.json",card)

    interface="""# Stage 18 — Applied Interface Specification

## Common evidence envelope
Every transferred record should carry, where applicable:

- OpenAlex entity ID
- snapshot_date = 2026-09-23
- signal year / target / horizon
- metric or feature name
- evidence class
- Stage 17 robustness flags
- source release / artifact
- frontier_ytd flag for 2026
- point-in-time limitation

Never collapse these fields into an unexplained score.

## Academic Research Landscape
Use Stage 15 topic/keyword feature matrices as the browse layer, Stage 14 convergence candidates as the novelty layer, and Stage 17 flags as an optional retrospective-evidence overlay. Signal badges must say **retrospective association**, not forecast or certainty.

## Lab Graph
Use Stage 13 network artifacts for Work↔Author↔Institution relationships and Stage 14 topic convergence for topic context. Lab membership is not a canonical OpenAlex entity. Keep:
- CONFIRMED_LAB — only with external first-party evidence
- PROBABLE_LAB — inferred cluster with validation/confidence evidence
- COLLABORATION_CLUSTER — collaboration topology without lab claim
- UNKNOWN

Stage 17 robustness does not by itself confirm a laboratory.

## ATLAS
Ingest Stage 14 novelty/convergence candidates and Stage 15 feature rows. A research signal may become an ATLAS alert only when the transfer row says retrospective_signal_use_allowed=true. Tail alerts additionally require tail_alert_evidence=true. Preserve entity, year, target, horizon and provenance so ATLAS can re-evaluate rather than memorize a score.

## NOUS
Store the supplied knowledge card plus definitions, limitations and artifact registry. Treat it as evidence context. Do not convert retrospective association into factual certainty or user preference.

## ChemE Research Intelligence
Apply explicit OpenAlex Domain/Field/Subfield filters at query time. Global Stage 17 evidence is descriptive/contextual only until the same relationship is revalidated in the selected ChemE slice. Stage 18 therefore emits HOLD_FOR_DOMAIN_VALIDATION rather than a ChemE-specific predictive claim.

## Venture / Business Scouting
Research emergence may trigger a research question, not a business conclusion. Any downstream commercial assessment must add external evidence for markets, companies, patents, regulation, customers, pricing/costs and commercialization timelines. Do not treat OpenAlex growth as investment or product-market-fit prediction.

## 2026 handling
All 2026 observations remain FRONTIER_YTD and must not be displayed as a completed-year historical confirmation.
"""
    (out/"APPLIED_INTERFACE_SPEC.md").write_text(interface,encoding="utf-8")

    matrixp=out/"TRANSFER_MATRIX.parquet"
    qa={
      "upstream":upstream,
      "base_stage17_evidence_rows":len(flags),
      "expected_base_stage17_evidence_rows":229,
      "matrix_rows":len(matrix),
      "targets_present":sorted({r["target_system"] for r in matrix}),
      "all_targets_present":sorted({r["target_system"] for r in matrix})==sorted(TARGETS),
      "duplicate_matrix_keys":con.execute(f"SELECT count(*)-count(DISTINCT (target_system,entity_type,research_target,horizon_years,feature)) FROM read_parquet('{str(matrixp).replace(chr(39),chr(39)*2)}')").fetchone()[0],
      "strict_point_in_time_true_rows":con.execute(f"SELECT count(*) FROM read_parquet('{str(matrixp).replace(chr(39),chr(39)*2)}') WHERE strict_point_in_time_validity").fetchone()[0],
      "chemE_signal_allowed_rows":con.execute(f"SELECT count(*) FROM read_parquet('{str(matrixp).replace(chr(39),chr(39)*2)}') WHERE target_system='ChemE Research Intelligence' AND retrospective_signal_use_allowed").fetchone()[0],
      "venture_signal_allowed_rows":con.execute(f"SELECT count(*) FROM read_parquet('{str(matrixp).replace(chr(39),chr(39)*2)}') WHERE target_system='Venture / Business Scouting' AND retrospective_signal_use_allowed").fetchone()[0],
      "labgraph_non_topic_rows":con.execute(f"SELECT count(*) FROM read_parquet('{str(matrixp).replace(chr(39),chr(39)*2)}') WHERE target_system='Lab Graph' AND entity_type<>'topic'").fetchone()[0],
      "labgraph_invalid_family_rows":con.execute(f"SELECT count(*) FROM read_parquet('{str(matrixp).replace(chr(39),chr(39)*2)}') WHERE target_system='Lab Graph' AND feature_family NOT IN ('convergence','collaboration_breadth')").fetchone()[0],
      "knowledge_card_has_snapshot":card["snapshot_date"]==RELEASE,
      "knowledge_card_has_limitations":len(card["limitations"])>0,
      "probable_lab_separation_present":"PROBABLE_LAB" in interface and "CONFIRMED_LAB" in interface,
      "composite_transfer_score_emitted":False,
      "production_model_weights_emitted":False,
    }
    qa["pass"]=(
      all(upstream.values()) and qa["base_stage17_evidence_rows"]==229
      and qa["all_targets_present"] and qa["duplicate_matrix_keys"]==0
      and qa["strict_point_in_time_true_rows"]==0
      and qa["chemE_signal_allowed_rows"]==0 and qa["venture_signal_allowed_rows"]==0
      and qa["labgraph_non_topic_rows"]==0 and qa["labgraph_invalid_family_rows"]==0
      and qa["knowledge_card_has_snapshot"] and qa["knowledge_card_has_limitations"]
      and qa["probable_lab_separation_present"]
      and not qa["composite_transfer_score_emitted"] and not qa["production_model_weights_emitted"]
    )
    write_json(out/"STAGE18_VALIDATION.json",qa)

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="STAGE18_FINAL_GATE.json":
            item={"bytes":p.stat().st_size,"sha256":sha256_file(p)}
            if p.suffix==".parquet":
                item["rows"]=con.execute(f"SELECT count(*) FROM read_parquet('{str(p).replace(chr(39),chr(39)*2)}')").fetchone()[0]
            outputs[p.name]=item

    gate={
      "stage":18,
      "complete":bool(qa["pass"]),
      "release":RELEASE,
      "base_evidence_rows":len(flags),
      "expanded_transfer_matrix_rows":len(matrix),
      "transfer_targets":TARGETS,
      "strict_point_in_time_validity_established":False,
      "composite_transfer_score_emitted":False,
      "production_model_weights_emitted":False,
      "qa_pass":bool(qa["pass"]),
      "outputs":outputs,
    }
    write_json(out/"STAGE18_FINAL_GATE.json",gate)

    report=f"""# OpenAlex Stage 18 — TRANSFER_MATRIX

Status: **{'COMPLETE' if gate['complete'] else 'FAIL'}**

Frozen snapshot: {RELEASE}

- Stage 17 feature/target/horizon evidence rows preserved: {len(flags):,}
- Expanded downstream transfer rows: {len(matrix):,}
- Transfer targets: {len(TARGETS)}
- Strict point-in-time predictive validity established: **NO**
- Composite transfer score emitted: **NO**
- Production model weights emitted: **NO**

## Target routing
Academic Research Landscape receives descriptive layers plus explicitly robust retrospective signal overlays.
Lab Graph consumes Stage 13 structural network evidence and Stage 14 topic convergence; Stage 17 association rows are limited to topic convergence/breadth context.
ATLAS can use robust retrospective signals for technology scouting while preserving provenance and caveats.
NOUS receives a structured evidence card and transfer policy.
ChemE Research Intelligence receives global evidence as context but predictive use is held until ChemE-specific domain/subfield validation.
Venture / Business Scouting receives scholarly emergence only as scouting context, never as commercial-success or investment prediction.

## Critical carried-forward limitation
Strict historical point-in-time validity remains **NOT TESTABLE FROM A SINGLE SNAPSHOT** because historical OpenAlex records may have been backfilled or corrected and the frozen 2026 taxonomy is applied retrospectively.

Stage 19 is the final reproducibility audit, executive summary and PROGRAM COMPLETE closure.
"""
    (out/"OPENALEX_STAGE18_TRANSFER_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate,indent=2))
    if not gate["complete"]: raise SystemExit(2)

if __name__=="__main__":
    main()
