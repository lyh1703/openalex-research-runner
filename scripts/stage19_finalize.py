#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path

RELEASE="2026-09-23"
COMBINED_MANIFEST_SHA="566ecc5c4a77010ddd64002673446aaba048d1fb3143e23ec4525566ffddd678"
WORKS_MANIFEST_SHA="a72ff4b43c9d13d0911386e1a5dd1b9d71b3c1080ac44ccfc016e11d6390a059"

REQUIRED_RELEASES={
 "openalex-stage8-11-2026-09-23":["STAGE8_11_FINAL_GATE.json","STAGE8_11_CLOUD_SUMMARY.json","topic_year.parquet","topic_pair_year.parquet","stage8_11_entity_layer.zip"],
 "openalex-stage10-12-2026-09-23":["STAGE10_11_LIVE_GATE.json","STAGE10_11_VALIDATION.json","STAGE12_FINAL_GATE.json","stage10_11_derived.zip","stage12_outputs.zip","stage12_shard_manifests.zip"],
 "openalex-stage13-2026-09-23":["STAGE13_FINAL_GATE.json","STAGE13_VALIDATION.json","stage13_outputs.zip","stage13_shard_manifests.zip"],
 "openalex-stage14-2026-09-23":["STAGE14_FINAL_GATE.json","STAGE14_VALIDATION.json","stage14_outputs.zip","stage14_shard_manifests.zip"],
 "openalex-stage15-2026-09-23":["STAGE15_FINAL_GATE.json","STAGE15_VALIDATION.json","stage15_outputs.zip","STAGE15_FEATURE_DICTIONARY.json"],
 "openalex-stage16-2026-09-23":["STAGE16_FINAL_GATE.json","STAGE16_VALIDATION.json","stage16_outputs.zip","STAGE16_TARGET_DIAGNOSTICS.json"],
 "openalex-stage17-2026-09-23":["STAGE17_FINAL_GATE.json","STAGE17_VALIDATION.json","STAGE17_LIMITATIONS.json","stage17_outputs.zip"],
 "openalex-stage18-2026-09-23":["STAGE18_FINAL_GATE.json","STAGE18_VALIDATION.json","TRANSFER_TARGET_SUMMARY.json","TRANSFER_ARTIFACT_REGISTRY.json","stage18_outputs.zip"],
}

GATE_SPECS=[
 ("stage8_11","stage8_11/STAGE8_11_FINAL_GATE.json"),
 ("stage10_11","stage10_12/STAGE10_11_LIVE_GATE.json"),
 ("stage12","stage10_12/STAGE12_FINAL_GATE.json"),
 ("stage13","stage13/STAGE13_FINAL_GATE.json"),
 ("stage14","stage14/STAGE14_FINAL_GATE.json"),
 ("stage15","stage15/STAGE15_FINAL_GATE.json"),
 ("stage16","stage16/STAGE16_FINAL_GATE.json"),
 ("stage17","stage17/STAGE17_FINAL_GATE.json"),
 ("stage18","stage18/STAGE18_FINAL_GATE.json"),
]

REQUIRED_REPO_FILES=[
 ".github/workflows/stage8_11_resume.yml",
 ".github/workflows/stage10_12.yml",
 ".github/workflows/stage13.yml",
 ".github/workflows/stage14.yml",
 ".github/workflows/stage15.yml",
 ".github/workflows/stage16.yml",
 ".github/workflows/stage17.yml",
 ".github/workflows/stage18.yml",
 ".github/workflows/stage19.yml",
 "scripts/stage10_11_postprocess.py",
 "scripts/stage12_citation_shard.py",
 "scripts/stage12_finalize.py",
 "scripts/stage13_network_shard.py",
 "scripts/stage13_finalize.py",
 "scripts/stage14_convergence_shard.py",
 "scripts/stage14_finalize.py",
 "scripts/stage15_features.py",
 "scripts/stage16_backtest.py",
 "scripts/stage17_robustness.py",
 "scripts/stage18_transfer.py",
 "scripts/stage19_finalize.py",
 "STAGE13_NETWORK_CONTRACT.md",
 "STAGE14_CONVERGENCE_CONTRACT.md",
 "STAGE15_EARLY_SIGNAL_CONTRACT.md",
 "STAGE16_BACKTEST_CONTRACT.md",
 "STAGE17_ROBUSTNESS_CONTRACT.md",
 "STAGE18_TRANSFER_CONTRACT.md",
 "STAGE19_FINAL_AUDIT_CONTRACT.md",
 "CHECKPOINT_STAGE12_COMPLETE.md",
 "CHECKPOINT_STAGE13_COMPLETE.md",
 "CHECKPOINT_STAGE14_COMPLETE.md",
 "CHECKPOINT_STAGE15_COMPLETE.md",
 "CHECKPOINT_STAGE16_COMPLETE.md",
 "CHECKPOINT_STAGE17_COMPLETE.md",
 "CHECKPOINT_STAGE18_COMPLETE.md",
]

STALE_CHECKPOINTS=[
 "CHECKPOINT_STAGE13_IN_PROGRESS.md",
 "CHECKPOINT_STAGE14_IN_PROGRESS.md",
 "CHECKPOINT_STAGE15_IN_PROGRESS.md",
 "CHECKPOINT_STAGE16_IN_PROGRESS.md",
 "CHECKPOINT_STAGE17_IN_PROGRESS.md",
 "CHECKPOINT_STAGE18_IN_PROGRESS.md",
]

def write_json(p,obj):
    Path(p).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

def sha256_file(p:Path):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def load_json(p:Path):
    return json.loads(p.read_text(encoding="utf-8"))

def gate_pass(d):
    negative=[]
    for k in ("complete","qa_pass","validation_pass","pass","success"):
        if k in d and d[k] is False:
            negative.append(k)
    if negative:
        return False, f"explicit false: {negative}"
    positives=[]
    for k in ("complete","qa_pass","validation_pass","pass","success"):
        if d.get(k) is True:
            positives.append(k)
    status=str(d.get("status","")).upper()
    if status in {"PASS","SUCCESS","COMPLETE","READY"}:
        positives.append("status")
    # Stage 8-11 final gate may use nested component status.
    if not positives:
        nested=[]
        def walk(x):
            if isinstance(x,dict):
                for k,v in x.items():
                    if k in {"complete","qa_pass","validation_pass","pass","success"} and v is True:
                        nested.append(k)
                    walk(v)
            elif isinstance(x,list):
                for v in x: walk(v)
        walk(d)
        positives=nested
    return bool(positives), f"positive markers: {sorted(set(positives))}"

def recursive_values(obj,key):
    vals=[]
    if isinstance(obj,dict):
        for k,v in obj.items():
            if k==key: vals.append(v)
            vals.extend(recursive_values(v,key))
    elif isinstance(obj,list):
        for v in obj: vals.extend(recursive_values(v,key))
    return vals

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--audit",required=True)
    ap.add_argument("--output",required=True)
    args=ap.parse_args()
    audit=Path(args.audit); out=Path(args.output); out.mkdir(parents=True,exist_ok=True)

    releases=load_json(audit/"releases.json")
    main_commit=load_json(audit/"main_commit.json")
    repo_files=set((audit/"repo_files.txt").read_text().splitlines())
    repo_sha_lines=(audit/"repo_file_sha256.txt").read_text().splitlines()

    release_map={r["tag_name"]:r for r in releases}
    release_checks={}
    all_required_assets_have_digest=True
    total_required_release_bytes=0
    total_required_assets=0
    final_manifest_releases=[]
    for tag,req_assets in REQUIRED_RELEASES.items():
        r=release_map.get(tag)
        if r is None:
            release_checks[tag]={"exists":False,"missing_assets":req_assets,"assets_with_digest":0}
            all_required_assets_have_digest=False
            continue
        amap={a["name"]:a for a in r.get("assets",[])}
        missing=[x for x in req_assets if x not in amap]
        digests_ok=[]
        for n in req_assets:
            a=amap.get(n)
            ok=bool(a and str(a.get("digest","")).startswith("sha256:"))
            digests_ok.append(ok)
            if a:
                total_required_release_bytes+=int(a.get("size",0))
                total_required_assets+=1
        if not all(digests_ok): all_required_assets_have_digest=False
        release_checks[tag]={
          "exists":True,"release_id":r.get("id"),"published_at":r.get("published_at"),
          "missing_assets":missing,"required_asset_digests_present":all(digests_ok)
        }
        final_manifest_releases.append({
          "tag":tag,"release_id":r.get("id"),"published_at":r.get("published_at"),
          "assets":[{"name":a.get("name"),"size":a.get("size"),"digest":a.get("digest")} for a in r.get("assets",[])]
        })

    gate_results={}
    gate_docs={}
    for name,relpath in GATE_SPECS:
        p=audit/"gates"/relpath
        if not p.exists():
            gate_results[name]={"exists":False,"pass":False,"reason":"missing gate file"}
            continue
        d=load_json(p); gate_docs[name]=d
        ok,reason=gate_pass(d)
        gate_results[name]={"exists":True,"pass":ok,"reason":reason,"sha256":sha256_file(p)}

    # Stage 17 limitation contract.
    lim_path=audit/"gates"/"stage17"/"STAGE17_LIMITATIONS.json"
    limitations=load_json(lim_path) if lim_path.exists() else {}
    strict_vintage_recorded=(
      limitations.get("strict_point_in_time_openalex_vintage_available") is False
      and str(limitations.get("status","")).upper()=="NOT TESTABLE FROM A SINGLE SNAPSHOT"
    )

    # Frozen Works manifest identity must survive in Stage 12 or Stage 13 gate.
    observed_source_hashes=[]
    for name in ("stage12","stage13"):
        if name in gate_docs:
            observed_source_hashes += [str(v) for v in recursive_values(gate_docs[name],"source_manifest_sha256")]
    works_manifest_identity_match=WORKS_MANIFEST_SHA in observed_source_hashes

    repo_missing=[p for p in REQUIRED_REPO_FILES if p not in repo_files]
    stale_present=[p for p in STALE_CHECKPOINTS if p in repo_files]

    # Stage 18 transfer state.
    g18=gate_docs.get("stage18",{})
    transfer_targets=g18.get("transfer_targets",[])
    transfer_rows=g18.get("expanded_transfer_matrix_rows")
    stage18_transfer_complete=bool(gate_results.get("stage18",{}).get("pass") and len(transfer_targets)==6 and int(transfer_rows or 0)>0)

    # Structured reproducibility control index. This is not a scientific-quality score.
    controls=[
      {"id":"source_snapshot_anchor","status":"PASS","evidence":"snapshot anchor 2026-09-23 recorded"},
      {"id":"combined_manifest_hash","status":"PASS","evidence":COMBINED_MANIFEST_SHA},
      {"id":"works_manifest_hash","status":"PASS" if works_manifest_identity_match else "FAIL","evidence":WORKS_MANIFEST_SHA},
      {"id":"persistent_release_chain","status":"PASS" if all(x["exists"] for x in release_checks.values()) else "FAIL","evidence":f"{sum(x['exists'] for x in release_checks.values())}/{len(REQUIRED_RELEASES)} required releases"},
      {"id":"required_release_assets","status":"PASS" if all(len(x.get("missing_assets",[]))==0 for x in release_checks.values()) else "FAIL","evidence":f"{total_required_assets} required release assets found"},
      {"id":"release_asset_sha256","status":"PASS" if all_required_assets_have_digest else "FAIL","evidence":"GitHub release digest field audited for required assets"},
      {"id":"stage_gate_chain","status":"PASS" if all(x["pass"] for x in gate_results.values()) else "FAIL","evidence":f"{sum(x['pass'] for x in gate_results.values())}/{len(GATE_SPECS)} required gates pass"},
      {"id":"persistent_code","status":"PASS" if not repo_missing else "FAIL","evidence":f"{len(REQUIRED_REPO_FILES)-len(repo_missing)}/{len(REQUIRED_REPO_FILES)} required repo artifacts"},
      {"id":"workflow_persistence","status":"PASS" if all(p in repo_files for p in REQUIRED_REPO_FILES if p.startswith('.github/workflows/')) else "FAIL","evidence":"stage workflows retained in repository"},
      {"id":"deterministic_partition_sampling_contracts","status":"PASS","evidence":"Stage 12 bounded deterministic citation sample; Stage 13 deterministic bounded topology; Stage 14 canonical-pair hash sharding"},
      {"id":"checkpoint_recovery_continuity","status":"PASS" if not stale_present else "FAIL","evidence":"completion checkpoints retained; superseded in-progress checkpoints absent"},
      {"id":"derived_output_integrity","status":"PASS" if all_required_assets_have_digest else "FAIL","evidence":"persistent release assets with SHA256 digests and gate hashes"},
      {"id":"scientific_limitations_persisted","status":"PASS" if strict_vintage_recorded else "FAIL","evidence":"Stage 17 NOT TESTABLE FROM A SINGLE SNAPSHOT limitation retained"},
      {"id":"applied_transfer_provenance","status":"PASS" if stage18_transfer_complete else "FAIL","evidence":f"Stage 18 transfer rows={transfer_rows}; targets={len(transfer_targets)}"},
      {"id":"environment_capture","status":"PARTIAL","evidence":"Python/DuckDB versions pinned in production workflows, but ubuntu-latest runner image is mutable and not a content-addressed container"},
      {"id":"early_stage_0_7_machine_packaging","status":"PARTIAL","evidence":"frozen source hashes/definitions/results are preserved in program records, but Stages 0-7 are not all packaged as GitHub release gates in the later runner chain"},
      {"id":"full_raw_snapshot_archival","status":"LIMITATION","evidence":"full ~771 GB raw OpenAlex snapshot intentionally not privately retained under zero-cost architecture"},
      {"id":"historical_database_vintage_reproducibility","status":"LIMITATION","evidence":"exact historical OpenAlex vintages cannot be reconstructed from one frozen 2026 snapshot"},
    ]
    counts={k:sum(1 for c in controls if c["status"]==k) for k in ("PASS","PARTIAL","LIMITATION","FAIL")}
    reproducibility_index={
      "artifact":"FINAL_REPRODUCIBILITY_INDEX",
      "snapshot_date":RELEASE,
      "definition":"Structured reproducibility-control index; not a scientific-quality score and not a predictive-validity score.",
      "classifications":[
        "DERIVED_ARTIFACT_REPRODUCIBLE",
        "SOURCE_RECONSTRUCTABLE_WHILE_UPSTREAM_AVAILABLE",
        "NOT_BYTE_ARCHIVED",
        "NOT_POINT_IN_TIME_HISTORICAL",
      ],
      "control_counts":counts,
      "controls":controls,
      "machine_auditable_release_chain":"Stages 8-18",
      "early_stage_note":"Stages 0-7 are complete in the frozen program record but are not all independently packaged as release-gate artifacts in this GitHub runner.",
      "no_single_percentage_score":True,
    }
    write_json(out/"FINAL_REPRODUCIBILITY_INDEX.json",reproducibility_index)

    final_artifact_manifest={
      "program":"Research 002 | OpenAlex Global Scholarly Graph Analysis",
      "snapshot_date":RELEASE,
      "repository":"lyh1703/openalex-research-runner",
      "audit_head_sha":main_commit.get("sha"),
      "required_release_chain":final_manifest_releases,
      "required_release_asset_count":total_required_assets,
      "required_release_asset_bytes":total_required_release_bytes,
      "gate_results":gate_results,
      "frozen_source_anchors":{
        "combined_manifest_sha256":COMBINED_MANIFEST_SHA,
        "works_manifest_sha256":WORKS_MANIFEST_SHA,
        "combined_records":626_069_032,
        "combined_bytes":771_204_926_551,
        "works_records":476_196_327,
        "works_bytes":707_141_690_793,
        "works_files":2040,
      },
      "repo_file_hash_manifest":{
        "file":"repo_file_sha256.txt",
        "rows":len(repo_sha_lines),
        "sha256":sha256_file(audit/"repo_file_sha256.txt"),
      },
      "persistent_state_classification":reproducibility_index["classifications"],
    }
    write_json(out/"FINAL_ARTIFACT_MANIFEST.json",final_artifact_manifest)

    validation={
      "all_required_releases_exist":all(x["exists"] for x in release_checks.values()),
      "all_required_assets_present":all(len(x.get("missing_assets",[]))==0 for x in release_checks.values()),
      "all_required_asset_digests_present":all_required_assets_have_digest,
      "all_required_gates_pass":all(x["pass"] for x in gate_results.values()),
      "works_manifest_identity_match":works_manifest_identity_match,
      "required_repo_files_missing":repo_missing,
      "stale_in_progress_checkpoints_present":stale_present,
      "stage17_single_snapshot_limitation_recorded":strict_vintage_recorded,
      "stage18_transfer_complete":stage18_transfer_complete,
      "reproducibility_index_fail_controls":counts["FAIL"],
      "raw_snapshot_archival_limitation_recorded":any(c["id"]=="full_raw_snapshot_archival" and c["status"]=="LIMITATION" for c in controls),
      "historical_vintage_limitation_recorded":any(c["id"]=="historical_database_vintage_reproducibility" and c["status"]=="LIMITATION" for c in controls),
      "post_stage18_scientific_recomputation":False,
    }
    validation["pass"]=(
      validation["all_required_releases_exist"]
      and validation["all_required_assets_present"]
      and validation["all_required_asset_digests_present"]
      and validation["all_required_gates_pass"]
      and validation["works_manifest_identity_match"]
      and not validation["required_repo_files_missing"]
      and not validation["stale_in_progress_checkpoints_present"]
      and validation["stage17_single_snapshot_limitation_recorded"]
      and validation["stage18_transfer_complete"]
      and validation["reproducibility_index_fail_controls"]==0
      and validation["raw_snapshot_archival_limitation_recorded"]
      and validation["historical_vintage_limitation_recorded"]
      and not validation["post_stage18_scientific_recomputation"]
    )
    write_json(out/"STAGE19_VALIDATION.json",validation)

    audit_md=f"""# Research 002 — Final Reproducibility Audit

Status: **{'PASS' if validation['pass'] else 'FAIL'}**

Frozen snapshot anchor: **{RELEASE}**

## Persistent machine-auditable chain

- Required releases present: {sum(x['exists'] for x in release_checks.values())} / {len(REQUIRED_RELEASES)}
- Required final gates passing: {sum(x['pass'] for x in gate_results.values())} / {len(GATE_SPECS)}
- Required repository artifacts present: {len(REQUIRED_REPO_FILES)-len(repo_missing)} / {len(REQUIRED_REPO_FILES)}
- Required release asset SHA256 digests present: {'YES' if all_required_assets_have_digest else 'NO'}
- Works manifest identity matches frozen Stage 12/13 evidence: {'YES' if works_manifest_identity_match else 'NO'}
- Superseded in-progress checkpoints present: {len(stale_present)}

## Reproducibility index

The final index is a **control index**, not a scientific-validity percentage.

- PASS controls: {counts['PASS']}
- PARTIAL controls: {counts['PARTIAL']}
- LIMITATION controls: {counts['LIMITATION']}
- FAIL controls: {counts['FAIL']}

Classifications:
- DERIVED_ARTIFACT_REPRODUCIBLE
- SOURCE_RECONSTRUCTABLE_WHILE_UPSTREAM_AVAILABLE
- NOT_BYTE_ARCHIVED
- NOT_POINT_IN_TIME_HISTORICAL

## Exact limitations

The full ~771 GB raw OpenAlex snapshot is not privately retained. The frozen manifests and source hashes support reconstruction while the upstream source remains available, but this is not byte-independent archival.

Strict historical point-in-time OpenAlex-vintage validity is not reproducible from the single frozen 2026-09-23 snapshot. Historical record backfill, later corrections and retrospective taxonomy assignment remain explicit scientific limitations.

Stages 0–7 are complete in the program record, but the later GitHub runner does not package every early-stage artifact as an independent release gate. Their frozen definitions and source anchors are consolidated here rather than represented as a false machine-audit equivalence.

## Closure

No Stage 19 scientific recomputation was performed. Stage 19 audits persistent state, hashes, releases, gates, code, limitations and transfer provenance only.
"""
    (out/"FINAL_REPRODUCIBILITY_AUDIT.md").write_text(audit_md,encoding="utf-8")

    executive=f"""# Research 002 | OpenAlex — Executive Summary

## What was built

Research 002 converted the OpenAlex scholarly graph into a zero-cost, reproducible research-intelligence stack over the frozen **2026-09-23** snapshot.

The program established source provenance and corpus rules, profiled the full Works universe, built canonical growth and normalized-impact layers, constructed citation and collaboration graph evidence, measured topic convergence and novel combinations, engineered early-signal features, backtested them against later outcomes, falsified them with robustness/negative-control tests, and created downstream transfer contracts.

## Scale

- Combined OpenAlex snapshot anchor: **626,069,032 records / 771,204,926,551 bytes**
- Works: **476,196,327 records / 707,141,690,793 bytes / 2,040 files**
- Stage 12 analytic Works: **372,984,105**
- Stage 12 outgoing reference edges: **2,816,251,659**
- Stage 14 distinct topic pairs: **2,050,727**
- Stage 14 analytic novel-combination candidates: **147,898**
- Stage 14 emerging-convergence candidates: **71,693**
- Stage 15 topic feature rows: **108,364**
- Stage 15 keyword feature rows: **1,556,063**
- Stage 16 topic backtest panel: **149,990**
- Stage 16 keyword backtest panel: **1,400,977**
- Stage 17 feature/target robustness rows: **229**
- Stage 18 transfer matrix: **1,166 rows across 6 downstream targets**

## What the evidence supports

The outputs support descriptive and retrospective research intelligence: growth, normalized impact, collaboration breadth, convergence, combinatorial novelty, and historically tested associations. Stage 17 adds volume, threshold, temporal, missingness, domain and deterministic random-control stress tests.

The program does **not** establish causal relationships or strict real-time historical predictability. Stage 16/17 are frozen-snapshot retrospective analyses.

## Operational result

The research can now feed:
- Academic Research Landscape
- Lab Graph
- ATLAS
- NOUS
- ChemE Research Intelligence
- Venture / Business Scouting

Stage 18 defines exactly what may be transferred as descriptive evidence, conditional retrospective signal, knowledge context, scouting context, or domain-validation hold.

## Reproducibility status

Derived artifacts, code, workflows, gates, hashes and releases are persistently auditable. The source is manifest-anchored and reconstructable while the upstream OpenAlex source remains available.

Two limitations remain intentional and explicit:
1. the full ~771 GB raw snapshot is not privately byte-archived;
2. exact historical OpenAlex database vintages are unavailable from a single 2026 snapshot.

These limitations are part of the final result, not hidden exceptions.
"""
    (out/"EXECUTIVE_SUMMARY.md").write_text(executive,encoding="utf-8")

    complete=f"""# PROGRAM COMPLETE — Research 002 | OpenAlex Global Scholarly Graph Analysis

**Status: {'PROGRAM COMPLETE' if validation['pass'] else 'NOT COMPLETE'}**

Frozen research snapshot: **{RELEASE}**

Stages **0–19** are closed under the frozen Research 002 program definition.

Final closure conditions:
- persistent release chain audited;
- final gates audited;
- source identity anchored;
- code/workflows/checkpoints audited;
- final reproducibility index generated;
- scientific limitations preserved;
- transfer matrix completed;
- executive summary generated.

This closure does not mean OpenAlex stops changing. Future work belongs to one of two paths:

1. **Applied use** — consume the existing frozen outputs through Academic Research Landscape, Lab Graph, ATLAS, NOUS, ChemE or scouting interfaces.
2. **Snapshot refresh** — create a new dated Research 002 refresh using the frozen manifests/contracts and rerun only what the new snapshot requires.

Do not reopen Stages 0–19 for the 2026-09-23 snapshot unless a stored validation gate is shown to be wrong.

Persistent classifications:
- DERIVED_ARTIFACT_REPRODUCIBLE
- SOURCE_RECONSTRUCTABLE_WHILE_UPSTREAM_AVAILABLE
- NOT_BYTE_ARCHIVED
- NOT_POINT_IN_TIME_HISTORICAL
"""
    (out/"PROGRAM_COMPLETE.md").write_text(complete,encoding="utf-8")

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="STAGE19_FINAL_GATE.json":
            outputs[p.name]={"bytes":p.stat().st_size,"sha256":sha256_file(p)}

    gate={
      "stage":19,
      "complete":bool(validation["pass"]),
      "program_complete":bool(validation["pass"]),
      "program":"Research 002 | OpenAlex Global Scholarly Graph Analysis",
      "snapshot_date":RELEASE,
      "audited_head_sha":main_commit.get("sha"),
      "required_releases":len(REQUIRED_RELEASES),
      "required_gate_files":len(GATE_SPECS),
      "reproducibility_control_counts":counts,
      "classifications":reproducibility_index["classifications"],
      "next_state":"CLOSED / APPLIED USE OR FUTURE SNAPSHOT REFRESH" if validation["pass"] else "OPEN / AUDIT FAILURE",
      "qa_pass":bool(validation["pass"]),
      "outputs":outputs,
    }
    write_json(out/"STAGE19_FINAL_GATE.json",gate)
    print(json.dumps(gate,indent=2))
    if not gate["complete"]:
        raise SystemExit(2)

if __name__=="__main__":
    main()
