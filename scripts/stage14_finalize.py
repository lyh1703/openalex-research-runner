#!/usr/bin/env python3
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import duckdb

RELEASE="2026-09-23"

def q(x): return "'" + str(x).replace("'","''") + "'"
def sha(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()
def scalar(con,sql): return con.execute(sql).fetchone()[0]
def write_json(p,o): Path(p).write_text(json.dumps(o,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

def merge_union(con,paths,out):
    plist="["+",".join(q(str(x)) for x in paths)+"]"
    con.execute(f"COPY (SELECT * FROM read_parquet({plist},union_by_name=true)) TO {q(out)} (FORMAT PARQUET,COMPRESSION ZSTD)")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",required=True); ap.add_argument("--prepare",required=True); ap.add_argument("--output",required=True); ap.add_argument("--shards",type=int,default=16)
    a=ap.parse_args()
    root=Path(a.root); prep=Path(a.prepare); out=Path(a.output); out.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect("/tmp/stage14_finalize.duckdb")
    con.execute("SET threads=4"); con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='10GB'")
    con.execute("SET temp_directory='/tmp/openalex14_final_temp'"); con.execute("SET max_temp_directory_size='12GB'")

    prepare=json.loads((prep/"STAGE14_PREPARE.json").read_text())
    if not prepare.get("pass"): raise RuntimeError("Stage14 prepare gate did not pass")

    manifests=[]
    for i in range(a.shards):
        p=root/f"shard-{i}"/"SHARD_MANIFEST.json"
        if not p.exists(): raise RuntimeError(f"missing shard manifest {i}")
        m=json.loads(p.read_text())
        if m.get("shard")!=i or m.get("shards")!=a.shards or m.get("release")!=RELEASE: raise RuntimeError(f"shard identity mismatch {i}")
        if not m.get("qa",{}).get("pass"): raise RuntimeError(f"shard QA failed {i}")
        manifests.append(m)

    names=["pair_year_primary_signals.parquet","novel_combination_candidates.parquet","emerging_convergence_candidates.parquet","cross_domain_novel_candidates.parquet"]
    for name in names:
        merge_union(con,[root/f"shard-{i}"/name for i in range(a.shards)],out/name)

    # Merge hierarchy coassignment layers.
    fpaths=[root/f"shard-{i}"/"field_pair_partial.parquet" for i in range(a.shards)]
    plist="["+",".join(q(str(x)) for x in fpaths)+"]"
    con.execute(f"""
      COPY (
        SELECT publication_year,field_a_id,field_b_id,
               sum(topic_pair_coassignment_events) topic_pair_coassignment_events,
               sum(observed_topic_pair_year_rows) observed_topic_pair_year_rows,
               sum(novel_topic_pair_events) novel_topic_pair_events,
               sum(emerging_topic_pair_events) emerging_topic_pair_events
        FROM read_parquet({plist},union_by_name=true)
        GROUP BY 1,2,3
      ) TO {q(out/'field_pair_coassignment_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)
    dpaths=[root/f"shard-{i}"/"domain_pair_partial.parquet" for i in range(a.shards)]
    dlist="["+",".join(q(str(x)) for x in dpaths)+"]"
    con.execute(f"""
      COPY (
        SELECT publication_year,domain_a_id,domain_b_id,
               sum(topic_pair_coassignment_events) topic_pair_coassignment_events,
               sum(observed_topic_pair_year_rows) observed_topic_pair_year_rows,
               sum(novel_topic_pair_events) novel_topic_pair_events,
               sum(emerging_topic_pair_events) emerging_topic_pair_events
        FROM read_parquet({dlist},union_by_name=true)
        GROUP BY 1,2,3
      ) TO {q(out/'domain_pair_coassignment_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    sig=out/"pair_year_primary_signals.parquet"
    con.execute(f"""
      COPY (
        WITH s AS (SELECT * FROM read_parquet({q(sig)})),
        e AS (
          SELECT publication_year,topic_id_a topic_id,topic_id_b partner_id,coassigned_works,lift,hierarchy_distance,analytic_novel_combination,emerging_convergence FROM s
          UNION ALL
          SELECT publication_year,topic_id_b topic_id,topic_id_a partner_id,coassigned_works,lift,hierarchy_distance,analytic_novel_combination,emerging_convergence FROM s
        )
        SELECT publication_year,topic_id,
               count(DISTINCT partner_id) active_partners,
               sum(coassigned_works) topic_pair_coassignment_events,
               count(DISTINCT partner_id) FILTER(WHERE analytic_novel_combination) novel_partners,
               count(DISTINCT partner_id) FILTER(WHERE emerging_convergence) emerging_partners,
               count(DISTINCT partner_id) FILTER(WHERE hierarchy_distance IN ('cross_field_same_domain','cross_domain')) cross_field_partners,
               count(DISTINCT partner_id) FILTER(WHERE hierarchy_distance='cross_domain') cross_domain_partners,
               median(lift) median_pair_lift,max(lift) max_pair_lift
        FROM e GROUP BY 1,2
      ) TO {q(out/'topic_convergence_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    # Sum sensitivity counts because canonical pair hash assigns each pair to exactly one shard.
    novel_grid={}; emerging_grid={}; exp={"pair_year_rows":0,"distinct_pairs":0,"novel_candidates":0}
    for m in manifests:
        s=m["sensitivity"]
        for k,v in s["novel_threshold_grid"].items(): novel_grid[k]=novel_grid.get(k,0)+int(v)
        for k,v in s["emerging_threshold_grid"].items(): emerging_grid[k]=emerging_grid.get(k,0)+int(v)
        for k in exp: exp[k]+=int(s["expansion"][k])
    sensitivity={"novel_threshold_grid":novel_grid,"emerging_threshold_grid":emerging_grid,"expansion_tierA_summary":exp}
    write_json(out/"STAGE14_THRESHOLD_SENSITIVITY.json",sensitivity)

    total_metrics=sum(int(m["qa"]["pair_metric_rows"]) for m in manifests)
    total_pairs=sum(int(m["qa"]["distinct_pairs"]) for m in manifests)
    sum_novel=sum(int(m["qa"]["novel_candidates"]) for m in manifests)
    sum_emerging=sum(int(m["qa"]["emerging_candidates"]) for m in manifests)
    sum_cross_domain=sum(int(m["qa"]["cross_domain_novel"]) for m in manifests)
    sum_hierarchy_missing=sum(int(m["qa"]["hierarchy_missing"]) for m in manifests)

    novel=out/"novel_combination_candidates.parquet"; emerg=out/"emerging_convergence_candidates.parquet"; cross=out/"cross_domain_novel_candidates.parquet"
    qa={
      "prepare_pass":bool(prepare["pass"]),
      "shards":a.shards,
      "all_shard_qa_pass":all(m["qa"]["pass"] for m in manifests),
      "primary_pair_year_rows_evaluated":total_metrics,
      "distinct_topic_pairs":total_pairs,
      "signal_rows_persisted_coassigned_ge_10":scalar(con,f"SELECT count(*) FROM read_parquet({q(sig)})"),
      "duplicate_signal_pair_year_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (publication_year,topic_id_a,topic_id_b)) FROM read_parquet({q(sig)})"),
      "self_pairs":scalar(con,f"SELECT count(*) FROM read_parquet({q(sig)}) WHERE topic_id_a=topic_id_b"),
      "novel_candidates":scalar(con,f"SELECT count(*) FROM read_parquet({q(novel)})"),
      "emerging_candidates":scalar(con,f"SELECT count(*) FROM read_parquet({q(emerg)})"),
      "cross_domain_novel_candidates":scalar(con,f"SELECT count(*) FROM read_parquet({q(cross)})"),
      "candidate_count_matches_shards":None,
      "confirmed_2026_novel":scalar(con,f"SELECT count(*) FROM read_parquet({q(novel)}) WHERE publication_year=2026"),
      "confirmed_2026_emerging":scalar(con,f"SELECT count(*) FROM read_parquet({q(emerg)}) WHERE publication_year=2026"),
      "novel_threshold_violations":scalar(con,f"""SELECT count(*) FROM read_parquet({q(novel)}) WHERE NOT(publication_year<=2025 AND publication_year=first_substantial_year_25 AND coassigned_works>=25 AND topic_a_works>=100 AND topic_b_works>=100 AND lift>=1.25)"""),
      "emerging_threshold_violations":scalar(con,f"""SELECT count(*) FROM read_parquet({q(emerg)}) WHERE NOT(publication_year<=2025 AND coassigned_works>=50 AND pair_works_lag3>=25 AND pair_cagr_3y>=0.20 AND lift>=1.5 AND log2_lift_change_3y>0 AND topic_a_works>=100 AND topic_b_works>=100)"""),
      "formula_lift_max_abs_error":scalar(con,f"""SELECT max(abs(lift-coassigned_works*total_works/nullif(topic_a_works*topic_b_works,0))) FROM read_parquet({q(sig)}) WHERE lift IS NOT NULL"""),
      "formula_jaccard_max_abs_error":scalar(con,f"""SELECT max(abs(jaccard-coassigned_works/nullif(topic_a_works+topic_b_works-coassigned_works,0))) FROM read_parquet({q(sig)}) WHERE jaccard IS NOT NULL"""),
      "npmi_out_of_range":scalar(con,f"SELECT count(*) FROM read_parquet({q(sig)}) WHERE npmi IS NOT NULL AND (npmi < -1.0000001 OR npmi > 1.0000001)"),
      "hierarchy_missing_pair_year_rows_evaluated":sum_hierarchy_missing,
    }
    qa["candidate_count_matches_shards"]=(qa["novel_candidates"]==sum_novel and qa["emerging_candidates"]==sum_emerging and qa["cross_domain_novel_candidates"]==sum_cross_domain)
    qa["pass"]=(
      qa["prepare_pass"] and qa["all_shard_qa_pass"] and qa["candidate_count_matches_shards"]
      and qa["duplicate_signal_pair_year_keys"]==0 and qa["self_pairs"]==0
      and qa["confirmed_2026_novel"]==0 and qa["confirmed_2026_emerging"]==0
      and qa["novel_threshold_violations"]==0 and qa["emerging_threshold_violations"]==0
      and (qa["formula_lift_max_abs_error"] or 0)<=1e-10 and (qa["formula_jaccard_max_abs_error"] or 0)<=1e-10
      and qa["npmi_out_of_range"]==0 and qa["novel_candidates"]>0 and qa["emerging_candidates"]>0
    )
    write_json(out/"STAGE14_VALIDATION.json",qa)

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.suffix in {".parquet",".json"} and p.name!="STAGE14_FINAL_GATE.json":
            x={"bytes":p.stat().st_size,"sha256":sha(p)}
            if p.suffix==".parquet": x["rows"]=scalar(con,f"SELECT count(*) FROM read_parquet({q(p)})")
            outputs[p.name]=x
    gate={
      "stage":14,"complete":bool(qa["pass"]),"release":RELEASE,
      "primary_universe":"core + Tier A; 2000-2025 confirmed, 2026 frontier YTD",
      "definition_note":"Novelty means first substantial emergence within the 2000+ frozen analytic window; it is not proof of first-ever historical occurrence.",
      "primary_pair_year_rows_evaluated":total_metrics,"distinct_topic_pairs":total_pairs,
      "novel_combination_candidates":qa["novel_candidates"],"emerging_convergence_candidates":qa["emerging_candidates"],
      "cross_domain_novel_candidates":qa["cross_domain_novel_candidates"],
      "hierarchy_missing_pair_year_rows":sum_hierarchy_missing,"qa_pass":bool(qa["pass"]),"outputs":outputs
    }
    write_json(out/"STAGE14_FINAL_GATE.json",gate)
    report=f"""# OpenAlex Stage 14 — Convergence / Novel Combination

Status: **{'COMPLETE' if gate['complete'] else 'FAIL'}**

- Frozen snapshot: {RELEASE}
- Primary: Core + Tier A
- Confirmed historical years: 2000–2025
- 2026: frontier YTD only
- Pair-year rows evaluated: {total_metrics:,}
- Distinct topic pairs: {total_pairs:,}
- Persisted decision-grade signal rows (coassigned >= 10): {qa['signal_rows_persisted_coassigned_ge_10']:,}
- Analytic novel combinations: {qa['novel_candidates']:,}
- Emerging convergence candidates: {qa['emerging_candidates']:,}
- Cross-domain novel candidates: {qa['cross_domain_novel_candidates']:,}

Novelty is explicitly bounded to the 2000+ analytic window. No opaque composite score is used; prevalence, Jaccard, overlap coefficient, lift, PMI/NPMI, growth, lift change and hierarchy distance remain separate evidence features.

The full Stage 14 calculation is hash-sharded by canonical topic pair, so all years of a pair remain in the same shard. This preserves first-seen/lag logic while avoiding the monolithic spill/OOM failure encountered in the first attempt.
"""
    (out/"OPENALEX_STAGE14_CONVERGENCE_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate,indent=2))
    if not gate["complete"]: raise SystemExit(2)

if __name__=="__main__": main()
