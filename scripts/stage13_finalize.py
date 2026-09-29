#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import duckdb

RELEASE="2026-09-23"
WORKS_SHA="a72ff4b43c9d13d0911386e1a5dd1b9d71b3c1080ac44ccfc016e11d6390a059"
EXPECTED_ANALYTIC_WORKS=372_984_105
STAGE7_FULL_INSTITUTION_DISTINCT_SUM=316_822_743
STAGE7_FULL_COUNTRY_DISTINCT_SUM=210_214_260

def q(s): return "'" + str(s).replace("'","''") + "'"
def sha256_file(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def merge_sum(con,paths,out_path,keys,sums):
    plist="["+",".join(q(p) for p in paths)+"]"
    expr=[*keys]+[f"sum({x}) AS {x}" for x in sums]
    sql=f"SELECT {','.join(expr)} FROM read_parquet({plist},union_by_name=true) GROUP BY {','.join(str(i+1) for i in range(len(keys)))}"
    con.execute(f"COPY ({sql}) TO {q(out_path)} (FORMAT PARQUET,COMPRESSION ZSTD)")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",required=True)
    ap.add_argument("--output",required=True)
    ap.add_argument("--shards",type=int,default=16)
    args=ap.parse_args()
    root=Path(args.root); out=Path(args.output); out.mkdir(parents=True,exist_ok=True)

    manifests=[]
    for i in range(args.shards):
        p=root/f"shard-{i}"/"SHARD_MANIFEST.json"
        if not p.exists(): raise RuntimeError(f"Missing shard manifest {i}")
        m=json.loads(p.read_text())
        if m.get("source_manifest_sha256")!=WORKS_SHA or m.get("release")!=RELEASE:
            raise RuntimeError(f"Frozen-source mismatch in shard {i}")
        manifests.append(m)

    ids=[x for m in manifests for x in m["assigned_file_indices"]]
    coverage_pass=(len(ids)==2040 and len(set(ids))==2040 and sorted(ids)==list(range(2040)))
    if not coverage_pass: raise RuntimeError(f"Works file coverage failure: n={len(ids)}, unique={len(set(ids))}")

    con=duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='10GB'")
    con.execute("SET temp_directory='/tmp/openalex13_final'")
    con.execute("SET max_temp_directory_size='8GB'")

    specs={
      "institution_activity_year.parquet":(["pub_year","corpus","tier","institution_id","country_code"],["works"]),
      "country_activity_year.parquet":(["pub_year","corpus","tier","country_code"],["works"]),
      "country_pair_year.parquet":(["pub_year","corpus","tier","country_a","country_b"],["works"]),
      "institution_pair_sample_year.parquet":(["pub_year","corpus","tier","institution_a","institution_b"],["works"]),
      "author_activity_sample_year.parquet":(["pub_year","corpus","tier","author_id"],["works","corresponding_works"]),
      "author_pair_sample_year.parquet":(["pub_year","corpus","tier","author_a","author_b"],["works"]),
      "author_institution_sample_year.parquet":(["pub_year","corpus","tier","author_id","institution_id"],["works","corresponding_works"]),
      "authors_count_hist.parquet":(["pub_year","corpus","tier","authors_count"],["works"]),
      "institutions_count_hist.parquet":(["pub_year","corpus","tier","institutions_distinct_count"],["works"]),
      "countries_count_hist.parquet":(["pub_year","corpus","tier","countries_distinct_count"],["works"]),
    }
    for name,(keys,sums) in specs.items():
        paths=[str(root/f"shard-{i}"/name) for i in range(args.shards)]
        for p in paths:
            if not Path(p).exists(): raise RuntimeError(f"Missing shard output {p}")
        merge_sum(con,paths,str(out/name),keys,sums)

    analytic_works=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'authors_count_hist.parquet')})").fetchone()[0]
    inst_hist_works=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'institutions_count_hist.parquet')})").fetchone()[0]
    country_hist_works=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'countries_count_hist.parquet')})").fetchone()[0]
    inst_incidence=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'institution_activity_year.parquet')})").fetchone()[0]
    country_incidence=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'country_activity_year.parquet')})").fetchone()[0]
    country_pair_work_events=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'country_pair_year.parquet')})").fetchone()[0]
    inst_pair_sample_events=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'institution_pair_sample_year.parquet')})").fetchone()[0]
    author_pair_sample_events=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'author_pair_sample_year.parquet')})").fetchone()[0]
    author_inst_sample_events=con.execute(f"SELECT sum(works) FROM read_parquet({q(out/'author_institution_sample_year.parquet')})").fetchone()[0]

    totals={}
    for k in ["works","inst_sample_work_hash","inst_pair_eligible_sample_works","author_sample_work_hash",
              "author_pair_eligible_sample_works","hyperauthor_works","hyperinstitution_works","multicountry_gt32_works"]:
        totals[k]=sum(int(m["totals"].get(k,0)) for m in manifests)

    qa={
      "source_file_coverage_pass":coverage_pass,
      "analytic_works":analytic_works,
      "expected_stage12_analytic_works":EXPECTED_ANALYTIC_WORKS,
      "analytic_works_match_stage12":analytic_works==EXPECTED_ANALYTIC_WORKS,
      "histogram_work_totals_equal":analytic_works==inst_hist_works==country_hist_works,
      "institution_incidence":inst_incidence,
      "country_incidence":country_incidence,
      "stage7_full_institution_distinct_sum_bound":STAGE7_FULL_INSTITUTION_DISTINCT_SUM,
      "stage7_full_country_distinct_sum_bound":STAGE7_FULL_COUNTRY_DISTINCT_SUM,
      "institution_incidence_subset_bound_pass":inst_incidence<=STAGE7_FULL_INSTITUTION_DISTINCT_SUM,
      "country_incidence_subset_bound_pass":country_incidence<=STAGE7_FULL_COUNTRY_DISTINCT_SUM,
      "country_pair_canonical_order_violations":con.execute(f"SELECT count(*) FROM read_parquet({q(out/'country_pair_year.parquet')}) WHERE country_a>=country_b").fetchone()[0],
      "institution_pair_canonical_order_violations":con.execute(f"SELECT count(*) FROM read_parquet({q(out/'institution_pair_sample_year.parquet')}) WHERE institution_a>=institution_b").fetchone()[0],
      "author_pair_canonical_order_violations":con.execute(f"SELECT count(*) FROM read_parquet({q(out/'author_pair_sample_year.parquet')}) WHERE author_a>=author_b").fetchone()[0],
      "country_pair_work_events":country_pair_work_events,
      "institution_pair_sample_work_events":inst_pair_sample_events,
      "author_pair_sample_work_events":author_pair_sample_events,
      "author_institution_sample_work_events":author_inst_sample_events,
      "sampling_totals":totals,
    }

    duplicate_checks={}
    for name,(keys,_) in specs.items():
        keyexpr=",".join(keys)
        duplicate_checks[name]=con.execute(f"SELECT count(*)-count(DISTINCT ({keyexpr})) FROM read_parquet({q(out/name)})").fetchone()[0]
    qa["duplicate_key_rows"]=duplicate_checks
    qa["pass"]=(
      qa["source_file_coverage_pass"] and qa["analytic_works_match_stage12"] and qa["histogram_work_totals_equal"]
      and qa["institution_incidence_subset_bound_pass"] and qa["country_incidence_subset_bound_pass"]
      and qa["country_pair_canonical_order_violations"]==0
      and qa["institution_pair_canonical_order_violations"]==0
      and qa["author_pair_canonical_order_violations"]==0
      and all(v==0 for v in duplicate_checks.values())
    )
    (out/"STAGE13_VALIDATION.json").write_text(json.dumps(qa,indent=2),encoding="utf-8")

    outputs={}
    for p in sorted(out.glob("*.parquet")):
        outputs[p.name]={"bytes":p.stat().st_size,"sha256":sha256_file(p),
                         "rows":con.execute(f"SELECT count(*) FROM read_parquet({q(p)})").fetchone()[0]}

    gate={
      "stage":13,"complete":bool(qa["pass"]),"release":RELEASE,"source_manifest_sha256":WORKS_SHA,
      "works_files_covered":2040,"analytic_works_2000_2026_nonretracted":analytic_works,
      "full_census_layers":["institution_activity_year","country_activity_year","country_pair_year","authors_count_hist","institutions_count_hist","countries_count_hist"],
      "bounded_topology_layers":{
        "institution_pair_sample":{"work_hash_mod":64,"max_institutions_per_work":32,"eligible_sample_works":totals["inst_pair_eligible_sample_works"]},
        "author_pair_sample":{"work_hash_mod":512,"max_authors_per_work":50,"eligible_sample_works":totals["author_pair_eligible_sample_works"]},
        "author_institution_sample":{"work_hash_mod":512,"max_authors_per_work":50}
      },
      "hyperedge_audit":{
        "authors_count_gt_50_works":totals["hyperauthor_works"],
        "institutions_distinct_count_gt_32_works":totals["hyperinstitution_works"],
        "countries_distinct_count_gt_32_works":totals["multicountry_gt32_works"]
      },
      "qa_pass":bool(qa["pass"]),"outputs":outputs
    }
    (out/"STAGE13_FINAL_GATE.json").write_text(json.dumps(gate,indent=2),encoding="utf-8")
    report=f"""# OpenAlex Stage 13 — Author / Institution / Country Network

Status: **{'COMPLETE' if gate['complete'] else 'FAIL'}**

- Frozen snapshot: {RELEASE}
- Works files covered: 2,040 / 2,040
- Analytic Works: {analytic_works:,}
- Institution incidence events: {inst_incidence:,}
- Country incidence events: {country_incidence:,}
- Country-pair work events: {country_pair_work_events:,}
- Institution-pair sampled work events: {inst_pair_sample_events:,}
- Author-pair sampled work events: {author_pair_sample_events:,}
- Author-institution sampled work events: {author_inst_sample_events:,}

## Architecture
Global institution/country activity and the country collaboration network are full-census layers for the Stage-13 analytic universe. Institution-pair and author topology are deterministic bounded layers to avoid uncontrolled pair explosion from large consortium works. Hyperauthorship/hyperinstitution works are measured rather than silently discarded.

## Sampling contract
- institution pairs: hash(work.id) % 64 == 0; 2–32 institutions/work
- author pairs and author-institution: hash(work.id) % 512 == 0; 2–50 authors/work
- special NULL/deleted author IDs excluded from person metrics
- canonical pair ordering prevents directional duplicates

## Downstream
These outputs are designed for Academic Research Landscape / Lab Graph, Stage 14 convergence, and Stage 15 early-signal features.
"""
    (out/"OPENALEX_STAGE13_NETWORK_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate,indent=2))
    if not gate["complete"]: raise SystemExit(2)

if __name__=="__main__":
    main()
