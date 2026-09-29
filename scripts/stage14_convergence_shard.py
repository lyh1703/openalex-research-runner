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
def write_json(p,o): Path(p).write_text(json.dumps(o,indent=2,default=str),encoding="utf-8")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pair",required=True); ap.add_argument("--topic",required=True); ap.add_argument("--base",required=True)
    ap.add_argument("--topic-dim-glob",required=True); ap.add_argument("--output",required=True)
    ap.add_argument("--shard",type=int,required=True); ap.add_argument("--shards",type=int,default=16)
    a=ap.parse_args()
    out=Path(a.output); out.mkdir(parents=True,exist_ok=True)
    db=Path(f"/tmp/stage14_s{a.shard}.duckdb")
    if db.exists(): db.unlink()
    con=duckdb.connect(str(db))
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='/tmp/openalex14_s{a.shard}_temp'")
    con.execute("SET max_temp_directory_size='8GB'")
    hexpr=f"(hash(least(topic_id_a,topic_id_b) || '|' || greatest(topic_id_a,topic_id_b)) % {a.shards}) = {a.shard}"

    con.execute(f"""
      CREATE TABLE pair_primary AS
      SELECT publication_year,
             least(topic_id_a,topic_id_b) topic_id_a,
             greatest(topic_id_a,topic_id_b) topic_id_b,
             sum(coassigned_works) coassigned_works,
             sum(joint_score_sum) joint_score_sum
      FROM read_parquet({q(a.pair)})
      WHERE corpus='core' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
        AND topic_id_a IS NOT NULL AND topic_id_b IS NOT NULL AND topic_id_a<>topic_id_b
        AND {hexpr}
      GROUP BY 1,2,3
    """)
    con.execute(f"""
      CREATE TABLE topic_primary AS
      SELECT publication_year,topic_id,sum(works) works
      FROM read_parquet({q(a.topic)})
      WHERE corpus='core' AND tier='A' AND publication_year BETWEEN 2000 AND 2026 AND topic_id IS NOT NULL
      GROUP BY 1,2
    """)
    con.execute(f"""
      CREATE TABLE denom_primary AS
      SELECT publication_year,sum(works) total_works
      FROM read_parquet({q(a.base)})
      WHERE kind='universe' AND corpus='core' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
      GROUP BY 1
    """)
    con.execute(f"""
      CREATE TABLE topic_dim AS
      SELECT DISTINCT id topic_id,display_name topic_name,
             subfield.id subfield_id,subfield.display_name subfield_name,
             field.id field_id,field.display_name field_name,
             domain.id domain_id,domain.display_name domain_name
      FROM read_parquet({q(a.topic_dim_glob)},union_by_name=true)
      WHERE id IS NOT NULL
    """)
    con.execute("""
      CREATE TABLE pair_history AS
      SELECT topic_id_a,topic_id_b,
             min(publication_year) first_seen_year_2000plus,
             min(CASE WHEN coassigned_works>=25 THEN publication_year END) first_substantial_year_25
      FROM pair_primary GROUP BY 1,2
    """)
    con.execute("""
      CREATE TABLE cur2 AS
      WITH cur0 AS (
        SELECT p.publication_year,p.topic_id_a,p.topic_id_b,p.coassigned_works,p.joint_score_sum,
               ta.works topic_a_works,tb.works topic_b_works,d.total_works,
               h.first_seen_year_2000plus,h.first_substantial_year_25,
               da.topic_name topic_a_name,db.topic_name topic_b_name,
               da.subfield_id subfield_a_id,db.subfield_id subfield_b_id,
               da.subfield_name subfield_a_name,db.subfield_name subfield_b_name,
               da.field_id field_a_id,db.field_id field_b_id,
               da.field_name field_a_name,db.field_name field_b_name,
               da.domain_id domain_a_id,db.domain_id domain_b_id,
               da.domain_name domain_a_name,db.domain_name domain_b_name
        FROM pair_primary p
        LEFT JOIN topic_primary ta ON ta.publication_year=p.publication_year AND ta.topic_id=p.topic_id_a
        LEFT JOIN topic_primary tb ON tb.publication_year=p.publication_year AND tb.topic_id=p.topic_id_b
        LEFT JOIN denom_primary d ON d.publication_year=p.publication_year
        LEFT JOIN pair_history h USING(topic_id_a,topic_id_b)
        LEFT JOIN topic_dim da ON da.topic_id=p.topic_id_a
        LEFT JOIN topic_dim db ON db.topic_id=p.topic_id_b
      ), cur1 AS (
        SELECT *,
          joint_score_sum/nullif(coassigned_works,0) mean_joint_score,
          coassigned_works/nullif(total_works,0) pair_prevalence,
          coassigned_works/nullif(topic_a_works+topic_b_works-coassigned_works,0) jaccard,
          coassigned_works/nullif(least(topic_a_works,topic_b_works),0) overlap_coefficient,
          coassigned_works*total_works/nullif(topic_a_works*topic_b_works,0) lift,
          CASE
            WHEN domain_a_id IS NULL OR domain_b_id IS NULL THEN 'unknown'
            WHEN domain_a_id<>domain_b_id THEN 'cross_domain'
            WHEN field_a_id<>field_b_id THEN 'cross_field_same_domain'
            WHEN subfield_a_id<>subfield_b_id THEN 'cross_subfield_same_field'
            ELSE 'same_subfield'
          END hierarchy_distance
        FROM cur0
      )
      SELECT *,
        CASE WHEN lift>0 THEN log2(lift) END pmi_log2,
        CASE WHEN pair_prevalence>0 AND pair_prevalence<1 AND lift>0 THEN log2(lift)/(-log2(pair_prevalence)) END npmi
      FROM cur1
    """)
    con.execute("""
      CREATE TABLE pair_metrics AS
      SELECT c.*,
             p1.coassigned_works pair_works_lag1,
             p3.coassigned_works pair_works_lag3,
             p3.lift lift_lag3,
             CASE WHEN p1.coassigned_works>0 THEN c.coassigned_works/p1.coassigned_works-1 END pair_yoy,
             CASE WHEN p3.coassigned_works>=25 THEN pow(c.coassigned_works/p3.coassigned_works,1.0/3.0)-1 END pair_cagr_3y,
             CASE WHEN c.lift>0 AND p3.lift>0 THEN log2(c.lift)-log2(p3.lift) END log2_lift_change_3y,
             (c.publication_year<=2025 AND c.publication_year=c.first_substantial_year_25
              AND c.coassigned_works>=25 AND c.topic_a_works>=100 AND c.topic_b_works>=100 AND c.lift>=1.25) analytic_novel_combination,
             (c.publication_year<=2025 AND c.coassigned_works>=50 AND p3.coassigned_works>=25
              AND (pow(c.coassigned_works/p3.coassigned_works,1.0/3.0)-1)>=0.20
              AND c.lift>=1.5 AND p3.lift>0 AND c.lift>p3.lift
              AND c.topic_a_works>=100 AND c.topic_b_works>=100) emerging_convergence,
             c.publication_year=2026 frontier_ytd
      FROM cur2 c
      LEFT JOIN cur2 p1 ON p1.topic_id_a=c.topic_id_a AND p1.topic_id_b=c.topic_id_b AND p1.publication_year=c.publication_year-1
      LEFT JOIN cur2 p3 ON p3.topic_id_a=c.topic_id_a AND p3.topic_id_b=c.topic_id_b AND p3.publication_year=c.publication_year-3
    """)

    copies=[
      ("pair_year_primary_signals.parquet","SELECT * FROM pair_metrics WHERE coassigned_works>=10"),
      ("novel_combination_candidates.parquet","SELECT * FROM pair_metrics WHERE analytic_novel_combination"),
      ("emerging_convergence_candidates.parquet","SELECT * FROM pair_metrics WHERE emerging_convergence"),
      ("cross_domain_novel_candidates.parquet","SELECT * FROM pair_metrics WHERE analytic_novel_combination AND hierarchy_distance='cross_domain'"),
    ]
    for name,sql in copies:
        con.execute(f"COPY ({sql}) TO {q(out/name)} (FORMAT PARQUET,COMPRESSION ZSTD)")

    con.execute(f"""
      COPY (
        SELECT publication_year,least(field_a_id,field_b_id) field_a_id,greatest(field_a_id,field_b_id) field_b_id,
               sum(coassigned_works) topic_pair_coassignment_events,count(*) observed_topic_pair_year_rows,
               count(*) FILTER(WHERE analytic_novel_combination) novel_topic_pair_events,
               count(*) FILTER(WHERE emerging_convergence) emerging_topic_pair_events
        FROM pair_metrics
        WHERE field_a_id IS NOT NULL AND field_b_id IS NOT NULL AND field_a_id<>field_b_id
        GROUP BY 1,2,3
      ) TO {q(out/'field_pair_partial.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)
    con.execute(f"""
      COPY (
        SELECT publication_year,least(domain_a_id,domain_b_id) domain_a_id,greatest(domain_a_id,domain_b_id) domain_b_id,
               sum(coassigned_works) topic_pair_coassignment_events,count(*) observed_topic_pair_year_rows,
               count(*) FILTER(WHERE analytic_novel_combination) novel_topic_pair_events,
               count(*) FILTER(WHERE emerging_convergence) emerging_topic_pair_events
        FROM pair_metrics
        WHERE domain_a_id IS NOT NULL AND domain_b_id IS NOT NULL AND domain_a_id<>domain_b_id
        GROUP BY 1,2,3
      ) TO {q(out/'domain_pair_partial.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    novel_grid={}
    for mp in (10,25,50):
      for ml in (1.0,1.25,1.5,2.0):
        novel_grid[f"pair>={mp}|lift>={ml}"]=scalar(con,f"""SELECT count(*) FROM pair_metrics WHERE publication_year<=2025 AND publication_year=first_substantial_year_25 AND coassigned_works>={mp} AND topic_a_works>=100 AND topic_b_works>=100 AND lift>={ml}""")
    emerging_grid={}
    for mc in (0.10,0.20,0.50):
      for ml in (1.25,1.5,2.0):
        emerging_grid[f"cagr3>={mc}|lift>={ml}"]=scalar(con,f"""SELECT count(*) FROM pair_metrics WHERE publication_year<=2025 AND coassigned_works>=50 AND pair_works_lag3>=25 AND pair_cagr_3y>={mc} AND lift>={ml} AND log2_lift_change_3y>0 AND topic_a_works>=100 AND topic_b_works>=100""")

    ex_hash=f"(hash(least(topic_id_a,topic_id_b) || '|' || greatest(topic_id_a,topic_id_b)) % {a.shards}) = {a.shard}"
    expansion=con.execute(f"""
      WITH p AS (
        SELECT publication_year,least(topic_id_a,topic_id_b) aa,greatest(topic_id_a,topic_id_b) bb,sum(coassigned_works) pair_works
        FROM read_parquet({q(a.pair)})
        WHERE corpus='expansion' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
          AND topic_id_a IS NOT NULL AND topic_id_b IS NOT NULL AND topic_id_a<>topic_id_b AND {ex_hash}
        GROUP BY 1,2,3
      ), h AS (SELECT aa,bb,min(CASE WHEN pair_works>=25 THEN publication_year END) first25 FROM p GROUP BY 1,2),
      t AS (SELECT publication_year,topic_id,sum(works) works FROM read_parquet({q(a.topic)}) WHERE corpus='expansion' AND tier='A' AND publication_year BETWEEN 2000 AND 2026 GROUP BY 1,2),
      d AS (SELECT publication_year,sum(works) total_works FROM read_parquet({q(a.base)}) WHERE kind='universe' AND corpus='expansion' AND tier='A' AND publication_year BETWEEN 2000 AND 2026 GROUP BY 1),
      x AS (
        SELECT p.*,h.first25,ta.works wa,tb.works wb,d.total_works,p.pair_works*d.total_works/nullif(ta.works*tb.works,0) lift
        FROM p JOIN h USING(aa,bb)
        LEFT JOIN t ta ON ta.publication_year=p.publication_year AND ta.topic_id=p.aa
        LEFT JOIN t tb ON tb.publication_year=p.publication_year AND tb.topic_id=p.bb
        LEFT JOIN d USING(publication_year)
      )
      SELECT count(*),count(DISTINCT (aa,bb)),
             count(*) FILTER(WHERE publication_year<=2025 AND publication_year=first25 AND pair_works>=25 AND wa>=100 AND wb>=100 AND lift>=1.25)
      FROM x
    """).fetchone()

    qa={
      "pair_metric_rows":scalar(con,"SELECT count(*) FROM pair_metrics"),
      "distinct_pairs":scalar(con,"SELECT count(*) FROM pair_history"),
      "duplicate_pair_year_keys":scalar(con,"SELECT count(*)-count(DISTINCT (publication_year,topic_id_a,topic_id_b)) FROM pair_metrics"),
      "self_pairs":scalar(con,"SELECT count(*) FROM pair_metrics WHERE topic_id_a=topic_id_b"),
      "missing_topic_marginals":scalar(con,"SELECT count(*) FROM pair_metrics WHERE topic_a_works IS NULL OR topic_b_works IS NULL"),
      "missing_or_nonpositive_denominator":scalar(con,"SELECT count(*) FROM pair_metrics WHERE total_works IS NULL OR total_works<=0"),
      "pair_exceeds_marginal":scalar(con,"SELECT count(*) FROM pair_metrics WHERE coassigned_works>topic_a_works OR coassigned_works>topic_b_works"),
      "negative_joint_score":scalar(con,"SELECT count(*) FROM pair_metrics WHERE joint_score_sum<0"),
      "hierarchy_missing":scalar(con,"SELECT count(*) FROM pair_metrics WHERE hierarchy_distance='unknown'"),
      "confirmed_2026_novel":scalar(con,"SELECT count(*) FROM pair_metrics WHERE publication_year=2026 AND analytic_novel_combination"),
      "confirmed_2026_emerging":scalar(con,"SELECT count(*) FROM pair_metrics WHERE publication_year=2026 AND emerging_convergence"),
      "novel_candidates":scalar(con,"SELECT count(*) FROM pair_metrics WHERE analytic_novel_combination"),
      "emerging_candidates":scalar(con,"SELECT count(*) FROM pair_metrics WHERE emerging_convergence"),
      "cross_domain_novel":scalar(con,"SELECT count(*) FROM pair_metrics WHERE analytic_novel_combination AND hierarchy_distance='cross_domain'"),
      "novel_threshold_violations":scalar(con,"SELECT count(*) FROM pair_metrics WHERE analytic_novel_combination AND NOT(publication_year<=2025 AND publication_year=first_substantial_year_25 AND coassigned_works>=25 AND topic_a_works>=100 AND topic_b_works>=100 AND lift>=1.25)"),
      "emerging_threshold_violations":scalar(con,"SELECT count(*) FROM pair_metrics WHERE emerging_convergence AND NOT(publication_year<=2025 AND coassigned_works>=50 AND pair_works_lag3>=25 AND pair_cagr_3y>=0.20 AND lift>=1.5 AND log2_lift_change_3y>0 AND topic_a_works>=100 AND topic_b_works>=100)"),
      "npmi_out_of_range":scalar(con,"SELECT count(*) FROM pair_metrics WHERE npmi IS NOT NULL AND (npmi < -1.0000001 OR npmi > 1.0000001)"),
    }
    qa["pass"]=all(qa[k]==0 for k in ["duplicate_pair_year_keys","self_pairs","missing_topic_marginals","missing_or_nonpositive_denominator","pair_exceeds_marginal","negative_joint_score","confirmed_2026_novel","confirmed_2026_emerging","novel_threshold_violations","emerging_threshold_violations","npmi_out_of_range"])
    sensitivity={"novel_threshold_grid":novel_grid,"emerging_threshold_grid":emerging_grid,"expansion":{"pair_year_rows":expansion[0],"distinct_pairs":expansion[1],"novel_candidates":expansion[2]}}
    write_json(out/"SHARD_QA.json",qa); write_json(out/"SHARD_SENSITIVITY.json",sensitivity)

    outputs={}
    for p in sorted(out.glob("*.parquet")):
        outputs[p.name]={"bytes":p.stat().st_size,"sha256":sha(p),"rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(p)})")}
    manifest={"stage":14,"release":RELEASE,"shard":a.shard,"shards":a.shards,"hash_partition":"hash(canonical topic pair) % shards",
              "qa":qa,"sensitivity":sensitivity,"outputs":outputs}
    write_json(out/"SHARD_MANIFEST.json",manifest)
    print(json.dumps(manifest,indent=2))
    if not qa["pass"]: raise SystemExit(2)

if __name__=="__main__": main()
