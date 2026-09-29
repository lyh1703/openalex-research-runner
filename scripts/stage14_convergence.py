#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json, math, os
from pathlib import Path
import duckdb

RELEASE="2026-09-23"
EXPECTED_TOPIC_PAIR_ROWS=53_119_699
PRIMARY_CORPUS="core"
PRIMARY_TIER="A"

def q(x):
    return "'" + str(x).replace("'","''") + "'"

def sha256_file(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""):
            h.update(b)
    return h.hexdigest()

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

def scalar(con, sql):
    return con.execute(sql).fetchone()[0]

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pair",required=True)
    ap.add_argument("--topic",required=True)
    ap.add_argument("--base",required=True)
    ap.add_argument("--topic-dim-glob",required=True)
    ap.add_argument("--output",required=True)
    args=ap.parse_args()

    pair=Path(args.pair); topic=Path(args.topic); base=Path(args.base)
    out=Path(args.output); out.mkdir(parents=True,exist_ok=True)
    db=Path("/tmp/stage14.duckdb")
    if db.exists(): db.unlink()
    con=duckdb.connect(str(db))
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='12GB'")
    con.execute("SET temp_directory='/tmp/openalex14_temp'")
    con.execute("SET max_temp_directory_size='20GB'")

    input_rows=scalar(con,f"SELECT count(*) FROM read_parquet({q(pair)})")
    pair_source_canonical_violations=scalar(con,f"""
      SELECT count(*) FROM read_parquet({q(pair)})
      WHERE topic_id_a IS NULL OR topic_id_b IS NULL OR topic_id_a>=topic_id_b
    """)
    pair_source_self_pairs=scalar(con,f"SELECT count(*) FROM read_parquet({q(pair)}) WHERE topic_id_a=topic_id_b")

    # Primary pair layer, canonicalized defensively.
    con.execute(f"""
      CREATE TABLE pair_primary AS
      SELECT publication_year,
             least(topic_id_a,topic_id_b) topic_id_a,
             greatest(topic_id_a,topic_id_b) topic_id_b,
             sum(coassigned_works) coassigned_works,
             sum(joint_score_sum) joint_score_sum
      FROM read_parquet({q(pair)})
      WHERE corpus={q(PRIMARY_CORPUS)} AND tier={q(PRIMARY_TIER)}
        AND publication_year BETWEEN 2000 AND 2026
        AND topic_id_a IS NOT NULL AND topic_id_b IS NOT NULL
        AND topic_id_a<>topic_id_b
      GROUP BY 1,2,3
    """)

    con.execute(f"""
      CREATE TABLE topic_primary AS
      SELECT publication_year,topic_id,
             sum(works) works,
             sum(score_sum) score_sum,
             sum(primary_assignment_count) primary_assignment_count
      FROM read_parquet({q(topic)})
      WHERE corpus={q(PRIMARY_CORPUS)} AND tier={q(PRIMARY_TIER)}
        AND publication_year BETWEEN 2000 AND 2026
        AND topic_id IS NOT NULL
      GROUP BY 1,2
    """)

    con.execute(f"""
      CREATE TABLE denom_primary AS
      SELECT publication_year,sum(works) total_works
      FROM read_parquet({q(base)})
      WHERE kind='universe' AND corpus={q(PRIMARY_CORPUS)} AND tier={q(PRIMARY_TIER)}
        AND publication_year BETWEEN 2000 AND 2026
      GROUP BY 1
    """)

    # Frozen topic hierarchy.
    con.execute(f"""
      CREATE TABLE topic_dim AS
      SELECT DISTINCT id topic_id,display_name topic_name,
             subfield.id subfield_id,subfield.display_name subfield_name,
             field.id field_id,field.display_name field_name,
             domain.id domain_id,domain.display_name domain_name
      FROM read_parquet({q(args.topic_dim_glob)},union_by_name=true)
      WHERE id IS NOT NULL
    """)

    con.execute("""
      CREATE TABLE pair_history AS
      SELECT topic_id_a,topic_id_b,
             min(publication_year) first_seen_year_2000plus,
             min(CASE WHEN coassigned_works>=25 THEN publication_year END) first_substantial_year_25
      FROM pair_primary
      GROUP BY 1,2
    """)

    con.execute("""
      CREATE TABLE pair_metrics AS
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
      ), cur2 AS (
        SELECT *,
          CASE WHEN lift>0 THEN log2(lift) END pmi_log2,
          CASE WHEN pair_prevalence>0 AND pair_prevalence<1 AND lift>0
               THEN log2(lift)/(-log2(pair_prevalence)) END npmi
        FROM cur1
      )
      SELECT c.*,
             p1.coassigned_works pair_works_lag1,
             p3.coassigned_works pair_works_lag3,
             p3.lift lift_lag3,
             CASE WHEN p1.coassigned_works>0 THEN c.coassigned_works/p1.coassigned_works-1 END pair_yoy,
             CASE WHEN p3.coassigned_works>=25 AND c.coassigned_works>=0
                  THEN pow(c.coassigned_works/p3.coassigned_works,1.0/3.0)-1 END pair_cagr_3y,
             CASE WHEN c.lift>0 AND p3.lift>0 THEN log2(c.lift)-log2(p3.lift) END log2_lift_change_3y,
             (c.publication_year<=2025
              AND c.publication_year=c.first_substantial_year_25
              AND c.coassigned_works>=25
              AND c.topic_a_works>=100 AND c.topic_b_works>=100
              AND c.lift>=1.25) analytic_novel_combination,
             (c.publication_year<=2025
              AND c.coassigned_works>=50
              AND p3.coassigned_works>=25
              AND (pow(c.coassigned_works/p3.coassigned_works,1.0/3.0)-1)>=0.20
              AND c.lift>=1.5 AND p3.lift>0 AND c.lift>p3.lift
              AND c.topic_a_works>=100 AND c.topic_b_works>=100) emerging_convergence,
             c.publication_year=2026 frontier_ytd
      FROM cur2 c
      LEFT JOIN cur2 p1
        ON p1.topic_id_a=c.topic_id_a AND p1.topic_id_b=c.topic_id_b
       AND p1.publication_year=c.publication_year-1
      LEFT JOIN cur2 p3
        ON p3.topic_id_a=c.topic_id_a AND p3.topic_id_b=c.topic_id_b
       AND p3.publication_year=c.publication_year-3
    """)

    # Decision-grade feature table: full historical calculation, compact persisted layer.
    con.execute(f"""
      COPY (
        SELECT * FROM pair_metrics
        WHERE coassigned_works>=10
      ) TO {q(out/'pair_year_primary_signals.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)
    con.execute(f"""
      COPY (
        SELECT * FROM pair_metrics
        WHERE analytic_novel_combination
        ORDER BY publication_year,coassigned_works DESC
      ) TO {q(out/'novel_combination_candidates.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)
    con.execute(f"""
      COPY (
        SELECT * FROM pair_metrics
        WHERE emerging_convergence
        ORDER BY publication_year,coassigned_works DESC
      ) TO {q(out/'emerging_convergence_candidates.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)
    con.execute(f"""
      COPY (
        SELECT * FROM pair_metrics
        WHERE analytic_novel_combination AND hierarchy_distance='cross_domain'
        ORDER BY publication_year,coassigned_works DESC
      ) TO {q(out/'cross_domain_novel_candidates.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    # Per-topic bridge / convergence activity.
    con.execute(f"""
      COPY (
        WITH sig AS (
          SELECT * FROM pair_metrics WHERE coassigned_works>=10
        ), e AS (
          SELECT publication_year,topic_id_a topic_id,topic_id_b partner_id,
                 coassigned_works,lift,hierarchy_distance,
                 analytic_novel_combination,emerging_convergence
          FROM sig
          UNION ALL
          SELECT publication_year,topic_id_b topic_id,topic_id_a partner_id,
                 coassigned_works,lift,hierarchy_distance,
                 analytic_novel_combination,emerging_convergence
          FROM sig
        )
        SELECT publication_year,topic_id,
               count(DISTINCT partner_id) active_partners,
               sum(coassigned_works) topic_pair_coassignment_events,
               count(DISTINCT partner_id) FILTER(WHERE analytic_novel_combination) novel_partners,
               count(DISTINCT partner_id) FILTER(WHERE emerging_convergence) emerging_partners,
               count(DISTINCT partner_id) FILTER(WHERE hierarchy_distance IN ('cross_field_same_domain','cross_domain')) cross_field_partners,
               count(DISTINCT partner_id) FILTER(WHERE hierarchy_distance='cross_domain') cross_domain_partners,
               median(lift) median_pair_lift,
               max(lift) max_pair_lift
        FROM e GROUP BY 1,2
      ) TO {q(out/'topic_convergence_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    # Hierarchy-level coassignment event layers. These are not unique-work counts.
    con.execute(f"""
      COPY (
        SELECT publication_year,
               least(field_a_id,field_b_id) field_a_id,
               greatest(field_a_id,field_b_id) field_b_id,
               sum(coassigned_works) topic_pair_coassignment_events,
               count(*) observed_topic_pair_year_rows,
               count(*) FILTER(WHERE analytic_novel_combination) novel_topic_pair_events,
               count(*) FILTER(WHERE emerging_convergence) emerging_topic_pair_events
        FROM pair_metrics
        WHERE field_a_id IS NOT NULL AND field_b_id IS NOT NULL AND field_a_id<>field_b_id
        GROUP BY 1,2,3
      ) TO {q(out/'field_pair_coassignment_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)
    con.execute(f"""
      COPY (
        SELECT publication_year,
               least(domain_a_id,domain_b_id) domain_a_id,
               greatest(domain_a_id,domain_b_id) domain_b_id,
               sum(coassigned_works) topic_pair_coassignment_events,
               count(*) observed_topic_pair_year_rows,
               count(*) FILTER(WHERE analytic_novel_combination) novel_topic_pair_events,
               count(*) FILTER(WHERE emerging_convergence) emerging_topic_pair_events
        FROM pair_metrics
        WHERE domain_a_id IS NOT NULL AND domain_b_id IS NOT NULL AND domain_a_id<>domain_b_id
        GROUP BY 1,2,3
      ) TO {q(out/'domain_pair_coassignment_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    # Sensitivity grids, preserving raw metrics rather than inventing a composite score.
    novel_grid={}
    for min_pair in (10,25,50):
        for min_lift in (1.0,1.25,1.5,2.0):
            key=f"pair>={min_pair}|lift>={min_lift}"
            novel_grid[key]=scalar(con,f"""
              SELECT count(*) FROM pair_metrics
              WHERE publication_year<=2025
                AND publication_year=first_substantial_year_25
                AND coassigned_works>={min_pair}
                AND topic_a_works>=100 AND topic_b_works>=100
                AND lift>={min_lift}
            """)
    emerging_grid={}
    for min_cagr in (0.10,0.20,0.50):
        for min_lift in (1.25,1.5,2.0):
            key=f"cagr3>={min_cagr}|lift>={min_lift}"
            emerging_grid[key]=scalar(con,f"""
              SELECT count(*) FROM pair_metrics
              WHERE publication_year<=2025
                AND coassigned_works>=50 AND pair_works_lag3>=25
                AND pair_cagr_3y>={min_cagr}
                AND lift>={min_lift}
                AND log2_lift_change_3y>0
                AND topic_a_works>=100 AND topic_b_works>=100
            """)

    # Expansion sensitivity summary only; never mixed into primary signal.
    expansion=con.execute(f"""
      WITH p AS (
        SELECT publication_year,least(topic_id_a,topic_id_b) a,greatest(topic_id_a,topic_id_b) b,
               sum(coassigned_works) pair_works
        FROM read_parquet({q(pair)})
        WHERE corpus='expansion' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
          AND topic_id_a IS NOT NULL AND topic_id_b IS NOT NULL AND topic_id_a<>topic_id_b
        GROUP BY 1,2,3
      ), h AS (
        SELECT a,b,min(CASE WHEN pair_works>=25 THEN publication_year END) first25 FROM p GROUP BY 1,2
      ), t AS (
        SELECT publication_year,topic_id,sum(works) works
        FROM read_parquet({q(topic)})
        WHERE corpus='expansion' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
        GROUP BY 1,2
      ), d AS (
        SELECT publication_year,sum(works) total_works
        FROM read_parquet({q(base)})
        WHERE kind='universe' AND corpus='expansion' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
        GROUP BY 1
      ), x AS (
        SELECT p.*,h.first25,ta.works wa,tb.works wb,d.total_works,
               p.pair_works*d.total_works/nullif(ta.works*tb.works,0) lift
        FROM p JOIN h USING(a,b)
        LEFT JOIN t ta ON ta.publication_year=p.publication_year AND ta.topic_id=p.a
        LEFT JOIN t tb ON tb.publication_year=p.publication_year AND tb.topic_id=p.b
        LEFT JOIN d USING(publication_year)
      )
      SELECT count(*) pair_year_rows,count(DISTINCT (a,b)) distinct_pairs,
             count(*) FILTER(WHERE publication_year<=2025 AND publication_year=first25 AND pair_works>=25 AND wa>=100 AND wb>=100 AND lift>=1.25) novel_candidates
      FROM x
    """).fetchone()
    sensitivity={
      "primary":{"corpus":"core","tier":"A"},
      "novel_threshold_grid":novel_grid,
      "emerging_threshold_grid":emerging_grid,
      "expansion_tierA_summary":{"pair_year_rows":expansion[0],"distinct_pairs":expansion[1],"novel_candidates":expansion[2]}
    }
    write_json(out/"STAGE14_THRESHOLD_SENSITIVITY.json",sensitivity)

    # QA
    qa={}
    qa["input_topic_pair_rows"]=input_rows
    qa["expected_topic_pair_rows"]=EXPECTED_TOPIC_PAIR_ROWS
    qa["input_row_count_match"]=input_rows==EXPECTED_TOPIC_PAIR_ROWS
    qa["source_noncanonical_or_null_pair_rows"]=pair_source_canonical_violations
    qa["source_self_pairs"]=pair_source_self_pairs
    qa["primary_pair_rows"]=scalar(con,"SELECT count(*) FROM pair_primary")
    qa["primary_distinct_pairs"]=scalar(con,"SELECT count(*) FROM pair_history")
    qa["final_duplicate_pair_year_keys"]=scalar(con,"SELECT count(*)-count(DISTINCT (publication_year,topic_id_a,topic_id_b)) FROM pair_metrics")
    qa["final_self_pairs"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE topic_id_a=topic_id_b")
    qa["missing_topic_marginals"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE topic_a_works IS NULL OR topic_b_works IS NULL")
    qa["missing_or_nonpositive_denominator"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE total_works IS NULL OR total_works<=0")
    qa["pair_exceeds_marginal_violations"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE coassigned_works>topic_a_works OR coassigned_works>topic_b_works")
    qa["negative_joint_score_rows"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE joint_score_sum<0")
    qa["hierarchy_missing_rows"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE hierarchy_distance='unknown'")
    qa["confirmed_2026_novel_rows"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE publication_year=2026 AND analytic_novel_combination")
    qa["confirmed_2026_emerging_rows"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE publication_year=2026 AND emerging_convergence")
    qa["novel_candidates"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE analytic_novel_combination")
    qa["emerging_candidates"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE emerging_convergence")
    qa["cross_domain_novel_candidates"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE analytic_novel_combination AND hierarchy_distance='cross_domain'")
    qa["novel_threshold_violations"]=scalar(con,"""
      SELECT count(*) FROM pair_metrics
      WHERE analytic_novel_combination AND NOT(
        publication_year<=2025 AND publication_year=first_substantial_year_25
        AND coassigned_works>=25 AND topic_a_works>=100 AND topic_b_works>=100 AND lift>=1.25)
    """)
    qa["emerging_threshold_violations"]=scalar(con,"""
      SELECT count(*) FROM pair_metrics
      WHERE emerging_convergence AND NOT(
        publication_year<=2025 AND coassigned_works>=50 AND pair_works_lag3>=25
        AND pair_cagr_3y>=0.20 AND lift>=1.5 AND log2_lift_change_3y>0
        AND topic_a_works>=100 AND topic_b_works>=100)
    """)
    qa["formula_lift_max_abs_error"]=scalar(con,"""
      SELECT max(abs(lift - coassigned_works*total_works/nullif(topic_a_works*topic_b_works,0)))
      FROM pair_metrics WHERE lift IS NOT NULL
    """)
    qa["formula_jaccard_max_abs_error"]=scalar(con,"""
      SELECT max(abs(jaccard - coassigned_works/nullif(topic_a_works+topic_b_works-coassigned_works,0)))
      FROM pair_metrics WHERE jaccard IS NOT NULL
    """)
    qa["npmi_out_of_range_rows"]=scalar(con,"SELECT count(*) FROM pair_metrics WHERE npmi IS NOT NULL AND (npmi < -1.0000001 OR npmi > 1.0000001)")
    qa["output_signal_rows"]=scalar(con,f"SELECT count(*) FROM read_parquet({q(out/'pair_year_primary_signals.parquet')})")

    fatal=[
      not qa["input_row_count_match"],
      qa["final_duplicate_pair_year_keys"]!=0,
      qa["final_self_pairs"]!=0,
      qa["missing_topic_marginals"]!=0,
      qa["missing_or_nonpositive_denominator"]!=0,
      qa["pair_exceeds_marginal_violations"]!=0,
      qa["negative_joint_score_rows"]!=0,
      qa["confirmed_2026_novel_rows"]!=0,
      qa["confirmed_2026_emerging_rows"]!=0,
      qa["novel_threshold_violations"]!=0,
      qa["emerging_threshold_violations"]!=0,
      (qa["formula_lift_max_abs_error"] or 0)>1e-10,
      (qa["formula_jaccard_max_abs_error"] or 0)>1e-10,
      qa["npmi_out_of_range_rows"]!=0,
      qa["novel_candidates"]<=0,
      qa["emerging_candidates"]<=0,
    ]
    qa["pass"]=not any(fatal)
    write_json(out/"STAGE14_VALIDATION.json",qa)

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.suffix in {".parquet",".json"} and p.name not in {"STAGE14_FINAL_GATE.json"}:
            item={"bytes":p.stat().st_size,"sha256":sha256_file(p)}
            if p.suffix==".parquet":
                item["rows"]=scalar(con,f"SELECT count(*) FROM read_parquet({q(p)})")
            outputs[p.name]=item

    gate={
      "stage":14,
      "complete":bool(qa["pass"]),
      "release":RELEASE,
      "primary_universe":"core + Tier A; 2000-2025 confirmed, 2026 frontier YTD",
      "definition_note":"Novelty is novelty within the 2000+ frozen analytic window, not a claim of first-ever historical occurrence.",
      "primary_pair_year_rows":qa["primary_pair_rows"],
      "distinct_topic_pairs":qa["primary_distinct_pairs"],
      "novel_combination_candidates":qa["novel_candidates"],
      "emerging_convergence_candidates":qa["emerging_candidates"],
      "cross_domain_novel_candidates":qa["cross_domain_novel_candidates"],
      "hierarchy_missing_pair_year_rows":qa["hierarchy_missing_rows"],
      "qa_pass":bool(qa["pass"]),
      "outputs":outputs
    }
    write_json(out/"STAGE14_FINAL_GATE.json",gate)

    report=f"""# OpenAlex Stage 14 — Convergence / Novel Combination

Status: **{'COMPLETE' if gate['complete'] else 'FAIL'}**

Frozen snapshot: {RELEASE}

## Primary universe
Core + Tier A. Years 2000–2025 are eligible for confirmed novelty/convergence; 2026 is frontier YTD only.

## Results
- Primary pair-year rows evaluated: {qa['primary_pair_rows']:,}
- Distinct topic pairs: {qa['primary_distinct_pairs']:,}
- Confirmed analytic novel combinations: {qa['novel_candidates']:,}
- Emerging convergence candidates: {qa['emerging_candidates']:,}
- Cross-domain novel candidates: {qa['cross_domain_novel_candidates']:,}
- Pair-year rows missing hierarchy enrichment: {qa['hierarchy_missing_rows']:,}

## Interpretation
Novelty means first substantial emergence within the 2000+ analytic window, with both topics already established and a positive association threshold. It is not evidence that the pair never appeared before 2000 or outside OpenAlex.

No opaque composite score is created. Pair prevalence, Jaccard, overlap coefficient, lift, PMI/NPMI, pair growth, lift change, first substantial year, and hierarchy distance remain separate features.

## Downstream
The resulting feature layers are suitable inputs for Stage 15 early-signal engineering and for the Academic Research Landscape / Lab Graph applied layer.
"""
    (out/"OPENALEX_STAGE14_CONVERGENCE_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate,indent=2))
    if not gate["complete"]:
        raise SystemExit(2)

if __name__=="__main__":
    main()
