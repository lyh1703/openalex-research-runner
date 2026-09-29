#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,hashlib,json,math,statistics
from collections import defaultdict
from pathlib import Path
import duckdb

RELEASE="2026-09-23"

TOPIC_FEATURES=[
 "share","raw_yoy","share_yoy","count_cagr_3y","share_cagr_3y","count_cagr_5y","share_cagr_5y",
 "share_log_robust_z","recent_share_log_slope","share_acceleration","growth_persistence_years",
 "mature_mean_fwci","mature_mean_normalized_percentile","mature_top_10_rate","mature_uncited_rate",
 "active_partners","novel_partners","emerging_partners","cross_field_partners","cross_domain_partners",
 "median_pair_lift","novel_partner_share","emerging_partner_share","cross_domain_partner_share",
 "institution_breadth_relative_global","country_breadth_relative_global",
 "share_percentile_year","share_cagr_3y_percentile_year","share_robust_z_percentile_year",
 "active_partners_percentile_year","cross_domain_partners_percentile_year","mature_fwci_percentile_year",
 "mature_top10_percentile_year","institution_breadth_percentile_year","country_breadth_percentile_year"
]
KEYWORD_FEATURES=[
 "share","raw_yoy","share_yoy","count_cagr_3y","share_cagr_3y","count_cagr_5y","share_cagr_5y",
 "share_log_robust_z","recent_share_log_slope","share_acceleration","growth_persistence_years",
 "mature_mean_fwci","mature_mean_normalized_percentile","mature_top_10_rate","mature_uncited_rate",
 "share_percentile_year","share_cagr_3y_percentile_year","mature_fwci_percentile_year"
]
TARGETS_TOPIC=[
 ("growth",3,"future_share_growth","growth_breakout_event","growth_outcome_percentile","growth_eligible"),
 ("growth",5,"future_share_growth","growth_breakout_event","growth_outcome_percentile","growth_eligible"),
 ("cross_domain_convergence",3,"future_cross_domain_partner_delta","convergence_breakout_event","convergence_outcome_percentile","convergence_eligible"),
 ("cross_domain_convergence",5,"future_cross_domain_partner_delta","convergence_breakout_event","convergence_outcome_percentile","convergence_eligible"),
 ("realized_cohort_impact",3,"eventual_cohort_mean_fwci","impact_breakout_event","impact_outcome_percentile","impact_eligible"),
]
TARGETS_KEYWORD=[
 ("growth",3,"future_share_growth","growth_breakout_event","growth_outcome_percentile","growth_eligible"),
 ("growth",5,"future_share_growth","growth_breakout_event","growth_outcome_percentile","growth_eligible"),
 ("realized_cohort_impact",3,"eventual_cohort_mean_fwci","impact_breakout_event","impact_outcome_percentile","impact_eligible"),
]

def q(x): return "'" + str(x).replace("'","''") + "'"
def scalar(con,sql): return con.execute(sql).fetchone()[0]
def sha256_file(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()
def write_json(p,obj):
    Path(p).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
def finite(x):
    return x is not None and isinstance(x,(int,float)) and math.isfinite(float(x))

def create_topic_panel(con,src,out):
    con.execute(f"CREATE VIEW topic AS SELECT * FROM read_parquet({q(src)})")
    con.execute("""
      CREATE TABLE topic_panel_base AS
      SELECT
        3 horizon,t.year signal_year,t.year+3 target_year,t.topic_id,t.topic_name,t.domain_id,t.domain_name,t.field_id,t.field_name,
        t.primary_topic_works signal_works,t.share signal_share,
        CASE WHEN t.year+3<=2025 THEN true ELSE false END growth_eligible,
        CASE WHEN t.year+3<=2025 THEN true ELSE false END convergence_eligible,
        CASE WHEN t.year+3<=2025 THEN coalesce(f3.share,0.0)/nullif(t.share,0)-1 END future_share_growth,
        CASE WHEN t.year+3<=2025 THEN coalesce(f3.primary_topic_works,0) END future_works,
        CASE WHEN t.year+3<=2025 THEN coalesce(f3.cross_domain_partners,0)-coalesce(t.cross_domain_partners,0) END future_cross_domain_partner_delta,
        CASE WHEN t.year+3<=2025 THEN coalesce(f3.cross_domain_partners,0) END future_cross_domain_partners,
        (t.year+3<=2025 AND f3.mature_impact_available AND f3.mature_impact_cohort_year=t.year) impact_eligible,
        CASE WHEN t.year+3<=2025 AND f3.mature_impact_available AND f3.mature_impact_cohort_year=t.year THEN f3.mature_mean_fwci END eventual_cohort_mean_fwci,
        CASE WHEN t.year+3<=2025 AND f3.mature_impact_available AND f3.mature_impact_cohort_year=t.year THEN f3.mature_impact_cohort_year END eventual_impact_cohort_year,
        t.* EXCLUDE(year,topic_id,topic_name,domain_id,domain_name,field_id,field_name)
      FROM topic t
      LEFT JOIN topic f3 ON f3.topic_id=t.topic_id AND f3.year=t.year+3
      WHERE t.year BETWEEN 2005 AND 2022 AND t.primary_topic_works>=50

      UNION ALL

      SELECT
        5 horizon,t.year signal_year,t.year+5 target_year,t.topic_id,t.topic_name,t.domain_id,t.domain_name,t.field_id,t.field_name,
        t.primary_topic_works signal_works,t.share signal_share,
        CASE WHEN t.year+5<=2025 THEN true ELSE false END growth_eligible,
        CASE WHEN t.year+5<=2025 THEN true ELSE false END convergence_eligible,
        CASE WHEN t.year+5<=2025 THEN coalesce(f5.share,0.0)/nullif(t.share,0)-1 END future_share_growth,
        CASE WHEN t.year+5<=2025 THEN coalesce(f5.primary_topic_works,0) END future_works,
        CASE WHEN t.year+5<=2025 THEN coalesce(f5.cross_domain_partners,0)-coalesce(t.cross_domain_partners,0) END future_cross_domain_partner_delta,
        CASE WHEN t.year+5<=2025 THEN coalesce(f5.cross_domain_partners,0) END future_cross_domain_partners,
        false impact_eligible,
        NULL::DOUBLE eventual_cohort_mean_fwci,
        NULL::BIGINT eventual_impact_cohort_year,
        t.* EXCLUDE(year,topic_id,topic_name,domain_id,domain_name,field_id,field_name)
      FROM topic t
      LEFT JOIN topic f5 ON f5.topic_id=t.topic_id AND f5.year=t.year+5
      WHERE t.year BETWEEN 2005 AND 2020 AND t.primary_topic_works>=50
    """)
    con.execute("""
      CREATE TABLE topic_growth_rank AS
      SELECT horizon,signal_year,topic_id,
             percent_rank() OVER(PARTITION BY horizon,signal_year ORDER BY future_share_growth) growth_outcome_percentile
      FROM topic_panel_base WHERE growth_eligible AND future_share_growth IS NOT NULL
    """)
    con.execute("""
      CREATE TABLE topic_conv_rank AS
      SELECT horizon,signal_year,topic_id,
             percent_rank() OVER(PARTITION BY horizon,signal_year ORDER BY future_cross_domain_partner_delta) convergence_outcome_percentile
      FROM topic_panel_base WHERE convergence_eligible AND future_cross_domain_partner_delta IS NOT NULL
    """)
    con.execute("""
      CREATE TABLE topic_impact_rank AS
      SELECT horizon,signal_year,topic_id,
             percent_rank() OVER(PARTITION BY horizon,signal_year ORDER BY eventual_cohort_mean_fwci) impact_outcome_percentile
      FROM topic_panel_base WHERE impact_eligible AND eventual_cohort_mean_fwci IS NOT NULL
    """)
    con.execute(f"""
      COPY (
        SELECT b.*,
               gr.growth_outcome_percentile,
               CASE WHEN gr.growth_outcome_percentile IS NOT NULL THEN gr.growth_outcome_percentile>=0.90 END growth_breakout_event,
               cr.convergence_outcome_percentile,
               CASE WHEN cr.convergence_outcome_percentile IS NOT NULL THEN cr.convergence_outcome_percentile>=0.90 END convergence_breakout_event,
               ir.impact_outcome_percentile,
               CASE WHEN ir.impact_outcome_percentile IS NOT NULL THEN ir.impact_outcome_percentile>=0.90 END impact_breakout_event
        FROM topic_panel_base b
        LEFT JOIN topic_growth_rank gr USING(horizon,signal_year,topic_id)
        LEFT JOIN topic_conv_rank cr USING(horizon,signal_year,topic_id)
        LEFT JOIN topic_impact_rank ir USING(horizon,signal_year,topic_id)
        ORDER BY horizon,signal_year,topic_id
      ) TO {q(out)} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

def create_keyword_panel(con,src,out):
    con.execute(f"CREATE VIEW keyword AS SELECT * FROM read_parquet({q(src)})")
    con.execute("""
      CREATE TABLE keyword_panel_base AS
      SELECT
        3 horizon,k.year signal_year,k.year+3 target_year,k.keyword_id,
        k.keyword_works signal_works,k.share signal_share,
        CASE WHEN k.year+3<=2025 THEN true ELSE false END growth_eligible,
        CASE WHEN k.year+3<=2025 THEN coalesce(f3.share,0.0)/nullif(k.share,0)-1 END future_share_growth,
        CASE WHEN k.year+3<=2025 THEN coalesce(f3.keyword_works,0) END future_works,
        (k.year+3<=2025 AND f3.mature_impact_available AND f3.mature_impact_cohort_year=k.year) impact_eligible,
        CASE WHEN k.year+3<=2025 AND f3.mature_impact_available AND f3.mature_impact_cohort_year=k.year THEN f3.mature_mean_fwci END eventual_cohort_mean_fwci,
        CASE WHEN k.year+3<=2025 AND f3.mature_impact_available AND f3.mature_impact_cohort_year=k.year THEN f3.mature_impact_cohort_year END eventual_impact_cohort_year,
        k.* EXCLUDE(year,keyword_id)
      FROM keyword k
      LEFT JOIN keyword f3 ON f3.keyword_id=k.keyword_id AND f3.year=k.year+3
      WHERE k.year BETWEEN 2005 AND 2022 AND k.keyword_works>=50

      UNION ALL

      SELECT
        5 horizon,k.year signal_year,k.year+5 target_year,k.keyword_id,
        k.keyword_works signal_works,k.share signal_share,
        CASE WHEN k.year+5<=2025 THEN true ELSE false END growth_eligible,
        CASE WHEN k.year+5<=2025 THEN coalesce(f5.share,0.0)/nullif(k.share,0)-1 END future_share_growth,
        CASE WHEN k.year+5<=2025 THEN coalesce(f5.keyword_works,0) END future_works,
        false impact_eligible,
        NULL::DOUBLE eventual_cohort_mean_fwci,
        NULL::BIGINT eventual_impact_cohort_year,
        k.* EXCLUDE(year,keyword_id)
      FROM keyword k
      LEFT JOIN keyword f5 ON f5.keyword_id=k.keyword_id AND f5.year=k.year+5
      WHERE k.year BETWEEN 2005 AND 2020 AND k.keyword_works>=50
    """)
    con.execute("""
      CREATE TABLE keyword_growth_rank AS
      SELECT horizon,signal_year,keyword_id,
             percent_rank() OVER(PARTITION BY horizon,signal_year ORDER BY future_share_growth) growth_outcome_percentile
      FROM keyword_panel_base WHERE growth_eligible AND future_share_growth IS NOT NULL
    """)
    con.execute("""
      CREATE TABLE keyword_impact_rank AS
      SELECT horizon,signal_year,keyword_id,
             percent_rank() OVER(PARTITION BY horizon,signal_year ORDER BY eventual_cohort_mean_fwci) impact_outcome_percentile
      FROM keyword_panel_base WHERE impact_eligible AND eventual_cohort_mean_fwci IS NOT NULL
    """)
    con.execute(f"""
      COPY (
        SELECT b.*,
               gr.growth_outcome_percentile,
               CASE WHEN gr.growth_outcome_percentile IS NOT NULL THEN gr.growth_outcome_percentile>=0.90 END growth_breakout_event,
               ir.impact_outcome_percentile,
               CASE WHEN ir.impact_outcome_percentile IS NOT NULL THEN ir.impact_outcome_percentile>=0.90 END impact_breakout_event
        FROM keyword_panel_base b
        LEFT JOIN keyword_growth_rank gr USING(horizon,signal_year,keyword_id)
        LEFT JOIN keyword_impact_rank ir USING(horizon,signal_year,keyword_id)
        ORDER BY horizon,signal_year,keyword_id
      ) TO {q(out)} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

def evaluate_feature_years(con,panel_path,idcol,features,targets):
    rows=[]
    for target,horizon,outcome,event,pct,eligible in targets:
        for feature in features:
            sql=f"""
              WITH x AS (
                SELECT signal_year,{feature} feature_value,{outcome} outcome_value,
                       {event}::INTEGER event_value
                FROM read_parquet({q(panel_path)})
                WHERE horizon={horizon} AND {eligible}
                  AND {feature} IS NOT NULL
                  AND {outcome} IS NOT NULL
                  AND {event} IS NOT NULL
              ), r AS (
                SELECT *,
                       percent_rank() OVER(PARTITION BY signal_year ORDER BY feature_value) feature_pct,
                       percent_rank() OVER(PARTITION BY signal_year ORDER BY outcome_value) outcome_pct
                FROM x
              )
              SELECT signal_year,count(*) n,
                     sum(event_value) event_count,
                     avg(event_value) base_event_rate,
                     count(*) FILTER(WHERE feature_pct>=0.90) high_n,
                     sum(event_value) FILTER(WHERE feature_pct>=0.90) high_event_count,
                     avg(event_value) FILTER(WHERE feature_pct>=0.90) high_event_rate,
                     count(*) FILTER(WHERE feature_pct<=0.10) low_n,
                     sum(event_value) FILTER(WHERE feature_pct<=0.10) low_event_count,
                     avg(event_value) FILTER(WHERE feature_pct<=0.10) low_event_rate,
                     corr(feature_pct,outcome_pct) spearman_rho,
                     avg(outcome_value) mean_outcome,
                     avg(outcome_value) FILTER(WHERE feature_pct>=0.90) high_mean_outcome,
                     avg(outcome_value) FILTER(WHERE feature_pct<=0.10) low_mean_outcome
              FROM r GROUP BY signal_year
              HAVING count(*)>=50
              ORDER BY signal_year
            """
            for rec in con.execute(sql).fetchall():
                (year,n,ec,ber,hn,hec,her,ln,lec,ler,rho,mo,hmo,lmo)=rec
                rows.append({
                  "target":target,"horizon":horizon,"feature":feature,"signal_year":year,
                  "n":n,"event_count":ec,"base_event_rate":ber,
                  "high_n":hn,"high_event_count":hec or 0,"high_event_rate":her,
                  "low_n":ln,"low_event_count":lec or 0,"low_event_rate":ler,
                  "spearman_rho":rho,"mean_outcome":mo,
                  "high_mean_outcome":hmo,"low_mean_outcome":lmo
                })
    return rows

def summarize(rows):
    groups=defaultdict(list)
    for r in rows:
        groups[(r["target"],r["horizon"],r["feature"])].append(r)
    out=[]
    for (target,horizon,feature),grp in groups.items():
        periods=[
          ("all",grp),
          ("2005_2014",[r for r in grp if r["signal_year"]<=2014]),
          ("2015_plus",[r for r in grp if r["signal_year"]>=2015]),
        ]
        for period,sub in periods:
            if not sub: continue
            n=sum(r["n"] for r in sub)
            ec=sum(r["event_count"] for r in sub)
            hn=sum(r["high_n"] for r in sub)
            hec=sum(r["high_event_count"] for r in sub)
            ln=sum(r["low_n"] for r in sub)
            lec=sum(r["low_event_count"] for r in sub)
            rhos=[float(r["spearman_rho"]) for r in sub if finite(r["spearman_rho"])]
            base=ec/n if n else None
            high=hec/hn if hn else None
            low=lec/ln if ln else None
            out.append({
              "period":period,"target":target,"horizon":horizon,"feature":feature,
              "years":len(sub),"first_year":min(r["signal_year"] for r in sub),"last_year":max(r["signal_year"] for r in sub),
              "n":n,"event_count":ec,"base_event_rate":base,
              "high_n":hn,"high_event_count":hec,"high_event_rate":high,
              "high_tail_lift":(high/base if base and high is not None else None),
              "low_n":ln,"low_event_count":lec,"low_event_rate":low,
              "low_tail_lift":(low/base if base and low is not None else None),
              "mean_annual_spearman_rho":statistics.fmean(rhos) if rhos else None,
              "median_annual_spearman_rho":statistics.median(rhos) if rhos else None,
              "positive_rho_year_fraction":(sum(x>0 for x in rhos)/len(rhos) if rhos else None),
            })
    return out

def write_csv(path,rows):
    if not rows: raise RuntimeError(f"No rows for {path}")
    fields=list(rows[0].keys())
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)

def csv_to_parquet(con,csv_path,out_path):
    con.execute(f"COPY (SELECT * FROM read_csv_auto({q(csv_path)},HEADER=TRUE,SAMPLE_SIZE=-1,NULLSTR='')) TO {q(out_path)} (FORMAT PARQUET,COMPRESSION ZSTD)")

def diagnostics(con,topic_panel,keyword_panel):
    d={"topic":{},"keyword":{}}
    for h in [3,5]:
        d["topic"][f"growth_{h}y"]={
          "rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND growth_eligible"),
          "years":scalar(con,f"SELECT count(DISTINCT signal_year) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND growth_eligible"),
          "event_rate":scalar(con,f"SELECT avg(growth_breakout_event::INTEGER) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND growth_eligible"),
          "missing_future_entity_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND growth_eligible AND future_works=0"),
        }
        d["topic"][f"convergence_{h}y"]={
          "rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND convergence_eligible"),
          "years":scalar(con,f"SELECT count(DISTINCT signal_year) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND convergence_eligible"),
          "event_rate":scalar(con,f"SELECT avg(convergence_breakout_event::INTEGER) FROM read_parquet({q(topic_panel)}) WHERE horizon={h} AND convergence_eligible"),
        }
        d["keyword"][f"growth_{h}y"]={
          "rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE horizon={h} AND growth_eligible"),
          "years":scalar(con,f"SELECT count(DISTINCT signal_year) FROM read_parquet({q(keyword_panel)}) WHERE horizon={h} AND growth_eligible"),
          "event_rate":scalar(con,f"SELECT avg(growth_breakout_event::INTEGER) FROM read_parquet({q(keyword_panel)}) WHERE horizon={h} AND growth_eligible"),
          "missing_future_entity_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE horizon={h} AND growth_eligible AND future_works=0"),
        }
    d["topic"]["realized_cohort_impact_3y"]={
      "rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE horizon=3 AND impact_eligible"),
      "years":scalar(con,f"SELECT count(DISTINCT signal_year) FROM read_parquet({q(topic_panel)}) WHERE horizon=3 AND impact_eligible"),
      "event_rate":scalar(con,f"SELECT avg(impact_breakout_event::INTEGER) FROM read_parquet({q(topic_panel)}) WHERE horizon=3 AND impact_eligible"),
    }
    d["keyword"]["realized_cohort_impact_3y"]={
      "rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE horizon=3 AND impact_eligible"),
      "years":scalar(con,f"SELECT count(DISTINCT signal_year) FROM read_parquet({q(keyword_panel)}) WHERE horizon=3 AND impact_eligible"),
      "event_rate":scalar(con,f"SELECT avg(impact_breakout_event::INTEGER) FROM read_parquet({q(keyword_panel)}) WHERE horizon=3 AND impact_eligible"),
    }
    return d

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",required=True)
    ap.add_argument("--output",required=True)
    args=ap.parse_args()
    inp=Path(args.input); out=Path(args.output); out.mkdir(parents=True,exist_ok=True)

    gate=json.loads((inp/"STAGE15_FINAL_GATE.json").read_text())
    if not gate.get("complete") or not gate.get("qa_pass"):
        raise RuntimeError("Stage 15 gate is not complete/PASS")

    topic_src=inp/"topic_early_signal_features.parquet"
    keyword_src=inp/"keyword_early_signal_features.parquet"
    for p in [topic_src,keyword_src]:
        if not p.exists(): raise FileNotFoundError(p)

    con=duckdb.connect("/tmp/openalex_stage16.duckdb")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='12GB'")
    con.execute("SET temp_directory='/tmp/openalex16_temp'")
    con.execute("SET max_temp_directory_size='20GB'")

    topic_panel=out/"topic_backtest_panel.parquet"
    keyword_panel=out/"keyword_backtest_panel.parquet"
    create_topic_panel(con,topic_src,topic_panel)
    create_keyword_panel(con,keyword_src,keyword_panel)

    topic_year_rows=evaluate_feature_years(con,topic_panel,"topic_id",TOPIC_FEATURES,TARGETS_TOPIC)
    keyword_year_rows=evaluate_feature_years(con,keyword_panel,"keyword_id",KEYWORD_FEATURES,TARGETS_KEYWORD)
    topic_summary=summarize(topic_year_rows)
    keyword_summary=summarize(keyword_year_rows)

    tcsv=out/"topic_feature_backtest_year.csv"; kcsv=out/"keyword_feature_backtest_year.csv"
    tscsv=out/"topic_feature_backtest_summary.csv"; kscsv=out/"keyword_feature_backtest_summary.csv"
    write_csv(tcsv,topic_year_rows); write_csv(kcsv,keyword_year_rows)
    write_csv(tscsv,topic_summary); write_csv(kscsv,keyword_summary)
    csv_to_parquet(con,tcsv,out/"topic_feature_backtest_year.parquet")
    csv_to_parquet(con,kcsv,out/"keyword_feature_backtest_year.parquet")
    csv_to_parquet(con,tscsv,out/"topic_feature_backtest_summary.parquet")
    csv_to_parquet(con,kscsv,out/"keyword_feature_backtest_summary.parquet")
    for p in [tcsv,kcsv,tscsv,kscsv]: p.unlink()

    diag=diagnostics(con,topic_panel,keyword_panel)
    diag["backtest_type"]="retrospective frozen-snapshot backtest; not a strict point-in-time OpenAlex vintage backtest"
    diag["taxonomy_caveat"]="Frozen 2026 taxonomy is applied retrospectively; Stage 17 will stress-test this and record-backfill bias."
    write_json(out/"STAGE16_TARGET_DIAGNOSTICS.json",diag)

    tsummary=out/"topic_feature_backtest_summary.parquet"
    ksummary=out/"keyword_feature_backtest_summary.parquet"
    qa={
      "stage15_complete":bool(gate.get("complete") and gate.get("qa_pass")),
      "topic_panel_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)})"),
      "keyword_panel_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)})"),
      "topic_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (horizon,signal_year,topic_id)) FROM read_parquet({q(topic_panel)})"),
      "keyword_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (horizon,signal_year,keyword_id)) FROM read_parquet({q(keyword_panel)})"),
      "topic_2026_signal_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE signal_year=2026"),
      "keyword_2026_signal_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE signal_year=2026"),
      "topic_target_after_2025_eligible_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE (growth_eligible OR convergence_eligible) AND target_year>2025"),
      "keyword_target_after_2025_eligible_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE growth_eligible AND target_year>2025"),
      "topic_impact_timing_violations":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE impact_eligible AND eventual_impact_cohort_year<>signal_year"),
      "keyword_impact_timing_violations":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE impact_eligible AND eventual_impact_cohort_year<>signal_year"),
      "topic_unavailable_impact_nonnull":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_panel)}) WHERE NOT impact_eligible AND eventual_cohort_mean_fwci IS NOT NULL"),
      "keyword_unavailable_impact_nonnull":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_panel)}) WHERE NOT impact_eligible AND eventual_cohort_mean_fwci IS NOT NULL"),
      "topic_summary_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (period,target,horizon,feature)) FROM read_parquet({q(tsummary)})"),
      "keyword_summary_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (period,target,horizon,feature)) FROM read_parquet({q(ksummary)})"),
      "topic_invalid_rho":scalar(con,f"SELECT count(*) FROM read_parquet({q(tsummary)}) WHERE mean_annual_spearman_rho IS NOT NULL AND (NOT isfinite(mean_annual_spearman_rho) OR abs(mean_annual_spearman_rho)>1.0000001)"),
      "keyword_invalid_rho":scalar(con,f"SELECT count(*) FROM read_parquet({q(ksummary)}) WHERE mean_annual_spearman_rho IS NOT NULL AND (NOT isfinite(mean_annual_spearman_rho) OR abs(mean_annual_spearman_rho)>1.0000001)"),
      "topic_invalid_rates":scalar(con,f"SELECT count(*) FROM read_parquet({q(tsummary)}) WHERE (base_event_rate IS NOT NULL AND (base_event_rate<0 OR base_event_rate>1)) OR (high_event_rate IS NOT NULL AND (high_event_rate<0 OR high_event_rate>1)) OR (low_event_rate IS NOT NULL AND (low_event_rate<0 OR low_event_rate>1))"),
      "keyword_invalid_rates":scalar(con,f"SELECT count(*) FROM read_parquet({q(ksummary)}) WHERE (base_event_rate IS NOT NULL AND (base_event_rate<0 OR base_event_rate>1)) OR (high_event_rate IS NOT NULL AND (high_event_rate<0 OR high_event_rate>1)) OR (low_event_rate IS NOT NULL AND (low_event_rate<0 OR low_event_rate>1))"),
      "topic_growth_3y_years":diag["topic"]["growth_3y"]["years"],
      "keyword_growth_3y_years":diag["keyword"]["growth_3y"]["years"],
      "topic_backtest_summary_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(tsummary)})"),
      "keyword_backtest_summary_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(ksummary)})"),
      "production_score_or_weights_emitted":False,
    }
    qa["pass"]=(
      qa["stage15_complete"]
      and qa["topic_panel_rows"]>0 and qa["keyword_panel_rows"]>0
      and qa["topic_duplicate_keys"]==0 and qa["keyword_duplicate_keys"]==0
      and qa["topic_2026_signal_rows"]==0 and qa["keyword_2026_signal_rows"]==0
      and qa["topic_target_after_2025_eligible_rows"]==0 and qa["keyword_target_after_2025_eligible_rows"]==0
      and qa["topic_impact_timing_violations"]==0 and qa["keyword_impact_timing_violations"]==0
      and qa["topic_unavailable_impact_nonnull"]==0 and qa["keyword_unavailable_impact_nonnull"]==0
      and qa["topic_summary_duplicate_keys"]==0 and qa["keyword_summary_duplicate_keys"]==0
      and qa["topic_invalid_rho"]==0 and qa["keyword_invalid_rho"]==0
      and qa["topic_invalid_rates"]==0 and qa["keyword_invalid_rates"]==0
      and qa["topic_growth_3y_years"]>=10 and qa["keyword_growth_3y_years"]>=10
      and qa["topic_backtest_summary_rows"]>0 and qa["keyword_backtest_summary_rows"]>0
      and not qa["production_score_or_weights_emitted"]
    )
    write_json(out/"STAGE16_VALIDATION.json",qa)

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="STAGE16_FINAL_GATE.json":
            item={"bytes":p.stat().st_size,"sha256":sha256_file(p)}
            if p.suffix==".parquet":
                item["rows"]=scalar(con,f"SELECT count(*) FROM read_parquet({q(p)})")
            outputs[p.name]=item

    gate16={
      "stage":16,
      "complete":bool(qa["pass"]),
      "release":RELEASE,
      "backtest_type":"retrospective frozen-snapshot",
      "topic_panel_rows":qa["topic_panel_rows"],
      "keyword_panel_rows":qa["keyword_panel_rows"],
      "topic_growth_3y_years":qa["topic_growth_3y_years"],
      "keyword_growth_3y_years":qa["keyword_growth_3y_years"],
      "topic_realized_impact_rows":diag["topic"]["realized_cohort_impact_3y"]["rows"],
      "keyword_realized_impact_rows":diag["keyword"]["realized_cohort_impact_3y"]["rows"],
      "model_weights_emitted":False,
      "qa_pass":bool(qa["pass"]),
      "outputs":outputs
    }
    write_json(out/"STAGE16_FINAL_GATE.json",gate16)

    # Compact descriptive report; detailed per-feature evidence stays in Parquet.
    topic_all=con.execute(f"""
      SELECT target,horizon,count(*) rows,
             avg(mean_annual_spearman_rho) avg_feature_rho,
             median(mean_annual_spearman_rho) median_feature_rho,
             avg(high_tail_lift) avg_high_tail_lift,
             avg(low_tail_lift) avg_low_tail_lift
      FROM read_parquet({q(tsummary)}) WHERE period='all'
      GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
    kw_all=con.execute(f"""
      SELECT target,horizon,count(*) rows,
             avg(mean_annual_spearman_rho) avg_feature_rho,
             median(mean_annual_spearman_rho) median_feature_rho,
             avg(high_tail_lift) avg_high_tail_lift,
             avg(low_tail_lift) avg_low_tail_lift
      FROM read_parquet({q(ksummary)}) WHERE period='all'
      GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
    report=f"""# OpenAlex Stage 16 — Historical Backtesting

Status: **{'COMPLETE' if gate16['complete'] else 'FAIL'}**

Frozen snapshot: {RELEASE}

## Backtest scope
- Topic panel rows: {qa['topic_panel_rows']:,}
- Keyword panel rows: {qa['keyword_panel_rows']:,}
- Topic 3-year growth signal years: {qa['topic_growth_3y_years']}
- Keyword 3-year growth signal years: {qa['keyword_growth_3y_years']}
- Topic realized-cohort impact rows: {diag['topic']['realized_cohort_impact_3y']['rows']:,}
- Keyword realized-cohort impact rows: {diag['keyword']['realized_cohort_impact_3y']['rows']:,}

## Targets
1. 3-year and 5-year future scholarly-share growth.
2. 3-year and 5-year future cross-domain convergence gain for topics.
3. 3-year realized citation impact of the signal-year publication cohort.

Each Stage 15 feature is evaluated separately using annual Spearman rank association and high/low-decile event lift. No final score, model weight, or production ranking is selected in Stage 16.

## Topic aggregate diagnostic tuples
{topic_all}

## Keyword aggregate diagnostic tuples
{kw_all}

## Interpretation boundary
This is a retrospective frozen-snapshot backtest, not a strict point-in-time OpenAlex-vintage simulation. Record backfill, retrospective taxonomy assignment, missingness, field heterogeneity, threshold sensitivity and negative controls are deferred to Stage 17.
"""
    (out/"OPENALEX_STAGE16_BACKTEST_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate16,indent=2))
    if not gate16["complete"]: raise SystemExit(2)

if __name__=="__main__": main()
