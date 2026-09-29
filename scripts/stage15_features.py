#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import duckdb

RELEASE="2026-09-23"

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
    ap.add_argument("--s10",required=True)
    ap.add_argument("--s13",required=True)
    ap.add_argument("--s14",required=True)
    ap.add_argument("--topic-year",required=True)
    ap.add_argument("--topic-dim-glob",required=True)
    ap.add_argument("--output",required=True)
    args=ap.parse_args()

    s10=Path(args.s10); s13=Path(args.s13); s14=Path(args.s14)
    topic_year=Path(args.topic_year); out=Path(args.output); out.mkdir(parents=True,exist_ok=True)

    # Upstream gates.
    g10=json.loads((s10/"STAGE10_11_LIVE_GATE.json").read_text())
    g13=json.loads((s13/"STAGE13_FINAL_GATE.json").read_text())
    g14=json.loads((s14/"STAGE14_FINAL_GATE.json").read_text())
    upstream={
        "stage10_11_complete":bool(g10.get("complete")),
        "stage13_complete":bool(g13.get("complete")),
        "stage14_complete":bool(g14.get("complete")),
    }
    if not all(upstream.values()):
        raise RuntimeError(f"Upstream gate failed: {upstream}")

    con=duckdb.connect("/tmp/openalex_stage15.duckdb")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='12GB'")
    con.execute("SET temp_directory='/tmp/openalex15_temp'")
    con.execute("SET max_temp_directory_size='20GB'")

    tg=s10/"topic_growth_year.parquet"
    ti=s10/"primary_topic_impact_year.parquet"
    kg=s10/"keyword_growth_year.parquet"
    ki=s10/"keyword_impact_year.parquet"
    tc=s14/"topic_convergence_year.parquet"
    inst_hist=s13/"institutions_count_hist.parquet"
    country_hist=s13/"countries_count_hist.parquet"
    author_hist=s13/"authors_count_hist.parquet"
    inst_activity=s13/"institution_activity_year.parquet"
    country_activity=s13/"country_activity_year.parquet"
    country_pairs=s13/"country_pair_year.parquet"
    for p in [tg,ti,kg,ki,tc,topic_year,inst_hist,country_hist,author_hist,inst_activity,country_activity,country_pairs]:
        if not p.exists(): raise FileNotFoundError(p)

    # Frozen topic hierarchy.
    con.execute(f"""
      CREATE TABLE topic_dim AS
      SELECT DISTINCT id topic_id, display_name topic_name,
             subfield.id subfield_id, subfield.display_name subfield_name,
             field.id field_id, field.display_name field_name,
             domain.id domain_id, domain.display_name domain_name
      FROM read_parquet({q(args.topic_dim_glob)},union_by_name=true)
      WHERE id IS NOT NULL
    """)

    # Same-year global collaboration context.
    con.execute(f"""
      CREATE TABLE year_context AS
      WITH a AS (
        SELECT pub_year AS year,corpus,tier,
               sum(authors_count*works)::DOUBLE/nullif(sum(works),0) global_avg_authors_per_work,
               sum(works) analytic_works
        FROM read_parquet({q(author_hist)})
        GROUP BY 1,2,3
      ), i AS (
        SELECT pub_year AS year,corpus,tier,
               sum(institutions_distinct_count*works)::DOUBLE/nullif(sum(works),0) global_avg_institutions_per_work
        FROM read_parquet({q(inst_hist)})
        GROUP BY 1,2,3
      ), c AS (
        SELECT pub_year AS year,corpus,tier,
               sum(countries_distinct_count*works)::DOUBLE/nullif(sum(works),0) global_avg_countries_per_work
        FROM read_parquet({q(country_hist)})
        GROUP BY 1,2,3
      ), ai AS (
        SELECT pub_year AS year,corpus,tier,count(DISTINCT institution_id) active_institutions
        FROM read_parquet({q(inst_activity)})
        GROUP BY 1,2,3
      ), ac AS (
        SELECT pub_year AS year,corpus,tier,count(DISTINCT country_code) active_countries
        FROM read_parquet({q(country_activity)})
        GROUP BY 1,2,3
      ), cp AS (
        SELECT pub_year AS year,corpus,tier,
               sum(works) country_pair_work_events,
               count(*) country_pair_keys
        FROM read_parquet({q(country_pairs)})
        GROUP BY 1,2,3
      )
      SELECT a.year,a.corpus,a.tier,a.analytic_works,
             a.global_avg_authors_per_work,
             i.global_avg_institutions_per_work,
             c.global_avg_countries_per_work,
             ai.active_institutions,ac.active_countries,
             cp.country_pair_work_events,cp.country_pair_keys,
             cp.country_pair_work_events::DOUBLE/nullif(a.analytic_works,0) global_country_pair_events_per_work,
             a.year=2026 frontier_ytd
      FROM a
      LEFT JOIN i USING(year,corpus,tier)
      LEFT JOIN c USING(year,corpus,tier)
      LEFT JOIN ai USING(year,corpus,tier)
      LEFT JOIN ac USING(year,corpus,tier)
      LEFT JOIN cp USING(year,corpus,tier)
      WHERE a.corpus='core' AND a.tier='A' AND a.year BETWEEN 2000 AND 2026
    """)
    con.execute(f"COPY year_context TO {q(out/'year_context_features.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")

    # Multi-label topic breadth layer from Stage 9.
    con.execute(f"""
      CREATE TABLE topic_breadth AS
      SELECT publication_year AS year, topic_id,
             sum(works) multilabel_topic_works,
             sum(institution_distinct_sum) institution_incidence_sum,
             sum(country_distinct_sum) country_incidence_sum,
             sum(primary_assignment_count) primary_assignment_count,
             sum(institution_distinct_sum)::DOUBLE/nullif(sum(works),0) avg_institutions_per_multilabel_work,
             sum(country_distinct_sum)::DOUBLE/nullif(sum(works),0) avg_countries_per_multilabel_work
      FROM read_parquet({q(topic_year)})
      WHERE corpus='core' AND tier='A' AND publication_year BETWEEN 2000 AND 2026
      GROUP BY 1,2
    """)

    # Topic base features. Mature impact is lagged exactly three years.
    con.execute(f"""
      CREATE TABLE topic_base AS
      SELECT
        g.year,
        g.entity_id topic_id,
        d.topic_name,d.subfield_id,d.subfield_name,d.field_id,d.field_name,d.domain_id,d.domain_name,
        g.count primary_topic_works,
        g.denominator core_tierA_denominator,
        g.share,
        g.raw_yoy,
        g.share_yoy,
        g.count_cagr_3y,
        g.share_cagr_3y,
        g.count_cagr_5y,
        g.share_cagr_5y,
        g.share_log_robust_z,
        g.recent_share_log_slope,
        g.share_acceleration,
        g.event growth_event,
        g.low_base_flag,
        g.growth_persistence_years,
        g.decline_persistence_years,
        g.persistent_growth,
        g.persistent_decline,
        g.complete_year,
        g.eligible_for_episode_confirmation,
        g.year=2026 frontier_ytd,

        b.multilabel_topic_works,
        b.primary_assignment_count,
        b.avg_institutions_per_multilabel_work,
        b.avg_countries_per_multilabel_work,

        cv.active_partners,
        cv.topic_pair_coassignment_events,
        cv.novel_partners,
        cv.emerging_partners,
        cv.cross_field_partners,
        cv.cross_domain_partners,
        cv.median_pair_lift,
        cv.max_pair_lift,
        cv.novel_partners::DOUBLE/nullif(cv.active_partners,0) novel_partner_share,
        cv.emerging_partners::DOUBLE/nullif(cv.active_partners,0) emerging_partner_share,
        cv.cross_field_partners::DOUBLE/nullif(cv.active_partners,0) cross_field_partner_share,
        cv.cross_domain_partners::DOUBLE/nullif(cv.active_partners,0) cross_domain_partner_share,
        cv.topic_pair_coassignment_events::DOUBLE/nullif(b.multilabel_topic_works,0) coassignment_event_intensity,

        yc.global_avg_authors_per_work,
        yc.global_avg_institutions_per_work,
        yc.global_avg_countries_per_work,
        yc.active_institutions global_active_institutions,
        yc.active_countries global_active_countries,
        yc.global_country_pair_events_per_work,
        b.avg_institutions_per_multilabel_work/nullif(yc.global_avg_institutions_per_work,0) institution_breadth_relative_global,
        b.avg_countries_per_multilabel_work/nullif(yc.global_avg_countries_per_work,0) country_breadth_relative_global,

        g.year-3 mature_impact_cohort_year,
        mi.works mature_impact_cohort_works,
        mi.fwci_coverage mature_fwci_coverage,
        mi.mean_fwci_scored mature_mean_fwci,
        mi.normalized_percentile_coverage mature_normalized_percentile_coverage,
        mi.mean_normalized_percentile_scored mature_mean_normalized_percentile,
        mi.top_1_rate_scored mature_top_1_rate,
        mi.top_10_rate_scored mature_top_10_rate,
        mi.cited_by_percentile_coverage mature_cited_by_percentile_coverage,
        mi.mean_age_percentile_scored mature_mean_age_percentile,
        mi.citations_per_work mature_citations_per_work,
        mi.uncited_rate mature_uncited_rate,
        mi.eligible_for_primary_impact_comparison mature_impact_source_eligible,
        (mi.entity_id IS NOT NULL AND mi.year=g.year-3
          AND mi.maturity_class='full_4y_window'
          AND mi.eligible_for_primary_impact_comparison) mature_impact_available,

        (g.count_cagr_3y IS NOT NULL) growth_3y_available,
        (g.count_cagr_5y IS NOT NULL) growth_5y_available,
        (cv.topic_id IS NOT NULL) convergence_available,
        (b.topic_id IS NOT NULL) breadth_available
      FROM read_parquet({q(tg)}) g
      LEFT JOIN topic_dim d ON d.topic_id=g.entity_id
      LEFT JOIN topic_breadth b ON b.year=g.year AND b.topic_id=g.entity_id
      LEFT JOIN read_parquet({q(tc)}) cv ON cv.publication_year=g.year AND cv.topic_id=g.entity_id
      LEFT JOIN year_context yc ON yc.year=g.year
      LEFT JOIN read_parquet({q(ti)}) mi
        ON mi.corpus='core' AND mi.tier='A'
       AND mi.entity_id=g.entity_id AND mi.year=g.year-3
       AND mi.maturity_class='full_4y_window'
       AND mi.eligible_for_primary_impact_comparison
      WHERE g.corpus='core' AND g.year BETWEEN 2003 AND 2026
    """)

    # Cross-sectional percentiles calculated only among non-missing observations.
    percentile_specs=[
      ("share","share_percentile_year"),
      ("share_cagr_3y","share_cagr_3y_percentile_year"),
      ("share_log_robust_z","share_robust_z_percentile_year"),
      ("active_partners","active_partners_percentile_year"),
      ("cross_domain_partners","cross_domain_partners_percentile_year"),
      ("mature_mean_fwci","mature_fwci_percentile_year"),
      ("mature_top_10_rate","mature_top10_percentile_year"),
      ("institution_breadth_relative_global","institution_breadth_percentile_year"),
      ("country_breadth_relative_global","country_breadth_percentile_year"),
    ]
    pct_joins=[]
    pct_select=[]
    for i,(col,outcol) in enumerate(percentile_specs):
        alias=f"p{i}"
        con.execute(f"""
          CREATE TABLE {alias} AS
          SELECT year,topic_id,percent_rank() OVER(PARTITION BY year ORDER BY {col}) {outcol}
          FROM topic_base WHERE {col} IS NOT NULL
        """)
        pct_joins.append(f"LEFT JOIN {alias} USING(year,topic_id)")
        pct_select.append(outcol)

    con.execute(f"""
      CREATE TABLE topic_features AS
      SELECT b.*,
             ln(1+primary_topic_works) log1p_primary_topic_works,
             ln(1+coalesce(active_partners,0)) log1p_active_partners,
             {','.join(pct_select)}
      FROM topic_base b
      {' '.join(pct_joins)}
    """)

    # Backtest-safe output: all current-cohort impact fields are intentionally absent.
    con.execute(f"""
      COPY (
        SELECT * FROM topic_features
        ORDER BY year,topic_id
      ) TO {q(out/'topic_early_signal_features.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    # Snapshot enrichment: same-year impact for current descriptive screening only.
    con.execute(f"""
      COPY (
        SELECT f.year,f.topic_id,f.topic_name,f.field_id,f.field_name,f.domain_id,f.domain_name,
               ci.maturity_class current_impact_maturity_class,
               ci.works current_impact_works,
               ci.fwci_coverage current_fwci_coverage,
               ci.mean_fwci_scored current_mean_fwci,
               ci.normalized_percentile_coverage current_normalized_percentile_coverage,
               ci.mean_normalized_percentile_scored current_mean_normalized_percentile,
               ci.top_1_rate_scored current_top_1_rate,
               ci.top_10_rate_scored current_top_10_rate,
               ci.citations_per_work current_citations_per_work,
               ci.uncited_rate current_uncited_rate,
               false backtest_safe,
               'same-publication-cohort impact from frozen 2026-09-23 snapshot; descriptive/current screening only' timing_note
        FROM topic_features f
        LEFT JOIN read_parquet({q(ti)}) ci
          ON ci.corpus='core' AND ci.tier='A' AND ci.entity_id=f.topic_id AND ci.year=f.year
        ORDER BY f.year,f.topic_id
      ) TO {q(out/'topic_snapshot_enrichment.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    # Keyword backtest-safe feature matrix.
    con.execute(f"""
      CREATE TABLE keyword_base AS
      SELECT
        g.year,
        g.entity_id keyword_id,
        g.count keyword_works,
        g.denominator core_tierA_denominator,
        g.share,
        g.raw_yoy,
        g.share_yoy,
        g.count_cagr_3y,
        g.share_cagr_3y,
        g.count_cagr_5y,
        g.share_cagr_5y,
        g.share_log_robust_z,
        g.recent_share_log_slope,
        g.share_acceleration,
        g.event growth_event,
        g.low_base_flag,
        g.growth_persistence_years,
        g.decline_persistence_years,
        g.persistent_growth,
        g.persistent_decline,
        g.complete_year,
        g.eligible_for_episode_confirmation,
        g.year=2026 frontier_ytd,
        g.year-3 mature_impact_cohort_year,
        mi.works mature_impact_cohort_works,
        mi.fwci_coverage mature_fwci_coverage,
        mi.mean_fwci_scored mature_mean_fwci,
        mi.normalized_percentile_coverage mature_normalized_percentile_coverage,
        mi.mean_normalized_percentile_scored mature_mean_normalized_percentile,
        mi.top_1_rate_scored mature_top_1_rate,
        mi.top_10_rate_scored mature_top_10_rate,
        mi.cited_by_percentile_coverage mature_cited_by_percentile_coverage,
        mi.mean_age_percentile_scored mature_mean_age_percentile,
        mi.citations_per_work mature_citations_per_work,
        mi.uncited_rate mature_uncited_rate,
        (mi.entity_id IS NOT NULL AND mi.year=g.year-3
          AND mi.maturity_class='full_4y_window'
          AND mi.eligible_for_primary_impact_comparison) mature_impact_available,
        (g.count_cagr_3y IS NOT NULL) growth_3y_available,
        (g.count_cagr_5y IS NOT NULL) growth_5y_available
      FROM read_parquet({q(kg)}) g
      LEFT JOIN read_parquet({q(ki)}) mi
        ON mi.corpus='core' AND mi.tier='A'
       AND mi.entity_id=g.entity_id AND mi.year=g.year-3
       AND mi.maturity_class='full_4y_window'
       AND mi.eligible_for_primary_impact_comparison
      WHERE g.corpus='core' AND g.year BETWEEN 2003 AND 2026
    """)
    con.execute("""
      CREATE TABLE kp_share AS
      SELECT year,keyword_id,percent_rank() OVER(PARTITION BY year ORDER BY share) share_percentile_year
      FROM keyword_base WHERE share IS NOT NULL
    """)
    con.execute("""
      CREATE TABLE kp_cagr AS
      SELECT year,keyword_id,percent_rank() OVER(PARTITION BY year ORDER BY share_cagr_3y) share_cagr_3y_percentile_year
      FROM keyword_base WHERE share_cagr_3y IS NOT NULL
    """)
    con.execute("""
      CREATE TABLE kp_fwci AS
      SELECT year,keyword_id,percent_rank() OVER(PARTITION BY year ORDER BY mature_mean_fwci) mature_fwci_percentile_year
      FROM keyword_base WHERE mature_mean_fwci IS NOT NULL
    """)
    con.execute(f"""
      COPY (
        SELECT k.*,ln(1+keyword_works) log1p_keyword_works,
               ps.share_percentile_year,pc.share_cagr_3y_percentile_year,pf.mature_fwci_percentile_year
        FROM keyword_base k
        LEFT JOIN kp_share ps USING(year,keyword_id)
        LEFT JOIN kp_cagr pc USING(year,keyword_id)
        LEFT JOIN kp_fwci pf USING(year,keyword_id)
        ORDER BY year,keyword_id
      ) TO {q(out/'keyword_early_signal_features.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)
    """)

    topic_out=out/"topic_early_signal_features.parquet"
    keyword_out=out/"keyword_early_signal_features.parquet"
    snapshot_out=out/"topic_snapshot_enrichment.parquet"

    # Feature dictionary.
    dictionary={
      "release":RELEASE,
      "primary_universe":"Core + Tier A",
      "timing_contract":{
        "signal_time":"end of publication year t",
        "growth":"year t and prior only",
        "convergence":"pair history through year t only",
        "breadth_network":"year t",
        "mature_impact":"publication cohort t-3, full_4y_window and eligible_for_primary_impact_comparison only",
        "snapshot_enrichment":"same-year/current frozen snapshot impact; not backtest-safe",
        "retrospective_caveat":"OpenAlex record backfill and frozen 2026 taxonomy can still create retrospective-data bias; Stage 17 handles robustness."
      },
      "topic_families":{
        "momentum":["primary_topic_works","share","raw_yoy","share_yoy","count_cagr_3y","share_cagr_3y","count_cagr_5y","share_cagr_5y","share_log_robust_z","recent_share_log_slope","share_acceleration","growth_persistence_years","persistent_growth"],
        "mature_impact":["mature_fwci_coverage","mature_mean_fwci","mature_mean_normalized_percentile","mature_top_1_rate","mature_top_10_rate","mature_mean_age_percentile","mature_citations_per_work","mature_uncited_rate"],
        "convergence":["active_partners","novel_partners","emerging_partners","cross_field_partners","cross_domain_partners","median_pair_lift","max_pair_lift","novel_partner_share","emerging_partner_share","cross_field_partner_share","cross_domain_partner_share","coassignment_event_intensity"],
        "breadth":["avg_institutions_per_multilabel_work","avg_countries_per_multilabel_work","institution_breadth_relative_global","country_breadth_relative_global"],
        "global_context":["global_avg_authors_per_work","global_avg_institutions_per_work","global_avg_countries_per_work","global_active_institutions","global_active_countries","global_country_pair_events_per_work"],
        "cross_sectional":["share_percentile_year","share_cagr_3y_percentile_year","share_robust_z_percentile_year","active_partners_percentile_year","cross_domain_partners_percentile_year","mature_fwci_percentile_year","mature_top10_percentile_year","institution_breadth_percentile_year","country_breadth_percentile_year"]
      },
      "semantic_notes":{
        "primary_topic_works":"exclusive primary-topic count from Stage 10",
        "multilabel_topic_works":"multi-label topics[] assignment count from Stage 9; not interchangeable with primary_topic_works",
        "institution_country_breadth":"sum of per-Work distinct incidence divided by topic Works; not global unique entity count",
        "country_pair_work_events":"pair-event count, not unique Works",
        "no_composite_score":True,
        "stage12_direct_topic_network_feature":"not emitted because retained Stage 12 topology is not an exact topic-keyed historical graph"
      }
    }
    write_json(out/"STAGE15_FEATURE_DICTIONARY.json",dictionary)

    # Validation.
    topic_cols=[r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q(topic_out)})").fetchall()]
    keyword_cols=[r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q(keyword_out)})").fetchall()]
    banned=[c for c in topic_cols if c.startswith("current_") or c in {"early_signal_score","composite_score","rank_score"}]
    pct_cols=[x[1] for x in percentile_specs]
    pct_condition=" OR ".join([f"({c} IS NOT NULL AND ({c}<0 OR {c}>1))" for c in pct_cols])
    qa={
      "upstream":upstream,
      "topic_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)})"),
      "keyword_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_out)})"),
      "snapshot_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(snapshot_out)})"),
      "year_context_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(out/'year_context_features.parquet')})"),
      "topic_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (year,topic_id)) FROM read_parquet({q(topic_out)})"),
      "keyword_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (year,keyword_id)) FROM read_parquet({q(keyword_out)})"),
      "convergence_duplicate_source_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (publication_year,topic_id)) FROM read_parquet({q(tc)})"),
      "topic_2026_nonfrontier":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE year=2026 AND NOT frontier_ytd"),
      "keyword_2026_nonfrontier":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_out)}) WHERE year=2026 AND NOT frontier_ytd"),
      "topic_mature_impact_timing_violations":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE mature_impact_available AND mature_impact_cohort_year<>year-3"),
      "keyword_mature_impact_timing_violations":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_out)}) WHERE mature_impact_available AND mature_impact_cohort_year<>year-3"),
      "topic_percentile_out_of_range":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE {pct_condition}"),
      "keyword_percentile_out_of_range":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_out)}) WHERE (share_percentile_year IS NOT NULL AND (share_percentile_year<0 OR share_percentile_year>1)) OR (share_cagr_3y_percentile_year IS NOT NULL AND (share_cagr_3y_percentile_year<0 OR share_cagr_3y_percentile_year>1)) OR (mature_fwci_percentile_year IS NOT NULL AND (mature_fwci_percentile_year<0 OR mature_fwci_percentile_year>1))"),
      "invalid_topic_breadth_ratio":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE (institution_breadth_relative_global IS NOT NULL AND global_avg_institutions_per_work<=0) OR (country_breadth_relative_global IS NOT NULL AND global_avg_countries_per_work<=0)"),
      "backtest_safe_banned_columns":banned,
      "current_snapshot_backtest_safe_true":scalar(con,f"SELECT count(*) FROM read_parquet({q(snapshot_out)}) WHERE backtest_safe"),
      "topic_hierarchy_missing_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE topic_name IS NULL"),
      "topic_mature_impact_available_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE mature_impact_available"),
      "keyword_mature_impact_available_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(keyword_out)}) WHERE mature_impact_available"),
      "topic_convergence_available_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE convergence_available"),
      "topic_breadth_available_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE breadth_available"),
      "primary_assignment_vs_growth_mismatch_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(topic_out)}) WHERE primary_assignment_count IS NOT NULL AND primary_assignment_count<>primary_topic_works"),
    }
    qa["pass"]=(
      all(qa["upstream"].values())
      and qa["topic_rows"]>0 and qa["keyword_rows"]>0
      and qa["topic_duplicate_keys"]==0 and qa["keyword_duplicate_keys"]==0
      and qa["convergence_duplicate_source_keys"]==0
      and qa["topic_2026_nonfrontier"]==0 and qa["keyword_2026_nonfrontier"]==0
      and qa["topic_mature_impact_timing_violations"]==0 and qa["keyword_mature_impact_timing_violations"]==0
      and qa["topic_percentile_out_of_range"]==0 and qa["keyword_percentile_out_of_range"]==0
      and qa["invalid_topic_breadth_ratio"]==0
      and len(qa["backtest_safe_banned_columns"])==0
      and qa["current_snapshot_backtest_safe_true"]==0
      and qa["topic_mature_impact_available_rows"]>0
      and qa["keyword_mature_impact_available_rows"]>0
      and qa["topic_convergence_available_rows"]>0
      and qa["topic_breadth_available_rows"]>0
    )
    write_json(out/"STAGE15_VALIDATION.json",qa)

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="STAGE15_FINAL_GATE.json":
            item={"bytes":p.stat().st_size,"sha256":sha256_file(p)}
            if p.suffix==".parquet":
                item["rows"]=scalar(con,f"SELECT count(*) FROM read_parquet({q(p)})")
            outputs[p.name]=item

    gate={
      "stage":15,
      "complete":bool(qa["pass"]),
      "release":RELEASE,
      "primary_universe":"Core + Tier A",
      "feature_timing":"end-year t; mature impact cohort t-3 only in backtest-safe matrices",
      "topic_rows":qa["topic_rows"],
      "keyword_rows":qa["keyword_rows"],
      "topic_mature_impact_available_rows":qa["topic_mature_impact_available_rows"],
      "keyword_mature_impact_available_rows":qa["keyword_mature_impact_available_rows"],
      "topic_convergence_available_rows":qa["topic_convergence_available_rows"],
      "topic_breadth_available_rows":qa["topic_breadth_available_rows"],
      "frontier_year":2026,
      "composite_score_emitted":False,
      "qa_pass":bool(qa["pass"]),
      "outputs":outputs
    }
    write_json(out/"STAGE15_FINAL_GATE.json",gate)

    report=f"""# OpenAlex Stage 15 — Early-Signal Feature Engineering

Status: **{'COMPLETE' if gate['complete'] else 'FAIL'}**

Frozen snapshot: {RELEASE}

## Outputs
- Topic backtest-safe feature rows: {qa['topic_rows']:,}
- Keyword backtest-safe feature rows: {qa['keyword_rows']:,}
- Topic rows with lagged mature impact available: {qa['topic_mature_impact_available_rows']:,}
- Topic rows with convergence features: {qa['topic_convergence_available_rows']:,}
- Topic rows with collaboration-breadth features: {qa['topic_breadth_available_rows']:,}

## Timing rule
Historical signal year t uses growth/convergence/network information through t and mature impact only from publication cohort t-3. Same-year current-snapshot impact is isolated in a separate enrichment table and explicitly marked not backtest-safe.

## Feature philosophy
No opaque early-signal score or ranking is emitted. Momentum, mature impact, convergence, collaboration breadth, global network context, and cross-sectional transforms remain separate features so Stage 16 can test predictive value rather than assume weights in advance.

## Important caveat
The timing contract prevents direct future-citation leakage, but OpenAlex record backfill and use of the frozen 2026 taxonomy can still create retrospective-data bias. Stage 17 robustness analysis must test these effects.
"""
    (out/"OPENALEX_STAGE15_EARLY_SIGNAL_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate,indent=2))
    if not gate["complete"]:
        raise SystemExit(2)

if __name__=="__main__":
    main()
