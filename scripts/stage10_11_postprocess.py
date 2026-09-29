#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,hashlib,json,os,sys
from pathlib import Path
import duckdb
from stage10_growth import analyze_series,detect_episodes,synthetic_self_test as growth_test,DEFAULT_CONFIG
from stage11_impact import impact_profile,synthetic_self_test as impact_test

IMPACT_SUM_COLS=[
 'works','cited_by_sum','uncited_works','fwci_nonmissing','fwci_sum',
 'normalized_percentile_nonmissing','normalized_percentile_sum',
 'top_1_percent_works','top_10_percent_works','cited_by_percentile_nonmissing',
 'cited_by_percentile_midpoint_sum','institution_distinct_sum','country_distinct_sum']

def sha256_file(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()

def write_json(p,obj):
    Path(p).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding='utf-8')

def to_parquet(con,csv_path,parquet_path):
    con.execute(f"COPY (SELECT * FROM read_csv_auto('{str(csv_path).replace(chr(39),chr(39)*2)}',HEADER=TRUE,SAMPLE_SIZE=-1,NULLSTR='')) TO '{str(parquet_path).replace(chr(39),chr(39)*2)}' (FORMAT PARQUET,COMPRESSION ZSTD)")

def growth_for(con,query,out_prefix,outdir):
    cur=con.execute(query); cols=[d[0] for d in cur.description]; idx={c:i for i,c in enumerate(cols)}
    growth_csv=outdir/f'{out_prefix}_growth_year.csv'; episodes_csv=outdir/f'{out_prefix}_growth_episodes.csv'
    grow_fields=['corpus','entity_id','year','count','denominator','share','raw_yoy','share_yoy','count_cagr_3y','share_cagr_3y','count_cagr_5y','share_cagr_5y','share_log_robust_z','recent_share_log_slope','share_acceleration','event','frontier_ytd','complete_year','low_base_flag','growth_persistence_years','decline_persistence_years','persistent_growth','persistent_decline','eligible_for_episode_confirmation']
    ep_fields=['corpus','entity_id','kind','start_year','end_year','years','peak_abs_robust_z']; nrows=neps=0
    with growth_csv.open('w',newline='',encoding='utf-8') as gf, episodes_csv.open('w',newline='',encoding='utf-8') as ef:
        gw=csv.DictWriter(gf,fieldnames=grow_fields);gw.writeheader(); ew=csv.DictWriter(ef,fieldnames=ep_fields);ew.writeheader()
        key=None; buf=[]
        def flush(k,b):
            nonlocal nrows,neps
            if not b:return
            analyzed=analyze_series(b); corpus,eid=k
            for r in analyzed:
                gw.writerow({'corpus':corpus,'entity_id':eid,**{x:r.get(x) for x in grow_fields if x not in ('corpus','entity_id')}});nrows+=1
            for e in detect_episodes(analyzed):
                ew.writerow({'corpus':corpus,'entity_id':eid,**e});neps+=1
        while True:
            rows=cur.fetchmany(10000)
            if not rows:break
            for t in rows:
                k=(t[idx['corpus']],t[idx['entity_id']]); r={'year':int(t[idx['year']]),'count':int(t[idx['count']]),'denominator':float(t[idx['denominator']]) if t[idx['denominator']] is not None else None}
                if key is None:key=k
                if k!=key: flush(key,buf);buf=[];key=k
                buf.append(r)
        flush(key,buf)
    gp=outdir/f'{out_prefix}_growth_year.parquet'; ep=outdir/f'{out_prefix}_growth_episodes.parquet'
    to_parquet(con,growth_csv,gp);to_parquet(con,episodes_csv,ep);growth_csv.unlink();episodes_csv.unlink()
    return {'growth_rows':nrows,'episode_rows':neps}

def impact_copy(con,source_sql,outpath):
    q=f"""
    WITH x AS ({source_sql})
    SELECT *,
      CASE WHEN year<=2022 THEN 'full_4y_window' WHEN year=2023 THEN 'partial_4y_window' WHEN year IN (2024,2025) THEN 'early_window' WHEN year>=2026 THEN 'frontier_ytd' ELSE 'unknown' END maturity_class,
      cited_by_sum::DOUBLE/NULLIF(works,0) citations_per_work,
      uncited_works::DOUBLE/NULLIF(works,0) uncited_rate,
      fwci_nonmissing::DOUBLE/NULLIF(works,0) fwci_coverage,
      fwci_sum::DOUBLE/NULLIF(fwci_nonmissing,0) mean_fwci_scored,
      normalized_percentile_nonmissing::DOUBLE/NULLIF(works,0) normalized_percentile_coverage,
      normalized_percentile_sum::DOUBLE/NULLIF(normalized_percentile_nonmissing,0) mean_normalized_percentile_scored,
      top_1_percent_works::DOUBLE/NULLIF(normalized_percentile_nonmissing,0) top_1_rate_scored,
      top_10_percent_works::DOUBLE/NULLIF(normalized_percentile_nonmissing,0) top_10_rate_scored,
      cited_by_percentile_nonmissing::DOUBLE/NULLIF(works,0) cited_by_percentile_coverage,
      cited_by_percentile_midpoint_sum::DOUBLE/NULLIF(cited_by_percentile_nonmissing,0) mean_age_percentile_scored,
      works<25 low_volume_flag,
      (fwci_nonmissing::DOUBLE/NULLIF(works,0))<0.50 OR fwci_nonmissing=0 low_fwci_coverage_flag,
      (works>=25 AND fwci_nonmissing::DOUBLE/NULLIF(works,0)>=0.50 AND year<=2022) eligible_for_primary_impact_comparison,
      fwci_sum::DOUBLE/NULLIF(fwci_nonmissing,0) fwci_relative_to_world
    FROM x
    """
    con.execute(f"COPY ({q}) TO '{str(outpath)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
    return con.execute(f"SELECT count(*) FROM read_parquet('{str(outpath)}')").fetchone()[0]

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',required=True);ap.add_argument('--output',required=True);a=ap.parse_args()
    inp=Path(a.input);out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect();con.execute("SET threads=4");con.execute("SET preserve_insertion_order=false");con.execute("SET memory_limit='10GB'")
    base=inp/'base_dimensions_year.parquet'; topic=inp/'topic_year.parquet'; kw=inp/'keyword_year.parquet'
    for p in (base,topic,kw):
        if not p.exists():raise FileNotFoundError(p)
    gtest=growth_test();itest=impact_test()
    con.execute(f"CREATE VIEW base AS SELECT * FROM read_parquet('{base}')");con.execute(f"CREATE VIEW kw AS SELECT * FROM read_parquet('{kw}')")
    den="SELECT publication_year year,corpus,sum(works) denominator FROM base WHERE kind='universe' AND tier='A' GROUP BY 1,2"
    stats={}
    qtopic=f"""WITH d AS ({den}), s AS (SELECT publication_year year,corpus,id entity_id,sum(works) count FROM base WHERE kind='primary_topic' AND tier='A' GROUP BY 1,2,3) SELECT s.corpus,s.entity_id,s.year,s.count,d.denominator FROM s JOIN d USING(year,corpus) ORDER BY 1,2,3"""
    stats['topic']=growth_for(con,qtopic,'topic',out)
    qkw=f"""WITH d AS ({den}), s AS (SELECT publication_year year,corpus,keyword_id entity_id,sum(works) count FROM kw WHERE tier='A' GROUP BY 1,2,3) SELECT s.corpus,s.entity_id,s.year,s.count,d.denominator FROM s JOIN d USING(year,corpus) ORDER BY 1,2,3"""
    stats['keyword']=growth_for(con,qkw,'keyword',out)
    for kind,name in [('primary_domain','domain'),('primary_field','field'),('primary_subfield','subfield')]:
        q=f"""WITH d AS ({den}), s AS (SELECT publication_year year,corpus,id entity_id,sum(works) count FROM base WHERE kind='{kind}' AND tier='A' GROUP BY 1,2,3) SELECT s.corpus,s.entity_id,s.year,s.count,d.denominator FROM s JOIN d USING(year,corpus) ORDER BY 1,2,3"""
        stats[name]=growth_for(con,q,name,out)
    sumexpr=','.join(f'sum({c}) {c}' for c in IMPACT_SUM_COLS)
    impact_specs={
      'impact_coverage_by_year_corpus':f"SELECT publication_year year,corpus,tier,{sumexpr} FROM base WHERE kind='universe' GROUP BY 1,2,3",
      'primary_topic_impact_year':f"SELECT publication_year year,corpus,tier,id entity_id,{sumexpr} FROM base WHERE kind='primary_topic' GROUP BY 1,2,3,4",
      'primary_subfield_impact_year':f"SELECT publication_year year,corpus,tier,id entity_id,{sumexpr} FROM base WHERE kind='primary_subfield' GROUP BY 1,2,3,4",
      'primary_field_impact_year':f"SELECT publication_year year,corpus,tier,id entity_id,{sumexpr} FROM base WHERE kind='primary_field' GROUP BY 1,2,3,4",
      'primary_domain_impact_year':f"SELECT publication_year year,corpus,tier,id entity_id,{sumexpr} FROM base WHERE kind='primary_domain' GROUP BY 1,2,3,4",
      'keyword_impact_year':f"SELECT publication_year year,corpus,tier,keyword_id entity_id,{sumexpr} FROM kw GROUP BY 1,2,3,4",
    }
    impact_rows={}
    for name,q in impact_specs.items():impact_rows[name]=impact_copy(con,q,out/f'{name}.parquet')
    bias={}
    rows=con.execute("""SELECT corpus,sum(works),sum(fwci_nonmissing),sum(fwci_sum),sum(normalized_percentile_nonmissing),sum(cited_by_sum) FROM base WHERE kind='universe' AND publication_year BETWEEN 2000 AND 2025 GROUP BY corpus ORDER BY corpus""").fetchall()
    for corpus,works,fn,fs,nn,cs in rows:
        bias[corpus]={'works':works,'fwci_coverage':fn/works if works else None,'mean_fwci_scored':fs/fn if fn else None,'normalized_percentile_coverage':nn/works if works else None,'citations_per_work':cs/works if works else None}
    write_json(out/'growth_coverage_diagnostics.json',{'release':'2026-09-23','primary_universe':'Tier A, corpus separated, 2000-2025 complete + 2026 YTD frontier','stats':stats,'synthetic_validation':gtest})
    write_json(out/'growth_threshold_sensitivity.json',{'status':'deferred to Stage 17 robustness; Stage 10 defaults frozen','default_config':DEFAULT_CONFIG.__dict__})
    write_json(out/'impact_maturity_diagnostics.json',{'maturity':{'<=2022':'full_4y_window','2023':'partial_4y_window','2024-2025':'early_window','>=2026':'frontier_ytd'},'rows':impact_rows,'synthetic_validation':itest})
    write_json(out/'impact_coverage_bias.json',bias)
    stage6_baseline={'works_total':476_196_327,'cited_by_sum_total':2_929_611_340,'fwci_nonmissing_total':278_298_714,'fwci_missing_total':197_897_613}
    qa={}
    qa['input_rows']={'base_dimensions_year':con.execute('SELECT count(*) FROM base').fetchone()[0],'topic_year':con.execute(f"SELECT count(*) FROM read_parquet('{topic}')").fetchone()[0],'keyword_year':con.execute('SELECT count(*) FROM kw').fetchone()[0]}
    qa['analytic_universe']=dict(zip(['works','cited_by_sum','fwci_nonmissing','normalized_percentile_nonmissing'],con.execute("SELECT sum(works),sum(cited_by_sum),sum(fwci_nonmissing),sum(normalized_percentile_nonmissing) FROM base WHERE kind='universe'").fetchone()))
    qa['stage6_baseline']=stage6_baseline
    qa['stage6_compatibility']={'works_subset_le_full':qa['analytic_universe']['works']<=stage6_baseline['works_total'],'citation_sum_subset_le_full':qa['analytic_universe']['cited_by_sum']<=stage6_baseline['cited_by_sum_total'],'note':'Stage 8-11 aggregate is 2000-2026 non-retracted analytic subset; Stage 6 baseline is all frozen Works, so equality is not expected.'}
    violation_sql={
      'negative_or_null_universe':"SELECT count(*) FROM base WHERE kind='universe' AND (publication_year IS NULL OR corpus IS NULL OR works<0 OR cited_by_sum<0)",
      'fwci_coverage_invalid':"SELECT count(*) FROM base WHERE fwci_nonmissing<0 OR fwci_nonmissing>works",
      'normalized_percentile_coverage_invalid':"SELECT count(*) FROM base WHERE normalized_percentile_nonmissing<0 OR normalized_percentile_nonmissing>works",
      'cited_by_percentile_coverage_invalid':"SELECT count(*) FROM base WHERE cited_by_percentile_nonmissing<0 OR cited_by_percentile_nonmissing>works",
      'uncited_invalid':"SELECT count(*) FROM base WHERE uncited_works<0 OR uncited_works>works",
      'top_tail_invalid':"SELECT count(*) FROM base WHERE top_1_percent_works<0 OR top_10_percent_works<0 OR top_1_percent_works>top_10_percent_works OR top_10_percent_works>normalized_percentile_nonmissing",
      'tierA_denominator_nonpositive':"WITH d AS (SELECT publication_year,corpus,sum(works) denominator FROM base WHERE kind='universe' AND tier='A' GROUP BY 1,2) SELECT count(*) FROM d WHERE denominator<=0 OR denominator IS NULL",
    }
    qa['violations']={k:con.execute(v).fetchone()[0] for k,v in violation_sql.items()}
    qa['derived_uniqueness']={}
    for n,keys in [
      ('topic_growth_year','corpus,entity_id,year'),('keyword_growth_year','corpus,entity_id,year'),('domain_growth_year','corpus,entity_id,year'),('field_growth_year','corpus,entity_id,year'),('subfield_growth_year','corpus,entity_id,year'),
      ('impact_coverage_by_year_corpus','corpus,tier,year'),('primary_topic_impact_year','corpus,tier,entity_id,year'),('primary_subfield_impact_year','corpus,tier,entity_id,year'),('primary_field_impact_year','corpus,tier,entity_id,year'),('primary_domain_impact_year','corpus,tier,entity_id,year'),('keyword_impact_year','corpus,tier,entity_id,year')]:
        pp=out/f'{n}.parquet';qa['derived_uniqueness'][n]=con.execute(f"SELECT count(*)-count(DISTINCT ({keys})) FROM read_parquet('{pp}')").fetchone()[0]
    qa['formula_recalc']={
      'growth_share_max_abs_error':con.execute(f"SELECT max(abs(share-count::DOUBLE/nullif(denominator,0))) FROM read_parquet('{out/'topic_growth_year.parquet'}') WHERE share IS NOT NULL").fetchone()[0],
      'impact_fwci_coverage_max_abs_error':con.execute(f"SELECT max(abs(fwci_coverage-fwci_nonmissing::DOUBLE/nullif(works,0))) FROM read_parquet('{out/'impact_coverage_by_year_corpus.parquet'}') WHERE fwci_coverage IS NOT NULL").fetchone()[0],
      'impact_mean_fwci_max_abs_error':con.execute(f"SELECT max(abs(mean_fwci_scored-fwci_sum::DOUBLE/nullif(fwci_nonmissing,0))) FROM read_parquet('{out/'impact_coverage_by_year_corpus.parquet'}') WHERE mean_fwci_scored IS NOT NULL").fetchone()[0],
    }
    qa['semantic_contract']={'cited_by_count':'lifetime point-in-time citation count; age-sensitive','cited_by_percentile_year':'publication-year citation percentile','fwci':'field/type/year-normalized citation impact; missing is never imputed to zero','citation_normalized_percentile':'normalized percentile, kept separate from FWCI and raw citations','2026':'frontier YTD; excluded from confirmed growth episodes','primary_impact_maturity':'<=2022 only for full four-year-window primary comparisons'}
    qa_pass=(all(v==0 for v in qa['violations'].values()) and all(v==0 for v in qa['derived_uniqueness'].values()) and qa['stage6_compatibility']['works_subset_le_full'] and qa['stage6_compatibility']['citation_sum_subset_le_full'] and all((v is None or abs(v)<1e-12) for v in qa['formula_recalc'].values()))
    qa['pass']=qa_pass;write_json(out/'STAGE10_11_VALIDATION.json',qa)
    (out/'OPENALEX_STAGE10_11_POSTPROCESS_REPORT.md').write_text('# OpenAlex Stage 10-11 Derived Post-processing\n\n'+f"Status: **{'PASS' if qa_pass else 'FAIL'}**\n\n"+'- Stage 10: growth/share/burst/decline/persistence derived from frozen aggregate tables.\n- Stage 11: raw citations, same-year percentile, FWCI, and citation-normalized percentile remain semantically separate.\n- Missing FWCI is preserved as missing and is not converted to zero.\n- 2026 is frontier YTD; confirmed historical impact comparisons use mature windows only.\n- Stage 6 full-snapshot baseline is used as a subset-consistency bound, not as an equality target.\n',encoding='utf-8')
    files={p.name:{'bytes':p.stat().st_size,'sha256':sha256_file(p)} for p in sorted(out.iterdir()) if p.is_file()}
    gate={'stage10_complete':bool(gtest.get('pass')) and stats['topic']['growth_rows']>0 and stats['keyword']['growth_rows']>0 and qa_pass,'stage11_complete':bool(itest.get('pass')) and all(v>0 for v in impact_rows.values()) and qa_pass,'validation_pass':qa_pass,'release':'2026-09-23','files':files}
    gate['complete']=gate['stage10_complete'] and gate['stage11_complete'];write_json(out/'STAGE10_11_LIVE_GATE.json',gate);print(json.dumps(gate,indent=2))
    if not gate['complete']:raise SystemExit(2)
if __name__=='__main__':main()
