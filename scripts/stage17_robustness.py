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

TOPIC_TARGETS=[
 ("growth",3,"future_share_growth","growth_eligible"),
 ("growth",5,"future_share_growth","growth_eligible"),
 ("cross_domain_convergence",3,"future_cross_domain_partner_delta","convergence_eligible"),
 ("cross_domain_convergence",5,"future_cross_domain_partner_delta","convergence_eligible"),
 ("realized_cohort_impact",3,"eventual_cohort_mean_fwci","impact_eligible"),
]
KEYWORD_TARGETS=[
 ("growth",3,"future_share_growth","growth_eligible"),
 ("growth",5,"future_share_growth","growth_eligible"),
 ("realized_cohort_impact",3,"eventual_cohort_mean_fwci","impact_eligible"),
]

VOLUME_THRESHOLDS=[25,50,100,250]
EVENT_THRESHOLDS=[0.80,0.90,0.95]
NEGATIVE_SEEDS=list(range(20))

def q(x): return "'" + str(x).replace("'","''") + "'"
def finite(x): return x is not None and isinstance(x,(int,float)) and math.isfinite(float(x))
def sign(x,eps=1e-12):
    if not finite(x) or abs(float(x))<=eps: return 0
    return 1 if float(x)>0 else -1
def sha256_file(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()
def write_json(p,obj): Path(p).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
def scalar(con,sql): return con.execute(sql).fetchone()[0]

def write_csv(path,rows):
    if not rows: raise RuntimeError(f"No rows produced for {path}")
    fields=[]
    for r in rows:
        for k in r:
            if k not in fields: fields.append(k)
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore")
        w.writeheader()
        for r in rows: w.writerow(r)

def csv_to_parquet(con,csv_path,out_path):
    con.execute(f"COPY (SELECT * FROM read_csv_auto({q(csv_path)},HEADER=TRUE,SAMPLE_SIZE=-1,NULLSTR='')) TO {q(out_path)} (FORMAT PARQUET,COMPRESSION ZSTD)")

def make_long(con,view_name,idcol,features,has_domain):
    domain_select=", domain_id, domain_name" if has_domain else ""
    parts=[]
    for feat in features:
        parts.append(f"""SELECT horizon,signal_year,target_year,{idcol} entity_id,signal_works,mature_impact_available{domain_select},
          {q(feat)} feature_name,CAST({feat} AS DOUBLE) feature_value,
          future_share_growth,growth_eligible,
          {"future_cross_domain_partner_delta,convergence_eligible," if has_domain else ""}
          eventual_cohort_mean_fwci,impact_eligible
          FROM {view_name}""")
    table=view_name+"_long"
    con.execute(f"CREATE TABLE {table} AS " + " UNION ALL ".join(parts))
    return table

def target_condition(target,outcome,eligible):
    return f"{eligible} AND {outcome} IS NOT NULL"

def annual_metrics_query(table,target,horizon,outcome,eligible,volume,extra_where="TRUE",has_domain=False,domain_mode=False):
    dom_cols=",domain_id,domain_name" if domain_mode else ""
    part_cols="signal_year,feature_name" + (",domain_id" if domain_mode else "")
    group_cols="signal_year,feature_name" + (",domain_id,domain_name" if domain_mode else "")
    return f"""
      WITH x AS (
        SELECT signal_year,feature_name,feature_value,{outcome} outcome_value{dom_cols}
        FROM {table}
        WHERE horizon={int(horizon)}
          AND {target_condition(target,outcome,eligible)}
          AND signal_works>={int(volume)}
          AND feature_value IS NOT NULL
          AND {extra_where}
          {"AND domain_id IS NOT NULL" if domain_mode else ""}
      ), r AS (
        SELECT *,
          percent_rank() OVER(PARTITION BY {part_cols} ORDER BY feature_value) feature_pct,
          percent_rank() OVER(PARTITION BY {part_cols} ORDER BY outcome_value) outcome_pct
        FROM x
      )
      SELECT {group_cols},count(*) n,
             corr(feature_pct,outcome_pct) spearman_rho,
             sum((outcome_pct>=0.90)::INTEGER) event_count,
             avg((outcome_pct>=0.90)::INTEGER) base_event_rate,
             count(*) FILTER(WHERE feature_pct>=0.90) high_n,
             sum((outcome_pct>=0.90)::INTEGER) FILTER(WHERE feature_pct>=0.90) high_event_count,
             avg((outcome_pct>=0.90)::INTEGER) FILTER(WHERE feature_pct>=0.90) high_event_rate,
             count(*) FILTER(WHERE feature_pct<=0.10) low_n,
             sum((outcome_pct>=0.90)::INTEGER) FILTER(WHERE feature_pct<=0.10) low_event_count,
             avg((outcome_pct>=0.90)::INTEGER) FILTER(WHERE feature_pct<=0.10) low_event_rate
      FROM r
      GROUP BY {group_cols}
      HAVING count(*)>=50
    """

def aggregate_annual(rows,entity_type,target,horizon,axis,level,domain=False):
    groups=defaultdict(list)
    for rec in rows:
        if domain:
            year,feature,domain_id,domain_name,n,rho,ec,ber,hn,hec,her,ln,lec,ler=rec
            key=(feature,domain_id,domain_name)
        else:
            year,feature,n,rho,ec,ber,hn,hec,her,ln,lec,ler=rec
            key=(feature,)
        groups[key].append({
            "year":year,"n":int(n),"rho":rho,"event_count":int(ec or 0),"base":ber,
            "high_n":int(hn or 0),"high_event_count":int(hec or 0),"high_rate":her,
            "low_n":int(ln or 0),"low_event_count":int(lec or 0),"low_rate":ler
        })
    out=[]
    for key,grp in groups.items():
        n=sum(x["n"] for x in grp); ec=sum(x["event_count"] for x in grp)
        hn=sum(x["high_n"] for x in grp); hec=sum(x["high_event_count"] for x in grp)
        ln=sum(x["low_n"] for x in grp); lec=sum(x["low_event_count"] for x in grp)
        base=ec/n if n else None; high=hec/hn if hn else None; low=lec/ln if ln else None
        rhos=[float(x["rho"]) for x in grp if finite(x["rho"])]
        row={
          "entity_type":entity_type,"target":target,"horizon":horizon,"axis":axis,"level":level,
          "feature":key[0],"years":len(grp),"first_year":min(x["year"] for x in grp),"last_year":max(x["year"] for x in grp),
          "n":n,"base_event_rate":base,
          "high_n":hn,"high_event_rate":high,"high_tail_lift":(high/base if base and high is not None else None),
          "low_n":ln,"low_event_rate":low,"low_tail_lift":(low/base if base and low is not None else None),
          "mean_annual_spearman_rho":statistics.fmean(rhos) if rhos else None,
          "median_annual_spearman_rho":statistics.median(rhos) if rhos else None,
          "positive_rho_year_fraction":sum(x>0 for x in rhos)/len(rhos) if rhos else None,
        }
        if domain:
            row["domain_id"]=key[1]; row["domain_name"]=key[2]
        out.append(row)
    return out

def volume_sensitivity(con,table,entity_type,targets,has_domain):
    out=[]; baseline_annual={}
    for target,horizon,outcome,eligible in targets:
        for v in VOLUME_THRESHOLDS:
            rows=con.execute(annual_metrics_query(table,target,horizon,outcome,eligible,v)).fetchall()
            agg=aggregate_annual(rows,entity_type,target,horizon,"volume",f"works>={v}")
            for r in agg: r["volume_threshold"]=v
            out.extend(agg)
            if v==50:
                baseline_annual[(target,horizon)]=rows
    return out,baseline_annual

def event_threshold_sensitivity(con,table,entity_type,targets):
    out=[]
    for target,horizon,outcome,eligible in targets:
        sql=f"""
          WITH x AS (
            SELECT signal_year,feature_name,feature_value,{outcome} outcome_value
            FROM {table}
            WHERE horizon={horizon} AND {eligible} AND {outcome} IS NOT NULL
              AND signal_works>=50 AND feature_value IS NOT NULL
          ), r AS (
            SELECT *,
              percent_rank() OVER(PARTITION BY signal_year,feature_name ORDER BY feature_value) feature_pct,
              percent_rank() OVER(PARTITION BY signal_year,feature_name ORDER BY outcome_value) outcome_pct
            FROM x
          ), th AS (SELECT * FROM (VALUES (0.80),(0.90),(0.95)) t(threshold)),
          y AS (
            SELECT threshold,signal_year,feature_name,count(*) n,
              corr(feature_pct,outcome_pct) spearman_rho,
              sum((outcome_pct>=threshold)::INTEGER) event_count,
              count(*) FILTER(WHERE feature_pct>=threshold) high_n,
              sum((outcome_pct>=threshold)::INTEGER) FILTER(WHERE feature_pct>=threshold) high_event_count,
              count(*) FILTER(WHERE feature_pct<=1-threshold) low_n,
              sum((outcome_pct>=threshold)::INTEGER) FILTER(WHERE feature_pct<=1-threshold) low_event_count
            FROM r CROSS JOIN th
            GROUP BY threshold,signal_year,feature_name
            HAVING count(*)>=50
          )
          SELECT threshold,feature_name,count(*) years,min(signal_year),max(signal_year),
                 sum(n) n,sum(event_count) event_count,
                 sum(high_n) high_n,sum(high_event_count) high_event_count,
                 sum(low_n) low_n,sum(low_event_count) low_event_count,
                 avg(spearman_rho) FILTER(WHERE isfinite(spearman_rho)) mean_rho,
                 median(spearman_rho) FILTER(WHERE isfinite(spearman_rho)) median_rho
          FROM y GROUP BY threshold,feature_name
        """
        for rec in con.execute(sql).fetchall():
            th,feature,years,fy,ly,n,ec,hn,hec,ln,lec,mrho,medrho=rec
            base=ec/n if n else None; high=hec/hn if hn else None; low=lec/ln if ln else None
            out.append({
              "entity_type":entity_type,"target":target,"horizon":horizon,"axis":"event_threshold",
              "level":f"top_{int(round((1-float(th))*100))}pct","event_threshold":float(th),"feature":feature,
              "years":years,"first_year":fy,"last_year":ly,"n":n,"base_event_rate":base,
              "high_n":hn,"high_event_rate":high,"high_tail_lift":(high/base if base and high is not None else None),
              "low_n":ln,"low_event_rate":low,"low_tail_lift":(low/base if base and low is not None else None),
              "mean_annual_spearman_rho":mrho,"median_annual_spearman_rho":medrho
            })
    return out

def temporal_stability(baseline_annual,entity_type):
    out=[]
    for (target,horizon),rows in baseline_annual.items():
        for period,pred in [
          ("2005_2014",lambda y:y<=2014),
          ("2015_plus",lambda y:y>=2015),
        ]:
            sub=[r for r in rows if pred(int(r[0]))]
            out.extend(aggregate_annual(sub,entity_type,target,horizon,"temporal",period))
    return out

def missingness_sensitivity(con,table,entity_type,targets):
    out=[]
    for target,horizon,outcome,eligible in targets:
        for level,where in [("all","TRUE"),("mature_impact_available","mature_impact_available")]:
            rows=con.execute(annual_metrics_query(table,target,horizon,outcome,eligible,50,where)).fetchall()
            out.extend(aggregate_annual(rows,entity_type,target,horizon,"missingness",level))
    return out

def domain_heterogeneity(con,table,targets):
    out=[]
    for target,horizon,outcome,eligible in targets:
        rows=con.execute(annual_metrics_query(table,target,horizon,outcome,eligible,50,"TRUE",domain_mode=True)).fetchall()
        out.extend(aggregate_annual(rows,"topic",target,horizon,"domain","within_domain",domain=True))
    return out

def negative_controls(con,panel_view,entity_type,idcol,targets):
    seed_rows=[]; envelope=[]
    for target,horizon,outcome,eligible in targets:
        sql=f"""
          WITH base AS (
            SELECT signal_year,{idcol} entity_id,{outcome} outcome_value
            FROM {panel_view}
            WHERE horizon={horizon} AND {eligible} AND {outcome} IS NOT NULL AND signal_works>=50
          ), seeds AS (SELECT * FROM range(0,20) t(seed)),
          x AS (
            SELECT b.*,s.seed,hash(cast(b.entity_id AS VARCHAR)||'|'||cast(b.signal_year AS VARCHAR)||'|'||cast(s.seed AS VARCHAR)) random_feature
            FROM base b CROSS JOIN seeds s
          ), r AS (
            SELECT *,
              percent_rank() OVER(PARTITION BY seed,signal_year ORDER BY random_feature) feature_pct,
              percent_rank() OVER(PARTITION BY seed,signal_year ORDER BY outcome_value) outcome_pct
            FROM x
          ), y AS (
            SELECT seed,signal_year,count(*) n,corr(feature_pct,outcome_pct) rho,
                   sum((outcome_pct>=0.90)::INTEGER) event_count,
                   count(*) FILTER(WHERE feature_pct>=0.90) high_n,
                   sum((outcome_pct>=0.90)::INTEGER) FILTER(WHERE feature_pct>=0.90) high_event_count,
                   count(*) FILTER(WHERE feature_pct<=0.10) low_n,
                   sum((outcome_pct>=0.90)::INTEGER) FILTER(WHERE feature_pct<=0.10) low_event_count
            FROM r GROUP BY seed,signal_year HAVING count(*)>=50
          )
          SELECT seed,count(*) years,min(signal_year),max(signal_year),
                 sum(n) n,sum(event_count) event_count,
                 sum(high_n) high_n,sum(high_event_count) high_event_count,
                 sum(low_n) low_n,sum(low_event_count) low_event_count,
                 avg(rho) FILTER(WHERE isfinite(rho)) mean_rho,
                 median(rho) FILTER(WHERE isfinite(rho)) median_rho
          FROM y GROUP BY seed ORDER BY seed
        """
        local=[]
        for rec in con.execute(sql).fetchall():
            seed,years,fy,ly,n,ec,hn,hec,ln,lec,mrho,medrho=rec
            base=ec/n if n else None; high=hec/hn if hn else None; low=lec/ln if ln else None
            row={
              "entity_type":entity_type,"target":target,"horizon":horizon,"seed":int(seed),
              "years":years,"first_year":fy,"last_year":ly,"n":n,
              "base_event_rate":base,"high_tail_lift":(high/base if base and high is not None else None),
              "low_tail_lift":(low/base if base and low is not None else None),
              "mean_annual_spearman_rho":mrho,"median_annual_spearman_rho":medrho
            }
            seed_rows.append(row); local.append(row)
        absrho=sorted(abs(float(r["mean_annual_spearman_rho"])) for r in local if finite(r["mean_annual_spearman_rho"]))
        liftdev=sorted(abs(float(r["high_tail_lift"])-1) for r in local if finite(r["high_tail_lift"]))
        lowdev=sorted(abs(float(r["low_tail_lift"])-1) for r in local if finite(r["low_tail_lift"]))
        def p95(xs):
            if not xs: return None
            idx=min(len(xs)-1,max(0,math.ceil(0.95*len(xs))-1))
            return xs[idx]
        envelope.append({
          "entity_type":entity_type,"target":target,"horizon":horizon,"seeds":len(local),
          "p95_abs_mean_rho":p95(absrho),
          "p95_abs_high_tail_lift_deviation":p95(liftdev),
          "p95_abs_low_tail_lift_deviation":p95(lowdev),
          "mean_abs_mean_rho":statistics.fmean(absrho) if absrho else None,
        })
    return seed_rows,envelope

def build_flags(volume_rows,event_rows,temporal_rows,missing_rows,domain_rows,envelopes):
    vol=defaultdict(dict); evt=defaultdict(dict); temp=defaultdict(dict); miss=defaultdict(dict); dom=defaultdict(list); env={}
    for r in volume_rows:
        key=(r["entity_type"],r["target"],r["horizon"],r["feature"]); vol[key][int(r["volume_threshold"])]=r
    for r in event_rows:
        key=(r["entity_type"],r["target"],r["horizon"],r["feature"]); evt[key][float(r["event_threshold"])]=r
    for r in temporal_rows:
        key=(r["entity_type"],r["target"],r["horizon"],r["feature"]); temp[key][r["level"]]=r
    for r in missing_rows:
        key=(r["entity_type"],r["target"],r["horizon"],r["feature"]); miss[key][r["level"]]=r
    for r in domain_rows:
        key=(r["entity_type"],r["target"],r["horizon"],r["feature"]); dom[key].append(r)
    for r in envelopes: env[(r["entity_type"],r["target"],r["horizon"])]=r

    out=[]
    for key,vmap in vol.items():
        if 50 not in vmap: continue
        base=vmap[50]; brho=base.get("mean_annual_spearman_rho"); bsign=sign(brho)
        vol_signs=[sign(vmap[v].get("mean_annual_spearman_rho")) for v in VOLUME_THRESHOLDS if v in vmap and sign(vmap[v].get("mean_annual_spearman_rho"))!=0]
        tmap=temp.get(key,{})
        early=tmap.get("2005_2014",{}).get("mean_annual_spearman_rho")
        late=tmap.get("2015_plus",{}).get("mean_annual_spearman_rho")
        emap=evt.get(key,{})
        hlifts=[emap[t].get("high_tail_lift") for t in EVENT_THRESHOLDS if t in emap and finite(emap[t].get("high_tail_lift"))]
        mmap=miss.get(key,{})
        allrho=mmap.get("all",{}).get("mean_annual_spearman_rho")
        avrho=mmap.get("mature_impact_available",{}).get("mean_annual_spearman_rho")
        envelope=env.get((key[0],key[1],key[2]),{})
        drows=dom.get(key,[])
        dsigns=[sign(r.get("mean_annual_spearman_rho")) for r in drows if sign(r.get("mean_annual_spearman_rho"))!=0]
        same_domain=None
        if dsigns and bsign!=0: same_domain=sum(s==bsign for s in dsigns)/len(dsigns)
        out.append({
          "entity_type":key[0],"target":key[1],"horizon":key[2],"feature":key[3],
          "baseline_mean_annual_spearman_rho":brho,
          "baseline_high_tail_lift":base.get("high_tail_lift"),
          "baseline_low_tail_lift":base.get("low_tail_lift"),
          "sign_stable_across_volume_thresholds":(len(vol_signs)>=3 and len(set(vol_signs))==1),
          "sign_stable_early_vs_late":(bsign!=0 and sign(early)==bsign and sign(late)==bsign),
          "event_threshold_high_tail_direction_stable":(len(hlifts)==len(EVENT_THRESHOLDS) and all((x-1)>0 for x in hlifts) or len(hlifts)==len(EVENT_THRESHOLDS) and all((x-1)<0 for x in hlifts)),
          "missingness_subset_sign_stable":(bsign!=0 and sign(allrho)==bsign and sign(avrho)==bsign),
          "domain_same_sign_fraction":same_domain,
          "above_random_null_abs_rho":(finite(brho) and finite(envelope.get("p95_abs_mean_rho")) and abs(float(brho))>float(envelope["p95_abs_mean_rho"])),
          "high_tail_lift_above_random_null":(finite(base.get("high_tail_lift")) and finite(envelope.get("p95_abs_high_tail_lift_deviation")) and abs(float(base["high_tail_lift"])-1)>float(envelope["p95_abs_high_tail_lift_deviation"])),
          "low_tail_lift_above_random_null":(finite(base.get("low_tail_lift")) and finite(envelope.get("p95_abs_low_tail_lift_deviation")) and abs(float(base["low_tail_lift"])-1)>float(envelope["p95_abs_low_tail_lift_deviation"])),
          "null_p95_abs_mean_rho":envelope.get("p95_abs_mean_rho"),
          "null_p95_abs_high_tail_lift_deviation":envelope.get("p95_abs_high_tail_lift_deviation"),
          "strict_point_in_time_validity":False,
        })
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",required=True)
    ap.add_argument("--output",required=True)
    args=ap.parse_args()
    inp=Path(args.input); out=Path(args.output); out.mkdir(parents=True,exist_ok=True)

    gate=json.loads((inp/"STAGE16_FINAL_GATE.json").read_text())
    if not gate.get("complete") or not gate.get("qa_pass"):
        raise RuntimeError("Stage 16 gate not PASS")
    topic=inp/"topic_backtest_panel.parquet"; keyword=inp/"keyword_backtest_panel.parquet"
    for p in [topic,keyword]:
        if not p.exists(): raise FileNotFoundError(p)

    con=duckdb.connect("/tmp/openalex_stage17.duckdb")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='12GB'")
    con.execute("SET temp_directory='/tmp/openalex17_temp'")
    con.execute("SET max_temp_directory_size='20GB'")
    con.execute(f"CREATE VIEW topic_panel AS SELECT * FROM read_parquet({q(topic)})")
    con.execute(f"CREATE VIEW keyword_panel AS SELECT * FROM read_parquet({q(keyword)})")

    topic_long=make_long(con,"topic_panel","topic_id",TOPIC_FEATURES,True)
    keyword_long=make_long(con,"keyword_panel","keyword_id",KEYWORD_FEATURES,False)

    topic_vol,topic_baseline=volume_sensitivity(con,topic_long,"topic",TOPIC_TARGETS,True)
    keyword_vol,keyword_baseline=volume_sensitivity(con,keyword_long,"keyword",KEYWORD_TARGETS,False)
    volume_rows=topic_vol+keyword_vol

    event_rows=event_threshold_sensitivity(con,topic_long,"topic",TOPIC_TARGETS)+event_threshold_sensitivity(con,keyword_long,"keyword",KEYWORD_TARGETS)
    temporal_rows=temporal_stability(topic_baseline,"topic")+temporal_stability(keyword_baseline,"keyword")
    missing_rows=missingness_sensitivity(con,topic_long,"topic",TOPIC_TARGETS)+missingness_sensitivity(con,keyword_long,"keyword",KEYWORD_TARGETS)
    domain_rows=domain_heterogeneity(con,topic_long,TOPIC_TARGETS)

    neg_topic,env_topic=negative_controls(con,"topic_panel","topic","topic_id",TOPIC_TARGETS)
    neg_keyword,env_keyword=negative_controls(con,"keyword_panel","keyword","keyword_id",KEYWORD_TARGETS)
    neg_rows=neg_topic+neg_keyword; envelopes=env_topic+env_keyword

    flags=build_flags(volume_rows,event_rows,temporal_rows,missing_rows,domain_rows,envelopes)

    datasets=[
      ("volume_sensitivity",volume_rows),
      ("event_threshold_sensitivity",event_rows),
      ("temporal_stability",temporal_rows),
      ("missingness_sensitivity",missing_rows),
      ("topic_domain_heterogeneity",domain_rows),
      ("negative_control_seed_results",neg_rows),
      ("negative_control_envelope",envelopes),
      ("feature_robustness_flags",flags),
    ]
    for name,rows in datasets:
        csvp=out/f"{name}.csv"; parq=out/f"{name}.parquet"
        write_csv(csvp,rows); csv_to_parquet(con,csvp,parq); csvp.unlink()

    limitations={
      "release":RELEASE,
      "strict_point_in_time_openalex_vintage_available":False,
      "status":"NOT TESTABLE FROM A SINGLE SNAPSHOT",
      "unresolved_biases":[
        "OpenAlex records may have been backfilled or corrected after the historical signal year.",
        "The frozen 2026-09-23 topic taxonomy is applied retrospectively to historical Works.",
        "Absence from a future entity-year aggregate is treated as zero future share/count for growth outcomes.",
        "Random negative controls calibrate the analysis procedure but do not establish causality.",
        "Field/domain heterogeneity and coverage filters can reduce sample sizes and precision."
      ],
      "mitigations_executed":[
        "early-vs-late temporal split",
        "minimum-volume sensitivity",
        "future-event threshold sensitivity",
        "mature-impact missingness subset",
        "within-domain topic tests",
        "20 deterministic random-feature negative controls per target/horizon"
      ],
      "interpretation":"Stage 16/17 support retrospective association and transfer screening, not a claim of strict historical real-time predictability."
    }
    write_json(out/"STAGE17_LIMITATIONS.json",limitations)

    # Validation.
    volp=out/"volume_sensitivity.parquet"; evtp=out/"event_threshold_sensitivity.parquet"; tempp=out/"temporal_stability.parquet"
    missp=out/"missingness_sensitivity.parquet"; domp=out/"topic_domain_heterogeneity.parquet"; negp=out/"negative_control_seed_results.parquet"
    envp=out/"negative_control_envelope.parquet"; flagp=out/"feature_robustness_flags.parquet"
    qa={
      "stage16_complete":bool(gate.get("complete") and gate.get("qa_pass")),
      "topic_2026_signal_rows":scalar(con,"SELECT count(*) FROM topic_panel WHERE signal_year=2026"),
      "keyword_2026_signal_rows":scalar(con,"SELECT count(*) FROM keyword_panel WHERE signal_year=2026"),
      "topic_future_after_2025_eligible_rows":scalar(con,"SELECT count(*) FROM topic_panel WHERE target_year>2025 AND (growth_eligible OR convergence_eligible OR impact_eligible)"),
      "keyword_future_after_2025_eligible_rows":scalar(con,"SELECT count(*) FROM keyword_panel WHERE target_year>2025 AND (growth_eligible OR impact_eligible)"),
      "volume_thresholds":sorted(int(x[0]) for x in con.execute(f"SELECT DISTINCT volume_threshold FROM read_parquet({q(volp)})").fetchall()),
      "event_thresholds":sorted(float(x[0]) for x in con.execute(f"SELECT DISTINCT event_threshold FROM read_parquet({q(evtp)})").fetchall()),
      "temporal_levels":sorted(str(x[0]) for x in con.execute(f"SELECT DISTINCT level FROM read_parquet({q(tempp)})").fetchall()),
      "missingness_levels":sorted(str(x[0]) for x in con.execute(f"SELECT DISTINCT level FROM read_parquet({q(missp)})").fetchall()),
      "domain_null_labels":scalar(con,f"SELECT count(*) FROM read_parquet({q(domp)}) WHERE domain_id IS NULL OR domain_name IS NULL"),
      "negative_seed_min":scalar(con,f"SELECT min(seed) FROM read_parquet({q(negp)})"),
      "negative_seed_max":scalar(con,f"SELECT max(seed) FROM read_parquet({q(negp)})"),
      "negative_seed_group_bad_counts":scalar(con,f"SELECT count(*) FROM (SELECT entity_type,target,horizon,count(DISTINCT seed) n FROM read_parquet({q(negp)}) GROUP BY 1,2,3 HAVING n<>20)"),
      "negative_nonfinite_rho":scalar(con,f"SELECT count(*) FROM read_parquet({q(negp)}) WHERE mean_annual_spearman_rho IS NOT NULL AND NOT isfinite(mean_annual_spearman_rho)"),
      "negative_nonfinite_high_lift":scalar(con,f"SELECT count(*) FROM read_parquet({q(negp)}) WHERE high_tail_lift IS NOT NULL AND NOT isfinite(high_tail_lift)"),
      "envelope_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(envp)})"),
      "feature_flag_rows":scalar(con,f"SELECT count(*) FROM read_parquet({q(flagp)})"),
      "volume_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (entity_type,target,horizon,feature,volume_threshold)) FROM read_parquet({q(volp)})"),
      "event_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (entity_type,target,horizon,feature,event_threshold)) FROM read_parquet({q(evtp)})"),
      "temporal_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (entity_type,target,horizon,feature,level)) FROM read_parquet({q(tempp)})"),
      "flag_duplicate_keys":scalar(con,f"SELECT count(*)-count(DISTINCT (entity_type,target,horizon,feature)) FROM read_parquet({q(flagp)})"),
      "strict_point_in_time_limitation_recorded":limitations["strict_point_in_time_openalex_vintage_available"] is False and limitations["status"]=="NOT TESTABLE FROM A SINGLE SNAPSHOT",
      "composite_robustness_score_emitted":False,
      "production_weights_emitted":False,
    }
    qa["pass"]=(
      qa["stage16_complete"]
      and qa["topic_2026_signal_rows"]==0 and qa["keyword_2026_signal_rows"]==0
      and qa["topic_future_after_2025_eligible_rows"]==0 and qa["keyword_future_after_2025_eligible_rows"]==0
      and qa["volume_thresholds"]==VOLUME_THRESHOLDS
      and qa["event_thresholds"]==EVENT_THRESHOLDS
      and qa["temporal_levels"]==["2005_2014","2015_plus"]
      and qa["missingness_levels"]==["all","mature_impact_available"]
      and qa["domain_null_labels"]==0
      and qa["negative_seed_min"]==0 and qa["negative_seed_max"]==19
      and qa["negative_seed_group_bad_counts"]==0
      and qa["negative_nonfinite_rho"]==0 and qa["negative_nonfinite_high_lift"]==0
      and qa["envelope_rows"]>0 and qa["feature_flag_rows"]>0
      and qa["volume_duplicate_keys"]==0 and qa["event_duplicate_keys"]==0 and qa["temporal_duplicate_keys"]==0 and qa["flag_duplicate_keys"]==0
      and qa["strict_point_in_time_limitation_recorded"]
      and not qa["composite_robustness_score_emitted"] and not qa["production_weights_emitted"]
    )
    write_json(out/"STAGE17_VALIDATION.json",qa)

    outputs={}
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="STAGE17_FINAL_GATE.json":
            item={"bytes":p.stat().st_size,"sha256":sha256_file(p)}
            if p.suffix==".parquet": item["rows"]=scalar(con,f"SELECT count(*) FROM read_parquet({q(p)})")
            outputs[p.name]=item

    # Descriptive counts only; not a ranking.
    flag_counts=con.execute(f"""
      SELECT entity_type,target,horizon,count(*) feature_target_pairs,
             sum(above_random_null_abs_rho::INTEGER) above_random_null_abs_rho,
             sum(sign_stable_across_volume_thresholds::INTEGER) sign_stable_volume,
             sum(sign_stable_early_vs_late::INTEGER) sign_stable_time
      FROM read_parquet({q(flagp)})
      GROUP BY 1,2,3 ORDER BY 1,2,3
    """).fetchall()

    gate17={
      "stage":17,"complete":bool(qa["pass"]),"release":RELEASE,
      "robustness_axes":["volume","event_threshold","temporal","missingness","domain_heterogeneity","deterministic_random_negative_controls"],
      "random_seeds_per_target_horizon":20,
      "feature_target_flag_rows":qa["feature_flag_rows"],
      "strict_point_in_time_validity_established":False,
      "strict_point_in_time_limitation":"NOT TESTABLE FROM A SINGLE SNAPSHOT",
      "composite_robustness_score_emitted":False,
      "qa_pass":bool(qa["pass"]),"outputs":outputs
    }
    write_json(out/"STAGE17_FINAL_GATE.json",gate17)

    report=f"""# OpenAlex Stage 17 — Bias / Negative Controls / Robustness

Status: **{'COMPLETE' if gate17['complete'] else 'FAIL'}**

Frozen snapshot: {RELEASE}

## Executed stress tests
- minimum signal volume: 25 / 50 / 100 / 250 Works
- future-event definitions: top 20% / 10% / 5%
- early-vs-late temporal split: 2005–2014 vs 2015+
- mature-impact-available subset vs full eligible universe
- within-domain topic tests
- 20 deterministic random-feature negative controls for every target/horizon

## Diagnostic coverage
- Feature/target robustness-flag rows: {qa['feature_flag_rows']:,}
- Random-control envelopes: {qa['envelope_rows']:,}

## Feature/target diagnostic counts
{flag_counts}

These counts are descriptive diagnostics, not a ranking and not a composite score.

## Critical limitation
Strict point-in-time predictive validity is **NOT TESTABLE FROM A SINGLE SNAPSHOT**. OpenAlex record backfill and the frozen 2026 taxonomy applied retrospectively can make a historical signal look cleaner than it would have appeared at the time. Stage 17 quantifies several forms of instability and calibrates the procedure with negative controls, but it does not eliminate this vintage-data limitation.

## Next use
Stage 18 may construct a transferability matrix for ATLAS / NOUS / Academic Research Landscape / Lab Graph and related applied layers, but it must carry forward the Stage 17 limitations and must not treat retrospective association as causal proof.
"""
    (out/"OPENALEX_STAGE17_ROBUSTNESS_REPORT.md").write_text(report,encoding="utf-8")
    print(json.dumps(gate17,indent=2))
    if not gate17["complete"]: raise SystemExit(2)

if __name__=="__main__":
    main()
