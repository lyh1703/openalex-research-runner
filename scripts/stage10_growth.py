#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlex Stage 10 — Growth / Burst / Decline Analytics

Consumes the aggregate tables produced by the combined Stage 8–11 runner.
It never rescans the 476M Works snapshot.

Key separations
---------------
- raw publication-volume growth
- denominator-adjusted share growth
- acceleration
- robust burst / decline shock
- persistence
- low-base noise
- 2026 YTD frontier status

Citation/impact is intentionally deferred to Stage 11.
Prediction is intentionally deferred to Stage 15.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence
import math
import statistics
import json

COMPLETE_YEAR_START = 2000
COMPLETE_YEAR_END = 2025
FRONTIER_YEAR = 2026

@dataclass(frozen=True)
class GrowthConfig:
    short_window: int = 3
    medium_window: int = 5
    trailing_baseline_window: int = 5
    min_count_for_rate: int = 25
    min_count_for_burst: int = 50
    min_years_for_trend: int = 5
    robust_z_burst: float = 3.5
    robust_z_decline: float = -3.5
    min_share_change_burst: float = 0.20
    max_share_change_decline: float = -0.20
    min_persistent_years: int = 2
    log_pseudocount: float = 1.0

DEFAULT_CONFIG = GrowthConfig()

def safe_share(count, denominator):
    if count is None or denominator is None or denominator <= 0:
        return None
    return count / denominator

def pct_change(current, previous):
    if current is None or previous is None or previous <= 0:
        return None
    return current / previous - 1.0

def cagr(current, previous, years, min_base=25):
    if current is None or previous is None or years <= 0 or previous < min_base or current < 0:
        return None
    return (current / previous) ** (1.0 / years) - 1.0

def linear_slope(xs, ys):
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mx=statistics.mean(xs); my=statistics.mean(ys)
    den=sum((x-mx)**2 for x in xs)
    if den == 0:
        return None
    return sum((x-mx)*(y-my) for x,y in zip(xs,ys))/den

def log_slope(years, values, pseudocount=1.0):
    pairs=[(y,v) for y,v in zip(years,values) if v is not None and v >= 0]
    if len(pairs)<2:
        return None
    return linear_slope(
        [float(y) for y,_ in pairs],
        [math.log(v+pseudocount) for _,v in pairs]
    )

def median_abs_deviation(values):
    if not values:
        return 0.0
    med=statistics.median(values)
    return statistics.median([abs(x-med) for x in values])

def robust_z(value, baseline):
    vals=[float(v) for v in baseline if v is not None and math.isfinite(v)]
    if value is None or len(vals)<3:
        return None
    med=statistics.median(vals)
    mad=median_abs_deviation(vals)
    if mad==0:
        return 0.0 if value==med else None
    return 0.6744897501960817*(value-med)/mad

def trailing_robust_z(series, idx, window):
    if idx<=0:
        return None
    return robust_z(series[idx],series[max(0,idx-window):idx])

def classify_event(*, count, previous_count, share, previous_share, share_robust_z, config=DEFAULT_CONFIG):
    if previous_count is None or previous_share is None or share is None:
        return "insufficient_history"
    if previous_count < config.min_count_for_rate:
        return "low_base"
    sc=pct_change(share,previous_share)
    if sc is None:
        return "insufficient_history"
    if count >= config.min_count_for_burst and share_robust_z is not None:
        if share_robust_z >= config.robust_z_burst and sc >= config.min_share_change_burst:
            return "burst"
        if share_robust_z <= config.robust_z_decline and sc <= config.max_share_change_decline:
            return "decline_shock"
    if sc>0:return "growth"
    if sc<0:return "decline"
    return "flat"

def consecutive_flags(flags):
    out=[]; run=0
    for x in flags:
        run=run+1 if x else 0
        out.append(run)
    return out

def analyze_series(rows, config=DEFAULT_CONFIG):
    rows=sorted(rows,key=lambda r:r["year"])
    years=[int(r["year"]) for r in rows]
    counts=[int(r["count"]) for r in rows]
    denoms=[float(r["denominator"]) if r.get("denominator") is not None else None for r in rows]
    shares=[safe_share(c,d) for c,d in zip(counts,denoms)]
    logshares=[math.log(s) if s is not None and s>0 else None for s in shares]

    gflags=[]; dflags=[]; out=[]
    for i,r in enumerate(rows):
        prev_count=counts[i-1] if i>0 else None
        prev_share=shares[i-1] if i>0 else None
        rz=trailing_robust_z(logshares,i,config.trailing_baseline_window)
        event=classify_event(
            count=counts[i],previous_count=prev_count,
            share=shares[i],previous_share=prev_share,
            share_robust_z=rz,config=config
        )
        gflags.append(event in ("growth","burst"))
        dflags.append(event in ("decline","decline_shock"))

        c3=s3=c5=s5=None
        if i>=config.short_window:
            c3=cagr(counts[i],counts[i-config.short_window],config.short_window,config.min_count_for_rate)
            s0=shares[i-config.short_window];s1=shares[i]
            if s0 is not None and s0>0 and s1 is not None:
                s3=(s1/s0)**(1/config.short_window)-1
        if i>=config.medium_window:
            c5=cagr(counts[i],counts[i-config.medium_window],config.medium_window,config.min_count_for_rate)
            s0=shares[i-config.medium_window];s1=shares[i]
            if s0 is not None and s0>0 and s1 is not None:
                s5=(s1/s0)**(1/config.medium_window)-1

        recent_start=max(0,i-config.short_window+1)
        prior_end=recent_start
        prior_start=max(0,prior_end-config.short_window)
        recent=log_slope(years[recent_start:i+1],shares[recent_start:i+1],config.log_pseudocount)
        prior=log_slope(years[prior_start:prior_end],shares[prior_start:prior_end],config.log_pseudocount) if prior_end-prior_start>=2 else None
        accel=recent-prior if recent is not None and prior is not None else None

        out.append({
            **r,
            "share":shares[i],
            "raw_yoy":pct_change(counts[i],prev_count),
            "share_yoy":pct_change(shares[i],prev_share),
            "count_cagr_3y":c3,"share_cagr_3y":s3,
            "count_cagr_5y":c5,"share_cagr_5y":s5,
            "share_log_robust_z":rz,
            "recent_share_log_slope":recent,
            "share_acceleration":accel,
            "event":event,
            "frontier_ytd":years[i]==FRONTIER_YEAR,
            "complete_year":COMPLETE_YEAR_START<=years[i]<=COMPLETE_YEAR_END,
            "low_base_flag":prev_count is not None and prev_count<config.min_count_for_rate
        })

    gp=consecutive_flags(gflags);dp=consecutive_flags(dflags)
    for i,row in enumerate(out):
        row["growth_persistence_years"]=gp[i]
        row["decline_persistence_years"]=dp[i]
        row["persistent_growth"]=gp[i]>=config.min_persistent_years
        row["persistent_decline"]=dp[i]>=config.min_persistent_years
        row["eligible_for_episode_confirmation"]=row["complete_year"]
    return out

def detect_episodes(rows):
    episodes=[];cur=None
    for r in rows:
        if not r.get("complete_year"):
            continue
        kind=None
        if r["event"]=="burst" or r.get("persistent_growth"):kind="growth"
        elif r["event"]=="decline_shock" or r.get("persistent_decline"):kind="decline"
        if kind is None:
            if cur:episodes.append(cur);cur=None
            continue
        if cur and cur["kind"]==kind and r["year"]==cur["end_year"]+1:
            cur["end_year"]=r["year"];cur["years"]+=1
            cur["peak_abs_robust_z"]=max(cur["peak_abs_robust_z"],abs(r.get("share_log_robust_z") or 0))
        else:
            if cur:episodes.append(cur)
            cur={"kind":kind,"start_year":r["year"],"end_year":r["year"],"years":1,
                 "peak_abs_robust_z":abs(r.get("share_log_robust_z") or 0)}
    if cur:episodes.append(cur)
    return episodes

def synthetic_self_test():
    rows=[]
    counts=[100,110,120,130,140,150,165,180,500,700,900,1000]
    for y,c in zip(range(2014,2026),counts):
        rows.append({"year":y,"count":c,"denominator":100000})
    a=analyze_series(rows)
    assert len(a)==12
    assert abs(a[-1]["share"]-0.01)<1e-12
    assert any(r["event"]=="burst" for r in a)

    low=analyze_series([
        {"year":2020,"count":1,"denominator":100000},
        {"year":2021,"count":2,"denominator":100000},
        {"year":2022,"count":5,"denominator":100000},
        {"year":2023,"count":50,"denominator":100000},
    ])
    assert low[-1]["event"]=="low_base"

    frontier=analyze_series([
        {"year":2024,"count":100,"denominator":10000},
        {"year":2025,"count":120,"denominator":10000},
        {"year":2026,"count":90,"denominator":7000},
    ])
    assert frontier[-1]["frontier_ytd"] is True
    assert frontier[-1]["eligible_for_episode_confirmation"] is False

    mix=analyze_series([
        {"year":2024,"count":100,"denominator":1000},
        {"year":2025,"count":110,"denominator":1500},
    ])
    assert mix[-1]["raw_yoy"]>0
    assert mix[-1]["share_yoy"]<0

    return {
        "pass":True,
        "tests":[
            "share normalization",
            "robust burst detection",
            "low-base suppression",
            "2026 YTD exclusion",
            "raw-growth/share-decline separation"
        ]
    }

if __name__=="__main__":
    print(json.dumps(synthetic_self_test(),ensure_ascii=False,indent=2))
