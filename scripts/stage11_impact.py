#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlex Stage 11 — Citation / FWCI / Impact Normalization

Consumes per-year/per-topic/per-keyword aggregate inputs from the combined
Stage 8–11 runner. It does NOT rescan the 476M Works snapshot by itself.

OpenAlex FWCI semantics (frozen into this research contract):
- citations received / citations expected
- expected over same publication year, work type, and primary subfield
- articles are further split by journal vs conference proceedings
- citation window = publication year + following 3 years
- 1.0 = world average for that normalized cohort

Stage-11 design separates:
1. lifetime popularity: cited_by_count
2. age-normalized citation percentile: cited_by_percentile_year
3. field/type/year normalized impact: fwci
4. normalized percentile/top-1/top-10 flags: citation_normalized_percentile
5. metric coverage / missingness
6. maturity of the 4-year citation window
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Any
import math
import json

SNAPSHOT_YEAR = 2026
FULL_FOUR_YEAR_WINDOW_THROUGH = 2022

@dataclass(frozen=True)
class ImpactConfig:
    min_works_for_aggregate: int = 25
    min_fwci_coverage: float = 0.50
    mature_year_max: int = FULL_FOUR_YEAR_WINDOW_THROUGH
    epsilon: float = 1e-12

DEFAULT_CONFIG = ImpactConfig()

def safe_div(n, d):
    if n is None or d is None or d == 0:
        return None
    return n / d

def maturity_class(publication_year: int | None) -> str:
    if publication_year is None:
        return "unknown"
    if publication_year <= FULL_FOUR_YEAR_WINDOW_THROUGH:
        return "full_4y_window"
    if publication_year == 2023:
        return "partial_4y_window"
    if publication_year in (2024, 2025):
        return "early_window"
    if publication_year >= 2026:
        return "frontier_ytd"
    return "unknown"

def aggregate_impact(row: Mapping[str, Any], config: ImpactConfig = DEFAULT_CONFIG) -> dict:
    year = int(row["year"])
    works = int(row["works"])
    fwci_n = int(row.get("fwci_nonmissing") or 0)
    np_n = int(row.get("normalized_percentile_nonmissing") or 0)
    agep_n = int(row.get("cited_by_percentile_nonmissing") or 0)

    fwci_cov = safe_div(fwci_n, works)
    normalized_cov = safe_div(np_n, works)
    agep_cov = safe_div(agep_n, works)

    out = {
        **dict(row),
        "maturity_class": maturity_class(year),
        "citations_per_work": safe_div(row.get("cited_by_sum"), works),
        "uncited_rate": safe_div(row.get("uncited_works"), works),
        "fwci_coverage": fwci_cov,
        "mean_fwci_scored": safe_div(row.get("fwci_sum"), fwci_n),
        "normalized_percentile_coverage": normalized_cov,
        "mean_normalized_percentile_scored": safe_div(row.get("normalized_percentile_sum"), np_n),
        "top_1_rate_scored": safe_div(row.get("top_1_percent_works"), np_n),
        "top_10_rate_scored": safe_div(row.get("top_10_percent_works"), np_n),
        "cited_by_percentile_coverage": agep_cov,
        "mean_age_percentile_scored": safe_div(row.get("cited_by_percentile_midpoint_sum"), agep_n),
        "low_volume_flag": works < config.min_works_for_aggregate,
        "low_fwci_coverage_flag": fwci_cov is None or fwci_cov < config.min_fwci_coverage,
        "eligible_for_primary_impact_comparison": (
            works >= config.min_works_for_aggregate
            and fwci_cov is not None
            and fwci_cov >= config.min_fwci_coverage
            and year <= config.mature_year_max
        ),
    }
    return out

def weighted_merge(rows: list[Mapping[str, Any]]) -> dict:
    keys_sum = [
        "works", "cited_by_sum", "uncited_works",
        "fwci_nonmissing", "fwci_sum",
        "normalized_percentile_nonmissing", "normalized_percentile_sum",
        "top_1_percent_works", "top_10_percent_works",
        "cited_by_percentile_nonmissing", "cited_by_percentile_midpoint_sum",
    ]
    out = {k: 0 for k in keys_sum}
    for r in rows:
        for k in keys_sum:
            out[k] += r.get(k) or 0
    return out

def relative_to_world(mean_fwci_scored):
    if mean_fwci_scored is None:
        return None
    return mean_fwci_scored

def impact_profile(row: Mapping[str, Any], config: ImpactConfig = DEFAULT_CONFIG) -> dict:
    a = aggregate_impact(row, config)
    a["fwci_relative_to_world"] = relative_to_world(a["mean_fwci_scored"])
    return a

def synthetic_self_test():
    a = impact_profile({
        "year": 2020,
        "works": 100,
        "cited_by_sum": 500,
        "uncited_works": 20,
        "fwci_nonmissing": 80,
        "fwci_sum": 96.0,
        "normalized_percentile_nonmissing": 80,
        "normalized_percentile_sum": 48.0,
        "top_1_percent_works": 2,
        "top_10_percent_works": 12,
        "cited_by_percentile_nonmissing": 100,
        "cited_by_percentile_midpoint_sum": 60.0,
    })
    assert abs(a["citations_per_work"] - 5.0) < 1e-12
    assert abs(a["fwci_coverage"] - 0.8) < 1e-12
    assert abs(a["mean_fwci_scored"] - 1.2) < 1e-12
    assert abs(a["top_10_rate_scored"] - 0.15) < 1e-12
    assert a["maturity_class"] == "full_4y_window"
    assert a["eligible_for_primary_impact_comparison"] is True

    b = impact_profile({
        "year": 2025,
        "works": 100,
        "cited_by_sum": 100,
        "uncited_works": 50,
        "fwci_nonmissing": 90,
        "fwci_sum": 100.0,
        "normalized_percentile_nonmissing": 90,
        "normalized_percentile_sum": 45.0,
        "top_1_percent_works": 1,
        "top_10_percent_works": 9,
        "cited_by_percentile_nonmissing": 100,
        "cited_by_percentile_midpoint_sum": 50.0,
    })
    assert b["maturity_class"] == "early_window"
    assert b["eligible_for_primary_impact_comparison"] is False

    c = impact_profile({
        "year": 2020,
        "works": 100,
        "cited_by_sum": 0,
        "uncited_works": 100,
        "fwci_nonmissing": 0,
        "fwci_sum": 0.0,
        "normalized_percentile_nonmissing": 0,
        "normalized_percentile_sum": 0.0,
        "top_1_percent_works": 0,
        "top_10_percent_works": 0,
        "cited_by_percentile_nonmissing": 100,
        "cited_by_percentile_midpoint_sum": 10.0,
    })
    assert c["mean_fwci_scored"] is None
    assert c["low_fwci_coverage_flag"] is True

    m = weighted_merge([
        {"works":10,"fwci_nonmissing":10,"fwci_sum":20},
        {"works":90,"fwci_nonmissing":90,"fwci_sum":90},
    ])
    assert m["works"] == 100
    assert abs(m["fwci_sum"]/m["fwci_nonmissing"] - 1.1) < 1e-12

    return {
        "pass": True,
        "tests": [
            "citation-per-work aggregation",
            "FWCI coverage and mean",
            "top-tail normalized percentile rate",
            "4-year maturity classification",
            "recent-cohort exclusion from primary mature comparison",
            "missing FWCI is not treated as zero",
            "weighted aggregate recomputation"
        ]
    }

if __name__ == "__main__":
    print(json.dumps(synthetic_self_test(), ensure_ascii=False, indent=2))
