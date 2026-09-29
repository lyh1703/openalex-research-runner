#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import duckdb

EXPECTED_TOPIC_PAIR_ROWS=53_119_699

def q(x): return "'" + str(x).replace("'","''") + "'"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pair",required=True)
    ap.add_argument("--topic",required=True)
    ap.add_argument("--base",required=True)
    ap.add_argument("--topic-dim-glob",required=True)
    ap.add_argument("--output",required=True)
    a=ap.parse_args()
    out=Path(a.output); out.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    pair_rows=con.execute(f"SELECT count(*) FROM read_parquet({q(a.pair)})").fetchone()[0]
    prep={
      "expected_topic_pair_rows":EXPECTED_TOPIC_PAIR_ROWS,
      "input_topic_pair_rows":pair_rows,
      "input_row_count_match":pair_rows==EXPECTED_TOPIC_PAIR_ROWS,
      "source_self_pairs":con.execute(f"SELECT count(*) FROM read_parquet({q(a.pair)}) WHERE topic_id_a=topic_id_b").fetchone()[0],
      "source_null_pair_rows":con.execute(f"SELECT count(*) FROM read_parquet({q(a.pair)}) WHERE topic_id_a IS NULL OR topic_id_b IS NULL").fetchone()[0],
      "source_noncanonical_pair_rows":con.execute(f"SELECT count(*) FROM read_parquet({q(a.pair)}) WHERE topic_id_a>topic_id_b").fetchone()[0],
      "primary_source_rows":con.execute(f"SELECT count(*) FROM read_parquet({q(a.pair)}) WHERE corpus='core' AND tier='A' AND publication_year BETWEEN 2000 AND 2026 AND topic_id_a IS NOT NULL AND topic_id_b IS NOT NULL AND topic_id_a<>topic_id_b").fetchone()[0],
      "topic_year_rows":con.execute(f"SELECT count(*) FROM read_parquet({q(a.topic)})").fetchone()[0],
      "base_dimension_rows":con.execute(f"SELECT count(*) FROM read_parquet({q(a.base)})").fetchone()[0],
      "topic_dimension_rows":con.execute(f"SELECT count(*) FROM read_parquet({q(a.topic_dim_glob)},union_by_name=true)").fetchone()[0],
    }
    prep["pass"]=prep["input_row_count_match"] and prep["source_self_pairs"]==0 and prep["source_null_pair_rows"]==0
    (out/"STAGE14_PREPARE.json").write_text(json.dumps(prep,indent=2),encoding="utf-8")
    print(json.dumps(prep,indent=2))
    if not prep["pass"]: raise SystemExit(2)

if __name__=="__main__": main()
