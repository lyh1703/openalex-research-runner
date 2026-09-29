#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json, os, time, urllib.request
from pathlib import Path
import duckdb

RELEASE="2026-09-23"
WORKS_SHA="a72ff4b43c9d13d0911386e1a5dd1b9d71b3c1080ac44ccfc016e11d6390a059"
WORKS_ROWS=476_196_327
MANIFEST_URL="https://openalex.s3.amazonaws.com/data/parquet/works/manifest.json"
HTTPS="https://openalex.s3.amazonaws.com"
NULL_AUTHOR="https://openalex.org/A9999999999"
DELETED_AUTHOR="https://openalex.org/A5317838346"
TIER_A={"article","conference-paper","preprint","data-paper","software-paper"}
TIER_B={"review","book","book-chapter","dissertation","report","dataset","software","standard"}
TIER_C={"conference-abstract","editorial","letter","book-review","peer-review","reference-entry"}

def q(s): return "'" + str(s).replace("'","''") + "'"
def qlist(xs): return "[" + ",".join(q(x) for x in xs) + "]"
def tier_sql(col="type"):
    def z(s): return ",".join(q(x) for x in sorted(s))
    return f"CASE WHEN {col} IN ({z(TIER_A)}) THEN 'A' WHEN {col} IN ({z(TIER_B)}) THEN 'B' WHEN {col} IN ({z(TIER_C)}) THEN 'C' ELSE 'D' END"

def sha256_file(p: Path):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def load_manifest():
    req=urllib.request.Request(MANIFEST_URL,headers={"User-Agent":"OpenAlex-Stage13/1.0","Accept-Encoding":"identity"})
    raw=urllib.request.urlopen(req,timeout=90).read()
    sha=hashlib.sha256(raw).hexdigest()
    m=json.loads(raw)
    if sha!=WORKS_SHA or m.get("date")!=RELEASE or int(m.get("record_count",-1))!=WORKS_ROWS:
        raise RuntimeError("Frozen Works manifest mismatch")
    files=[]
    for i,f in enumerate(m["files"]):
        u=f["url"]; meta=f.get("meta") or {}
        if u.startswith("s3://openalex/"): u=HTTPS+"/"+u[len("s3://openalex/"):]
        files.append({"index":i,"url":u,"bytes":int(meta.get("content_length") or 0),"rows":int(meta.get("record_count") or 0)})
    if len(files)!=2040: raise RuntimeError(f"Expected 2040 Works files, got {len(files)}")
    return files

def balanced(files,n):
    bins=[{"bytes":0,"files":[]} for _ in range(n)]
    for x in sorted(files,key=lambda z:z["bytes"],reverse=True):
        b=min(bins,key=lambda z:z["bytes"]); b["files"].append(x); b["bytes"]+=x["bytes"]
    return bins

def copyq(con,sql,path):
    con.execute(f"COPY ({sql}) TO {q(str(path))} (FORMAT PARQUET, COMPRESSION ZSTD)")

def merge_sum(con,glob_path,out_path,keys,sums,maxs=()):
    expr=[*keys]+[f"sum({x}) AS {x}" for x in sums]+[f"max({x}) AS {x}" for x in maxs]
    sql=f"SELECT {','.join(expr)} FROM read_parquet({q(str(glob_path))},union_by_name=true) GROUP BY {','.join(str(i+1) for i in range(len(keys)))}"
    copyq(con,sql,out_path)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--shard",type=int,required=True)
    ap.add_argument("--shards",type=int,default=16)
    ap.add_argument("--output",required=True)
    ap.add_argument("--max-files",type=int,default=0)
    ap.add_argument("--batch-files",type=int,default=2)
    args=ap.parse_args()

    out=Path(args.output); out.mkdir(parents=True,exist_ok=True)
    parts=out/"parts"; parts.mkdir(exist_ok=True)
    files=load_manifest(); bins=balanced(files,args.shards)
    assigned=sorted(bins[args.shard]["files"],key=lambda z:z["index"])
    if args.max_files>0: assigned=assigned[:args.max_files]
    if not assigned: raise RuntimeError("No files assigned")

    con=duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='10GB'")
    con.execute("SET temp_directory='/tmp/openalex13_temp'")
    con.execute("SET max_temp_directory_size='8GB'")
    con.execute("INSTALL httpfs"); con.execute("LOAD httpfs")
    tier=tier_sql("type")
    required=[]

    for bi in range(0,len(assigned),args.batch_files):
        group=assigned[bi:bi+args.batch_files]
        tag=f"{bi//args.batch_files:04d}"
        batch_outputs={
          "institution_activity":parts/f"institution_activity_{tag}.parquet",
          "country_activity":parts/f"country_activity_{tag}.parquet",
          "country_pair":parts/f"country_pair_{tag}.parquet",
          "inst_pair_sample":parts/f"inst_pair_sample_{tag}.parquet",
          "author_activity_sample":parts/f"author_activity_sample_{tag}.parquet",
          "author_pair_sample":parts/f"author_pair_sample_{tag}.parquet",
          "author_inst_sample":parts/f"author_inst_sample_{tag}.parquet",
          "authors_hist":parts/f"authors_hist_{tag}.parquet",
          "institutions_hist":parts/f"institutions_hist_{tag}.parquet",
          "countries_hist":parts/f"countries_hist_{tag}.parquet",
          "batch_stats":parts/f"batch_stats_{tag}.json",
        }
        required.extend(batch_outputs.values())
        if all(p.exists() for p in batch_outputs.values()):
            continue

        urls=qlist([x["url"] for x in group])
        local=Path("/tmp")/f"openalex13_s{args.shard}_{tag}.parquet"
        if not local.exists():
            sql=f"""
            SELECT id AS work_id, publication_year AS pub_year,
                   CASE WHEN coalesce(is_xpac,false) THEN 'expansion' ELSE 'core' END AS corpus,
                   {tier} AS tier,
                   coalesce(authors_count,0) AS authors_count,
                   coalesce(institutions_distinct_count,0) AS institutions_distinct_count,
                   coalesce(countries_distinct_count,0) AS countries_distinct_count,
                   authorships, institutions
            FROM read_parquet({urls},union_by_name=true)
            WHERE publication_year BETWEEN 2000 AND 2026
              AND NOT coalesce(is_retracted,false)
            """
            copyq(con,sql,local)

        if not batch_outputs["authors_hist"].exists():
            copyq(con,f"SELECT corpus,tier,pub_year,authors_count,count(*) works FROM read_parquet({q(local)}) GROUP BY 1,2,3,4",batch_outputs["authors_hist"])
        if not batch_outputs["institutions_hist"].exists():
            copyq(con,f"SELECT corpus,tier,pub_year,institutions_distinct_count,count(*) works FROM read_parquet({q(local)}) GROUP BY 1,2,3,4",batch_outputs["institutions_hist"])
        if not batch_outputs["countries_hist"].exists():
            copyq(con,f"SELECT corpus,tier,pub_year,countries_distinct_count,count(*) works FROM read_parquet({q(local)}) GROUP BY 1,2,3,4",batch_outputs["countries_hist"])

        if not batch_outputs["institution_activity"].exists():
            sql=f"""
            WITH wi AS (
              SELECT DISTINCT work_id,pub_year,corpus,tier,i.id AS institution_id,i.country_code AS country_code
              FROM read_parquet({q(local)}), UNNEST(institutions) t(i)
              WHERE i.id IS NOT NULL
            )
            SELECT pub_year,corpus,tier,institution_id,any_value(country_code) country_code,count(*) works
            FROM wi GROUP BY 1,2,3,4
            """
            copyq(con,sql,batch_outputs["institution_activity"])

        if not batch_outputs["country_activity"].exists() or not batch_outputs["country_pair"].exists():
            wc=Path("/tmp")/f"openalex13_wc_s{args.shard}_{tag}.parquet"
            if not wc.exists():
                sql=f"""
                WITH a AS (
                  SELECT work_id,pub_year,corpus,tier,au
                  FROM read_parquet({q(local)}),UNNEST(authorships)t(au)
                )
                SELECT DISTINCT work_id,pub_year,corpus,tier,c AS country_code
                FROM a,UNNEST(au.countries)t2(c)
                WHERE c IS NOT NULL AND trim(c)<>''
                """
                copyq(con,sql,wc)
            if not batch_outputs["country_activity"].exists():
                copyq(con,f"SELECT pub_year,corpus,tier,country_code,count(*) works FROM read_parquet({q(wc)}) GROUP BY 1,2,3,4",batch_outputs["country_activity"])
            if not batch_outputs["country_pair"].exists():
                sql=f"""
                WITH wc AS (SELECT * FROM read_parquet({q(wc)}))
                SELECT a.pub_year,a.corpus,a.tier,a.country_code country_a,b.country_code country_b,count(*) works
                FROM wc a JOIN wc b
                  ON a.work_id=b.work_id AND a.pub_year=b.pub_year AND a.corpus=b.corpus AND a.tier=b.tier
                 AND a.country_code < b.country_code
                GROUP BY 1,2,3,4,5
                """
                copyq(con,sql,batch_outputs["country_pair"])
            if wc.exists(): wc.unlink()

        if not batch_outputs["inst_pair_sample"].exists():
            sql=f"""
            WITH sw AS (
              SELECT work_id,pub_year,corpus,tier,institutions
              FROM read_parquet({q(local)})
              WHERE hash(work_id)%64=0 AND institutions_distinct_count BETWEEN 2 AND 32
            ), wi AS (
              SELECT DISTINCT work_id,pub_year,corpus,tier,i.id institution_id
              FROM sw,UNNEST(institutions)t(i)
              WHERE i.id IS NOT NULL
            )
            SELECT a.pub_year,a.corpus,a.tier,a.institution_id institution_a,b.institution_id institution_b,count(*) works
            FROM wi a JOIN wi b
              ON a.work_id=b.work_id AND a.pub_year=b.pub_year AND a.corpus=b.corpus AND a.tier=b.tier
             AND a.institution_id < b.institution_id
            GROUP BY 1,2,3,4,5
            """
            copyq(con,sql,batch_outputs["inst_pair_sample"])

        need_author=not (batch_outputs["author_activity_sample"].exists() and batch_outputs["author_pair_sample"].exists() and batch_outputs["author_inst_sample"].exists())
        if need_author:
            wa=Path("/tmp")/f"openalex13_wa_s{args.shard}_{tag}.parquet"
            if not wa.exists():
                sql=f"""
                WITH sw AS (
                  SELECT work_id,pub_year,corpus,tier,authorships
                  FROM read_parquet({q(local)})
                  WHERE hash(work_id)%512=0 AND authors_count BETWEEN 2 AND 50
                )
                SELECT work_id,pub_year,corpus,tier,
                       au.author.id author_id,
                       coalesce(au.is_corresponding,false) is_corresponding,
                       au.institutions author_institutions
                FROM sw,UNNEST(authorships)t(au)
                WHERE au.author.id IS NOT NULL
                  AND au.author.id NOT IN ({q(NULL_AUTHOR)},{q(DELETED_AUTHOR)})
                """
                copyq(con,sql,wa)
            if not batch_outputs["author_activity_sample"].exists():
                copyq(con,f"SELECT pub_year,corpus,tier,author_id,count(DISTINCT work_id) works,count(DISTINCT work_id) FILTER(WHERE is_corresponding) corresponding_works FROM read_parquet({q(wa)}) GROUP BY 1,2,3,4",batch_outputs["author_activity_sample"])
            if not batch_outputs["author_pair_sample"].exists():
                sql=f"""
                WITH x AS (SELECT DISTINCT work_id,pub_year,corpus,tier,author_id FROM read_parquet({q(wa)}))
                SELECT a.pub_year,a.corpus,a.tier,a.author_id author_a,b.author_id author_b,count(*) works
                FROM x a JOIN x b
                  ON a.work_id=b.work_id AND a.pub_year=b.pub_year AND a.corpus=b.corpus AND a.tier=b.tier
                 AND a.author_id < b.author_id
                GROUP BY 1,2,3,4,5
                """
                copyq(con,sql,batch_outputs["author_pair_sample"])
            if not batch_outputs["author_inst_sample"].exists():
                sql=f"""
                WITH ai AS (
                  SELECT DISTINCT work_id,pub_year,corpus,tier,author_id,is_corresponding,i.id institution_id
                  FROM read_parquet({q(wa)}) LEFT JOIN UNNEST(author_institutions)t(i) ON true
                  WHERE i.id IS NOT NULL
                )
                SELECT pub_year,corpus,tier,author_id,institution_id,count(DISTINCT work_id) works,
                       count(DISTINCT work_id) FILTER(WHERE is_corresponding) corresponding_works
                FROM ai GROUP BY 1,2,3,4,5
                """
                copyq(con,sql,batch_outputs["author_inst_sample"])
            if wa.exists(): wa.unlink()

        stats=con.execute(f"""
          SELECT count(*) works,
                 count(*) FILTER(WHERE hash(work_id)%64=0) inst_sample_work_hash,
                 count(*) FILTER(WHERE hash(work_id)%64=0 AND institutions_distinct_count BETWEEN 2 AND 32) inst_pair_eligible_sample_works,
                 count(*) FILTER(WHERE hash(work_id)%512=0) author_sample_work_hash,
                 count(*) FILTER(WHERE hash(work_id)%512=0 AND authors_count BETWEEN 2 AND 50) author_pair_eligible_sample_works,
                 count(*) FILTER(WHERE authors_count>50) hyperauthor_works,
                 count(*) FILTER(WHERE institutions_distinct_count>32) hyperinstitution_works,
                 count(*) FILTER(WHERE countries_distinct_count>32) multicountry_gt32_works
          FROM read_parquet({q(local)})
        """).fetchone()
        doc={"works":stats[0],"inst_sample_work_hash":stats[1],"inst_pair_eligible_sample_works":stats[2],
             "author_sample_work_hash":stats[3],"author_pair_eligible_sample_works":stats[4],
             "hyperauthor_works":stats[5],"hyperinstitution_works":stats[6],"multicountry_gt32_works":stats[7],
             "source_file_indices":[x["index"] for x in group]}
        batch_outputs["batch_stats"].write_text(json.dumps(doc,indent=2),encoding="utf-8")
        if local.exists(): local.unlink()

    # Merge batch parts inside this shard.
    merge_sum(con,parts/"institution_activity_*.parquet",out/"institution_activity_year.parquet",
              ["pub_year","corpus","tier","institution_id","country_code"],["works"])
    merge_sum(con,parts/"country_activity_*.parquet",out/"country_activity_year.parquet",
              ["pub_year","corpus","tier","country_code"],["works"])
    merge_sum(con,parts/"country_pair_*.parquet",out/"country_pair_year.parquet",
              ["pub_year","corpus","tier","country_a","country_b"],["works"])
    merge_sum(con,parts/"inst_pair_sample_*.parquet",out/"institution_pair_sample_year.parquet",
              ["pub_year","corpus","tier","institution_a","institution_b"],["works"])
    merge_sum(con,parts/"author_activity_sample_*.parquet",out/"author_activity_sample_year.parquet",
              ["pub_year","corpus","tier","author_id"],["works","corresponding_works"])
    merge_sum(con,parts/"author_pair_sample_*.parquet",out/"author_pair_sample_year.parquet",
              ["pub_year","corpus","tier","author_a","author_b"],["works"])
    merge_sum(con,parts/"author_inst_sample_*.parquet",out/"author_institution_sample_year.parquet",
              ["pub_year","corpus","tier","author_id","institution_id"],["works","corresponding_works"])
    merge_sum(con,parts/"authors_hist_*.parquet",out/"authors_count_hist.parquet",
              ["pub_year","corpus","tier","authors_count"],["works"])
    merge_sum(con,parts/"institutions_hist_*.parquet",out/"institutions_count_hist.parquet",
              ["pub_year","corpus","tier","institutions_distinct_count"],["works"])
    merge_sum(con,parts/"countries_hist_*.parquet",out/"countries_count_hist.parquet",
              ["pub_year","corpus","tier","countries_distinct_count"],["works"])

    batch_stats=[json.loads(p.read_text()) for p in sorted(parts.glob("batch_stats_*.json"))]
    totals={k:sum(int(x.get(k,0)) for x in batch_stats) for k in [
      "works","inst_sample_work_hash","inst_pair_eligible_sample_works","author_sample_work_hash",
      "author_pair_eligible_sample_works","hyperauthor_works","hyperinstitution_works","multicountry_gt32_works"]}
    outputs={}
    for p in sorted(out.glob("*.parquet")):
        outputs[p.name]={"bytes":p.stat().st_size,"sha256":sha256_file(p),"rows":con.execute(f"SELECT count(*) FROM read_parquet({q(p)})").fetchone()[0]}
    manifest={"stage":13,"release":RELEASE,"shard":args.shard,"shards":args.shards,
              "source_manifest_sha256":WORKS_SHA,"assigned_file_indices":[x["index"] for x in assigned],
              "assigned_bytes":sum(x["bytes"] for x in assigned),"max_files":args.max_files,
              "sampling":{"institution_pair_work_hash_mod":64,"institution_pair_max_institutions":32,
                          "author_work_hash_mod":512,"author_pair_max_authors":50},
              "totals":totals,"outputs":outputs}
    (out/"SHARD_MANIFEST.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")

    # Pilot QA can be evaluated from any shard, including max-files=1.
    qa={
      "works_positive":totals["works"]>0,
      "institution_activity_rows":outputs["institution_activity_year.parquet"]["rows"],
      "country_activity_rows":outputs["country_activity_year.parquet"]["rows"],
      "country_pair_rows":outputs["country_pair_year.parquet"]["rows"],
      "institution_pair_sample_rows":outputs["institution_pair_sample_year.parquet"]["rows"],
      "author_pair_sample_rows":outputs["author_pair_sample_year.parquet"]["rows"],
      "author_institution_sample_rows":outputs["author_institution_sample_year.parquet"]["rows"],
    }
    qa["pass"]=qa["works_positive"] and qa["institution_activity_rows"]>0 and qa["country_activity_rows"]>0
    (out/"SHARD_QA.json").write_text(json.dumps(qa,indent=2),encoding="utf-8")
    print(json.dumps({"manifest":manifest,"qa":qa},indent=2))

if __name__=="__main__":
    main()
