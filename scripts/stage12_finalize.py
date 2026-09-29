#!/usr/bin/env python3
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import duckdb
WORKS_SHA='a72ff4b43c9d13d0911386e1a5dd1b9d71b3c1080ac44ccfc016e11d6390a059'; RELEASE='2026-09-23'
def sha(p):
 h=hashlib.sha256();f=open(p,'rb')
 for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 f.close();return h.hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--root',required=True);ap.add_argument('--output',required=True);a=ap.parse_args();root=Path(a.root);out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
 mans=[]
 for i in range(8):
  p=root/f'shard-{i}'/'SHARD_MANIFEST.json'
  if not p.exists():raise RuntimeError(f'missing shard manifest {i}')
  m=json.loads(p.read_text())
  if m['source_manifest_sha256']!=WORKS_SHA or m['release']!=RELEASE:raise RuntimeError('frozen source mismatch')
  mans.append(m)
 ids=[x for m in mans for x in m['assigned_files']]
 if len(ids)!=2040 or sorted(ids)!=list(range(2040)) or len(set(ids))!=2040:raise RuntimeError(f'file coverage failure: {len(ids)}')
 con=duckdb.connect();con.execute("SET threads=4");con.execute("SET preserve_insertion_order=false")
 pats=[str(root/f'shard-{i}'/'citation_degree_year.parquet') for i in range(8)]
 con.execute(f"COPY (SELECT publication_year,corpus,tier,sum(works) works,sum(reference_edges) reference_edges,sum(citation_indegree_sum) citation_indegree_sum,sum(works_with_references) works_with_references,sum(cited_works) cited_works,sum(indegree_ge_10) indegree_ge_10,sum(indegree_ge_100) indegree_ge_100,sum(indegree_ge_1000) indegree_ge_1000,sum(outdegree_ge_10) outdegree_ge_10,sum(outdegree_ge_50) outdegree_ge_50,sum(outdegree_ge_100) outdegree_ge_100,max(max_indegree) max_indegree,max(max_outdegree) max_outdegree FROM read_parquet({repr(pats)}) GROUP BY 1,2,3) TO '{out/'citation_degree_year.parquet'}' (FORMAT PARQUET,COMPRESSION ZSTD)")
 pats=[str(root/f'shard-{i}'/'citation_degree_joint_histogram.parquet') for i in range(8)]
 con.execute(f"COPY (SELECT corpus,tier,indegree_log2_bin,outdegree_log2_bin,sum(works) works FROM read_parquet({repr(pats)}) GROUP BY 1,2,3,4) TO '{out/'citation_degree_joint_histogram.parquet'}' (FORMAT PARQUET,COMPRESSION ZSTD)")
 pats=[str(root/f'shard-{i}'/'citation_top_candidates.parquet') for i in range(8)]
 con.execute(f"COPY (SELECT * EXCLUDE(rn) FROM (SELECT *,row_number() OVER(PARTITION BY metric ORDER BY CASE WHEN metric='indegree' THEN indegree ELSE outdegree END DESC) rn FROM read_parquet({repr(pats)})) WHERE rn<=5000) TO '{out/'citation_top_candidates.parquet'}' (FORMAT PARQUET,COMPRESSION ZSTD)")
 eps=[str(root/f'shard-{i}'/'citation_edge_sample.parquet') for i in range(8) if (root/f'shard-{i}'/'citation_edge_sample.parquet').exists()]
 if eps:con.execute(f"COPY (SELECT * FROM read_parquet({repr(eps)},union_by_name=true)) TO '{out/'citation_edge_sample.parquet'}' (FORMAT PARQUET,COMPRESSION ZSTD)")
 stats=con.execute(f"SELECT sum(works),sum(reference_edges),sum(citation_indegree_sum),sum(works_with_references),sum(cited_works),max(max_indegree),max(max_outdegree) FROM read_parquet('{out/'citation_degree_year.parquet'}')").fetchone()
 sample_edges=con.execute(f"SELECT count(*) FROM read_parquet('{out/'citation_edge_sample.parquet'}')").fetchone()[0] if (out/'citation_edge_sample.parquet').exists() else 0
 if sample_edges:
  sample_self=con.execute(f"SELECT count(*) FROM read_parquet('{out/'citation_edge_sample.parquet'}') WHERE citing_work_id=cited_work_id").fetchone()[0]
  sample_distinct=con.execute(f"SELECT count(*) FROM (SELECT DISTINCT citing_work_id,cited_work_id FROM read_parquet('{out/'citation_edge_sample.parquet'}'))").fetchone()[0]
  sample_duplicate=sample_edges-sample_distinct
  con.execute(f"COPY (SELECT citing_work_id,cited_work_id,publication_year,corpus,tier,citing_primary_topic_id,count(*) multiplicity FROM read_parquet('{out/'citation_edge_sample.parquet'}') GROUP BY ALL) TO '{out/'citation_edge_sample_dedup.parquet'}' (FORMAT PARQUET,COMPRESSION ZSTD)")
 else:
  sample_self=sample_duplicate=sample_distinct=0
 outputs={p.name:{'bytes':p.stat().st_size,'sha256':sha(p)} for p in out.glob('*.parquet')}
 gate={'stage':12,'complete':True,'release':RELEASE,'source_manifest_sha256':WORKS_SHA,'source_files_covered':2040,'analytic_works_2000_2026_nonretracted':stats[0],'reference_edges_analytic':stats[1],'citation_indegree_sum_analytic':stats[2],'works_with_references':stats[3],'cited_works':stats[4],'max_indegree':stats[5],'max_outdegree':stats[6],'bounded_actual_edge_sample_edges':sample_edges,'bounded_edge_sample_distinct_pairs':sample_distinct,'bounded_edge_sample_duplicate_rows':sample_duplicate,'bounded_edge_sample_self_references':sample_self,'edge_policy':{'raw_source_edges':'preserve OpenAlex matched referenced_works multiplicity as observed','self_reference':'retain in raw QA sample; exclude from topology metrics only if a later hypothesis requires it','duplicates':'retain raw; provide deduplicated sample with multiplicity','unresolved_or_deleted_target':'retain target OpenAlex ID as provenance edge; node-existence resolution is not assumed from absence in the analytic 2000-2026 universe'},'full_edge_materialization':'intentionally not retained under zero-cost architecture; exact degree census uses scalar counts and topology is represented by a deterministic bounded edge sample','outputs':outputs}
 (out/'STAGE12_FINAL_GATE.json').write_text(json.dumps(gate,indent=2),encoding='utf-8')
 (out/'OPENALEX_STAGE12_CITATION_GRAPH_REPORT.md').write_text(f"# OpenAlex Stage 12 — Citation Graph\n\nStatus: **COMPLETE**\n\n- Frozen snapshot: {RELEASE}\n- Works source files covered: 2040 / 2040\n- Analytic works (2000–2026, non-retracted): {stats[0]:,}\n- Exact reference-edge count in analytic universe (sum of work outdegree): {stats[1]:,}\n- Citation indegree sum in analytic universe: {stats[2]:,}\n- Works with >=1 reference: {stats[3]:,}\n- Works cited >=1 time: {stats[4]:,}\n- Max observed indegree: {stats[5]:,}\n- Max observed outdegree: {stats[6]:,}\n- Deterministic bounded actual-edge sample: {sample_edges:,} edges\n- Distinct sampled citing→cited pairs: {sample_distinct:,}\n- Duplicate sampled edge rows: {sample_duplicate:,}\n- Sampled self-references: {sample_self:,}\n\n## Architecture decision\nThe multi-billion full edge table is not permanently materialized. Exact degree statistics are computed over the full frozen Works snapshot, while explicit graph topology is retained as a reproducible bounded sample. This preserves provenance and keeps the research within the zero-cost storage constraint. Full edge expansion remains reproducible from referenced_works if a later hypothesis requires it.\n",encoding='utf-8')
 print(json.dumps(gate,indent=2))
if __name__=='__main__':main()
