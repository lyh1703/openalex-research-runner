#!/usr/bin/env python3
from __future__ import annotations
import argparse,hashlib,json,math,os,time,urllib.request
from pathlib import Path
import duckdb
RELEASE='2026-09-23'; WORKS_SHA='a72ff4b43c9d13d0911386e1a5dd1b9d71b3c1080ac44ccfc016e11d6390a059'; WORKS_ROWS=476196327
MANIFEST_URL='https://openalex.s3.amazonaws.com/data/parquet/works/manifest.json'; HTTPS='https://openalex.s3.amazonaws.com'
TIER_A={'article','conference-paper','preprint','data-paper','software-paper'}
TIER_B={'review','book','book-chapter','dissertation','report','dataset','software','standard'}
TIER_C={'conference-abstract','editorial','letter','book-review','peer-review','reference-entry'}
def q(s):return "'"+str(s).replace("'","''")+"'"
def qlist(xs):return '['+','.join(q(x) for x in xs)+']'
def tier_sql(col='type'):
    def z(s):return ','.join(q(x) for x in sorted(s))
    return f"CASE WHEN {col} IN ({z(TIER_A)}) THEN 'A' WHEN {col} IN ({z(TIER_B)}) THEN 'B' WHEN {col} IN ({z(TIER_C)}) THEN 'C' ELSE 'D' END"
def get_manifest():
    req=urllib.request.Request(MANIFEST_URL,headers={'User-Agent':'OpenAlex-Stage12/1.0','Accept-Encoding':'identity'})
    raw=urllib.request.urlopen(req,timeout=90).read(); sha=hashlib.sha256(raw).hexdigest();m=json.loads(raw)
    if sha!=WORKS_SHA or m.get('date')!=RELEASE or int(m.get('record_count',-1))!=WORKS_ROWS:raise RuntimeError('frozen manifest mismatch')
    fs=[]
    for i,f in enumerate(m['files']):
        u=f['url']; meta=f.get('meta') or {}
        if u.startswith('s3://openalex/'):u=HTTPS+'/'+u[len('s3://openalex/'):]
        fs.append({'index':i,'url':u,'bytes':int(meta.get('content_length') or 0),'rows':int(meta.get('record_count') or 0)})
    return m,fs
def balanced(fs,n=8):
    bins=[{'bytes':0,'files':[]} for _ in range(n)]
    for x in sorted(fs,key=lambda z:z['bytes'],reverse=True):
        b=min(bins,key=lambda z:z['bytes']);b['files'].append(x);b['bytes']+=x['bytes']
    return bins
def sha256_file(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--shard',type=int,required=True);ap.add_argument('--output',required=True);a=ap.parse_args()
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True); parts=out/'parts';parts.mkdir(exist_ok=True)
    m,fs=get_manifest(); bins=balanced(fs,8); assigned=sorted(bins[a.shard]['files'],key=lambda z:z['index'])
    con=duckdb.connect();con.execute("SET threads=4");con.execute("SET preserve_insertion_order=false");con.execute("SET memory_limit='10GB'");con.execute("SET temp_directory='/tmp/openalex12'");con.execute("SET max_temp_directory_size='8GB'");con.execute("INSTALL httpfs");con.execute("LOAD httpfs")
    schema={r[0]:r[1] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q(assigned[0]['url'])})").fetchall()}
    if 'referenced_works_count' in schema: refexpr='coalesce(referenced_works_count,0)'
    elif 'referenced_works' in schema: refexpr='coalesce(array_length(referenced_works),0)'
    else: raise RuntimeError('No referenced works field found')
    tier=tier_sql('type'); batch_size=8
    for bi in range(0,len(assigned),batch_size):
        group=assigned[bi:bi+batch_size]; urls=qlist([x['url'] for x in group]); tag=f'{bi//batch_size:03d}'
        deg=parts/f'degree_{tag}.parquet'; hist=parts/f'hist_{tag}.parquet'; top=parts/f'top_{tag}.parquet'
        local=Path('/tmp')/f'openalex12_shard{a.shard}_{tag}.parquet'
        if not (deg.exists() and hist.exists() and top.exists()):
            sql=f"""SELECT id work_id,publication_year,CASE WHEN coalesce(is_xpac,false) THEN 'expansion' ELSE 'core' END corpus,{tier} tier,coalesce(cited_by_count,0) indegree,{refexpr} outdegree,primary_topic.id primary_topic_id,primary_location.source.id source_id FROM read_parquet({urls},union_by_name=true) WHERE publication_year BETWEEN 2000 AND 2026 AND NOT coalesce(is_retracted,false)"""
            con.execute(f"COPY ({sql}) TO {q(local)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        if not deg.exists():
            sql=f"""SELECT publication_year,corpus,tier,count(*) works,sum(outdegree) reference_edges,sum(indegree) citation_indegree_sum,count(*) FILTER(WHERE outdegree>0) works_with_references,count(*) FILTER(WHERE indegree>0) cited_works,count(*) FILTER(WHERE indegree>=10) indegree_ge_10,count(*) FILTER(WHERE indegree>=100) indegree_ge_100,count(*) FILTER(WHERE indegree>=1000) indegree_ge_1000,count(*) FILTER(WHERE outdegree>=10) outdegree_ge_10,count(*) FILTER(WHERE outdegree>=50) outdegree_ge_50,count(*) FILTER(WHERE outdegree>=100) outdegree_ge_100,max(indegree) max_indegree,max(outdegree) max_outdegree FROM read_parquet({q(local)}) GROUP BY 1,2,3"""
            con.execute(f"COPY ({sql}) TO {q(deg)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        if not hist.exists():
            sql=f"""WITH b AS (SELECT corpus,tier,CASE WHEN indegree=0 THEN -1 ELSE floor(log2(indegree))::INT END indegree_log2_bin,CASE WHEN outdegree=0 THEN -1 ELSE floor(log2(outdegree))::INT END outdegree_log2_bin FROM read_parquet({q(local)})) SELECT corpus,tier,indegree_log2_bin,outdegree_log2_bin,count(*) works FROM b GROUP BY 1,2,3,4"""
            con.execute(f"COPY ({sql}) TO {q(hist)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        if not top.exists():
            sql=f"""WITH w AS (SELECT * FROM read_parquet({q(local)})), a AS (SELECT 'indegree' metric,* FROM w ORDER BY indegree DESC LIMIT 300), b AS (SELECT 'outdegree' metric,* FROM w ORDER BY outdegree DESC LIMIT 300) SELECT * FROM a UNION ALL SELECT * FROM b"""
            con.execute(f"COPY ({sql}) TO {q(top)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        if local.exists(): local.unlink()
    sample_files=[]
    for frac in (0.125,0.375,0.625,0.875): sample_files.append(assigned[min(len(assigned)-1,int(frac*len(assigned)))])
    sample_parts=[]
    if 'referenced_works' in schema:
        for j,x in enumerate(sample_files):
            p=parts/f'edge_sample_{j}.parquet';sample_parts.append(p)
            if p.exists():continue
            sql=f"""WITH w AS (SELECT id citing_work_id,publication_year,CASE WHEN coalesce(is_xpac,false) THEN 'expansion' ELSE 'core' END corpus,{tier} tier,primary_topic.id citing_primary_topic_id,referenced_works FROM read_parquet({q(x['url'])}) WHERE publication_year BETWEEN 2000 AND 2026 AND NOT coalesce(is_retracted,false) AND abs(hash(id))%100=0) SELECT citing_work_id,r cited_work_id,publication_year,corpus,tier,citing_primary_topic_id FROM w,UNNEST(referenced_works)u(r) WHERE r IS NOT NULL"""
            con.execute(f"COPY ({sql}) TO {q(p)} (FORMAT PARQUET,COMPRESSION ZSTD)")
    con.execute(f"COPY (SELECT publication_year,corpus,tier,sum(works) works,sum(reference_edges) reference_edges,sum(citation_indegree_sum) citation_indegree_sum,sum(works_with_references) works_with_references,sum(cited_works) cited_works,sum(indegree_ge_10) indegree_ge_10,sum(indegree_ge_100) indegree_ge_100,sum(indegree_ge_1000) indegree_ge_1000,sum(outdegree_ge_10) outdegree_ge_10,sum(outdegree_ge_50) outdegree_ge_50,sum(outdegree_ge_100) outdegree_ge_100,max(max_indegree) max_indegree,max(max_outdegree) max_outdegree FROM read_parquet({q(str(parts/'degree_*.parquet'))}) GROUP BY 1,2,3) TO {q(out/'citation_degree_year.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
    con.execute(f"COPY (SELECT corpus,tier,indegree_log2_bin,outdegree_log2_bin,sum(works) works FROM read_parquet({q(str(parts/'hist_*.parquet'))}) GROUP BY 1,2,3,4) TO {q(out/'citation_degree_joint_histogram.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
    con.execute(f"COPY (SELECT * EXCLUDE(rn) FROM (SELECT *,row_number() OVER(PARTITION BY metric ORDER BY CASE WHEN metric='indegree' THEN indegree ELSE outdegree END DESC) rn FROM read_parquet({q(str(parts/'top_*.parquet'))})) WHERE rn<=5000) TO {q(out/'citation_top_candidates.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
    if sample_parts:
        con.execute(f"COPY (SELECT * FROM read_parquet({qlist([str(p) for p in sample_parts])},union_by_name=true)) TO {q(out/'citation_edge_sample.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
        sample_n=con.execute(f"SELECT count(*) FROM read_parquet({q(out/'citation_edge_sample.parquet')})").fetchone()[0]
    else: sample_n=0
    manifest={'stage':12,'release':RELEASE,'shard':a.shard,'source_manifest_sha256':WORKS_SHA,'assigned_files':[x['index'] for x in assigned],'assigned_bytes':sum(x['bytes'] for x in assigned),'ref_count_expression':refexpr,'edge_sample_file_indices':[x['index'] for x in sample_files],'edge_sample_rule':'abs(hash(work.id)) % 100 == 0 within 4 stratified shard files','edge_sample_edges':sample_n,'outputs':{}}
    for p in out.glob('*.parquet'):manifest['outputs'][p.name]={'bytes':p.stat().st_size,'sha256':sha256_file(p)}
    (out/'SHARD_MANIFEST.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps(manifest,indent=2))
if __name__=='__main__':main()
