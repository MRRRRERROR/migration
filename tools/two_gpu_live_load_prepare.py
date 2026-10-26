"""Encode original ShareGPT once with the actual model template, then freeze the pool."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import struct
from transformers import AutoTokenizer

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--source',required=True);a=p.parse_args()
    root=Path(a.root);root.mkdir(exist_ok=False,parents=True);(root/'inputs').mkdir()
    src=Path(a.source);raw=src.read_text();records=json.loads(raw) if raw.lstrip().startswith('[') else [json.loads(l) for l in raw.split('\n') if l.strip()]
    tok=AutoTokenizer.from_pretrained('/data/models/Qwen2.5-7B-Instruct',local_files_only=True)
    pool=[];seen=set();counts=dict(raw_records=len(records),duplicates=0,length_rejected=0,no_human=0)
    for index,row in enumerate(records):
        conv=row.get('conversations') or row.get('conversation') or row.get('messages') or []
        text=next((c.get('value',c.get('content','')) for c in conv if c.get('from',c.get('role')) in ('human','user')),None)
        if not text:counts['no_human']+=1;continue
        digest=hashlib.sha256(text.strip().encode()).hexdigest()
        if digest in seen:counts['duplicates']+=1;continue
        seen.add(digest)
        ids=tok.apply_chat_template([{'role':'user','content':text}],tokenize=True,add_generation_prompt=True)
        if not 128<=len(ids)<=4096:counts['length_rejected']+=1;continue
        pool.append(dict(prompt_index=len(pool),source_record=index,text=text,text_sha256=digest,
            token_ids=ids,input_tokens=len(ids),input_sha256=hashlib.sha256(struct.pack(f'<{len(ids)}I',*ids)).hexdigest()))
    if not pool:raise RuntimeError('Empty eligible pool')
    with (root/'inputs/prompts.jsonl').open('x') as f:
        for row in pool:f.write(json.dumps(row,ensure_ascii=False)+'\n')
    longs=[r['prompt_index'] for r in pool if 2048<=r['input_tokens']<=4096]
    selected=random.Random(101).sample(longs,16) if len(longs)>=16 else longs
    (root/'coverage_prompt_indices.json').write_text(json.dumps(selected))
    lengths=sorted(r['input_tokens'] for r in pool)
    origin=src.parent.parent/'workload_manifest.json'
    manifest=dict(source_file=str(src),source_sha256=hashlib.sha256(src.read_bytes()).hexdigest(),
        prior_source_manifest=json.loads(origin.read_text()) if origin.exists() else None,
        counts=counts,pool_size=len(pool),long_pool_size=len(longs),dedup='SHA256 stripped first original human text',
        template='actual Qwen2.5 apply_chat_template(tokenize=True,add_generation_prompt=True), exactly once',
        sampling='uniform with replacement; independent source/background Poisson streams',
        length_distribution=dict(min=lengths[0],median=lengths[len(lengths)//2],p95=lengths[int(.95*(len(lengths)-1))],max=lengths[-1]),
        context_length=9216,max_tokens=4096,temperature=0,ignore_eos=False,seed=101)
    (root/'workload_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    print(json.dumps(manifest,ensure_ascii=False))
if __name__=='__main__':main()
