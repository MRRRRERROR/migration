"""Original ShareGPT prompts encoded by the actual Qwen2.5 tokenizer."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import urllib.request
from transformers import AutoTokenizer
from two_gpu_power_metrics import poisson_trace

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--source');p.add_argument('--dataset-host',default='huggingface.co');a=p.parse_args()
    root=Path(a.root);root.mkdir(parents=True,exist_ok=True);inputs=root/'inputs';inputs.mkdir(exist_ok=True)
    source=Path(a.source) if a.source else inputs/'sharegpt_gpt4.jsonl'
    origin=a.source
    if not source.exists():
        api='https://'+a.dataset_host+'/api/datasets/shibing624/sharegpt_gpt4'
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        meta=json.load(opener.open(api,timeout=30))
        names=[r['rfilename'] for r in meta['siblings'] if r['rfilename'].endswith(('.jsonl','.json'))]
        names.sort(key=lambda n:(n!='sharegpt_gpt4.jsonl',n))
        if not names:raise RuntimeError('Original ShareGPT dataset has no JSON file')
        origin='https://'+a.dataset_host+'/datasets/shibing624/sharegpt_gpt4/resolve/main/'+names[0]
        source=inputs/Path(names[0]).name
        with opener.open(origin,timeout=120) as response, source.open('xb') as out:
            while chunk:=response.read(1048576):out.write(chunk)
    tok=AutoTokenizer.from_pretrained('/data/models/Qwen2.5-7B-Instruct',local_files_only=True)
    raw=source.read_text();records=json.loads(raw) if raw.lstrip().startswith('[') else [json.loads(s) for s in raw.split('\n') if s.strip()]
    prompts=[];seen=set();counters={'raw_records':len(records),'duplicates':0,'length_rejected':0,'no_human_text':0}
    for index,row in enumerate(records):
        conv=row.get('conversations') or row.get('conversation') or row.get('messages') or []
        text=next((c.get('value',c.get('content','')) for c in conv if c.get('from',c.get('role')) in ('human','user')),None)
        if not text and 'instruction' in row:text=row['instruction']+('\n'+row['input'] if row.get('input') else '')
        if not text:counters['no_human_text']+=1;continue
        digest=hashlib.sha256(text.strip().encode()).hexdigest()
        if digest in seen:counters['duplicates']+=1;continue
        seen.add(digest)
        n=len(tok.encode(text,add_special_tokens=False))
        if not 128<=n<=4096:counters['length_rejected']+=1;continue
        encoded=tok.apply_chat_template([{'role':'user','content':text}],tokenize=False,add_generation_prompt=True)
        token_count=len(tok.encode(encoded,add_special_tokens=False))
        if not 128<=token_count<=4096:
            counters['length_rejected']+=1;continue
        prompts.append({'prompt_index':len(prompts),'source_record':index,'sha256':digest,'text':text,
                        'prompt':encoded,'raw_input_tokens':n,'input_tokens':token_count})
        if len(prompts)>=10000:break
    if not prompts:raise RuntimeError('No eligible original ShareGPT prompts')
    with (inputs/'prompts.jsonl').open('x') as out:
        for row in prompts:out.write(json.dumps(row,ensure_ascii=False)+'\n')
    manifest={'source':origin,'source_file':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'prompt_count':len(prompts),'target_prompt_count':10000,'counters':counters,
        'dedup':'SHA256 of stripped original human text; saved text remains original',
        'filter':'128..4096 original text tokens and 128..4096 actual input tokens after the model chat template',
        'max_encoded_input_tokens':max(r['input_tokens'] for r in prompts),'sampling':'uniform random with replacement; separate seeded Poisson streams',
        'context_length':9216,'max_output_tokens':4096,'temperature':0,'ignore_eos':'omitted',
        'seeds':[101,102,103,104,105], 'candidate_rates':{'S':1.,'D':.3},'formal_duration_s':900,'t0_offset_s':300}
    (root/'workload_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    order=[{'mode':m,'seed':s} for s in manifest['seeds'] for m in 'ABC'];random.Random(20261006).shuffle(order)
    (root/'formal_order.json').write_text(json.dumps(order,indent=2))
    print(json.dumps(manifest,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
