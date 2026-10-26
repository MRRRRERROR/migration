"""Finite acceptance evidence audit, never changes process state."""
import argparse
import json
from pathlib import Path
import hashlib
import time
import psutil
from two_gpu_live_nvml import NVML

def main(root):
    if not (root/'completed.json').exists():raise RuntimeError('Finite run has not completed')
    rounds=[];processes=[];native_hashes={}
    prompts=[json.loads(line) for line in (root/'inputs/prompts.jsonl').open() if line.strip()]
    for scope in ('main','coverage'):
        for mode in 'BCDE':
            d=root/(scope+'_'+mode);s=json.loads((d/'summary.json').read_text());a=json.loads((d/'analysis.json').read_text())
            for row in json.loads((d/'native_loaded.json').read_text()):native_hashes.setdefault(row['path'],set()).add(row['sha256'])
            replay=json.loads((d/'replay.json').read_text())
            expected=json.loads((root/('frozen_'+scope+'_trace.json')).read_text())
            fields=('request_id','planned_offset_s','prompt_index','initial_role','seed')
            replay_equal=[{k:r[k] for k in fields} for r in replay]==[{k:r[k] for k in fields} for r in expected]
            clients=[json.loads(line) for line in (d/'request_results.jsonl').open() if line.strip()]
            input_hashes_match=all(r['input_sha256']==prompts[r['prompt_index']]['input_sha256'] and r['input_tokens']==prompts[r['prompt_index']]['input_tokens'] for r in clients)
            rounds.append(dict(round=d.name,replay_equal=replay_equal,functional_checks=a['functional_checks'],
                input_hashes_match=input_hashes_match,
                source_t0_decoding=a['t0_source_decoding'],real_takeovers=a['unique_kv_takeovers'] if mode=='C' else a['real_token_takeovers'],
                original_strict=a['original_strict_validity'],counts=a['migration_outcome_counts']))
            for ident in json.loads((d/'owned_processes.json').read_text()):
                row={**ident,'round':d.name,'owned_process_still_alive':False}
                try:
                    p=psutil.Process(ident['pid'])
                    if p.create_time()==ident['created'] and p.status()!=psutil.STATUS_ZOMBIE:
                        row['owned_process_still_alive']=True;row['command']=p.cmdline()
                except psutil.NoSuchProcess:pass
                processes.append(row)
    guards=json.loads((root/'actual_old_revocation_protections.json').read_text())['files']
    preserved=[dict(path=g['path'],expected=g['sha256'],actual=hashlib.sha256(Path(g['path']).read_bytes()).hexdigest()) for g in guards]
    nv=NVML();readings=[]
    for attempt in range(2):
        if attempt:time.sleep(10)
        readings.append([nv.row(1,'S'),nv.row(3,'D')])
    idle=all(not json.loads(r['compute_pids']) and r['utilization_pct']==0 and r['memory_mib']<=100 for sample in readings for r in sample)
    result=dict(rounds=rounds,owned_processes=processes,selected_gpu_final_checks=readings,selected_gpus_idle=idle,
        native_hashes={k:sorted(v) for k,v in native_hashes.items()},native_hashes_consistent=all(len(v)==1 for v in native_hashes.values()),
        old_protections=preserved,old_protections_unchanged=all(g['expected']==g['actual'] for g in preserved),
        all_replays_identical=all(r['replay_equal'] for r in rounds),
        all_request_input_hashes_match=all(r['input_hashes_match'] for r in rounds),
        all_functional_checks_passed=all(all(r['functional_checks'].values()) for r in rounds),
        no_owned_services_remaining=not any(r['owned_process_still_alive'] for r in processes),
        normal_unload_qualified=False,formal_experiment_run=False,wall_ns=time.time_ns(),
        limitations=['One seed only; no production/cross-region/system-energy claim',
            'token_live zero observed background rounds is not multi-round catch-up coverage',
            'Native KV bytes measured per combined batch, exact per-request allocation unavailable',
            'Additional route/native control framing and network header bytes unavailable',
            'Coverage t0 has no target background request in flight in observed runs; trace remains unchanged',
            'Legacy strict raw 404/abort validity retained independently',
            'Native lease recheck observation has additional D2H overhead'])
    (root/'final_acceptance_audit.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in result.items() if k in ('selected_gpus_idle','old_protections_unchanged','all_replays_identical','all_functional_checks_passed','no_owned_services_remaining')}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();main(Path(a.root))
