"""Evidence correlation after a round, preserving every original raw counter."""
import argparse
import csv
import json
from pathlib import Path
import re
import sys
import os
from collections import Counter,defaultdict

def load(path):
    if not path.exists():return []
    with path.open(encoding='utf-8') as f:return [json.loads(line) for line in f if line.strip()]
def write_csv(path,rows):
    keys=list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader()
        for row in rows:w.writerow({k:json.dumps(v) if isinstance(v,(list,dict)) else v for k,v in row.items()})
def enrich(folder,blocked=False,output_dir=None):
    output=Path(output_dir) if output_dir else folder
    output.mkdir(parents=True,exist_ok=True)
    summary=json.loads((folder/('blocked_round_observations.json' if blocked else 'summary.json')).read_text(encoding='utf-8'));clients=load(folder/'request_results.jsonl')
    byid={r['request_id']:r for r in clients}
    def client_id(raw):
        if not raw:return None
        for value in (raw,raw.removeprefix('cmpl-'),raw.removeprefix('cmpl-').removesuffix('-0')):
            if value in byid:return value
        return next((k for k in byid if k in raw),None)
    locations=defaultdict(dict)
    for role in ('S','D'):
        for p in (folder/('state_'+role)).glob('*.jsonl'):
            for sample in load(p):
                for request in sample['requests']:
                    rid=client_id(request['request_id'])
                    if not rid:continue
                    info=locations[rid].setdefault(role,{'first_monotonic_s':sample['monotonic_s'],'last_monotonic_s':sample['monotonic_s'],'max_computed_tokens':0,'max_generated_tokens':0,'observed_states':[]})
                    info['last_monotonic_s']=sample['monotonic_s'];info['max_computed_tokens']=max(info['max_computed_tokens'],request['computed_tokens'])
                    info['max_generated_tokens']=max(info['max_generated_tokens'],request['generated_tokens'])
                    if request['state'] not in info['observed_states']:info['observed_states'].append(request['state'])
    for r in clients:
        r['execution_observations']=locations[r['request_id']]
        r['actual_execution_locations']=list(locations[r['request_id']])
        r['execution_observation_scope']='EngineCore scheduler samples at 200 ms; short requests may finish between samples'
        t0=summary['timing']['t0_monotonic_s'];blocks=r.get('output_blocks',[])
        r['t0_crossing_output_block_gap_s']=max((b-a for a,b in zip(blocks,blocks[1:]) if a<=t0<=b),default=None)
    events=load(folder/'migration_events.jsonl');native=load(folder/'kvt_attempt_trace.jsonl')
    attempts=defaultdict(list)
    for event in native:
        key=event.get('attempt_id')
        if key:attempts[key].append(event)
    migrations=[]
    for attempt,observations in attempts.items():
        raw=observations[0]['request_id'];rid=client_id(raw);client=byid.get(rid,{})
        related=[e for e in events if e.get('request_id')==raw or client_id(e.get('request_id'))==rid]
        stages=[e.get('stage') for e in observations];names=[e.get('event_name') for e in related]
        committed='migration_done' in names or 'dst_resume' in names
        cancelled=any(name in names for name in ('migration_abort','migration_attempt_cancelled'))
        result='committed' if committed else 'cancelled' if cancelled else 'unresolved'
        evidence_class='unclassified'
        cancel_events=[e for e in related if e.get('event_name') in ('migration_abort','migration_attempt_cancelled')]
        source_completed_before_cancel=bool(client.get('completed') and client.get('route_role')=='S' and cancel_events
            and client.get('finish_wall_ns',float('inf'))<=min(e['timestamp_ns'] for e in cancel_events)
            and not committed)
        if cancelled and source_completed_before_cancel:evidence_class='source_completed_before_migration_only_cancel'
        elif cancelled and client.get('completed') and 'D' not in locations.get(rid,{}):evidence_class='source_completed_without_observed_target_execution'
        elif committed and client.get('completed'):evidence_class='handoff_and_client_completed'
        stamp=[e.get('timestamp_ns',int(e.get('timestamp',0)*1e9)) for e in observations]
        outcome_events=[e for e in related if e.get('event_name') in ('migration_done','migration_attempt_cancelled','migration_abort')]
        end_stamp=max((e['timestamp_ns'] for e in outcome_events),default=max(stamp))
        migrations.append({'attempt_id':attempt,'request_id':raw,'client_request_id':rid,'start_wall_ns':min(stamp),'end_wall_ns':end_stamp,
            'result':result,'evidence_class':evidence_class,'stages':stages,'events':names,
            'outcome_events':outcome_events,'source_client_completed_before_cancel':source_completed_before_cancel,
            'client_finish_wall_ns':client.get('finish_wall_ns'),'client_finish_reason':client.get('finish_reason'),
            'observed_destination_states':locations.get(rid,{}).get('D',{}).get('observed_states',[]),
            'computed_tokens_start':observations[0].get('computed_tokens'),'seen_tokens_start':observations[0].get('seen_tokens'),
            'transfer_bytes':None,'transfer_bytes_missing_reason':'per-attempt byte count requires native SendStubMetrics correlation; raw log retained',
            'retry_index':int(attempt.rsplit('#mig',1)[1])-1 if '#mig' in attempt else None,
            'wait_recompute_events':[e for e in related if any(w in str(e).lower() for w in ('wait','recompute','timeout'))]})
    native_bytes=[]
    for log in folder.glob('vllm_*.log'):
        lines=log.read_text(encoding='utf-8',errors='replace').splitlines()
        for i,line in enumerate(lines):
            finish=re.search(r'StepIdx=([^,]+),State=([12]),FinishedReq=([^,\s]+)',line)
            if not finish:continue
            metric_line=next((x for x in lines[i:i+4] if 'SendStubMetrics' in x),None)
            value=re.search(r'SbSizeTotal=([0-9.]+)',metric_line or '')
            if value:native_bytes.append({'request_id':finish[3],'client_request_id':client_id(finish[3]),'step':finish[1],'native_state':finish[2],
                'transfer_bytes':float(value[1]),'source_log':str(log),'evidence':metric_line})
    for row in migrations:
        linked=[b for b in native_bytes if b['client_request_id']==row['client_request_id']]
        siblings=[m for m in migrations if m['client_request_id']==row['client_request_id']]
        if len(siblings)==1 and linked:
            row['transfer_bytes']=sum(b['transfer_bytes'] for b in linked)
            row['transfer_bytes_missing_reason']=None
            row['transfer_bytes_scope']='Native request send summaries; includes copied stages and cancelled copies; a single observed attempt'
    # Keep the exact historic strict counter formula and annotate acceptance separately.
    previous=folder/('blocked_enriched_observations.json' if blocked else 'enriched_summary.json')
    cached=json.loads(previous.read_text(encoding='utf-8')).get('original_raw_counters') if previous.exists() else None
    try:
        if cached and 'unavailable' not in cached:
            raw=cached
        else:
            os.environ.setdefault('MIGRATION_POLICY_ROOT',str(folder.parent))
            import run_migration_policy_baselines as original
            raw=original.count_patterns(folder)
    except Exception as error:
        raw={'unavailable':repr(error)}
    summary['original_raw_counters']=raw
    runtime_text='\n'.join(line for p in folder.glob('vllm_*.log') for line in p.read_text(encoding='utf-8',errors='replace').splitlines() if '[entrypoints/logger.py:' not in line and ' prompt: ' not in line)
    summary['runtime_only_counts']={'kvt500':runtime_text.count('KVTResp(code=500'),'traceback':runtime_text.count('Traceback (most recent call last)'),
        'runtime_error':runtime_text.count('RuntimeError:'),'scope':'prompt text logged by entrypoints.logger excluded; raw counters retained'}
    excluded_markers=[]
    for path in folder.glob('vllm_*.log'):
        for line_no,line in enumerate(path.read_text(encoding='utf-8',errors='replace').splitlines(),1):
            if '[entrypoints/logger.py:' not in line and ' prompt: ' not in line:continue
            for marker in ('Traceback (most recent call last)','KVTResp(code=500','RuntimeError:'):
                if marker in line:
                    excluded_markers.append({'source_log':path.name,'line':line_no,'marker':marker,
                        'classification':'marker within request prompt log, excluded from runtime-only counter',
                        'log_prefix':line[:180],'marker_context':line[max(0,line.index(marker)-60):line.index(marker)+180]})
    (output/'prompt_log_marker_evidence.json').write_text(json.dumps(excluded_markers,ensure_ascii=False,indent=2),encoding='utf-8')
    strict_keys=('abort_count','KVTResp_code_404_count','KVTResp_code_500_count','EngineCore_fatal_count','Traceback_count')
    summary['original_raw_strict_validity']='unavailable' if 'unavailable' in raw else 'invalid' if any(raw.get(k,0) for k in strict_keys) else 'valid'
    summary['original_raw_strict_invalid_reasons']={key:raw[key] for key in strict_keys if raw.get(key,0)}
    summary['migration_attempt_classification']=dict(Counter(r['result'] for r in migrations))
    summary['migration_evidence_classes']=dict(Counter(r['evidence_class'] for r in migrations))
    summary['classification_scope']='Observed 404/cancellation with client completion is not automatically a user failure; lack of sampled target execution alone is not proof of no dual execution.'
    write_csv(output/'request_metrics.csv',clients);write_csv(output/'migration_attempt_metrics.csv',migrations)
    write_csv(output/'native_transfer_bytes.csv',native_bytes)
    (output/('blocked_enriched_observations.json' if blocked else 'enriched_summary.json')).write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    return summary

def main():
    p=argparse.ArgumentParser();p.add_argument('--round',required=True);p.add_argument('--blocked-observations',action='store_true');p.add_argument('--output-dir');a=p.parse_args();enrich(Path(a.round),a.blocked_observations,a.output_dir)
if __name__=='__main__':main()
