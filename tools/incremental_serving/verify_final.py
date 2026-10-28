"""Offline audit of frozen code, native actions and recorded GPU interference.

Writes derived evidence only. No GPU calls, service commands or raw-file edits.
Run after copying closed rounds and shared services into a self-contained mirror.
"""
import argparse
from bisect import bisect_right
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

from analyze import read,lines,write,csvfile


def windows(root):
    result=[]
    for f in root.iterdir():
        if not f.is_dir() or not (f/'measurement_rows.json').exists():continue
        timeline=read(f/'timeline.json');start=timeline['start_monotonic_s']
        rows=read(f/'measurement_rows.json');decisions=lines(f/'decisions.jsonl');states=lines(f/'states.jsonl')
        times=[r['finish_monotonic_s'] for r in rows if r.get('finish_monotonic_s') is not None]
        times += [d.get('terminal',{}).get('confirmed_monotonic_s',d['monotonic_s']) for d in decisions]
        times += [s['monotonic_s'] for s in states]
        end=max(times,default=start)
        cores={i:{s['engines'][i]['pid'] for s in states} for i in range(2)}
        result.append(dict(folder=f,start=start,end=end,wall_start=timeline['start_wall_ns'],
            wall_end=timeline['start_wall_ns']+round((end-start)*1e9),cores=cores,
            decisions=decisions,native=Counter(),native_request_ids=Counter(),power_samples=Counter(),
            unavailable_power=Counter(),foreign_samples=0))
    return sorted(result,key=lambda x:x['start'])


def main():
    parser=argparse.ArgumentParser();parser.add_argument('root');args=parser.parse_args();root=Path(args.root)
    audit=root/'final_audit';audit.mkdir(exist_ok=True)
    frozen=root/'formal_frozen_code.json';code=[]
    if frozen.exists():
        for name,expected in read(frozen).items():
            p=root/'execution_sources'/name
            actual=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
            code.append(dict(file=name,expected_sha256=expected,actual_sha256=actual,matches=expected==actual))
    csvfile(audit/'frozen_code_check.csv',code)
    rounds=windows(root);starts=[r['start'] for r in rounds];walls=[r['wall_start'] for r in rounds]
    def owner(t,clock='mono'):
        points=starts if clock=='mono' else walls;i=bisect_right(points,t)-1
        if i<0:return None
        r=rounds[i];end=r['end'] if clock=='mono' else r['wall_end']
        return r if t<=end else None
    native=root/'services_01'/'kv_events.jsonl';malformed=[]
    if native.exists():
        with native.open(encoding='utf-8') as inp:
            for line_number,line in enumerate(inp,1):
                if not line.strip():continue
                try:e=json.loads(line)
                except json.JSONDecodeError:
                    malformed.append(dict(file=str(native),line=line_number));continue
                t=e.get('timestamp_ns')
                if t is None:continue
                r=owner(t,'wall')
                if r is not None:
                    event=e.get('event_name',e.get('event','unknown'));r['native'][event]+=1
                    if event=='migration_initiated':r['native_request_ids'][e.get('request_id')]+=1
    power=root/'services_01'/'power.csv';foreign=[];handles={};writers={}
    try:
        if power.exists():
            with power.open(encoding='utf-8-sig',newline='') as inp:
                reader=csv.DictReader(inp)
                for row in reader:
                    try:r=owner(float(row['monotonic_s']));gpu=int(row['gpu_index'])
                    except (ValueError,KeyError):
                        malformed.append(dict(file=str(power),row=str(row)[:1000]));continue
                    if r is None:continue
                    f=r['folder'];key=str(f)
                    if key not in handles:
                        handles[key]=(f/'power.csv').open('w',encoding='utf-8-sig',newline='')
                        writers[key]=csv.DictWriter(handles[key],fieldnames=reader.fieldnames);writers[key].writeheader()
                    writers[key].writerow(row);r['power_samples'][gpu]+=1
                    r['unavailable_power'][gpu]+=row.get('power_w') in ('',None)
                    try:pids=json.loads(row.get('compute_pids') or '[]')
                    except json.JSONDecodeError:
                        malformed.append(dict(file=str(power),reason='compute_pids JSON invalid',row=row));continue
                    unexpected=[p for p in pids if p['pid'] not in r['cores'].get(gpu,set())]
                    if unexpected:
                        r['foreign_samples']+=1
                        foreign.append(dict(round=f.name,**row,unexpected_pids=json.dumps(unexpected)))
    finally:
        for f in handles.values():f.close()
    rows=[]
    for r in rounds:
        decisions=[d for d in r['decisions'] if d.get('action')=='running_kv_migration']
        expected=Counter(d['candidate']['request']['request_id'] for d in decisions
            if d.get('terminal',{}).get('classification')!='not_dispatched')
        selected=Counter(d['candidate']['request']['request_id'] for d in decisions)
        noncompleted=Counter(d['candidate']['request']['request_id'] for d in decisions
            if d.get('terminal',{}).get('classification') in ('not_dispatched','failed','cancelled'))
        unexpected=r['native_request_ids']-selected
        missing=selected-r['native_request_ids']
        explained=not unexpected and all(n<=noncompleted[rid] for rid,n in missing.items())
        terminals=lines(r['folder']/'migration_terminals.jsonl')
        unavailable=sum(d.get('reason')=='metrics_unavailable' for d in r['decisions'])
        missing_leases=0;duplicate_leases=0
        for terminal in terminals:
            leases=terminal.get('leases',[])
            acquired=Counter(e['lease_id'] for e in leases if e['event']=='acquire')
            released=Counter(e['lease_id'] for e in leases if e['event']=='release')
            missing_leases+=terminal['classification']=='completed' and not acquired
            duplicate_leases+=sum(max(0,n-1) for n in acquired.values())+sum(max(0,n-1) for n in released.values())
        rows.append(dict(round=r['folder'].name,start_monotonic_s=r['start'],end_monotonic_s=r['end'],
            selected_running_sessions=len(decisions),native_initiated=r['native']['migration_initiated'],
            expected_native_initiations=sum(expected.values()),
            selected_matches_native_initiation_ids=expected==r['native_request_ids'],
            native_session_accounting_explained=explained,
            unexpected_initiation_ids_json=json.dumps(unexpected,sort_keys=True),
            missing_initiation_ids_json=json.dumps(missing,sort_keys=True),
            native_migration_done=r['native']['migration_done'],native_cancel=r['native']['migration_attempt_cancelled'],
            controller_terminal_counts=json.dumps(Counter(t['classification'] for t in terminals),sort_keys=True),
            unbalanced_terminal_leases=sum(t.get('source_lease_balanced') is not True for t in terminals),
            completed_missing_acquire_evidence=missing_leases,duplicate_lease_events=duplicate_leases,
            all_native_event_counts=json.dumps(r['native'],sort_keys=True),foreign_gpu_samples=r['foreign_samples'],
            controller_decisions=len(r['decisions']),metrics_unavailable_decisions=unavailable,
            control_metrics_available_fraction=1-unavailable/len(r['decisions']) if r['decisions'] else None,
            gpu0_power_samples=r['power_samples'][0],gpu1_power_samples=r['power_samples'][1],
            gpu0_unavailable_power=r['unavailable_power'][0],gpu1_unavailable_power=r['unavailable_power'][1]))
    csvfile(audit/'native_and_resource_audit.csv',rows);csvfile(audit/'foreign_gpu_evidence.csv',foreign)
    write(audit/'malformed_records.json',malformed)
    summary=dict(closed_rounds=len(rows),frozen_files_checked=len(code),frozen_code_mismatches=sum(not r['matches'] for r in code),
        native_initiation_mismatches=sum(not r['selected_matches_native_initiation_ids'] for r in rows),
        unexplained_native_session_accounting=sum(not r['native_session_accounting_explained'] for r in rows),
        unbalanced_terminals=sum(r['unbalanced_terminal_leases'] for r in rows),foreign_gpu_samples=len(foreign),
        completed_missing_acquire_evidence=sum(r['completed_missing_acquire_evidence'] for r in rows),
        duplicate_lease_events=sum(r['duplicate_lease_events'] for r in rows),
        malformed_records=len(malformed),native_file_available=native.exists(),power_file_available=power.exists(),
        evidence_limit='NVML polling may miss activity between samples; absence of recorded foreign use is not continuous proof. Native counts retain event semantics; request_finished events are not logical user failures.')
    write(audit/'result.json',summary);print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':main()
