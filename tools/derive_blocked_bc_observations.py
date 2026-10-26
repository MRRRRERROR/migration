"""Recover recorded C measurements without creating a successful summary."""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import re
import statistics

from two_gpu_power_metrics import power_metrics


def stats(values):
    values=sorted(value for value in values if value is not None)
    return {'n':len(values),'mean':statistics.mean(values) if values else None,
            'p95':values[int(.95*(len(values)-1))] if values else None,'max':max(values) if values else None}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);args=parser.parse_args()
    root=Path(args.root);folder=root/'acceptance_C'
    requests=[json.loads(line) for line in (folder/'request_results.jsonl').read_text(encoding='utf-8').splitlines()]
    timeline=json.loads((folder/'timeline.json').read_text(encoding='utf-8'));t0=timeline['t0_monotonic_s']
    with (folder/'power.csv').open(encoding='utf-8') as stream:power=list(csv.DictReader(stream))
    logs='\n'.join(path.read_text(encoding='utf-8',errors='replace') for path in folder.glob('vllm_*.log'))
    summary={'name':'acceptance_C','mode':'C','seed':999,'acceptance_passed':False,'status':'blocked_target_native_shutdown_segfault',
        'scope':'Derived from preserved client/power records after controller halted; does not override or synthesize a successful round',
        'planned':sum(1 for line in (folder/'replay.jsonl').read_text().splitlines() if line),
        'sent':len(requests),'completed':sum(row.get('completed',False) for row in requests),
        'unfinished':sum(not row.get('completed',False) for row in requests),
        'finish_reasons':dict(Counter(row.get('finish_reason') for row in requests)),
        'send_delay_s':stats([row['send_delay_s'] for row in requests]),'timing':timeline,'power':power_metrics(power,t0),
        'max_output_block_gap_s':stats([row.get('max_output_block_gap_s') for row in requests]),
        'raw_counts':{'kvt500':logs.count('KVTResp(code=500'),'kvt404':logs.count('KVTResp(code=404'),
                      'cancel':len(re.findall(r'(?im)^.*abort req.*$',logs)),'traceback':logs.count('Traceback (most recent call last)'),
                      'committed_migration':logs.count('"event_name": "migration_done"'),
                      'segfault':logs.count('Segfault encountered'),'native_assert':logs.count('AssertFailed')},
        'raw_strict_validity':'invalid_native_teardown_failure'}
    summary['completion_rate']=summary['completed']/summary['planned']
    for name,predicate in (('D_background',lambda row:row['initial_role']=='D'),
                           ('new_transferred',lambda row:row['initial_role']=='S' and row.get('route_role')=='D'),
                           ('t0_S_inflight',lambda row:row.get('route_role')=='S' and row['actual_send_monotonic_s']<t0<row['finish_monotonic_s'])):
        selected=[row for row in requests if predicate(row)]
        summary[name]={'n':len(selected),'ttft_s':stats([row.get('ttft_s') for row in selected]),
                       'tpot_s':stats([row.get('tpot_s') for row in selected]),'e2e_s':stats([row.get('e2e_s') for row in selected])}
    (folder/'blocked_round_observations.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({key:summary[key] for key in ('status','sent','completed','finish_reasons','raw_counts')}))


if __name__=='__main__':main()
