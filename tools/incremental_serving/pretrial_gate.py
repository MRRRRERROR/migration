"""Decide formal/no-opportunity/invalid from retained pretrial evidence."""
import argparse
import json
from pathlib import Path
from analyze import opportunity,read,write,lines

def evaluate(root):
    root=Path(root)
    order=read(root/'pretrial_order.json');reasons=[];evidence={};attempts=0
    for row in order:
        folder=root/row['name']
        if not (folder/'validity.json').exists() or not read(folder/'validity.json')['valid']:
            reasons.append(row['name']+': missing/invalid round');continue
        rows=read(folder/'measurement_rows.json');planned=read(folder/'replay.json')
        if len(rows)!=len(planned) or len({r['request_id'] for r in rows})!=len(rows):reasons.append(row['name']+': identity count mismatch')
        if any(not r.get('completed') for r in rows):reasons.append(row['name']+': incomplete requests need diagnosis before expansion')
        decisions=lines(folder/'decisions.jsonl');opp=opportunity(folder);evidence[row['name']]=opp
        if any('error' in x for x in lines(folder/'running_attempts.jsonl')):
            reasons.append(row['name']+': ambiguous/failed migration RPC needs diagnosis')
        gaps=sum(d.get('reason')=='metrics_unavailable' for d in decisions)
        if not decisions or gaps/max(1,len(decisions))>.01:reasons.append(row['name']+': control metrics unavailable >1%')
        for d in decisions:
            if d.get('reason')=='action_exception':reasons.append(row['name']+': unhandled controller action')
            if row['mode']=='A' and d.get('action'):reasons.append(row['name']+': forbidden A action')
            if row['mode']=='B' and d.get('action')=='running_kv_migration':reasons.append(row['name']+': forbidden B action')
        if row['mode']=='C':attempts+=opp['running_attempts']
    if reasons:decision='invalid'
    elif attempts==0:decision='no_running_actions'
    else:decision='formal'
    # No action is an observation, not proof of no opportunity. The decision
    # preserves complete gap episodes and refusal journals for interpretation.
    result=dict(decision=decision,reasons=reasons,running_attempts=attempts,evidence=evidence,
        interpretation='No automatic favorable parameter search. If zero running actions, first deliver and explain measured imbalance/capacity/candidate evidence; do not mechanically run 30.')
    write(root/'pretrial_gate.json',result);return result


def main():
    p=argparse.ArgumentParser();p.add_argument('root');a=p.parse_args();result=evaluate(a.root)
    print(json.dumps({k:result[k] for k in ['decision','reasons','running_attempts']},ensure_ascii=False))

if __name__=='__main__':main()
