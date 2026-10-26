"""Independent completed-round semantic guard; stop controller on proven corruption."""
import argparse
import collections
import json
from pathlib import Path
import signal
import time
import psutil

def load(p):return [json.loads(l) for l in p.open() if l.strip()] if p.exists() else []
def difference(a,b):return next((i for i,(x,y) in enumerate(zip(a,b)) if x!=y),min(len(a),len(b)) if len(a)!=len(b) else None)

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--controller-file',default='controller.pid');p.add_argument('--identity-name',default='controller_identity.json');a=p.parse_args();root=Path(a.root)
    pid=int((root/a.controller_file).read_text());process=psutil.Process(pid)
    if str(root) not in ' '.join(process.cmdline()) or not any(x in ' '.join(process.cmdline()) for x in ('two_gpu_live_load.py','two_gpu_live_coverage.py','two_gpu_live_load_resume.py')):raise RuntimeError('Controller identity mismatch')
    created=process.create_time();(root/a.identity_name).write_text(json.dumps(dict(pid=pid,created=created,command=process.cmdline())))
    checked=set()
    while process.is_running() and process.status()!=psutil.STATUS_ZOMBIE:
        for folder in sorted(root.iterdir()):
            if not folder.is_dir() or folder.name in checked or not (folder/'summary.json').exists():continue
            # Final results JSON is only written after all request writers finish.
            clients=load(folder/'request_results.jsonl');core=sorted([r for f in folder.glob('outputs_*.jsonl') for r in load(f)],key=lambda x:x['time_ns'])
            seq=collections.defaultdict(list)
            for row in core:seq[row['request_id']].extend(row['tokens'])
            errors=[]
            for row in clients:
                if not row.get('completed'):continue
                actual=seq[(row.get('response_id') or '')+'-0'];expected=row['token_ids'];index=difference(actual,expected)
                if index is not None:errors.append(dict(request_id=row['request_id'],reason='Core/API sequence mismatch',index=index,core_length=len(actual),api_length=len(expected)))
            if folder.name.startswith(('main_','coverage_')) and not folder.name.endswith('_B'):
                scope=folder.name.rsplit('_',1)[0];reference={r['request_id']:r for r in load(root/(scope+'_B')/'request_results.jsonl')}
                for row in clients:
                    before=reference.get(row['request_id'])
                    if before and before.get('completed') and row.get('completed'):
                        index=difference(before['token_ids'],row['token_ids'])
                        if index is not None or before['text']!=row['text'] or before['finish_reason']!=row['finish_reason']:
                            errors.append(dict(request_id=row['request_id'],reason='Natural-drain reference mismatch',index=index,
                                baseline_token=before['token_ids'][index] if index is not None and index<len(before['token_ids']) else None,
                                actual_token=row['token_ids'][index] if index is not None and index<len(row['token_ids']) else None))
            result=dict(round=folder.name,checked_completed=sum(r.get('completed',False) for r in clients),errors=errors)
            (folder/'semantic_guard.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));checked.add(folder.name)
            if errors:
                result['controller_signal']='SIGTERM';result['controller_pid']=pid
                (root/'semantic_blocked.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
                if process.create_time()!=created:raise RuntimeError('Controller PID reused')
                process.send_signal(signal.SIGTERM)
                return
        if (root/'completed.json').exists() or (root/'blocked.json').exists():return
        time.sleep(2)
if __name__=='__main__':main()

