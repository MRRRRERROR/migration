"""Stop the recorded controller only on a real formal client failure."""
import argparse
import json
from pathlib import Path
import signal
import time
import psutil

def main(root):
    pid=int((root/'controller.pid').read_text());process=psutil.Process(pid)
    if not any(name in ' '.join(process.cmdline()) for name in ('two_gpu_formal_load.py','two_gpu_formal_resume.py','two_gpu_formal_continue.py')) or str(root) not in ' '.join(process.cmdline()):raise RuntimeError('Controller identity mismatch')
    created=process.create_time();(root/'controller_identity.json').write_text(json.dumps(dict(pid=pid,created=created,command=process.cmdline())))
    offsets={};pending={}
    while process.is_running() and process.status()!=psutil.STATUS_ZOMBIE:
        for file in root.glob('*/main_*/request_results.jsonl'):
            with file.open() as f:f.seek(offsets.get(str(file),0));text=f.read();offsets[str(file)]=f.tell()
            text=pending.get(str(file),'')+text;lines=text.split('\n');pending[str(file)]=lines.pop()
            for line in lines:
                if not line.strip():continue
                row=json.loads(line)
                if row.get('error') or not row.get('completed'):
                    cutoff_path=file.parent/'measurement_cutoff.json'
                    cutoff=json.loads(cutoff_path.read_text()) if cutoff_path.exists() else {}
                    critical=cutoff.get('critical',[])
                    protected=(row.get('error')=='user_cancel_after_measurement_cutoff' and critical
                        and all(x.get('kind')=='foreign_gpu_process' for x in critical)
                        and row.get('finish_monotonic_s',0)>=cutoff.get('measurement_cutoff_monotonic_s',float('inf')))
                    if protected:
                        with (root/'protected_foreign_cancels.jsonl').open('a') as out:
                            out.write(json.dumps(dict(round=str(file.parent.relative_to(root)),request_id=row['request_id'],
                                reason='Explicit cancellation after foreign-contaminated cutoff; retained failure, controller completes owned cleanup',
                                evidence=row,cutoff=cutoff),ensure_ascii=False)+'\n')
                        continue
                    evidence=dict(kind='formal_client_failure',round=str(file.parent.relative_to(root)),request_id=row['request_id'],
                        evidence=row,controller_pid=pid,controller_created=created,signal='SIGTERM',wall_ns=time.time_ns())
                    (root/'blocked.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2))
                    if process.create_time()!=created:raise RuntimeError('Controller PID reused')
                    process.send_signal(signal.SIGTERM);return
        if (root/'completed.json').exists() or (root/'blocked.json').exists():return
        time.sleep(1)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();main(Path(a.root))

