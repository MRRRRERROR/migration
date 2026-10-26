"""Classify existing exit evidence against the saved measurement cutoff; no repair."""
import argparse
import json
from pathlib import Path
import re

def main(root):
    findings=[]
    for folder in root.iterdir():
        if not folder.is_dir() or not (folder/'measurement_cutoff.json').exists():continue
        timeline=json.loads((folder/'measurement_cutoff.json').read_text());cutoff=timeline['measurement_cutoff_monotonic_s'];events=[]
        for path in folder.glob('vllm_*.log'):
            with path.open('rb') as f:
                f.seek(max(0,path.stat().st_size-160000));lines=f.read().decode('utf-8','replace').split('\n')
            for i,line in enumerate(lines):
                if not re.search('AssertFailed|Segfault encountered|Fatal Python error',line):continue
                context=lines[max(0,i-12):i+16];preceding=next((l for l in reversed(lines[:i]) if 'KVT_SHUTDOWN' in l and 'mono_ns=' in l),None)
                match=re.search(r'mono_ns=(\d+)',preceding or '')
                stamp=int(match[1])/1e9 if match else None
                events.append(dict(log=path.name,marker=line,preceding_shutdown_event=preceding,
                    preceding_shutdown_after_cutoff_s=stamp-cutoff if stamp else None,
                    measured_phase='after_cutoff_exit' if stamp and stamp>cutoff else 'not_proven',
                    exact_crash_timestamp=None,context=context))
        if events:
            value=dict(round=folder.name,measurement_cutoff_monotonic_s=cutoff,events=events,
                parent_shutdown_records=json.loads((folder/'shutdown.json').read_text()),
                native_exit_fault=True,normal_unload_qualified=False,
                interpretation='Parent return code 0 does not erase native exit-fault markers; no Barex repair performed.')
            (folder/'exit_phase_audit.json').write_text(json.dumps(value,ensure_ascii=False,indent=2));findings.append(value)
    (root/'exit_faults_summary.json').write_text(json.dumps(findings,ensure_ascii=False,indent=2))
    print(json.dumps([dict(round=x['round'],timing=[e['preceding_shutdown_after_cutoff_s'] for e in x['events']]) for x in findings]))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();main(Path(a.root))
