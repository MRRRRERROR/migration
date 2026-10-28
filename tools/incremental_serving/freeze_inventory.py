"""Small read-only inventory; reuses pre-measurement native binary hashes."""
import argparse
import hashlib
import json
from pathlib import Path
import time

def main():
    p=argparse.ArgumentParser();p.add_argument('root');a=p.parse_args();r=Path(a.root)
    def sha(f):return hashlib.sha256(f.read_bytes()).hexdigest()
    source=r/'execution_sources';archive=r/'failures/01_event_name_parser/execution_sources'
    engine=archive if archive.exists() else source
    value=dict(wall_ns=time.time_ns(),controller_sources={str(f):sha(f) for f in source.glob('*.py')},
        actually_tested_engine_hooks={name:dict(path=str(engine/name),sha256=sha(engine/name)) for name in ['engine_control.py','sitecustomize.py','policy.py']},
        native_binary_hashes_origin='services_01/native_loaded.json: captured from owned /proc/PID/maps before measurement; unique paths below',
        native={x['path']:x['sha256'] for x in json.loads((r/'services_01/native_loaded.json').read_text())},
        model_weights=[dict(path=str(f),realpath=str(f.resolve()),size=f.stat().st_size,mtime_ns=f.stat().st_mtime_ns) for f in Path('/data/models/Qwen2.5-7B-Instruct').glob('*.safetensors')],
        model_weight_hash_limitation='Weight files identified by real path, size and modification time; full weights not rehashed during live replay to avoid I/O interference.',
        public_reference=dict(url='https://github.com/MRRRRERROR/migration.git',status='read unavailable: web cache miss and bounded remote ls-remote timed out',code_imported=False),
        no_server_compatibility_files_modified=True,no_push_or_publish=True)
    (r/'audit/frozen_inventory.json').write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    print('INVENTORY_WRITTEN',len(value['native']))

if __name__=='__main__':main()
