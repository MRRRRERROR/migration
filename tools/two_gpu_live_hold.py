"""Close the main coordinator between phases; never interrupts measurement."""
import argparse
import json
from pathlib import Path
import signal
import time
import psutil

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();root=Path(a.root)
    identity=json.loads((root/'controller_identity.json').read_text());process=psutil.Process(identity['pid'])
    if process.create_time()!=identity['created']:raise RuntimeError('Coordinator PID changed')
    while process.is_running() and process.status()!=psutil.STATUS_ZOMBIE:
        if (root/'main_C/post_shutdown_gpu.json').exists() and not (root/'blocked.json').exists():
            record=dict(reason='Main measurements complete; switch coverage to a full NVML pre-baseline adapter',
                        pid=process.pid,created=process.create_time(),signal='SIGTERM',monotonic_s=time.monotonic())
            (root/'main_measurements_complete_hold.json').write_text(json.dumps(record,indent=2))
            if process.create_time()!=identity['created']:raise RuntimeError('Coordinator identity changed')
            process.send_signal(signal.SIGTERM);break
        if (root/'blocked.json').exists() or (root/'semantic_blocked.json').exists():break
        time.sleep(.1)
