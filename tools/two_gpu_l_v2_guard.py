"""Read current-attempt client journals safely across preserved cold retries."""
import argparse
import json
from pathlib import Path
import signal
import time
import psutil


def main(root):
    identity = json.loads((root / 'controller_identity.json').read_text())
    process = psutil.Process(identity['pid'])
    if process.create_time() != identity['created'] or str(root) not in ' '.join(process.cmdline()) or 'two_gpu_l_v2.py' not in ' '.join(process.cmdline()):
        raise RuntimeError('Controller identity mismatch')
    files = {}
    failures = []
    while process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
        journals = [p for p in (root / 'L').glob('*/request_results.jsonl') if p.parent.name.startswith(('main_', 'coverage_'))]
        for file in journals:
            st = file.stat()
            key = str(file)
            state = files.get(key)
            inode = (st.st_dev, st.st_ino)
            if state is None or state['inode'] != inode or st.st_size < state['offset']:
                state = files[key] = dict(inode=inode, offset=0, pending='')
            with file.open() as stream:
                stream.seek(state['offset'])
                text = state['pending'] + stream.read()
                state['offset'] = stream.tell()
            lines = text.split('\n')
            state['pending'] = lines.pop()
            for line in lines:
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get('error') or not row.get('completed'):
                    failures.append((file, row))
        retry = []
        for file, row in failures:
            cutoff_file = file.parent / 'measurement_cutoff.json'
            try:
                cutoff = json.loads(cutoff_file.read_text()) if cutoff_file.exists() else {}
            except json.JSONDecodeError:
                # Writer creates/truncates then finishes. Retain the failure for the next poll.
                retry.append((file, row))
                continue
            critical = cutoff.get('critical', [])
            protected = (row.get('error') == 'user_cancel_after_measurement_cutoff' and critical
                and all(x.get('kind') == 'foreign_gpu_process' for x in critical)
                and row.get('finish_monotonic_s', 0) >= cutoff.get('measurement_cutoff_monotonic_s', float('inf')))
            if protected:
                with (root / 'protected_foreign_cancels_guard_v2.jsonl').open('a') as out:
                    out.write(json.dumps(dict(round=str(file.parent.relative_to(root)), request_id=row['request_id'], evidence=row, cutoff=cutoff), ensure_ascii=False) + '\n')
                continue
            evidence = dict(kind='formal_client_failure', round=str(file.parent.relative_to(root)),
                request_id=row['request_id'], evidence=row, controller_pid=process.pid,
                controller_created=identity['created'], signal='SIGTERM', wall_ns=time.time_ns())
            (root / 'blocked.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
            if process.create_time() != identity['created']:
                raise RuntimeError('Controller PID reused')
            process.send_signal(signal.SIGTERM)
            return
        failures = retry
        if (root / 'completed.json').exists() or (root / 'blocked.json').exists():
            return
        time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    main(Path(args.root))
