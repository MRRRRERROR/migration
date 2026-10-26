"""Cold retry of foreign-GPU-contaminated trials, using unchanged frozen traces."""
import asyncio
import ctypes
import itertools
import json
from pathlib import Path
import time
import psutil
from two_gpu_live_load import append, dump


def install_observation(e):
    original_row = e.nv.row
    original_owned = e.owned

    def row(index, role):
        result = original_row(index, role)
        identities = []
        for item in json.loads(result['compute_pids']):
            try:
                p = psutil.Process(item['pid'])
                identities.append(dict(pid=p.pid, created=p.create_time(), ppid=p.ppid(),
                    username=p.username(), executable=p.exe()))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                identities.append(dict(pid=item['pid'], identity_unavailable=True))
        result['compute_identities'] = identities
        return result

    def owned():
        ids = original_owned()
        live = []
        for pid in ids:
            try:
                p = psutil.Process(pid)
                if p.create_time() == e.identities[pid]:
                    live.append(dict(pid=pid, created=p.create_time(), ppid=p.ppid(), command=p.cmdline()))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        append(e.base / 'owned_tree_identity_samples.jsonl', dict(wall_ns=time.time_ns(),
            scope=str(e.root.relative_to(e.base)), processes=live))
        return {item['pid'] for item in live}

    e.nv.row, e.owned = row, owned


def inventory(e):
    count = ctypes.c_uint()
    if e.nv.lib.nvmlDeviceGetCount_v2(ctypes.byref(count)) != 0 or count.value < 2:
        raise RuntimeError('GPU inventory query failed; no card is assumed idle')
    rows = []
    for index in range(count.value):
        try:
            rows.append(e.nv.row(index, 'inventory'))
        except Exception as error:
            rows.append(dict(gpu_index=index, query_error=repr(error)))
    return rows


def idle(row):
    return (not row.get('query_error') and row.get('gpu_uuid', '').startswith('GPU-')
        and row.get('memory_mib') is not None and row['memory_mib'] <= 100
        and row.get('utilization_pct') == 0 and not json.loads(row['compute_pids']))


async def select_pair(e, context, avoid=()):
    previous = tuple(e.gpus)
    while True:
        try:
            first = inventory(e)
            append(e.base / 'gpu_selection_observations.jsonl', dict(context=context, phase='first', rows=first))
            available = [r['gpu_index'] for r in first if idle(r)]
            options = list(itertools.combinations(available, 2))
            if not options:
                raise RuntimeError('Fewer than two verified idle GPUs')
            pair = min(options, key=lambda p: (sum(i in avoid for i in p), p != previous, p != (0, 2), p))
            await asyncio.sleep(10)
            second = inventory(e)
            append(e.base / 'gpu_selection_observations.jsonl', dict(context=context, phase='second', rows=second))
            lookup = {r['gpu_index']: r for r in second}
            initial = {r['gpu_index']: r for r in first}
            if not all(idle(lookup[i]) and initial[i]['gpu_uuid'] == lookup[i]['gpu_uuid'] for i in pair):
                continue
            e.gpus = list(pair)
            e.args.gpus = ','.join(map(str, pair))
            selected = dict(context=context, previous_gpus=previous, gpus=e.gpus,
                uuids={role: lookup[i]['gpu_uuid'] for role, i in zip('SD', pair)},
                reason='User authorized any other idle pair when selected cards are occupied',
                avoided_previous_interference_indices=list(avoid), wall_ns=time.time_ns())
            append(e.base / 'gpu_selection_history.jsonl', selected)
            dump(e.base / 'current_gpu_selection.json', selected)
            return
        except Exception as error:
            append(e.base / 'gpu_selection_observations.jsonl', dict(context=context, error=repr(error), wall_ns=time.time_ns()))
            dump(e.base / 'progress.json', dict(status='waiting_for_two_gpus', pending=context,
                completed_formal_rounds=e.completed, last_error=repr(error), gpus=e.gpus))
            await asyncio.sleep(60)


def archive_foreign(e, folder, tag, startup_busy=False):
    summary_file = folder / 'summary.json'
    if summary_file.exists():
        summary = json.loads(summary_file.read_text())
        if not summary['critical'] or any(c['kind'] != 'foreign_gpu_process' for c in summary['critical']):
            raise RuntimeError('A non-foreign measurement failure requires attention; no automatic retry')
        if not summary['cleanup_safe'] or not summary['kv_restored']:
            raise RuntimeError('Contaminated attempt has uncleared request/KV resources')
        regular=folder/'shutdown.json'
        manual=folder/'interrupted_shutdown.json'
        shutdown = json.loads((regular if regular.exists() else manual).read_text())
        if any(r.get('remaining_children') or (r.get('returncode') != 0 if regular.exists() else not r.get('exited')) for r in shutdown):
            raise RuntimeError('Contaminated attempt has residual owned processes')
        avoid = sorted({c['gpu']['gpu_index'] for c in summary['critical']})
    elif startup_busy and not (folder / 'owned_processes.json').exists():
        avoid = list(e.gpus)
    else:
        raise RuntimeError('Unfinished attempt cannot be safely classified/archived')
    identities = json.loads((folder / 'owned_processes.json').read_text()) if (folder / 'owned_processes.json').exists() else []
    for identity in identities:
        try:
            p = psutil.Process(identity['pid'])
            if p.create_time() == identity['created'] and p.status() != psutil.STATUS_ZOMBIE:
                raise RuntimeError('Recorded owned process remains alive; do not start another service')
        except psutil.NoSuchProcess:
            pass
    archive = e.base / 'interruptions' / (e.root.name + '_' + folder.name + '_foreign_' + tag + '_' + str(time.time_ns()))
    archive.mkdir(parents=True, exist_ok=False)
    if not folder.resolve().is_relative_to(e.base.resolve()):
        raise RuntimeError('Unsafe archive source')
    folder.rename(archive / folder.name)
    dump(archive / 'recovery_reason.json', dict(reason='foreign GPU interference',
        retry_same_frozen_trace=True, prior_gpus=e.gpus, avoid=avoid,
        no_foreign_process_terminated=True, wall_ns=time.time_ns()))
    print('FOREIGN_ATTEMPT_ARCHIVED', str(archive), flush=True)
    return avoid


async def run_with_gpu_retry(e, name, mode, trace, duration=600, t0=300):
    context = dict(scenario=e.root.name, name=name, mode=mode)
    avoid = []
    if (e.root / name).exists():
        archive_foreign(e, e.root / name, e.args.recovery_tag)
    while True:
        await select_pair(e, context, avoid)
        # Caller stores the frozen candidate/scene; GPU wait never changes it.
        stage = dict(status='calibrating' if mode == 'calibration' else 'running', scenario=e.root.name,
            completed_formal_rounds=e.completed, gpus=e.gpus)
        stage.update(candidate=int(name.split('_')[-1])) if mode == 'calibration' else stage.update(mode=mode)
        dump(e.base / 'progress.json', stage)
        try:
            return await e.run(name, mode, trace, duration, t0)
        except RuntimeError as error:
            busy_before_start = str(error).startswith('Authorized GPU busy/query unavailable:')
            avoid = archive_foreign(e, e.root / name, e.args.recovery_tag, busy_before_start)
            append(e.base / 'foreign_retry_history.jsonl', dict(context=context, error=repr(error),
                gpus=e.gpus, wall_ns=time.time_ns(), same_trace=True))
