"""Continue unchanged frozen candidates/cells after transient foreign GPU interference."""
import argparse
import asyncio
import json
from pathlib import Path
import shutil
import time
import traceback
import re
import fcntl
from two_gpu_formal_resume import ContinuedExperiment
from two_gpu_formal_load import ORDERS, frozen_trace
from two_gpu_live_load import dump, capacity
from two_gpu_formal_gpu_resume import install_observation, run_with_gpu_retry


async def execute(args):
    e = ContinuedExperiment(args)
    root = e.base
    lock = (root / 'experiment_execution.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    progress = json.loads((root / 'progress.json').read_text())
    install_observation(e)
    if not re.fullmatch(r'[a-z0-9_]{1,48}', args.recovery_tag):
        raise ValueError('Recovery tag must contain only lowercase letters, digits, underscore')
    archive = root / ('continuations/' + args.recovery_tag + '_before')
    archive.mkdir(parents=True, exist_ok=False)
    for name in ('blocked.json', 'report_ready.json', 'report.html', 'finish_audit.json', 'formal_analysis_index.json',
        'delivery_status.json','formal_delivery_checks.json','excluded_interrupted_attempts.json','artifact_manifest.json','DELIVERY.md'):
        p = root / name
        if p.exists():
            p.rename(archive / name)
    dump(root / ('interference_recovery_' + args.recovery_tag + '.json'), dict(
        reason='Transient foreign computations left both selected GPUs; resume unchanged frozen candidate',
        checkpoint=progress, gpus=e.gpus, gpu_checks='per-attempt all-GPU selection plus original deployment checks', wall_ns=time.time_ns(),
        interrupted_attempt=str(archive), new_candidates_added=False,
        basis='Frozen design explicitly preserves progress for continuation of unfinished rounds'))
    for scenario in 'ALH':
        path = root / scenario
        path.mkdir(exist_ok=True)
        e.root, e.scenario = path, scenario
        for name in ('workload_manifest.json', 'dependencies.json', 'model_configuration_hashes.json', 'effective_common_config.json', 'version_manifest.json'):
            if not (path / name).exists():
                shutil.copy2(root / name, path / name)
        dump(path / 'scheme_order.json', ORDERS[scenario])
        frozen_file = path / 'frozen_load.json'
        if frozen_file.exists():
            trace = json.loads((path / 'frozen_main_trace.json').read_text())
        else:
            cal_file = path / 'capacity_calibration.json'
            cal = json.loads(cal_file.read_text()) if cal_file.exists() else []
            candidates = [1., .75, .5] if scenario == 'L' else [.9, .6, .45]
            for i, rate in enumerate(candidates, 1):
                sr, dr = (rate, .3) if scenario == 'L' else (1., rate)
                existing = next((row for row in cal if row['candidate'] == i), None)
                if existing:
                    if (existing['source_rate'], existing['background_rate']) != (sr, dr):
                        raise RuntimeError('Frozen rates changed')
                    if existing['capacity']['stable']:
                        break
                    continue
                trace_file = path / f'calibration_trace_{i}.json'
                if trace_file.exists():
                    trace = json.loads(trace_file.read_text())['requests']
                else:
                    trace, seeds = frozen_trace(e.prompts, scenario, 300, sr, dr, 'calibration')
                    dump(trace_file, dict(requests=trace, seeds=seeds))
                e.scenario = None
                dump(root / 'progress.json', dict(status='calibrating', scenario=scenario, candidate=i,
                    completed_formal_rounds=e.completed, gpus=e.gpus))
                summary, records, samples = await run_with_gpu_retry(e, f'calibration_{i}', 'calibration', trace, 300, 300)
                e.scenario = scenario
                measure = capacity(summary, records, samples, sr + dr)
                cal.append(dict(candidate=i, source_rate=sr, background_rate=dr, capacity=measure))
                dump(cal_file, cal)
                print('CAPACITY', scenario, i, json.dumps(measure), flush=True)
                if measure['stable']:
                    break
            if not cal[-1]['capacity']['stable']:
                dump(path / 'capacity_blocked.json', dict(status='no capacity configuration within three frozen candidates', candidates=cal))
                continue
            trace, seeds = frozen_trace(e.prompts, scenario, 600, sr, dr)
            dump(path / 'frozen_main_trace.json', trace)
            dump(path / 'derived_seeds.json', seeds)
            dump(path / 'frozen_load.json', dict(source_rate=sr, background_rate=dr, seed=101,
                duration_s=600, t0_s=300, basis=cal[-1], scenario=scenario))
        for mode in ORDERS[scenario]:
            cell = scenario + '_' + mode
            if cell in e.completed:
                continue
            dump(root / 'progress.json', dict(status='running', scenario=scenario, mode=mode,
                completed_formal_rounds=e.completed, gpus=e.gpus))
            await run_with_gpu_retry(e, 'main_' + mode, mode, trace)
            e.compare_to_B()
            e.completed.append(cell)
            dump(root / 'progress.json', dict(status='between_rounds', completed_formal_rounds=e.completed, gpus=e.gpus))
    limits=[scenario for scenario in 'ALH' if (root/scenario/'capacity_blocked.json').exists()]
    dump(root / 'completed.json', dict(completed_formal_rounds=e.completed, planned=12,
        fully_completed=len(e.completed)==12, execution_finished=True, capacity_limited_scenes=limits,
        formal_five_seed_experiment=False, normal_unload_qualified=False, wall_ns=time.time_ns()))
    dump(root / 'progress.json', dict(status='finished', completed_formal_rounds=e.completed, gpus=e.gpus))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--gpus', required=True)
    parser.add_argument('--recovery-tag', required=True, help='Unique evidence tag; prior interrupted attempts are never overwritten')
    args = parser.parse_args()
    try:
        asyncio.run(execute(args))
    except BaseException as exc:
        dump(Path(args.root) / 'blocked.json', dict(error=repr(exc), traceback=traceback.format_exc(), wall_ns=time.time_ns()))
        raise
