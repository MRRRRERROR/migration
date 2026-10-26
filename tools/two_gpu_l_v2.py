"""Authorized L-only supplement: lower arrival rates, unchanged serving semantics."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import random
import shutil
import time
import traceback
from two_gpu_formal_resume import ContinuedExperiment
from two_gpu_formal_load import ORDERS, frozen_trace
from two_gpu_formal_gpu_resume import install_observation, run_with_gpu_retry
from two_gpu_live_load import dump, capacity
from two_gpu_live_report import first_difference


class SupplementExperiment(ContinuedExperiment):
    async def run(self, name, mode, trace, duration=600, t0=300, coverage=False):
        return await super().run(name, mode, trace, duration, t0, name.startswith('coverage_'))


def compare_coverage(base):
    def read(path):
        return [json.loads(line) for line in path.open() if line.strip()]
    reference = {r['request_id']: r for r in read(base / 'coverage_B/request_results.jsonl')}
    comparisons = []
    for mode in 'CDE':
        file = base / ('coverage_' + mode) / 'request_results.jsonl'
        if not file.exists():
            continue
        for row in read(file):
            old = reference.get(row['request_id'])
            if not old or not old.get('completed') or not row.get('completed'):
                continue
            comparisons.append(dict(mode=mode, request_id=row['request_id'],
                first_token=first_difference(old['token_ids'], row['token_ids']),
                text_equal=old['text'] == row['text'], finish_equal=old['finish_reason'] == row['finish_reason'],
                input_equal=old['input_sha256'] == row['input_sha256']))
    errors = [r for r in comparisons if r['first_token'] is not None or not all(r[k] for k in ('text_equal', 'finish_equal', 'input_equal'))]
    dump(base / 'coverage_paired_output_comparisons.json', dict(comparisons=comparisons, errors=errors))
    if errors:
        raise RuntimeError('Coverage output mismatch')


async def execute(args):
    e = SupplementExperiment(args)
    root = e.base
    lock = (root / 'experiment_execution.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    install_observation(e)
    path = root / 'L'
    path.mkdir(exist_ok=True)
    e.root = path
    plan = dict(version='L-v2', planned_formal_cells=4, source_candidates=[.35, .25, .15],
        background_rate=.3, formal_order=ORDERS['L'], master_seed=101, calibration_s=300,
        duration_s=600, t0_s=300, post_measurement_alive_s=30, max_output_tokens=4096,
        ignore_eos=False, kv_cache_memory_bytes=4294967296, normal_unload_qualified=False,
        old_results_preserved=True, conditional_coverage='16 fixed long prompts when main t0 L coverage is insufficient; B then C/D/E, 180s, trigger >=8 decoding within 60s')
    file = root / 'supplement_plan.json'
    if file.exists() and json.loads(file.read_text()) != plan:
        raise RuntimeError('Supplement frozen plan changed')
    dump(file, plan)
    common = json.loads((root / 'minimal_preflight_v2.json').read_text())
    old = Path(common['old_root'])
    config = json.loads((old / 'effective_common_config.json').read_text())
    config.update(gpus=e.gpus, gpu_authorization='User 2026-10-11 authorized continuation on idle physical GPUs 0,2; earlier authorization permits another idle pair if occupied')
    dump(root / 'effective_common_config.json', config)
    dump(root / 'independent_acceptance_state.json', dict(status='running', scope='L-v2 lower-rate supplement', normal_unload_qualified=False, old_revocation_preserved=True))
    for name in ('workload_manifest.json', 'dependencies.json', 'model_configuration_hashes.json', 'version_manifest.json', 'effective_common_config.json'):
        if not (path / name).exists():
            shutil.copy2(root / name, path / name)
    dump(path / 'scheme_order.json', ORDERS['L'])
    cal_file = path / 'capacity_calibration.json'
    cal = json.loads(cal_file.read_text()) if cal_file.exists() else []
    for i, sr in enumerate(plan['source_candidates'], 1):
        existing = next((row for row in cal if row['candidate'] == i), None)
        if existing:
            if existing['source_rate'] != sr or existing['background_rate'] != .3:
                raise RuntimeError('Frozen candidate changed')
            if existing['capacity']['stable']:
                break
            continue
        trace, seeds = frozen_trace(e.prompts, 'L', 300, sr, .3, 'calibration')
        dump(path / f'calibration_trace_{i}.json', dict(requests=trace, seeds=seeds))
        e.scenario = None
        summary, rows, samples = await run_with_gpu_retry(e, f'calibration_{i}', 'calibration', trace, 300, 300)
        measure = capacity(summary, rows, samples, sr + .3)
        cal.append(dict(candidate=i, source_rate=sr, background_rate=.3, capacity=measure))
        dump(cal_file, cal)
        print('CAPACITY', i, json.dumps(measure), flush=True)
        if measure['stable']:
            break
    if not cal[-1]['capacity']['stable']:
        dump(path / 'capacity_blocked.json', dict(status='three authorized lower-rate candidates failed', candidates=cal))
    else:
        frozen = path / 'frozen_main_trace.json'
        if frozen.exists():
            trace = json.loads(frozen.read_text())
        else:
            trace, seeds = frozen_trace(e.prompts, 'L', 600, sr, .3)
            dump(frozen, trace)
            dump(path / 'derived_seeds.json', seeds)
            dump(path / 'frozen_load.json', dict(source_rate=sr, background_rate=.3, seed=101,
                duration_s=600, t0_s=300, basis=cal[-1], scenario='L', version='L-v2'))
        e.scenario = 'L'
        for mode in ORDERS['L']:
            cell = 'L_' + mode
            if cell in e.completed:
                continue
            await run_with_gpu_retry(e, 'main_' + mode, mode, trace)
            e.compare_to_B()
            e.completed.append(cell)
            dump(root / 'progress.json', dict(status='between_rounds', completed_formal_rounds=e.completed, gpus=e.gpus))
        coverage_needed = any(not json.loads((path / ('main_' + m) / 'timeline.json').read_text()).get('coverage', {}).get('passed', False) for m in 'BCDE')
        dump(path / 'coverage_decision.json', dict(required=coverage_needed, reason='main t0 frozen L coverage insufficient' if coverage_needed else 'all main rounds met frozen L coverage'))
        if coverage_needed:
            longs = [i for i, p in enumerate(e.prompts) if 2048 <= p['input_tokens'] <= 4096]
            indices = random.Random(101).sample(longs, 16)
            fixed = [dict(request_id=f'coverage-S-{i:02d}', initial_role='S', planned_offset_s=0., prompt_index=idx, seed=101) for i, idx in enumerate(indices)]
            background, seeds = frozen_trace(e.prompts, 'L', 180, .15, .3, 'coverage')
            cover = sorted(fixed + [r for r in background if r['initial_role'] == 'D'], key=lambda r: r['planned_offset_s'])
            dump(path / 'coverage_prompt_indices.json', indices)
            dump(path / 'frozen_coverage_trace.json', cover)
            coverage_done = []
            for mode in 'BCDE':
                await run_with_gpu_retry(e, 'coverage_' + mode, mode, cover, 180, 0)
                coverage_done.append(mode)
                dump(path / 'coverage_progress.json', dict(completed=coverage_done))
                if mode == 'B' and not json.loads((path / 'coverage_B/timeline.json').read_text()).get('t0_monotonic_s'):
                    dump(path / 'coverage_insufficient.json', dict(reason='Natural EOS prevented B trigger; remaining coverage modes not run'))
                    break
                compare_coverage(path)
    limited = (path / 'capacity_blocked.json').exists()
    dump(root / 'completed.json', dict(completed_formal_rounds=e.completed, planned=4, fully_completed=len(e.completed) == 4,
        execution_finished=True, capacity_limited_scenes=['L'] if limited else [], normal_unload_qualified=False, wall_ns=time.time_ns()))
    dump(root / 'progress.json', dict(status='finished', completed_formal_rounds=e.completed, gpus=e.gpus))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--gpus', required=True)
    parser.add_argument('--recovery-tag', default='lv2')
    args = parser.parse_args()
    try:
        asyncio.run(execute(args))
    except BaseException as exc:
        dump(Path(args.root) / 'blocked.json', dict(error=repr(exc), traceback=traceback.format_exc(), wall_ns=time.time_ns()))
        raise
