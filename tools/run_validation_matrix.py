"""Paired sensitivity experiment. Run from repository root; no GPU required."""
import argparse
import copy
import csv
import hashlib
import json
import random
import statistics
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from llumnix_sim import Simulation
from llumnix_sim.latency import LatencyModel
from llumnix_sim.protocol import MigrationConfig
from llumnix_sim.scheduling import select_running, source_decisions

MODES = ('none', 'kv', 'token_stop', 'token_live')


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding='utf-8')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ClientTPOTProxy(LatencyModel):
    """Estimated batch time from client TPOT; no claim of kernel calibration.

    Total context is divided by batch count before lookup. Lexicographic nearest
    batch, then nearest average context; ties prefer smaller grid coordinates.
    No interpolation/extrapolated slope. Queries outside grid are counted.
    """
    def __init__(self, config, profile):
        super().__init__(config)
        self.rows = profile['results']
        self.queries = self.outside = 0

    def decode(self, batch_size, total_context):
        if not batch_size:
            return 0.0
        context = total_context / batch_size
        self.queries += 1
        self.outside += int(not min(r['batch_size'] for r in self.rows) <= batch_size <= max(
            r['batch_size'] for r in self.rows) or not min(r['tokens_per_request'] for r in self.rows)
            <= context <= max(r['tokens_per_request'] for r in self.rows))
        row = min(self.rows, key=lambda r: (abs(r['batch_size'] - batch_size),
                  abs(r['tokens_per_request'] - context), r['batch_size'], r['tokens_per_request']))
        return self._check(row['p50'])


def workload(cfg, load, bandwidth, seed):
    rng = random.Random(seed)
    requests = []
    def add(rid, instance, prompt, arrival=0):
        requests.append(dict(request_id=rid, instance=instance, prompt_length=prompt,
                             max_tokens=cfg['max_tokens'], arrival_ms=arrival,
                             output_length=rng.randint(*cfg['output_length_range'])))
    add('candidate', 'source', cfg['candidate_prompt_tokens'])
    for i in range(cfg['source_other_requests']):
        add(f'source-{i}', 'source', cfg['source_prompt_tokens'])
    # Generate new-source lengths before target lengths: all loads share source trace.
    for i, at in enumerate(cfg['new_source_arrivals_ms']):
        add(f'new-source-{i}', 'source', cfg['source_prompt_tokens'], at)
    for i in range(load):
        add(f'target-{i}', 'target', cfg['target_prompt_tokens'])
    return dict(instances=[dict(cfg['instance'], id=n, domain=d, compute=cfg['compute'])
                           for n, d in [('source', 'a'), ('target', 'b')]],
                links=[dict(source=s, target=t,
                            bandwidth_bytes_per_ms=bandwidth if s != t else cfg['local_bandwidth_bytes_per_ms'],
                            latency_ms=cfg['latency_ms'] if s != t else 0)
                       for s, t in [('a', 'b'), ('b', 'a'), ('a', 'a'), ('b', 'b')]],
                requests=requests, kv_bytes_per_token=cfg['kv_bytes_per_token'],
                output_wire_bytes_per_token=cfg['output_wire_bytes_per_token'])


def interval_gap(times, start, end):
    """Full inter-token gaps intersecting [start,end], including boundary gaps."""
    return max((b-a for a, b in zip(times, times[1:]) if b >= start and a <= end), default=None)


def run_one(cfg, trace, profile, seed, mode, folder):
    folder.mkdir(parents=True)
    save(folder/'config.json', dict(trace, experiment=cfg, seed=seed, mode=mode))
    sim = Simulation(trace)
    for node in sim.instances.values():
        node.compute = ClientTPOTProxy(cfg['compute'], profile)
    for row in trace['requests']:
        sim.add_request(**row)
    selected = []
    def decide():
        ids, budget, remaining = select_running(sim, 'source', policy=cfg['policy'], seed=seed,
                                                **cfg['selector'])
        selected.extend(ids)
        sim.event('experiment_selection', selected_request_ids=ids, policy=cfg['policy'],
                  budget=budget, remaining_budget=remaining)
        if mode != 'none':
            for rid in ids:
                sim.migrate(rid, 'target', MigrationConfig(mode=mode, **cfg['migration']))
    sim.clock.at(cfg['trigger_ms'], decide)
    error = None
    try:
        sim.run(until_ms=cfg['horizon_ms'])
        sim.check_invariants()
    except Exception:
        error = traceback.format_exc()
        (folder/'error.txt').write_text(error, encoding='utf-8')
    summary = sim.summary()
    summary['compute_model'] = 'client_TPOT_proxy_decode_with_illustrative_prefill_replay'
    summary['proxy_lookup'] = {n.id: dict(queries=n.compute.queries, outside_grid=n.compute.outside)
                               for n in sim.instances.values()}
    save(folder/'summary.json', summary)
    with (folder/'events.jsonl').open('w', encoding='utf-8') as f:
        for event in sim.events:
            f.write(json.dumps(event)+'\n')
    integrity = all(r.client_ids == r.output_ids == list(range(1000, 1000+row['output_length']))
                    for row in trace['requests'] for r in [sim.requests[row['request_id']]])
    completed = all(r['completed'] for r in summary['requests'].values())
    kv_freed = all(n.used == 0 for n in sim.instances.values())
    replay_ms = sum(min(e['duration_ms'], max(0, sim.clock.now-e['time_ms']))
                    for e in sim.events if e['event'] == 'batch_started'
                    and any(t['kind'] == 'replay' for t in e['tasks']))
    attempts = list(sim.attempts.values())
    audit = dict(error=error, selected=selected, completed=completed, exact_outputs=integrity,
                 kv_freed=kv_freed, unfinished=[rid for rid, r in summary['requests'].items() if not r['completed']],
                 noncompleted_migrations=[a for a in summary['migrations'] if a['state'] != 'completed'],
                 timeout_events=[e for e in sim.events if 'deadline' in e['event'] or
                                 'timeout' in str(e.get('reason', '')).lower()],
                 replay_active_batch_ms=replay_ms,
                 migration_started_ms=attempts[0].started if attempts else None,
                 migration_ended_ms=attempts[0].ended if attempts else None)
    save(folder/'audit.json', audit)
    return summary, audit


def compare(cfg, trace, results, load, bandwidth, seed):
    base = results['none'][0]
    rows = []
    groups = {r['request_id']: ('candidate' if r['request_id']=='candidate' else
              'target_existing' if r['instance']=='target' else
              'source_new' if r['arrival_ms']>0 else 'source_other') for r in trace['requests']}
    for mode, (summary, audit) in results.items():
        migration = summary['migrations'][0] if summary['migrations'] else None
        # Use each migration's own interval in both arms; baseline alone has no interval.
        end = audit['migration_ended_ms'] if audit['migration_ended_ms'] is not None else cfg['horizon_ms']
        times = summary['requests']['candidate']['token_timestamps_ms']
        gap = interval_gap(times, cfg['trigger_ms'], end) if mode != 'none' else None
        base_gap = interval_gap(base['requests']['candidate']['token_timestamps_ms'],
                                cfg['trigger_ms'], end) if mode != 'none' else None
        complete = audit['completed']
        makespan = max(r['e2e_ms']+q['arrival_ms'] for q in trace['requests']
                       for r in [summary['requests'][q['request_id']]]) if complete else None
        baseline_complete = all(r['completed'] for r in base['requests'].values())
        base_makespan = max(base['requests'][q['request_id']]['e2e_ms']+q['arrival_ms']
                            for q in trace['requests']) if baseline_complete else None
        row = dict(load=load, bandwidth_bytes_per_ms=bandwidth, seed=seed, mode=mode,
                   completed=complete, error=bool(audit['error']), unfinished=len(audit['unfinished']),
                   migration_state=migration['state'] if migration else 'not_requested',
                   migration_reason=migration['reason'] if migration else None,
                   timeout_count=len(audit['timeout_events']), makespan_ms=makespan,
                   delta_makespan_ms=makespan-base_makespan if makespan is not None and base_makespan is not None else None,
                   migration_window_max_gap_ms=gap, baseline_same_window_max_gap_ms=base_gap,
                   delta_window_max_gap_ms=gap-base_gap if gap is not None and base_gap is not None else None,
                   migration_duration_ms=migration['duration_ms'] if migration else None,
                   replay_active_batch_ms=audit['replay_active_batch_ms'],
                   transfer_bytes=sum(migration[k] for k in ('token_payload_bytes','kv_payload_bytes','control_bytes')) if migration else 0,
                   token_bytes=migration['token_payload_bytes'] if migration else 0,
                   kv_bytes=migration['kv_payload_bytes'] if migration else 0,
                   control_bytes=migration['control_bytes'] if migration else 0)
        paired = []
        for rid, request in summary['requests'].items():
            baseline = base['requests'][rid]
            delta = request['e2e_ms']-baseline['e2e_ms'] if request['completed'] and baseline['completed'] else None
            paired.append(dict(request_id=rid, group=groups[rid], completed=request['completed'],
                               e2e_ms=request['e2e_ms'], baseline_e2e_ms=baseline['e2e_ms'], delta_e2e_ms=delta))
        row['requests'] = paired
        for group in ('candidate','source_other','target_existing','source_new'):
            subset = [p for p in paired if p['group']==group]
            row[group+'_delta_mean_ms'] = statistics.mean(p['delta_e2e_ms'] for p in subset) if subset and all(
                p['delta_e2e_ms'] is not None for p in subset) else None
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=ROOT/'configs/simulator/validation_matrix.json')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.config = args.config.resolve()
    args.output.mkdir(parents=True, exist_ok=False)  # Preserve all previous runs.
    cfg = json.loads(args.config.read_text())
    profile_path = ROOT/cfg['tpot_profile']
    profile = json.loads(profile_path.read_text())
    save(args.output/'experiment.json', cfg)
    paths = list((ROOT/'llumnix_sim').glob('*.py'))+[Path(__file__), args.config, profile_path]
    save(args.output/'manifest.json', dict(command=sys.argv,
         files={str(p.relative_to(ROOT)):digest(p) for p in paths},
         decisions=source_decisions()[4], profile_metadata=profile['metadata']))
    all_rows = []
    def case(phase, load, bw, seed):
        trace = workload(cfg, load, bw, seed)
        folder = args.output/phase/f'load{load}_bw{bw}_seed{seed}'
        results = {mode:run_one(cfg, copy.deepcopy(trace), profile, seed, mode, folder/mode) for mode in MODES}
        rows = compare(cfg, trace, results, load, bw, seed)
        save(folder/'comparison.json', rows)
        checks = {mode: audit['completed'] and audit['exact_outputs'] and audit['kv_freed']
                  and audit['error'] is None and audit['selected']==['candidate']
                  for mode, (_, audit) in results.items()}
        # Real modeled work must appear; don't gate on positive migration benefit.
        checks['kv_bytes_charged'] = rows[1]['kv_bytes'] > 0 and rows[1]['migration_duration_ms'] is not None and rows[1]['migration_duration_ms'] > 0
        checks['replay_charged'] = all(rows[i]['replay_active_batch_ms'] > 0 for i in (2,3))
        save(folder/'checks.json', checks)
        for row in rows:
            row['phase'] = phase
        all_rows.extend(rows)
        save(args.output/'comparisons.json', all_rows)
        print(phase, load, bw, seed, checks, flush=True)
        return all(checks.values())
    # Validate central condition first; then all nine cells with the first seed.
    valid = case('smoke', cfg['target_loads'][1], cfg['bandwidth_bytes_per_ms'][1], cfg['seeds'][0])
    if not valid:
        raise RuntimeError('Smoke validation failed; raw evidence preserved')
    checks = [case('screen', load, bw, cfg['seeds'][0])
              for load in cfg['target_loads'] for bw in cfg['bandwidth_bytes_per_ms']]
    if not all(checks):
        raise RuntimeError('Screen validation failed; all first-seed cells preserved')
    for seed in cfg['seeds'][1:]:
        for load in cfg['target_loads']:
            for bw in cfg['bandwidth_bytes_per_ms']:
                case('repeat', load, bw, seed)
    flat = [{k:v for k,v in row.items() if k!='requests'} for row in all_rows]
    with (args.output/'comparisons.csv').open('w',newline='',encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=list(flat[0]))
        writer.writeheader()
        writer.writerows(flat)
    failures = [r for r in all_rows if r['error'] or r['unfinished'] or r['timeout_count'] or
                r['migration_state'] not in ('completed','not_requested')]
    save(args.output/'failures.json', failures)


if __name__ == '__main__':
    main()
