"""Run CPU-only KV/token migration experiments in virtual time.

Example: python tools/run_migration_simulation.py --config configs/simulator/cross_domain.json
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from llumnix_sim import Simulation
from llumnix_sim.protocol import MODES
from llumnix_sim.scheduling import source_decisions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--mode', choices=('none',) + MODES, help='Override migration modes; none disables migration')
    parser.add_argument('--output', type=Path, default=Path('results/token_simulation'))
    parser.add_argument('--until-ms', type=float, help='Optional finite observation horizon')
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding='utf-8'))
    sim = Simulation(config)
    for row in config.get('requests', []):
        sim.add_request(**row)
    effective_migrations = []
    if args.mode != 'none':
        for row in config.get('migrations', []):
            row = dict(row)
            if args.mode:
                row['mode'] = args.mode
            effective_migrations.append(row)
            sim.schedule_migration(**row)
    config['migrations'] = effective_migrations
    effective_selections = []
    if args.mode != 'none':
        for row in config.get('selections', []):
            row = dict(row)
            if args.mode:
                row['mode'] = args.mode
            sim.schedule_selection(**row)
            effective_selections.append(row)
    config['selections'] = effective_selections
    summary = sim.run(until_ms=args.until_ms)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    with (args.output / 'events.jsonl').open('w', encoding='utf-8') as output:
        for event in sim.events:
            output.write(json.dumps(event) + '\n')
    (args.output / 'config.json').write_text(json.dumps(config, indent=2), encoding='utf-8')
    manifest = {'mode_override': args.mode, 'until_ms': args.until_ms,
                'decision_source_sha256': source_decisions()[4],
                'scheduling_scope': 'snapshot replay gate and running-request selection; external pair intents',
                'command': sys.argv, 'compute_model': summary['compute_model'],
                'source_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in sorted((Path(__file__).resolve().parents[1] / 'llumnix_sim').glob('*.py'))},
                'note': 'Synthetic compute/link parameters: functional simulation, not measured GPU results.'}
    (args.output / 'run_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps({'output': str(args.output.resolve()), 'requests': len(summary['requests']),
                      'completed': sum(row['completed'] for row in summary['requests'].values()),
                      'migrations': summary['migrations']}, indent=2))


if __name__ == '__main__':
    main()
