"""Standalone tests: python -m unittest discover -s tests/simulator -v."""
import unittest
import random
import runpy
from pathlib import Path
from types import SimpleNamespace

from llumnix_sim import Simulation
from llumnix_sim.clock import Clock
from llumnix_sim.network import Link
from llumnix_sim.latency import LatencyModel
from llumnix_sim.protocol import MigrationConfig, ready


def scenario(latency=2.0, bandwidth=100000.0):
    return {'instances': [{'id': 's', 'domain': 'a'}, {'id': 't', 'domain': 'b'}],
            'links': [{'source': 'a', 'target': 'b', 'bandwidth_bytes_per_ms': bandwidth, 'latency_ms': latency},
                      {'source': 'b', 'target': 'a', 'bandwidth_bytes_per_ms': bandwidth, 'latency_ms': latency}]}


def execute(mode, config=None, **options):
    sim = Simulation(config or scenario())
    sim.add_request('r', 's', prompt_ids=[17, 28, 39] * 32, max_tokens=128)
    sim.schedule_migration('r', 't', 40, mode, **options)
    result = sim.run()
    return sim, result


class ClockNetworkTests(unittest.TestCase):
    def test_same_time_fifo_and_cancel(self):
        clock = Clock()
        observed = []
        clock.at(1, lambda: observed.append(1))
        clock.at(1, lambda: observed.append(2)).cancel()
        clock.at(1, lambda: observed.append(3))
        clock.run()
        self.assertEqual(observed, [1, 3])

    def test_shared_bandwidth_conservation(self):
        clock = Clock()
        link = Link(clock, 100, 5)
        finished = []
        link.send(1000, lambda: finished.append(clock.now))
        link.send(1000, lambda: finished.append(clock.now))
        clock.run()
        self.assertEqual(finished, [25.0, 25.0])
        self.assertEqual(link.bytes_sent, 2000)

    def test_flow_join_reschedules_existing_transfer(self):
        clock = Clock()
        link = Link(clock, 100, 0)
        finished = []
        link.send(1000, lambda: finished.append(('a', clock.now)))
        clock.at(5, lambda: link.send(1000, lambda: finished.append(('b', clock.now))))
        clock.run()
        self.assertEqual(finished, [('a', 15.0), ('b', 20.0)])


class MigrationTests(unittest.TestCase):
    def test_modes_preserve_budget_history_and_release_order(self):
        for mode in ('kv', 'token_stop', 'token_live'):
            with self.subTest(mode=mode):
                sim, result = execute(mode, final_gap_tokens=0)
                migration = result['migrations'][0]
                self.assertEqual(migration['state'], 'completed')
                self.assertEqual(result['requests']['r']['token_ids'], list(range(1000, 1128)))
                self.assertEqual(len(sim.requests['r'].prompt_ids), 96)
                self.assertEqual(result['requests']['r']['owner'], 't')
                self.assertTrue(all(node['remaining_kv_blocks'] == 0 for node in result['instances'].values()))
                names = [e['event'] for e in sim.events if e.get('attempt_id') == migration['attempt_id']]
                self.assertLess(names.index('source_output_flush_confirmed'), names.index('taken_over'))
                self.assertLess(names.index('taken_over'), names.index('gateway_route_confirmed'))
                self.assertLess(names.index('gateway_route_confirmed'), names.index('source_released'))
                if mode == 'kv':
                    self.assertGreater(migration['kv_payload_bytes'], 0)
                    self.assertEqual(migration['replay_tokens'], 0)
                else:
                    self.assertEqual(migration['kv_payload_bytes'], 0)
                    self.assertGreater(migration['replay_tokens'], 0)
                    self.assertGreater(migration['token_payload_bytes'], 0)

    def test_replay_suppressed_and_last_token_boundary(self):
        sim, _ = execute('token_stop')
        for row in sim.events:
            if row['event'] in ('target_ready', 'taken_over'):
                self.assertTrue(ready(row['computed'], 96, row['output_length']))
        generated = [e for e in sim.events if e['event'] == 'token_generated']
        self.assertEqual(len(generated), 128)

    def test_live_round_limit_and_no_empty_background_round(self):
        sim, result = execute('token_live', scenario(latency=15), max_rounds=2, final_gap_tokens=0)
        self.assertEqual(result['migrations'][0]['rounds'], 2)
        deltas = [e for e in sim.events if e.get('kind') == 'background_delta']
        self.assertEqual(len(deltas), 2)
        self.assertTrue(all(e['token_count'] > 0 for e in deltas))

    def test_timeout_cancels_target_before_source_resumes(self):
        sim, result = execute('token_stop', scenario(latency=20), timeout_ms=55)
        migration = result['migrations'][0]
        self.assertEqual(migration['state'], 'cancelled')
        self.assertEqual(migration['reason'], 'deadline')
        events = [e for e in sim.events if e.get('attempt_id') == migration['attempt_id']]
        cancelled = next(e for e in events if e['event'] == 'target_cancelled')
        resumed = next(e for e in events if e['event'] == 'migration_cancelled')
        self.assertGreaterEqual(resumed['time_ms'] - cancelled['time_ms'], 20)
        self.assertEqual(result['requests']['r']['owner'], 's')
        self.assertEqual(result['requests']['r']['output_tokens'], 128)

    def test_target_capacity_failure_keeps_source(self):
        config = scenario()
        config['instances'][1]['kv_blocks'] = 1
        sim, result = execute('token_stop', config)
        self.assertEqual(result['migrations'][0]['reason'], 'target_kv_capacity')
        self.assertEqual(result['requests']['r']['owner'], 's')
        self.assertEqual(sim.instances['t'].used, 0)

    def test_target_reconstruction_delays_existing_work(self):
        def run(migrate):
            config = scenario()
            config['instances'][1]['max_batch_tokens'] = 1
            sim = Simulation(config)
            sim.add_request('background', 't', prompt_length=32, max_tokens=100)
            sim.add_request('r', 's', prompt_length=96, max_tokens=128)
            if migrate:
                sim.schedule_migration('r', 't', 40, 'token_stop')
            return sim.run()
        baseline, migrated = run(False), run(True)
        self.assertGreater(migrated['instances']['t']['compute_ms'], baseline['instances']['t']['compute_ms'])
        self.assertGreater(migrated['requests']['background']['e2e_ms'], baseline['requests']['background']['e2e_ms'])

    def test_increased_rtt_increases_stop_pause(self):
        _, fast = execute('token_stop', scenario(latency=1))
        _, slow = execute('token_stop', scenario(latency=30))
        self.assertGreater(slow['migrations'][0]['source_pause_ms'], fast['migrations'][0]['source_pause_ms'])

    def test_restored_request_cannot_migrate_again(self):
        sim = Simulation(scenario())
        sim.add_request('r', 's', max_tokens=128)
        sim.schedule_migration('r', 't', 40, 'token_stop')
        sim.schedule_migration('r', 's', 200, 'token_stop')
        result = sim.run()
        self.assertEqual(result['migrations'][1]['reason'], 're_migration_of_restored_request_unsupported')

    def test_model_mismatch_rejected(self):
        config = scenario()
        config['instances'][1]['model_identity'] = 'other-tokenizer'
        _, result = execute('token_stop', config)
        self.assertEqual(result['migrations'][0]['reason'], 'model_tokenizer_identity_mismatch')

    def test_kv_layout_mismatch_rejects_kv_but_allows_token(self):
        config = scenario()
        config['instances'][1]['kv_layout'] = 'other-layout'
        _, result = execute('kv', config)
        self.assertEqual(result['migrations'][0]['reason'], 'kv_layout_mismatch')
        _, result = execute('token_stop', config)
        self.assertEqual(result['migrations'][0]['state'], 'completed')

    def test_missing_cross_domain_link_is_not_silently_local(self):
        config = scenario()
        config['links'] = []
        with self.assertRaisesRegex(ValueError, 'Missing explicit'):
            execute('token_stop', config)

    def test_deterministic_run(self):
        a, result_a = execute('token_live', max_rounds=2, final_gap_tokens=0)
        b, result_b = execute('token_live', max_rounds=2, final_gap_tokens=0)
        self.assertEqual(result_a, result_b)
        self.assertEqual(a.events, b.events)

    def test_invalid_config(self):
        for kwargs in ({'mode': 'bad'}, {'timeout_ms': float('nan')}, {'max_rounds': -1}):
            with self.assertRaises(ValueError):
                MigrationConfig(**kwargs)

    def test_source_completes_during_live_rebuild(self):
        sim = Simulation(scenario(latency=10))
        sim.add_request('r', 's', prompt_length=96, max_tokens=8)
        sim.schedule_migration('r', 't', 40, 'token_live', final_gap_tokens=0)
        result = sim.run()
        self.assertEqual(result['migrations'][0]['state'], 'cancelled')
        self.assertEqual(result['migrations'][0]['reason'], 'source_completed')
        self.assertEqual(result['requests']['r']['output_tokens'], 8)
        self.assertTrue(all(node.used == 0 for node in sim.instances.values()))

    def test_timeout_after_commit_never_resumes_source(self):
        reference, _ = execute('token_stop', scenario(latency=20))
        commit_time = next(e['time_ms'] for e in reference.events if e['event'] == 'taken_over')
        sim, result = execute('token_stop', scenario(latency=20), timeout_ms=commit_time - 40 + 0.1)
        self.assertEqual(result['migrations'][0]['state'], 'completed')
        self.assertTrue(any(e['event'] == 'deadline_after_commit_source_fenced' for e in sim.events))
        self.assertFalse(any(e['event'] == 'migration_cancelled' for e in sim.events))
        self.assertEqual(result['requests']['r']['output_tokens'], 128)

    def test_no_background_rounds_when_max_rounds_zero(self):
        _, result = execute('token_live', max_rounds=0, final_gap_tokens=0)
        self.assertEqual(result['migrations'][0]['rounds'], 0)

    def test_simultaneous_migrations_share_domain_link(self):
        config = scenario(latency=10)
        config['instances'].extend([{'id': 's2', 'domain': 'a'}, {'id': 't2', 'domain': 'b'}])
        sim = Simulation(config)
        for rid, source, target in [('r', 's', 't'), ('r2', 's2', 't2')]:
            sim.add_request(rid, source, prompt_length=96, max_tokens=128)
            sim.schedule_migration(rid, target, 40, 'token_stop')
        result = sim.run()
        self.assertTrue(all(a['state'] == 'completed' for a in result['migrations']))
        self.assertGreaterEqual(result['network']['a->b']['peak_flows'], 2)

    def test_legacy_decode_adapter_uses_total_context(self):
        legacy = SimpleNamespace(decode_latency={(8, 2048): [19]}, decode_model_params=(1, 0.01, 1),
                                 prefill_latency={128: [4]}, prefill_model_params=(0.02, 1))
        model = LatencyModel(legacy=legacy)
        self.assertEqual(model.decode(2, 2048), 19)
        self.assertEqual(model.prefill(128, 0), 4)
        self.assertGreater(model.prefill(128, 1024), model.prefill(128, 0))

    def test_gateway_domain_can_be_separate_from_compute_domains(self):
        config = scenario()
        config['domains'] = ['gateway']
        for domain in ('a', 'b'):
            config['links'].append({'source': domain, 'target': 'gateway',
                                    'bandwidth_bytes_per_ms': 10000, 'latency_ms': 5})
        config['links'].append({'source': 'gateway', 'target': 'a',
                                'bandwidth_bytes_per_ms': 10000, 'latency_ms': 5})
        sim = Simulation(config)
        sim.add_request('r', 's', gateway_domain='gateway')
        sim.schedule_migration('r', 't', 40, 'token_stop')
        result = sim.run()
        self.assertEqual(result['migrations'][0]['state'], 'completed')
        self.assertIsNone(sim.requests['r'].gateway_owner)

    def test_migration_concurrency_limit(self):
        sim = Simulation(scenario(latency=20))
        for rid in ('r', 'r2'):
            sim.add_request(rid, 's', max_tokens=128)
            sim.schedule_migration(rid, 't', 40, 'token_stop')
        result = sim.run()
        self.assertEqual(result['migrations'][1]['reason'], 'migration_concurrency_limit')

    def test_defaults_match_synced_v1_contract(self):
        root = Path(__file__).resolve().parents[2]
        path = root / ('server_snapshots/token_migration_20261011_01/project/python/llumnix/'
                       'llumnix/token_migration/config.py')
        reference = runpy.run_path(str(path))['MigrationConfig']()
        simulation = MigrationConfig()
        self.assertEqual(simulation.mode, reference.migration_mode)
        self.assertEqual(simulation.max_rounds, reference.token_migration_max_rounds)
        self.assertEqual(simulation.final_gap_tokens, reference.token_migration_final_gap_tokens)
        self.assertEqual(simulation.timeout_ms, reference.token_migration_timeout * 1000)

    def test_varied_workloads_preserve_outputs_and_release_all_memory(self):
        randomizer = random.Random(20261020)
        for case in range(60):
            config = scenario(latency=randomizer.choice([0, 2, 20, 100]),
                              bandwidth=randomizer.choice([1000, 100000, 1000000]))
            config['instances'][1]['max_batch_tokens'] = randomizer.choice([1, 16, 128])
            sim = Simulation(config)
            budget = randomizer.choice([8, 32, 128])
            sim.add_request('r', 's', prompt_length=randomizer.choice([1, 32, 256]), max_tokens=budget)
            sim.add_request('background', 't', prompt_length=32, max_tokens=32)
            sim.schedule_migration('r', 't', randomizer.choice([20, 40, 100]),
                                   randomizer.choice(['kv', 'token_stop', 'token_live']),
                                   max_rounds=randomizer.choice([0, 1, 3]),
                                   timeout_ms=randomizer.choice([10, 100, 30000]))
            result = sim.run()
            with self.subTest(case=case):
                self.assertEqual(result['requests']['r']['token_ids'], list(range(1000, 1000 + budget)))
                self.assertTrue(all(node.used == 0 for node in sim.instances.values()))


if __name__ == '__main__':
    unittest.main()
