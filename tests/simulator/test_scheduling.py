"""Independent small cases for snapshot decision adapters (no GPU required)."""
import unittest
from types import SimpleNamespace

from llumnix_sim import Simulation
from llumnix_sim.scheduling import ReplayGate, select_running
from test_migration import scenario


class SchedulingTests(unittest.TestCase):
    def test_replay_gate_alternates_and_finishes(self):
        gate = ReplayGate()
        normal = SimpleNamespace(request_id='normal', replay=False, paused=False, computed=10, length=11)
        replay = SimpleNamespace(request_id='replay', replay=True, paused=False, computed=0, length=11)
        self.assertEqual(gate.choose([normal, replay], 0), {'replay'})
        self.assertEqual(gate.choose([normal, replay], 1), {'normal'})
        normal.paused = True
        self.assertEqual(gate.choose([normal, replay], 2), {'replay'})
        self.assertEqual(gate.choose([normal, replay], 3), {'replay'})
        replay.computed = 10
        self.assertEqual(gate.choose([normal, replay], 4), set())

    def prepared(self):
        sim = Simulation(scenario())
        sim.add_request('short', 's', prompt_length=5, max_tokens=30)
        sim.add_request('long', 's', prompt_length=9, max_tokens=100)
        sim.run(until_ms=5)
        # Both have produced exactly one token: contexts 6, 10;
        # remaining budgets 29, 99; composite scores 29/6 < 99/10.
        self.assertEqual([len(r.output_ids) for r in sim.requests.values()], [1, 1])
        return sim

    def select(self, sim, policy, budget_type='NUM_REQ', budget=1):
        return select_running(sim, 's', policy=policy, budget_type=budget_type,
                              budget=budget, seed=7, composite_min_remaining=8,
                              composite_max_context=4096)

    def test_real_orderings_with_hand_calculated_features(self):
        sim = self.prepared()
        for policy, expected in [('SR', 'short'), ('LR', 'long'), ('FCR', 'short'),
                                 ('LCR', 'long'), ('REMAINING_LONGEST', 'long'),
                                 ('COMPOSITE', 'long')]:
            with self.subTest(policy=policy):
                self.assertEqual(self.select(sim, policy)[0], [expected])

    def test_real_token_budget_overshoot_is_preserved(self):
        ids, budget, left = self.select(self.prepared(), 'SR', 'TOKEN', 12)
        # 6 + 10 exceeds 12, but both individual costs <= 12: real behavior.
        self.assertEqual((ids, budget, left), (['short', 'long'], 12, 0))

    def test_waiting_policy_and_missing_budget_are_not_silently_accepted(self):
        sim = self.prepared()
        with self.assertRaisesRegex(ValueError, 'waiting'):
            self.select(sim, 'FCW')
        with self.assertRaises(ValueError):
            self.select(sim, 'SR', budget=None)

    def test_selection_reads_execution_time_state_and_keeps_rejections(self):
        sim = Simulation(scenario())
        sim.add_request('short', 's', prompt_length=5, max_tokens=30)
        sim.add_request('long', 's', prompt_length=9, max_tokens=100)
        sim.schedule_selection('s', 't', 5, policy='SR', budget_type='NUM_REQ',
                               budget=2, seed=7, composite_min_remaining=8,
                               composite_max_context=4096, mode='token_stop')
        result = sim.run()
        decision = next(e for e in sim.events if e['event'] == 'request_selection')
        self.assertEqual(decision['selected_request_ids'], ['short', 'long'])
        self.assertEqual([a['state'] for a in result['migrations']], ['completed', 'rejected'])
        self.assertTrue(all(r['completed'] for r in result['requests'].values()))

    def test_replay_batches_never_mix_with_normal_work(self):
        sim = Simulation(scenario())
        sim.add_request('r', 's', prompt_length=256, max_tokens=100)
        sim.add_request('background', 't', prompt_length=256, max_tokens=100)
        sim.schedule_migration('r', 't', 30, 'token_live')
        sim.run()
        replay_batches = 0
        for event in sim.events:
            if event['event'] == 'batch_started':
                kinds = {t['kind'] for t in event['tasks']}
                if 'replay' in kinds:
                    replay_batches += 1
                    self.assertEqual(kinds, {'replay'})
        self.assertGreater(replay_batches, 0)

    def test_seeded_order_is_repeatable(self):
        sim = self.prepared()
        self.assertEqual(self.select(sim, 'RANDOM', budget=2),
                         self.select(sim, 'RANDOM', budget=2))

    def test_remaining_policy_does_not_see_future_output_length(self):
        decisions = []
        for lengths in [(2, 20), (20, 2)]:
            sim = Simulation(scenario())
            sim.add_request('a', 's', prompt_length=5, max_tokens=100, output_length=lengths[0])
            sim.add_request('b', 's', prompt_length=5, max_tokens=50, output_length=lengths[1])
            sim.run(until_ms=5)
            decisions.append(self.select(sim, 'REMAINING_LONGEST')[0])
            result = sim.run()
            self.assertEqual([r['output_tokens'] for r in result['requests'].values()], list(lengths))
            self.assertTrue(all(r['finish_reason'] == 'stop' for r in result['requests'].values()))
            self.assertTrue(all(n['remaining_kv_blocks'] == 0 for n in result['instances'].values()))
        self.assertEqual(decisions, [['a'], ['a']])

    def test_early_completion_during_live_migration_is_not_success(self):
        sim = Simulation(scenario(latency=10))
        sim.add_request('r', 's', prompt_length=32, max_tokens=100, output_length=3)
        sim.schedule_migration('r', 't', 5, 'token_live')
        result = sim.run()
        self.assertEqual(result['requests']['r']['output_tokens'], 3)
        self.assertEqual(result['requests']['r']['finish_reason'], 'stop')
        self.assertEqual(result['migrations'][0]['state'], 'cancelled')
        self.assertEqual(result['migrations'][0]['reason'], 'source_completed')
        self.assertTrue(all(n['remaining_kv_blocks'] == 0 for n in result['instances'].values()))

    def test_explicit_length_survives_migration_in_all_modes(self):
        for mode in ('kv', 'token_stop', 'token_live'):
            with self.subTest(mode=mode):
                sim = Simulation(scenario())
                sim.add_request('r', 's', prompt_length=32, max_tokens=100, output_length=30)
                sim.schedule_migration('r', 't', 10, mode)
                result = sim.run()
                self.assertEqual(result['requests']['r']['output_tokens'], 30)
                self.assertEqual(result['requests']['r']['finish_reason'], 'stop')
                self.assertEqual(result['migrations'][0]['state'], 'completed')
                self.assertTrue(all(n['remaining_kv_blocks'] == 0 for n in result['instances'].values()))

    def test_workload_length_validation_and_default_provenance(self):
        for value in (0, -1, 101, True, 1.5):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'Request r'):
                Simulation(scenario()).add_request('r', 's', max_tokens=100, output_length=value)
        sim = Simulation(scenario())
        sim.add_request('r', 's', max_tokens=2)
        row = sim.run()['requests']['r']
        self.assertEqual(row['finish_reason'], 'length')
        self.assertEqual(row['workload_length_origin'], 'budget_limit_assumption')


if __name__ == '__main__':
    unittest.main()
