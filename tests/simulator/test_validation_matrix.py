import json
import unittest
from tools.run_validation_matrix import ClientTPOTProxy, ROOT, interval_gap, workload
from tools.analyze_validation_matrix import summarize


class ExperimentTests(unittest.TestCase):
    def test_aggregate_does_not_drop_missing_or_unplanned_seeds(self):
        self.assertIsNone(summarize([{'seed': 1, 'x': -3}, {'seed': 2, 'x': None}],
                                   [1, 2], 'x')['mean'])
        self.assertIsNone(summarize([{'seed': 1, 'x': -3}], [1, 2], 'x')['mean'])
        self.assertEqual(summarize([{'seed': 1, 'x': -3}, {'seed': 2, 'x': 5}],
                                  [1, 2], 'x')['mean'], 1)

    def test_proxy_uses_per_request_context_not_total(self):
        profile = {'results': [
            {'batch_size': 4, 'tokens_per_request': 100, 'p50': 7},
            {'batch_size': 4, 'tokens_per_request': 400, 'p50': 19}]}
        model = ClientTPOTProxy({}, profile)
        self.assertEqual(model.decode(4, 400), 7)
        self.assertEqual(model.decode(4, 1600), 19)
        self.assertEqual(model.outside, 0)

    def test_window_includes_gaps_crossing_boundaries(self):
        self.assertEqual(interval_gap([0, 10, 80, 90], 20, 40), 70)
        self.assertEqual(interval_gap([0, 10, 80, 90], 81, 85), 10)
        self.assertIsNone(interval_gap([0], 1, 5))

    def test_seed_changes_workload_without_changing_routes(self):
        cfg = json.loads((ROOT/'configs/simulator/validation_matrix.json').read_text())
        first = workload(cfg, 8, 125000, 101)
        self.assertEqual(first, workload(cfg, 8, 125000, 101))
        second = workload(cfg, 8, 125000, 102)
        self.assertNotEqual([r['output_length'] for r in first['requests']],
                            [r['output_length'] for r in second['requests']])
        for a,b in zip(first['requests'], second['requests']):
            self.assertEqual((a['request_id'],a['instance'],a['arrival_ms']),
                             (b['request_id'],b['instance'],b['arrival_ms']))

    def test_bandwidth_factor_cannot_change_request_trace(self):
        cfg = json.loads((ROOT/'configs/simulator/validation_matrix.json').read_text())
        self.assertEqual(workload(cfg, 8, 12500, 101)['requests'],
                         workload(cfg, 8, 1250000, 101)['requests'])
