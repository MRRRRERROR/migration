import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from two_gpu_power_metrics import power_metrics,poisson_trace

class PowerTests(unittest.TestCase):
    def data(self):
        return [{'role':role,'monotonic_s':i/5,'power_w':(200 if i/5<60 else 80) if role=='S' else (50 if i/5<60 else 90)}
                for i in range(0,3301) for role in ('S','D')]
    def test_windows_threshold_energy_and_total(self):
        metrics=power_metrics(self.data(),60)
        self.assertEqual(metrics['source_p0_w'],200)
        self.assertEqual(metrics['points']['1']['source_drop_w'],120)
        self.assertEqual(metrics['points']['1']['destination_increase_w'],40)
        self.assertEqual(metrics['points']['1']['total_power_change_w'],-80)
        self.assertEqual(metrics['thresholds']['0.5'],{'start_s':0,'confirmed_s':5})
        self.assertAlmostEqual(metrics['S_energy_60']['joules'],4800)
    def test_negative_change_and_missing_measurement(self):
        data=self.data()
        for row in data:
            if row['role']=='S' and row['monotonic_s']>=60:row['power_w']=220
        metrics=power_metrics(data,60)
        self.assertEqual(metrics['points']['1']['source_drop_w'],-20)
        self.assertIsNone(metrics['thresholds']['0.1'])
        self.assertIsNone(power_metrics([],60)['points']['1']['source_drop_w'])
    def test_seeded_poisson_pairing_and_independent_assignment(self):
        first=poisson_trace([0]*100,101,900,1,.3)
        self.assertEqual(first,poisson_trace([0]*100,101,900,1,.3))
        self.assertNotEqual(first,poisson_trace([0]*100,102,900,1,.3))
        self.assertTrue(all(0<r['planned_offset_s']<900 for r in first))
        self.assertEqual({r['initial_role'] for r in first},{'S','D'})
        self.assertEqual(len({r['request_id'] for r in first}),len(first))

if __name__=='__main__':unittest.main()
