import unittest
from two_gpu_live_report import energy, power

class MetricTests(unittest.TestCase):
    def test_irregular_linear_power_integral(self):
        rows=[dict(role='S',monotonic_s=str(t),power_w=str(100+200*t)) for t in (0,.15,.4,.5)]
        result=energy(rows,0,.5,'S')
        self.assertAlmostEqual(result['joules'],75)
        self.assertAlmostEqual(result['covered_s'],.5)

    def test_missing_interval_keeps_energy_missing(self):
        rows=[dict(role='S',monotonic_s=str(t),power_w='100') for t in (0,.2,1.1,1.3)]
        self.assertIsNone(energy(rows,0,1.3,'S')['joules'])

    def test_five_full_seconds_required_for_threshold(self):
        rows=[]
        for i in range(-300,601):
            t=i/5
            for role in ('S','D'):
                rows.append(dict(role=role,monotonic_s=str(t),power_w=str(100 if t<0 or role=='D' else 70)))
        result=power(rows,0,-60,120)
        self.assertEqual(result['thresholds']['0.25'],dict(first_window_start_s=0,confirmed_s=5))
        self.assertIsNone(result['thresholds']['0.5'])
        self.assertEqual(result['points']['1']['drop_w'],30)

    def test_threshold_can_be_confirmed_after_sending_cutoff(self):
        rows=[dict(role=role,monotonic_s=str(i/5),power_w=str(100 if i/5<8 or role=='D' else 40))
              for i in range(-300,101) for role in ('S','D')]
        result=power(rows,0,-60,10,20)
        self.assertEqual(result['thresholds']['0.5'],dict(first_window_start_s=8,confirmed_s=13))

if __name__=='__main__':unittest.main(verbosity=2)
