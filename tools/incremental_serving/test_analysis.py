"""Whole-request quantiles must not reward dropped/unfinished requests."""
import unittest
from runner import summary

class AnalysisTests(unittest.TestCase):
    def test_incomplete_denominator(self):
        rows=[dict(completed=True,e2e_s=i,ttft_s=.1,token_ids=[1,2],finish_reason='stop') for i in range(1,10)]
        rows.append(dict(completed=False,e2e_s=.001,error='fast failure',token_ids=[]))
        result=summary(rows)
        self.assertEqual(result['completion_rate'],.9)
        self.assertIsNone(result['e2e_p95_s'])
        self.assertEqual(result['e2e_p50_s'],5)
        rows[-1]=dict(completed=True,e2e_s=10,ttft_s=.1,token_ids=[1,2],finish_reason='stop')
        self.assertEqual(summary(rows)['e2e_p95_s'],10)

if __name__=='__main__':unittest.main(verbosity=2)
