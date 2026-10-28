"""CPU invariants; these do not qualify GPU handoff correctness."""
import unittest
from policy import TransferLedger,never_executed,choose_route,choose_waiting,choose_running,PolicyConfig

class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.req=dict(state='waiting',status='WAITING',computed_tokens=0,generated_tokens=0,
            ever_scheduled=False,preemptions=0,kv_blocks=0,request_id='r',logical_id='r',
            input_tokens=200,arrival_time=10)
    def test_preempted_reset_is_not_stateless(self):
        for change in (dict(ever_scheduled=True),dict(preemptions=1),dict(status='PREEMPTED'),dict(kv_blocks=1),dict(transfer_active=True)):
            self.assertFalse(never_executed({**self.req,**change}))
        self.assertTrue(never_executed(self.req))
    def test_retry_and_uncertain_ownership(self):
        l=TransferLedger();l.reserve('tx','r');l.reserve('tx','r');l.commit('tx')
        self.assertEqual(l.commit('tx')['state'],'committed')
        with self.assertRaises(ValueError):l.abort('tx')
        with self.assertRaises(ValueError):l.reserve('tx','other')
        l.reserve('aborted','q');l.abort('aborted')
        with self.assertRaises(ValueError):l.commit('aborted')
    def test_no_future_length_oracle(self):
        s=dict(running=5,waiting=1,reserved=0,kv_usage=.2,free_kv_blocks=1000,block_size=16,requests=[self.req])
        d={**s,'running':1,'waiting':0,'requests':[]}
        self.assertEqual(choose_route('x',[s,d],[0,0]),1)
        self.assertEqual(choose_waiting([s,d],20,PolicyConfig(),{})[0]['request']['request_id'],'r')
        run={**self.req,'state':'running','computed_tokens':500,'generated_tokens':300}
        other={**run,'request_id':'short','computed_tokens':300,'actual_future_length':100000}
        s['requests']=[run,other]
        self.assertEqual(choose_running([s,d],20,PolicyConfig(),{})[0]['request']['request_id'],'short')
        d['free_kv_blocks']=1
        self.assertEqual(choose_running([s,d],20,PolicyConfig(),{})[1],'destination_capacity_insufficient')

if __name__=='__main__':unittest.main(verbosity=2)
