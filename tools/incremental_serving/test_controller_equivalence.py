"""C's running-session cooldown must never disable B's waiting policy."""
import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import time
import unittest
from runner import Incremental
from policy import PolicyConfig

class WaitingEquivalenceTest(unittest.IsolatedAsyncioTestCase):
    async def test_waiting_transfer_during_running_cooldown(self):
        with tempfile.TemporaryDirectory() as temp:
            h=Incremental.__new__(Incremental);h.cfg=replace(PolicyConfig(),period_s=.01,persistent_checks=1)
            h.mode='C';h.critical=None;after=False;calls=[];stop=asyncio.Event()
            run=dict(request_id='r',logical_id='r',state='running',status='RUNNING',generated_tokens=10,computed_tokens=200,
                input_tokens=190,arrival_time=1,preemptions=0,kv_blocks=13,ever_scheduled=True,transfer_active=False)
            waiting={**run,'request_id':'w','logical_id':'w','state':'waiting','status':'WAITING',
                'generated_tokens':0,'computed_tokens':0,'preemptions':0,'kv_blocks':0,'ever_scheduled':False}
            def state(i):
                return dict(monotonic_s=time.monotonic(),running=5 if i==0 else 1,waiting=int(i==0 and after),reserved=0,
                    kv_usage=.1,free_kv_blocks=1000,block_size=16,migration_tasks=0,migration_finish=True,holds=[],
                    requests=([waiting if after else run] if i==0 else []))
            h.state=state
            async def trigger(*args):calls.append('running');return dict(rpc_success=True,started=time.monotonic())
            async def terminal(*args):
                nonlocal after
                after=True;return dict(classification='completed',source_lease_balanced=True,confirmed_monotonic_s=time.monotonic())
            async def transfer(*args):calls.append('waiting');stop.set()
            h.trigger_kv=trigger;h.wait_migration_terminal=terminal;h.transfer=transfer
            await asyncio.wait_for(h.control(Path(temp),stop),1)
            self.assertEqual(calls,['running','waiting'])
            self.assertIsNone(h.critical)

if __name__=='__main__':unittest.main(verbosity=2)
