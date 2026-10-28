"""CPU replay of retained real GPU evidence; never counted as a GPU run."""
import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from runner import Incremental
from policy import PolicyConfig

class NativeEvidenceTest(unittest.IsolatedAsyncioTestCase):
    async def test_actual_fields_partial_writes_and_lease_release(self):
        roots=[Path(__file__).resolve().parents[1],Path(__file__).resolve().parents[2]]
        root=next((p for p in roots if (p/'correctness_01/running_related_events.json').exists()),None)
        if root is None:self.skipTest('Retained real GPU fixture not present in this checkout')
        events=json.loads((root/'correctness_01/running_related_events.json').read_text())
        gate=json.loads((root/'correctness_01/result.json').read_text())['gates'][-1]
        leases=gate['leases'];acquire=next(e for e in leases if e['event']=='acquire');release=next(e for e in leases if e['event']=='release')
        request_id=acquire['request_id'];events=[e for e in events if e.get('event_name') in ['source_release','dst_resume','migration_done']]
        with tempfile.TemporaryDirectory() as temp:
            service=Path(temp);kv=service/'kv_events.jsonl';log=service/'vllm_S.log'
            kv.write_bytes(b'old unrelated evidence\n'*10000+b'partial older record');log.write_bytes(b'old unrelated log\n'*10000)
            h=Incremental.__new__(Incremental);h.service=service;h.cfg=PolicyConfig();h.runtime_check=lambda:None;h.critical=None
            cursors={name:dict(offset=f.stat().st_size,inode=f.stat().st_ino,starts_at_line_boundary=name!='events') for name,f in [('events',kv),('lease',log)]}
            value=dict(request_id=request_id,source=0,started=time.monotonic(),started_wall_ns=min(e['timestamp_ns'] for e in events+leases)-1,
                evidence_cursors=cursors,rpc_success=True)
            released_at=None
            async def producer():
                nonlocal released_at
                a=b'INFO KVT_BLOCK_LEASE '+json.dumps(acquire).encode()+b'\n'
                z=b'INFO KVT_BLOCK_LEASE '+json.dumps(release).encode()+b'\n'
                with log.open('ab') as f:f.write(a[:len(a)//2])
                await asyncio.sleep(.05)
                with log.open('ab') as f:f.write(a[len(a)//2:]+z[:len(z)//2])
                with kv.open('ab') as f:
                    f.write(b' older record suffix\n')
                    for e in events:f.write(json.dumps(e).encode()+b'\n')
                await asyncio.sleep(.3)
                with log.open('ab') as f:f.write(z[len(z)//2:])
                released_at=time.monotonic()
            task=asyncio.create_task(producer())
            result=await asyncio.wait_for(h.wait_migration_terminal(value,service),2)
            await task
            self.assertEqual(result['classification'],'completed');self.assertEqual(len(result['leases']),2)
            self.assertGreaterEqual(result['confirmed_monotonic_s'],released_at)
            self.assertLess(result['evidence_log_bytes_read'],kv.stat().st_size+log.stat().st_size-300000)

if __name__=='__main__':unittest.main(verbosity=2)
