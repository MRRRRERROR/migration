"""CPU-only transport failure: preserve source and log ambiguity, never retry."""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from harness import Harness

class Channel:
    closed=False
    async def close(self):self.closed=True
class Stub:
    async def Migrate(self,request,timeout):raise TimeoutError('injected CPU-only RPC deadline')

class FailureTest(unittest.IsolatedAsyncioTestCase):
    async def test_transport_error_is_not_success_or_definite_rejection(self):
        with tempfile.TemporaryDirectory() as tmp:
            h=Harness.__new__(Harness);h.service=Path(tmp)
            (h.service/'state_S').mkdir();(h.service/'state_D').mkdir()
            states=[dict(instance_id='cpu_source',request_id='same'),dict(instance_id='cpu_destination')]
            h.state=lambda i:states[i];before=json.dumps(states);channel=Channel()
            with patch('harness.grpc.aio.insecure_channel',return_value=channel),patch('harness.base.rpc.LlumletStub',return_value=Stub()):
                result=await h.trigger_kv(0,1,dict(request_id='cpu_only_request'),Path(tmp))
            self.assertIn('error',result);self.assertNotIn('rpc_success',result)
            self.assertEqual(json.dumps(states),before);self.assertTrue(channel.closed)
            saved=json.loads((Path(tmp)/'running_attempts.jsonl').read_text())
            self.assertEqual(saved['error'],result['error'])
            self.assertFalse(any(Path(tmp).rglob('commands/*.json')))

if __name__=='__main__':unittest.main(verbosity=2)
