"""CPU sockets only: independent POST connections and no ambiguous POST retry."""
import unittest
from aiohttp import web,ClientSession,ClientError
from runner import proxy_connector

class TransportTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.peers=[];self.dropped=0
        async def good(request):
            self.peers.append(request.transport.get_extra_info('peername'))
            await request.read();return web.json_response(dict(ok=True))
        async def drop(request):
            self.dropped+=1;await request.read();request.transport.close();return web.Response()
        app=web.Application();app.router.add_post('/good',good);app.router.add_post('/drop',drop)
        self.server=web.AppRunner(app);await self.server.setup();site=web.TCPSite(self.server,'127.0.0.1',0);await site.start()
        self.url='http://127.0.0.1:'+str(self.server.addresses[0][1])
    async def asyncTearDown(self):await self.server.cleanup()
    async def test_fresh_post_connections(self):
        async with ClientSession(connector=proxy_connector()) as session:
            for k in range(2):
                async with session.post(self.url+'/good',json=dict(logical_request=k)) as response:self.assertTrue((await response.json())['ok'])
        self.assertEqual(len(self.peers),2);self.assertNotEqual(self.peers[0],self.peers[1])
    async def test_ambiguous_generation_is_not_resent(self):
        async with ClientSession(connector=proxy_connector()) as session:
            with self.assertRaises(ClientError):
                async with session.post(self.url+'/drop',json=dict(logical_request='same')) as response:await response.read()
        self.assertEqual(self.dropped,1)

if __name__=='__main__':unittest.main(verbosity=2)
