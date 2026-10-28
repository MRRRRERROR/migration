"""Independent real Prometheus scraping. Never routes or changes requests."""
import argparse
import asyncio
import gzip
import json
from pathlib import Path
import time

import aiohttp

async def main():
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('--max-hours',type=float,default=12);a=p.parse_args();root=Path(a.root)
    deadline=time.monotonic()+a.max_hours*3600
    async with aiohttp.ClientSession() as session:
        with gzip.open(root/'services_01/prometheus.jsonl.gz','xt',encoding='utf-8') as out:
            while time.monotonic()<deadline and not (root/'STOP_METRICS').exists():
                for engine,port in enumerate([18270,18271]):
                    record=dict(engine=engine,monotonic_s=time.monotonic(),wall_ns=time.time_ns())
                    try:
                        async with session.get(f'http://127.0.0.1:{port}/metrics',timeout=aiohttp.ClientTimeout(total=3)) as response:
                            record.update(status=response.status,text=await response.text())
                    except Exception as error:record['error']=repr(error)
                    record['end_monotonic_s']=time.monotonic();out.write(json.dumps(record)+'\n');out.flush()
                await asyncio.sleep(1)

if __name__=='__main__':asyncio.run(main())
