"""Real single/two-instance teardown cases; never fall back to SIGKILL."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

import aiohttp
import psutil
from tests.local import utils
from two_gpu_nvml_monitor import NVML

PROJECT=Path(__file__).resolve().parents[1]
BAD=re.compile(r'AssertFailed|Segfault encountered|Fatal Python error|EngineCore encountered a fatal error|EngineCore failed to start|KVTResp\(code=500|illegal memory access',re.I)
def dump(p,x):p.write_text(json.dumps(x,indent=2,default=str)+'\n')
async def main(a):
    root=Path(a.root);root.mkdir(parents=True,exist_ok=False);nv=NVML();procs=[];servers=[];streams=[];owned={}
    for i in range(a.instances):
        if nv.processes(nv.handle(i)):raise RuntimeError('Approved GPU occupied')
    for port in [6379]+[p+i for i in range(a.instances) for p in (8100,31100,36218,49000)]:
        with socket.socket() as s:s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('127.0.0.1',port))
    def start(name,cmd,env=None):
        f=(root/(name+'.log')).open('x');streams.append(f)
        p=subprocess.Popen(cmd,shell=True,cwd=PROJECT,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,env={**os.environ,**(env or {})})
        procs.append(p);owned[p.pid]=psutil.Process(p.pid).create_time()
        dump(root/'owned_processes.json',[{'pid':p.pid,'created':owned[p.pid]} for p in procs]);return p
    redis=start('redis',utils.get_redis_command()+" --save '' --appendonly no")
    await asyncio.sleep(.5)
    commands=[]
    for i in range(a.instances):
        cmd=utils.get_vllm_command('neutral',8100+i,i,True,f'shutdown_{root.name}_{i}','HybridConnector')
        cmd=re.sub(r'--max-model-len\s+\S+','--max-model-len 9216',cmd)
        cmd=re.sub(r'--max-num-batched-tokens\s+\S+','--max-num-batched-tokens 8192',cmd)
        # This verified vLLM revision parses CUDA_VISIBLE_DEVICES as integers.
        # Pin PCI ordering and retain the physical indices already verified by NVML.
        cmd='CUDA_DEVICE_ORDER=PCI_BUS_ID '+cmd
        cmd+=' --dtype bfloat16 --generation-config auto --kv-cache-memory-bytes 4294967296'
        p=start(f'vllm_{i}',cmd,{'LLUMNIX_RPC_PORT':str(49000+i)});servers.append(p);commands.append(cmd)
    dump(root/'commands.json',commands);results=[]
    async with aiohttp.ClientSession() as session:
        for i,p in enumerate(servers):
            deadline=time.monotonic()+600
            while True:
                if p.poll() is not None:raise RuntimeError('Startup failed')
                try:
                    async with session.get(f'http://127.0.0.1:{8100+i}/health',timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        if resp.status==200:break
                except Exception:pass
                if time.monotonic()>deadline:raise RuntimeError('Startup timeout')
                await asyncio.sleep(.5)
            for n in range(a.requests):
                async with session.post(f'http://127.0.0.1:{8100+i}/v1/completions',json={'model':'/data/models/Qwen2.5-7B-Instruct',
                    'prompt':'Explain why request state must be drained before shutting down an inference server.',
                    'max_tokens':64,'temperature':0,'stream':False,'kv_transfer_params':{'ali_llumnix_disagg':False}}) as resp:
                    data=await resp.json();results.append({'instance':i,'status':resp.status,'response':data})
                    if resp.status!=200 or (data.get('choices') or [{}])[0].get('finish_reason') not in ('stop','length'):raise RuntimeError('Small request failed')
    dump(root/'requests.json',results)
    for i,server in enumerate(servers):
        parent=psutil.Process(server.pid);children=parent.children(recursive=True)
        targets=[parent]+children
        owned.update({p.pid:p.create_time() for p in targets})
        api=next(p for p in children if '/bin/vllm' in ' '.join(p.cmdline()))
        evidence={'instance':i,'method':a.stop,'api_pid':api.pid,'before':[{'pid':p.pid,'created':p.create_time(),'cmd':p.cmdline()} for p in targets],
                  'start_monotonic_s':time.monotonic(),'start_wall_ns':time.time_ns()}
        # Documentation recommends SIGTERM for graceful shutdown. The local test
        # helper adds a forbidden SIGKILL fallback, so reproduce only its SIGTERM
        # phase for the project_tree comparison.
        if a.stop=='project_tree':
            for p in reversed(children):
                try:p.send_signal(signal.SIGTERM)
                except psutil.NoSuchProcess:pass
            parent.send_signal(signal.SIGTERM)
        else:api.send_signal(signal.SIGTERM)
        deadline=time.monotonic()+90
        def live():
            out=[]
            for p in targets:
                try:
                    if p.is_running() and p.status()!=psutil.STATUS_ZOMBIE:out.append(p.pid)
                except psutil.Error:pass
            return out
        while live() and time.monotonic()<deadline:await asyncio.sleep(.2)
        server.poll();raw=(root/f'vllm_{i}.log').read_text(errors='replace')
        runtime='\n'.join(s for s in raw.splitlines() if '[entrypoints/logger.py:' not in s)
        life=[]
        for line in raw.splitlines():
            if 'KVT_PY_SHUTDOWN {' in line:
                life.append(json.loads(line.split('KVT_PY_SHUTDOWN ',1)[1]))
        loop_failures=[row for row in life if row['event'] in ('core_shutdown_end','python_atexit')
                       and any(loop['running'] for loop in row.get('loops',[]))]
        evidence.update({'end_monotonic_s':time.monotonic(),'remaining_processes':live(),'gpu':nv.row(i,str(i)),
            'fatal_markers':BAD.findall(runtime),'native_trace':[s for s in raw.splitlines() if s.startswith('KVT_SHUTDOWN')],
            'shell_returncode':server.returncode,'connector_loop_lifecycle_failures':loop_failures})
        dump(root/f'exit_{i}.json',evidence)
        print(json.dumps({k:evidence[k] for k in ('instance','remaining_processes','fatal_markers')}) ,flush=True)
        if live() or nv.processes(nv.handle(i)):raise RuntimeError('Teardown retained processes; preserving scene')
        await asyncio.sleep(.5)
    # Redis has no GPU/request state; close only this case's owned helper.
    parent=psutil.Process(redis.pid)
    for child in parent.children(recursive=True):child.terminate()
    parent.terminate();redis.wait(timeout=15)
    for f in streams:f.close()
    clean=all(not json.loads((root/f'exit_{i}.json').read_text())['fatal_markers'] and not json.loads((root/f'exit_{i}.json').read_text())['connector_loop_lifecycle_failures'] for i in range(a.instances))
    dump(root/'result.json',{'clean':clean,'instances':a.instances,'requests_per_instance':a.requests,'stop':a.stop,'gpu_after':[nv.row(i,str(i)) for i in range(a.instances)]})
    if not clean:sys.exit(2)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--instances',type=int,choices=(1,2),default=1)
    p.add_argument('--requests',type=int,default=0);p.add_argument('--stop',choices=('api_sigterm','project_tree'),default='api_sigterm')
    asyncio.run(main(p.parse_args()))
