"""Experiment-only observation at V1 boundaries; never reads scheduler from a thread."""
import importlib.abc
import importlib.machinery
import json
import os
from pathlib import Path
import sys
import time

if os.getenv('LIVE_LOAD_STATE_DIR'):
    def observe(core, force=False):
        now=time.monotonic()
        if not force and now-getattr(core,'_load_last_snapshot',0)<1:return
        core._load_last_snapshot=now
        root=Path(os.environ['LIVE_LOAD_STATE_DIR']);root.mkdir(parents=True,exist_ok=True)
        sched=core.scheduler
        running={r.request_id for r in sched.running};waiting={r.request_id for r in sched.waiting}
        requests=[]
        for rid,req in sched.requests.items():
            blocks=sched.kv_cache_manager.get_blocks(rid)
            groups=blocks.blocks if hasattr(blocks,'blocks') else blocks
            requests.append(dict(request_id=rid,input_tokens=len(req.prompt_token_ids or []),
                generated_tokens=req.num_output_tokens,computed_tokens=req.num_computed_tokens,
                state='running' if rid in running else 'waiting' if rid in waiting else str(req.status),
                kv_blocks=sum(len(g) for g in groups),paused=bool(getattr(req,'_token_paused',False)),
                replay=bool(getattr(req,'_token_replay',False))))
        runtime=getattr(core,'token_runtime',None)
        front=core.llumlet_proxy.migration_frontend
        value=dict(monotonic_s=now,wall_ns=time.time_ns(),pid=os.getpid(),requests=requests,
            running=len(running),waiting=len(waiting),free_kv_blocks=sched.kv_cache_manager.block_pool.get_num_free_blocks(),
            sessions={k:s['state'] for k,s in runtime.sessions.items()} if runtime else None,
            sources={k:s['state'] for k,s in runtime.sources.items()} if runtime else None,
            migration_tasks=len(front._migration_out_tasks),migrating_request_ids=list(front.migrating_reqs),
            last_compute_monotonic_s=getattr(core,'_load_last_compute',None),
            instance_id=core.vllm_config.instance_id,
            llumlet_address=core.llumlet_proxy.llumlet_grpc_address,
            preemptions=getattr(sched,'_load_preemptions',0),
            loaded_files={k:getattr(sys.modules.get(k),'__file__',None) for k in (
                'vllm.v1.engine.core','vllm.v1.core.sched.scheduler','vllm.v1.worker.gpu_model_runner',
                'llumnix.vllm_llumlet_proxy','llumnix.migration_frontend.vllm_v1.kvt_migration_frontend')})
        path=root/f'{os.getpid()}.json';tmp=path.with_suffix('.tmp')
        tmp.write_text(json.dumps(value));tmp.replace(path)
        with (root/f'{os.getpid()}.jsonl').open('a') as f:f.write(json.dumps(value)+'\n')

    class Loader(importlib.abc.Loader):
        def __init__(self,loader):self.loader=loader
        def create_module(self,spec):return self.loader.create_module(spec)
        def exec_module(self,module):
            self.loader.exec_module(module)
            if module.__name__.endswith('scheduler'):
                old=module.Scheduler._preempt_request
                def preempt(sched,*a,**kw):
                    sched._load_preemptions=getattr(sched,'_load_preemptions',0)+1
                    return old(sched,*a,**kw)
                module.Scheduler._preempt_request=preempt
                return
            cls=module.EngineCoreProc
            old=cls._process_engine_step
            def step(core,*a,**kw):
                result=old(core,*a,**kw)
                if result:core._load_last_compute=time.monotonic()
                observe(core)
                return result
            cls._process_engine_step=step
            handle=cls._handle_client_request
            def boundary(core,*a,**kw):
                result=handle(core,*a,**kw);observe(core,True);return result
            cls._handle_client_request=boundary
    class Finder(importlib.abc.MetaPathFinder):
        def find_spec(self,fullname,path=None,target=None):
            if fullname not in ('vllm.v1.engine.core','vllm.v1.core.sched.scheduler'):return None
            spec=importlib.machinery.PathFinder.find_spec(fullname,path)
            if spec and spec.loader:spec.loader=Loader(spec.loader)
            return spec
    sys.meta_path.insert(0,Finder())
