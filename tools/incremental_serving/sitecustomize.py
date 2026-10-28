"""Activated only for this experiment's processes by INCREMENTAL_ENGINE_DIR."""
import importlib.abc
import importlib.machinery
import json
import os
from pathlib import Path
import sys
import time

if os.getenv('INCREMENTAL_ENGINE_DIR'):
    class Loader(importlib.abc.Loader):
        def __init__(self,loader):self.loader=loader
        def create_module(self,spec):return self.loader.create_module(spec)
        def exec_module(self,m):
            self.loader.exec_module(m)
            if m.__name__.endswith('kvt_migration_frontend'):
                def selected(front,policy,reqs):
                    p=Path(os.environ['INCREMENTAL_ENGINE_DIR'])/'selected_running.json'
                    if not p.exists():return []
                    v=json.loads(p.read_text())
                    if time.monotonic()>v['valid_until'] or v['mode']!='C':return []
                    ids=[i for i,(req,n) in enumerate(reqs) if req.request_id==v['request_id'] and n>0]
                    front._active_migration_policy='incremental_short_current_context'
                    return ids[:1]
                m.KVTMigrationFrontend._get_sorted_indices=selected
                return
            if m.__name__.endswith('scheduler'):
                old=m.Scheduler.schedule
                def schedule(sched,*a,**kw):
                    ctl=getattr(sched,'_incremental_control',None)
                    held=[]
                    if ctl and ctl.hold_ids:
                        # Correctness fixture only; never enabled in measurements.
                        for req in list(sched.waiting):
                            if req.request_id in ctl.hold_ids or __import__('engine_control').logical_id(req.request_id) in ctl.hold_ids:
                                sched.waiting.remove_request(req);held.append(req)
                    try:result=old(sched,*a,**kw)
                    finally:
                        for req in reversed(held):sched.waiting.prepend_request(req)
                    if ctl:ctl.ever.update(result.num_scheduled_tokens)
                    return result
                m.Scheduler.schedule=schedule
                return
            cls=m.EngineCoreProc;old=cls._process_engine_step;handle=cls._handle_client_request
            def control(core):
                if not hasattr(core,'_incremental_control'):
                    from engine_control import BoundaryControl
                    core._incremental_control=BoundaryControl(core)
                    core.scheduler._incremental_control=core._incremental_control
                return core._incremental_control
            def step(core,*a,**kw):
                ctl=control(core);ctl.boundary();start=time.monotonic()
                result=old(core,*a,**kw)
                if result:ctl.steps.append(time.monotonic()-start)
                ctl.boundary();return result
            def boundary(core,*a,**kw):
                ctl=control(core);result=handle(core,*a,**kw);ctl.boundary();return result
            cls._process_engine_step=step;cls._handle_client_request=boundary
    class Finder(importlib.abc.MetaPathFinder):
        def find_spec(self,fullname,path=None,target=None):
            if fullname not in ('vllm.v1.engine.core','vllm.v1.core.sched.scheduler','llumnix.migration_frontend.vllm_v1.kvt_migration_frontend'):return None
            spec=importlib.machinery.PathFinder.find_spec(fullname,path)
            if spec and spec.loader:spec.loader=Loader(spec.loader)
            return spec
    sys.meta_path.insert(0,Finder())
