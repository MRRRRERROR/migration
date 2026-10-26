"""Experiment-only, read-only observer; frozen serving sources are untouched."""
import importlib.abc
import importlib.machinery
import json
import os
from pathlib import Path
import sys
import time

if os.environ.get('POWER_DRAIN_STATE_DIR'):
    class ObserverLoader(importlib.abc.Loader):
        def __init__(self,loader,name):self.loader,self.name=loader,name
        def create_module(self,spec):return self.loader.create_module(spec)
        def exec_module(self,module):
            self.loader.exec_module(module)
            if self.name.endswith('engine_proxy'):
                old=module.core_init
                def init(core,cfg):
                    result=old(core,cfg)
                    root=Path(os.environ['POWER_DRAIN_STATE_DIR']);root.mkdir(parents=True,exist_ok=True)
                    path=root/f'{os.getpid()}.json'
                    def observe():
                        sched=getattr(core,'scheduler',None)
                        if sched is None:return
                        now=time.monotonic()
                        if True:
                            try:
                                running={r.request_id for r in list(sched.running)}
                                waiting={r.request_id for r in list(sched.waiting)}
                                rows=[]
                                for rid,req in list(sched.requests.items()):
                                    blocks=sched.kv_cache_manager.get_blocks(rid)
                                    groups=blocks.blocks if hasattr(blocks,'blocks') else blocks
                                    ids=[[int(getattr(b,'block_id',b)) for b in group] for group in groups]
                                    rows.append({'request_id':rid,'state':'running' if rid in running else 'waiting' if rid in waiting else str(req.status),
                                        'input_tokens':len(req.prompt_token_ids or []),'generated_tokens':len(req.output_token_ids),
                                        'computed_tokens':req.num_computed_tokens,'kv_block_ids':ids,
                                        'kv_blocks':sum(map(len,ids)), 'kv_bytes':sum(map(len,ids))*16*cfg.model_config.hf_config.num_hidden_layers*cfg.model_config.hf_config.num_key_value_heads*(cfg.model_config.hf_config.hidden_size//cfg.model_config.hf_config.num_attention_heads)*2*2})
                                front=getattr(sys.modules.get('llumnix.migration_frontend.vllm_v1.kvt_migration_frontend'),'_power_drain_front',None)
                                payload={'monotonic_s':now,'wall_ns':time.time_ns(),'pid':os.getpid(),'requests':rows,
                                    'running':len(running),'waiting':len(waiting),
                                    'frontend_observed':front is not None,
                                    'migration_tasks':len(front._migration_out_tasks) if front else None,
                                    'migrating_request_ids_raw':list(front.migrating_reqs) if front else None,
                                    'migrating_request_ids':[rid for rid in front.migrating_reqs if rid in sched.requests] if front else None,
                                    'migration_finish_set':front.migration_finish_event.is_set() if front and front.migration_finish_event else None}
                                temp=path.with_suffix('.tmp');temp.write_text(json.dumps(payload));temp.replace(path)
                                with (root/f'{os.getpid()}.jsonl').open('a') as stream:stream.write(json.dumps(payload)+'\n')
                            except Exception as exc:
                                with (root/'observer_errors.jsonl').open('a') as stream:stream.write(json.dumps({'time':now,'error':repr(exc)})+'\n')
                    import threading
                    def loop():
                        while True:
                            observe();time.sleep(.2)
                    threading.Thread(target=loop,name='power-drain-read-only',daemon=True).start()
                    return result
                module.core_init=init
            else:
                cls=module.KvtMigrationFrontend if hasattr(module,'KvtMigrationFrontend') else module.KVTMigrationFrontend
                old=cls.__init__
                def init(front,*args,**kwargs):
                    old(front,*args,**kwargs);module._power_drain_front=front
                cls.__init__=init
    class ObserverFinder(importlib.abc.MetaPathFinder):
        def find_spec(self,fullname,path=None,target=None):
            if fullname not in ('blade_kvt.hybrid_connector.engine_proxy','llumnix.migration_frontend.vllm_v1.kvt_migration_frontend'):return None
            spec=importlib.machinery.PathFinder.find_spec(fullname,path)
            if spec and spec.loader:spec.loader=ObserverLoader(spec.loader,fullname)
            return spec
    sys.meta_path.insert(0,ObserverFinder())
