"""Diagnostic lifecycle logging only; no teardown behavior changes."""
import atexit
import importlib.abc
import importlib.machinery
import json
import os
import sys
import threading
import time

def trace(event):
    state=[]
    proxy=sys.modules.get('blade_kvt.hybrid_connector.engine_proxy')
    if proxy:
        for name in ('_g_sched_loop','_g_worker_loop'):
            loop=getattr(proxy,name,None)
            state.append({'name':name,'running':loop.is_running() if loop else None,'closed':loop.is_closed() if loop else None})
    row={'event':event,'pid':os.getpid(),'monotonic_ns':time.monotonic_ns(),'wall_ns':time.time_ns(),
        'python_finalizing':sys.is_finalizing(),'threads':[{'name':t.name,'daemon':t.daemon,'alive':t.is_alive()} for t in threading.enumerate()],
        'loops':state}
    os.write(2,('KVT_PY_SHUTDOWN '+json.dumps(row)+'\n').encode())

atexit.register(lambda:trace('python_atexit'))
class Loader(importlib.abc.Loader):
    def __init__(self,loader):self.loader=loader
    def create_module(self,spec):return self.loader.create_module(spec)
    def exec_module(self,module):
        self.loader.exec_module(module);old=module.EngineCore.shutdown
        def shutdown(core):
            trace('core_shutdown_begin')
            try:return old(core)
            finally:trace('core_shutdown_end')
        module.EngineCore.shutdown=shutdown
class Finder(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,path=None,target=None):
        if fullname!='vllm.v1.engine.core':return None
        spec=importlib.machinery.PathFinder.find_spec(fullname,path)
        if spec and spec.loader:spec.loader=Loader(spec.loader)
        return spec
sys.meta_path.insert(0,Finder())
