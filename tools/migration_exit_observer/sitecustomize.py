"""Opt-in instrumentation for owned migration-exit diagnostic processes only."""
import ctypes
import json
import os
from pathlib import Path
import runpy
import time
import traceback

if os.environ.get('MIGRATION_EXIT_DIAGNOSTIC')=='1':
    # Permit the same-user debugger to attach to these diagnostic processes
    # under Yama=1. This changes no host setting and grants access to no other PID.
    libc=ctypes.CDLL(None,use_errno=True)
    rc=libc.prctl(0x59616d61,ctypes.c_ulong(-1),0,0,0)
    if rc:
        raise RuntimeError('PR_SET_PTRACER diagnostic permission failed: '+str(ctypes.get_errno()))
    os.write(2,('MIGRATION_EXIT_OBSERVER '+json.dumps({'pid':os.getpid(),'monotonic_ns':time.monotonic_ns(),
        'event':'same_user_debugger_attach_enabled','prctl_result':rc})+'\n').encode())
    original_kill=os.kill
    def observed_kill(pid,sig):
        if sig==9:
            os.write(2,('MIGRATION_EXIT_FORCE_KILL '+json.dumps({'sender_pid':os.getpid(),'target_pid':pid,
                'signal':int(sig),'monotonic_ns':time.monotonic_ns(),'wall_ns':time.time_ns(),
                'stack':traceback.format_stack(limit=8)})+'\n').encode())
        return original_kill(pid,sig)
    os.kill=observed_kill
    import multiprocessing.process
    original_join=multiprocessing.process.BaseProcess.join
    def observed_join(process,timeout=None):
        result=original_join(process,timeout)
        if process.exitcode is not None:
            os.write(2,('MIGRATION_EXIT_CHILD_EXIT '+json.dumps({'parent_pid':os.getpid(),'child_pid':process.pid,
                'exitcode':process.exitcode,'join_timeout':timeout,'monotonic_ns':time.monotonic_ns()})+'\n').encode())
        return result
    multiprocessing.process.BaseProcess.join=observed_join
    tools=Path(__file__).resolve().parents[1]
    runpy.run_path(str(tools/'power_drain_observer/sitecustomize.py'))
    runpy.run_path(str(tools/'shutdown_lifecycle_observer/sitecustomize.py'))
