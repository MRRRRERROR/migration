"""One real migrated request, complete streams, then normal S/D teardown."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import aiohttp
from two_gpu_power_drain import Experiment, dump, preflight, rows, snapshot


async def run(args):
    root=Path(args.root);root.mkdir(parents=True,exist_ok=False)
    old=Path(args.workload_root)
    (root/'inputs').mkdir()
    shutil.copy2(old/'inputs/prompts.jsonl',root/'inputs/prompts.jsonl')
    e=Experiment(root,diagnostic=True)
    dump(root/'diagnostic_manifest.json',{'purpose':'Known-failing version diagnostic, never a formal round',
        'one_request':True,'real_migration_required':True,'normal_shutdown':'SIGTERM API entry, S before D like original C',
        'version':e.version,'preflight':preflight(e.nv),'instrumentation':'GDB break/return traces; not latency/performance data'})
    folder=root/'run';folder.mkdir()
    debugger=None;gdb_log=None;record=None;events=[]
    def event(name,**fields):
        row={'event':name,'monotonic_ns':time.monotonic_ns(),'wall_ns':time.time_ns(),**fields}
        events.append(row)
        with (root/'controller_events.jsonl').open('a') as stream:stream.write(json.dumps(row)+'\n')
        print(json.dumps(row),flush=True)
    try:
        await e.deploy(folder)
        d=snapshot(folder/'state_D');pid=d['pid']
        if not args.no_debugger:
            gdb_log=(root/'gdb.log').open('x')
            debugger=subprocess.Popen(['gdb','--nx','-q','--batch','-p',str(pid),'-x',str(Path(__file__).with_name('migration_exit_lifetime.gdb.py')),
            '-ex','continue','-ex','handle SIGSEGV nostop print pass','-ex','handle SIGABRT nostop print pass','-ex','continue'],
            stdout=gdb_log,stderr=subprocess.STDOUT,env={**os.environ,'MIGRATION_EXIT_GDB_TRACE':str(root/'native_lifetime.jsonl'),
                'MIGRATION_EXIT_FINISHED_MARKER':str(root/'clients_finished.json')})
            event('target_debugger_started',engine_pid=pid,debugger_pid=debugger.pid)
            deadline=time.monotonic()+60
            while not (root/'native_lifetime.jsonl').exists() or not any(row.get('event')=='debugger_ready' for row in rows(root/'native_lifetime.jsonl')):
                if debugger.poll() is not None:raise RuntimeError('Debugger failed to attach; preserve scene')
                if time.monotonic()>deadline:raise RuntimeError('Debugger readiness timeout; preserve scene')
                await asyncio.sleep(.1)
        old_req=next(row for row in rows(old/'acceptance_C/replay.jsonl') if row['request_id']=='p999-S-00014')
        record={'request_id':'real-migration-exit-one','initial_role':'S','prompt_index':old_req['prompt_index'],
                'seed':999,'mode':'A','planned_offset_s':0.,'t0_offset_s':1e9}
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            started=time.monotonic();task=asyncio.create_task(e.send(session,record,folder,started))
            while True:
                state=snapshot(folder/'state_S')
                if state and any(req['generated_tokens']>=32 for req in state['requests']):break
                if task.done():raise RuntimeError('Request ended before migration; no real handoff proven')
                if time.monotonic()-started>60:raise RuntimeError('Decode readiness timeout; preserve active request')
                await asyncio.sleep(.05)
            event('migration_trigger',source_snapshot=state)
            await e.migrate(folder)
            await asyncio.wait_for(asyncio.shield(task),timeout=600)
        event('client_stream_finished',request=record)
        if not record['completed']:raise RuntimeError('Client request failed; preserve scene')
        migration=rows(folder/'migration_events.jsonl')
        proof=[row for row in migration if row.get('request_id') and record['request_id'] in row['request_id'] and row.get('event_name') in ('dst_resume','migration_done')]
        if not proof:raise RuntimeError('No actual destination resume/commit; not a migration reproduction')
        deadline=time.monotonic()+60;stable=0
        while stable<3:
            states={role:snapshot(folder/f'state_{role}') for role in 'SD'}
            lease=await asyncio.to_thread(e.lease_state,folder)
            safe=all(state and not state['requests'] and state['frontend_observed'] and state['migration_tasks']==0
                     and not state['migrating_request_ids'] and state['migration_finish_set'] for state in states.values())
            safe=safe and not lease['held'] and lease['acquired']==lease['returned']
            stable=stable+1 if safe else 0
            if time.monotonic()>deadline:raise RuntimeError('Native/request drain proof unavailable; preserve scene')
            await asyncio.sleep(.2)
        dump(root/'clients_finished.json',{'states':states,'lease':lease,'request':record,'real_migration_proof':proof,
                                         'monotonic_ns':time.monotonic_ns(),'wall_ns':time.time_ns()})
        event('all_requests_finished_and_safe',lease=lease,real_migration=proof)
        await e.terminate(e.discovery['S']);event('source_normal_exit_begin')
        await e.terminate(e.servers['S']);event('source_normal_exit_end')
        await asyncio.sleep(1)
        await e.terminate(e.discovery['D']);event('target_normal_exit_begin')
        await e.terminate(e.servers['D']);event('target_normal_exit_end')
        text='\n'.join(path.read_text(errors='replace') for path in folder.glob('vllm_*.log'))
        if 'MIGRATION_EXIT_FORCE_KILL' in text or '"exitcode": -9' in text:
            raise RuntimeError('Native process forced to exit; not a clean shutdown')
        if debugger:
            deadline=time.monotonic()+10
            while debugger.poll() is None and time.monotonic()<deadline:await asyncio.sleep(.1)
            if 'Program terminated with signal' in (root/'gdb.log').read_text(errors='replace'):
                raise RuntimeError('Native debugger observed signal termination; not a clean shutdown')
        dump(root/'result.json',{'status':'clean_target_exit','real_migration':True,'client_completed':True})
    except Exception as error:
        event('diagnostic_error',error=repr(error))
        dump(root/'result.json',{'status':'diagnostic_failure','error':repr(error),'real_migration_proved':(root/'clients_finished.json').exists(),
             'client_completed':record.get('completed') if record else None,'scene_preserved':True})
    finally:
        if debugger:
            deadline=time.monotonic()+30
            while debugger.poll() is None and time.monotonic()<deadline:await asyncio.sleep(.2)
            event('debugger_status',returncode=debugger.poll(),still_running=debugger.poll() is None)
        # No forced shutdown: failed or unfinished model processes remain intact.
        # The normal service teardown above records any native fault.
        e.record_owned()
        dump(root/'owned_process_identities.json',e.owned)
        dump(root/'gpu_after.json',[e.nv.row(0,'S'),e.nv.row(1,'D')])
        for name,process,log in e.processes:
            if name=='redis' and all(server.poll() is not None for server in e.servers.values()):
                await e.terminate(process)
            log.flush()
        if gdb_log:gdb_log.flush()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);parser.add_argument('--workload-root',required=True);parser.add_argument('--no-debugger',action='store_true')
    asyncio.run(run(parser.parse_args()))


if __name__=='__main__':main()
