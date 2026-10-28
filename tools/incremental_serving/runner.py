"""Unified ingress, open Poisson replay and predeclared calibration/pretrial gates.

Real services only. Formal expansion is an explicit phase selected by the
pretrial validity/opportunity gate; failed/contaminated directories are retained.
"""
import argparse
import asyncio
import copy
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import time
import traceback

import aiohttp
from aiohttp import web

from harness import Harness,ROOT,base
from engine_control import atomic,append,logical_id
from policy import PolicyConfig,choose_route,choose_waiting,choose_running


def quantile(values,q):
    values=sorted(values)
    return values[max(0,math.ceil(q*len(values))-1)] if values else None


def summary(rows):
    completed=[r for r in rows if r.get('completed')]
    n=len(rows);end_to_end=sorted(r['e2e_s'] for r in completed)
    # Incomplete requests have no finite completion time; do not drop them.
    def unconditional(q):
        rank=math.ceil(q*n)
        return end_to_end[rank-1] if rank and len(end_to_end)>=rank else None
    return dict(requests=n,completed=len(completed),completion_rate=len(completed)/n if n else None,
        failed=sum(bool(r.get('error')) and not r.get('unfinished') for r in rows),
        unfinished=sum(bool(r.get('unfinished')) for r in rows),
        e2e_p50_s=unconditional(.5),e2e_p95_s=unconditional(.95),e2e_p99_s=unconditional(.99),
        ttft_p50_s=quantile([r['ttft_s'] for r in completed if r['ttft_s'] is not None],.5),
        ttft_p95_s=quantile([r['ttft_s'] for r in completed if r['ttft_s'] is not None],.95),
        finish_reasons={reason:sum(r.get('finish_reason')==reason for r in rows) for reason in ['stop','length',None]},
        output_tokens=base.stats([len(r['token_ids']) for r in completed]),
        send_lag_s=base.stats([r['send_lag_s'] for r in rows if 'send_lag_s' in r]))


def poisson(pool,rate,seed,duration,label):
    rng=random.Random(seed);offset=0.;rows=[]
    while True:
        offset+=rng.expovariate(rate)
        if offset>=duration:break
        prompt=rng.randrange(len(pool));p=pool[prompt]
        rows.append(dict(request_id=f'{label}_s{seed}_r{len(rows):06d}',prompt_index=prompt,
            input_token_ids=p['token_ids'],input_sha256=p['input_sha256'],planned_offset_s=offset,
            workload_seed=seed,generation=dict(max_tokens=4096,temperature=0,n=1,ignore_eos=False)))
    return rows


def proxy_connector():
    # Each non-idempotent generation POST gets its own connection. Never retry
    # an ambiguous POST and risk generating the same logical request twice.
    return aiohttp.TCPConnector(limit=0,force_close=True)


class Incremental(Harness):
    def __init__(self,root=ROOT,attach=False):
        super().__init__(root,attach=attach);self.cfg=PolicyConfig();self.extra={};self.location={};self.route_events=[]
        self.current=None;self.background=[];self.critical=None;self.mode='A';self.ban={};self.cool_until=0
        self.last_gap_source=None;self.persist=0;self.log_positions={};self.log_inodes={};self.log_tails={};self.completed_rounds=[]

    def check_critical(self):
        # Harness commands call this too. Full history is checked once before
        # the replay clock starts; subsequent checks read only appended bytes.
        self.runtime_check()

    def beat(self,phase,**fields):
        atomic(self.root/'progress.json',dict(phase=phase,wall_ns=time.time_ns(),monotonic_s=time.monotonic(),
            current=str(self.current) if self.current else None,completed_rounds=self.completed_rounds,**fields))

    async def ingress(self,request):
        rid=request.headers.get('X-Request-Id')
        if not rid:raise web.HTTPBadRequest(text='A stable logical request ID is required')
        states=[self.state(i) for i in range(2)];now=time.monotonic()
        if not all(states):raise web.HTTPServiceUnavailable(text='No engine state')
        observed={r['logical_id'] for s in states for r in s['requests']}
        for key in list(self.extra):
            if key in observed:del self.extra[key]
        extras=[sum(v==i for v in self.extra.values()) for i in range(2)]
        engine=choose_route(rid,states,extras);self.extra[rid]=engine
        self.location.setdefault(rid,set())
        route=dict(request_id=rid,monotonic_s=now,engine=engine,extras=extras,
            running=[s['running'] for s in states],waiting=[s['waiting'] for s in states],
            kv_usage=[s['kv_usage'] for s in states],snapshot_ages_s=[now-s['monotonic_s'] for s in states])
        if self.current:append(self.current/'routing.jsonl',route)
        stage='read_body';response_started=False
        try:
            body=await request.read()
            stage='upstream_headers'
            async with self.proxy_session.post(f'http://127.0.0.1:{base.APIS[engine]}/v1/completions',data=body,
                headers={'Content-Type':'application/json','X-Request-Id':rid},timeout=aiohttp.ClientTimeout(total=None)) as upstream:
                response=web.StreamResponse(status=upstream.status,headers={'Content-Type':upstream.headers.get('Content-Type','text/event-stream'),'X-Experiment-Initial-Engine':str(engine)})
                await response.prepare(request)
                response_started=True;stage='streaming'
                async for chunk in upstream.content.iter_any():await response.write(chunk)
                await response.write_eof();return response
        except aiohttp.ClientError as error:
            evidence=dict(request_id=rid,engine=engine,stage=stage,response_started=response_started,
                error_type=type(error).__name__,error=repr(error),monotonic_s=time.monotonic(),wall_ns=time.time_ns(),
                upstream_admission='unknown; do not retry',automatic_retry=False)
            if self.current:append(self.current/'gateway_transport_errors.jsonl',evidence)
            self.critical='Gateway upstream transport failure requires diagnosis: '+repr(error)
            raise
        finally:self.extra.pop(rid,None)

    async def start_gateway(self):
        self.proxy_session=aiohttp.ClientSession(connector=proxy_connector())
        app=web.Application(client_max_size=1024**2);app.router.add_post('/v1/completions',self.ingress)
        self.gateway_runner=web.AppRunner(app);await self.gateway_runner.setup()
        await web.TCPSite(self.gateway_runner,'127.0.0.1',18370).start()

    async def send(self,session,plan,start,folder):
        row={k:v for k,v in plan.items() if k!='input_token_ids'}
        now=time.monotonic();row.update(planned_monotonic_s=start+plan['planned_offset_s'],actual_send_monotonic_s=now,
            send_lag_s=now-start-plan['planned_offset_s'],chunks=[],token_ids=[],text='',error=None,
            sse_done=False,finish_reason=None,input_tokens=len(plan['input_token_ids']))
        payload=dict(model=base.MODEL,prompt=plan['input_token_ids'],max_tokens=4096,temperature=0,n=1,
            stream=True,return_token_ids=True,stream_options={'include_usage':True,'continuous_usage_stats':True},
            kv_transfer_params={'ali_llumnix_disagg':False})
        append(folder/'sends.jsonl',dict(request_id=plan['request_id'],planned_monotonic_s=row['planned_monotonic_s'],
            actual_send_monotonic_s=now,send_lag_s=row['send_lag_s']))
        try:
            async with session.post('http://127.0.0.1:18370/v1/completions',json=payload,
                headers={'X-Request-Id':plan['request_id']},timeout=aiohttp.ClientTimeout(total=None)) as response:
                row.update(http_status=response.status,initial_engine=response.headers.get('X-Experiment-Initial-Engine'))
                if response.status!=200:raise RuntimeError('HTTP '+str(response.status)+' '+await response.text())
                async for line in response.content:
                    if not line.startswith(b'data:'):continue
                    content=line[5:].strip()
                    if content==b'[DONE]':row['sse_done']=True;continue
                    data=json.loads(content);row['response_id']=data.get('id',row.get('response_id'))
                    if data.get('usage'):row['usage']=data['usage']
                    for choice in data.get('choices',[]):
                        text=choice.get('text') or '';ids=choice.get('token_ids') or []
                        if text or ids:row['chunks'].append(dict(time=time.monotonic(),token_count=len(ids)))
                        row['text']+=text;row['token_ids'].extend(ids)
                        row['finish_reason']=choice.get('finish_reason') or row['finish_reason']
                if not row['sse_done'] or row['finish_reason'] not in ('stop','length'):raise RuntimeError('Incomplete response')
        except Exception as error:row['error']=repr(error)
        finally:
            row['finish_monotonic_s']=time.monotonic();row['e2e_s']=row['finish_monotonic_s']-now
            row['ttft_s']=row['chunks'][0]['time']-now if row['chunks'] else None
            row['tpot_s']=(row['finish_monotonic_s']-row['chunks'][0]['time'])/(len(row['token_ids'])-1) if len(row['token_ids'])>1 and row['chunks'] else None
            row['max_output_block_gap_s']=max([b['time']-a['time'] for a,b in zip(row['chunks'],row['chunks'][1:])],default=None)
            row['completed']=row['error'] is None and row['sse_done']
            row['execution_engines_observed']=sorted(self.location.get(row['request_id'],set()))
            row['output_sha256']=hashlib.sha256(json.dumps(row['token_ids']).encode()).hexdigest()
            append(folder/'requests.jsonl',row)
        return row

    def runtime_check(self):
        for label,p,_ in self.processes:
            if p.poll() is not None:raise RuntimeError('Owned process stopped: '+label)
        for role in 'SD':
            p=self.service/('state_'+role)/'observer_failure.json'
            if p.exists():raise RuntimeError('Observer failure: '+p.read_text())
        for path in self.service.glob('vllm_*.log'):
            key=str(path);info=path.stat();position=self.log_positions.get(key,0)
            if key in self.log_inodes and (info.st_ino!=self.log_inodes[key] or info.st_size<position):
                raise RuntimeError('Owned serving log rotated/truncated unexpectedly: '+key)
            with path.open('rb') as f:
                f.seek(position);value=f.read();self.log_positions[key]=f.tell()
            self.log_inodes[key]=info.st_ino
            value=self.log_tails.get(key,'')+value.decode(errors='replace');self.log_tails[key]=value[-4096:]
            match=base.CRITICAL.search(value)
            if match:raise RuntimeError('Critical runtime error: '+str(path)+' '+value[max(0,match.start()-200):match.end()+2000])

    async def observe(self,folder,stop,start,duration=600):
        while not stop.is_set():
            try:
                self.runtime_check();states=[self.state(i) for i in range(2)];owned=self.owned()
                for i in range(2):
                    gpu=self.nv.row(i,str(i))
                    if any(p['pid'] not in owned for p in json.loads(gpu['compute_pids'])):
                        raise RuntimeError('Foreign task on experiment GPU; contaminated round: '+json.dumps(gpu))
                    for req in states[i]['requests']:
                        if req['ever_scheduled']:self.location.setdefault(req['logical_id'],set()).add(i)
                append(folder/'states.jsonl',dict(monotonic_s=time.monotonic(),elapsed_s=time.monotonic()-start,engines=states))
                self.beat('replaying' if time.monotonic()-start<duration else 'tail',elapsed_s=time.monotonic()-start)
            except Exception as error:
                self.critical=repr(error);append(folder/'critical.jsonl',dict(error=repr(error),traceback=traceback.format_exc(),monotonic_s=time.monotonic()));return
            await asyncio.sleep(.5)

    async def control(self,folder,stop):
        cfg=self.cfg;self.persist=0;self.last_gap_source=None;self.ban={}
        waiting_cool_until=0.;running_cool_until=0.
        while not stop.is_set() and not self.critical:
            start=time.monotonic();states=[self.state(i) for i in range(2)]
            record=dict(monotonic_s=start,mode=self.mode,states=states,action=None,
                waiting_cool_until=waiting_cool_until,running_cool_until=running_cool_until)
            try:
                if not all(s and start-s['monotonic_s']<=cfg.state_max_age_s for s in states):reason='metrics_unavailable'
                elif self.mode=='A':reason='scheme_forbids_transfer'
                elif any(s['migration_tasks'] or not s['migration_finish'] or s['holds'] for s in states):reason='migration_in_progress'
                else:
                    candidate,waiting_reason=choose_waiting(states,start,cfg,self.ban)
                    if start<waiting_cool_until:
                        candidate=None;waiting_reason='waiting_cooldown'
                    record['waiting_reason']=waiting_reason
                    if candidate:
                        record.update(action='waiting_transfer',candidate=candidate)
                        await self.transfer(candidate['source'],candidate['destination'],candidate['request'],folder)
                        reason='waiting_committed';waiting_cool_until=time.monotonic()+cfg.cooldown_s
                        running_cool_until=waiting_cool_until
                        self.ban[candidate['request']['logical_id']]=time.monotonic()+cfg.request_cooldown_s
                        self.persist=0
                    elif self.mode=='B':reason='running_forbidden_'+waiting_reason
                    elif start<running_cool_until:reason='cooldown'
                    else:
                        candidate,reason=choose_running(states,start,cfg,self.ban)
                        source=candidate['source'] if candidate else None
                        if source is not None and source==self.last_gap_source:self.persist+=1
                        else:self.persist=1 if source is not None else 0
                        self.last_gap_source=source;record['persistent_checks']=self.persist
                        if candidate and self.persist<cfg.persistent_checks:reason='difference_not_persistent'
                        elif candidate:
                            record.update(action='running_kv_migration',candidate=candidate,
                                source_other_ids=[r['logical_id'] for r in states[source]['requests'] if r['request_id']!=candidate['request']['request_id']],
                                destination_existing_ids=[r['logical_id'] for r in states[1-source]['requests']])
                            result=await self.trigger_kv(source,1-source,candidate['request'],folder)
                            record['rpc_result']=result
                            terminal=await self.wait_migration_terminal(result,folder)
                            record['terminal']=terminal;reason='running_'+terminal['classification']
                            running_cool_until=time.monotonic()+cfg.cooldown_s
                            self.ban[candidate['request']['logical_id']]=time.monotonic()+cfg.request_cooldown_s
                            self.persist=0
                record['reason']=reason
                if reason in ('metrics_unavailable','migration_in_progress','cooldown'):
                    self.persist=0;self.last_gap_source=None
            except Exception as error:
                # A queue start race before detach is an expected refusal. An
                # ambiguous ownership change is a hard gate, never a retry.
                current=[self.state(i) for i in range(2)]
                record.update(reason='action_exception',error=repr(error),after=current)
                text=str(error)
                safe_queue_race=(record.get('action')=='waiting_transfer' and 'Atomic never-executed claim rejected' in text and
                    ('Atomic never-executed check rejected' in text or 'Request absent or finished before claim' in text))
                if safe_queue_race and not any(s['holds'] for s in current):record['reason']='atomic_claim_rejected'
                else:
                    record['reason']='controller_action_failure';self.critical=repr(error)
            record['ended']=time.monotonic();append(folder/'decisions.jsonl',record)
            await asyncio.sleep(max(0,cfg.period_s-(time.monotonic()-start)))

    async def wait_migration_terminal(self,attempt,folder):
        rid=attempt['request_id'];deadline=attempt['started']+self.cfg.session_timeout_s
        # migrating_reqs is a lazily pruned selector cache in this version,
        # not an active-session indicator. Require actual handoff/cancel events
        # and matching source block-lease release instead of trusting RPC ACK.
        paths=dict(events=self.service/'kv_events.jsonl',lease=self.service/('vllm_'+('S','D')[attempt['source']]+'.log'))
        cursors={k:dict(attempt['evidence_cursors'][k],carry=b'') for k in paths}
        related=[];leases=[];read_bytes=0
        def appended(key):
            nonlocal read_bytes
            path=paths[key];cursor=cursors[key]
            if not path.exists():return []
            info=path.stat()
            if cursor['inode'] is not None and info.st_ino!=cursor['inode']:raise RuntimeError('Migration evidence log replaced')
            if info.st_size<cursor['offset']:raise RuntimeError('Migration evidence log truncated')
            with path.open('rb') as inp:inp.seek(cursor['offset']);raw=inp.read();cursor['offset']=inp.tell()
            read_bytes+=len(raw);cursor['inode']=info.st_ino
            parts=(cursor['carry']+raw).split(b'\n');cursor['carry']=parts.pop()
            if key=='events' and not cursor.get('starts_at_line_boundary',True) and parts:
                parts.pop(0);cursor['starts_at_line_boundary']=True
            return parts
        while time.monotonic()<deadline:
            self.runtime_check()
            for line in appended('events'):
                if line.strip():
                    e=json.loads(line)
                    if e.get('request_id')==rid and e.get('timestamp_ns',0)>=attempt['started_wall_ns']:related.append(e)
            names={e.get('event_name',e.get('event')) for e in related}
            for line in appended('lease'):
                if b'KVT_BLOCK_LEASE ' in line and rid.encode() in line:
                    event=json.loads(line.split(b'KVT_BLOCK_LEASE ',1)[1])
                    if event['timestamp_ns']>=attempt['started_wall_ns']:leases.append(event)
            acquired={e['lease_id'] for e in leases if e['event']=='acquire'}
            released={e['lease_id'] for e in leases if e['event']=='release'}
            balanced=acquired==released
            classification=None
            if 'migration_done' in names and 'dst_resume' in names and 'source_release' in names and balanced:classification='completed'
            elif names & {'migration_attempt_cancelled','migration_abort','invalid_migration_endpoint'} and balanced:
                classification='cancelled' if 'invalid_migration_endpoint' not in names else 'failed'
            elif attempt.get('rpc_success') is False and 'error' not in attempt and not names and not acquired:classification='not_dispatched'
            if classification:
                value=dict(classification=classification,request_id=rid,events=related,leases=leases,
                    source_lease_balanced=balanced,confirmed_monotonic_s=time.monotonic(),evidence_log_bytes_read=read_bytes,
                    evidence_log_read_bytes_are_not_transfer_bytes=True)
                append(folder/'migration_terminals.jsonl',value);return value
            await asyncio.sleep(.25)
        self.critical='Migration terminal/lease evidence missing at fixed timeout; preserve scene'
        raise RuntimeError(self.critical)

    async def round(self,name,mode,trace,duration=600):
        folder=self.root/name;folder.mkdir(exist_ok=False);self.current=folder
        await self.configure(mode);self.mode=mode;self.location={};self.extra={};self.critical=None
        # Identical cache reset followed by identical warmup, then reset again:
        # kernels remain warm while prefix-cache starting state is empty.
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            for k in range(4):
                for i in range(2):
                    async with session.post(f'http://127.0.0.1:{base.APIS[i]}/v1/completions',json=dict(model=base.MODEL,
                        prompt=self.prompts[k]['token_ids'],max_tokens=16,temperature=0,kv_transfer_params={'ali_llumnix_disagg':False})) as r:
                        if r.status!=200:raise RuntimeError('Round warmup failed: '+await r.text())
            await self.configure(mode)
            atomic(folder/'replay.json',trace);atomic(folder/'config.json',dict(mode=mode,duration_s=duration,tail_s=600,policy=asdict(self.cfg)))
            start=time.monotonic();atomic(folder/'timeline.json',dict(start_monotonic_s=start,start_wall_ns=time.time_ns()))
            stop=asyncio.Event();observer=asyncio.create_task(self.observe(folder,stop,start,duration));controller=asyncio.create_task(self.control(folder,stop))
            tasks=[];aborted=False
            for plan in trace:
                await asyncio.sleep(max(0,start+plan['planned_offset_s']-time.monotonic()))
                if self.critical:aborted=True;break
                tasks.append(asyncio.create_task(self.send(session,plan,start,folder)))
            if not aborted:await asyncio.sleep(max(0,start+duration-time.monotonic()))
            remaining=max(0,start+duration+600-time.monotonic())
            done,pending=await asyncio.wait(tasks,timeout=remaining)
            stop.set();await observer;await controller
            rows=[t.result() for t in done]
            if pending:
                for plan,task in zip(trace,tasks):
                    if task in pending:rows.append(dict(request_id=plan['request_id'],unfinished=True,completed=False,error='tail_deadline',planned_offset_s=plan['planned_offset_s']))
                atomic(folder/'tail_deadline.json',dict(unfinished_ids=[r['request_id'] for r in rows if r.get('unfinished')],states=[self.state(i) for i in range(2)]))
                self.critical='Unfinished requests at fixed tail deadline; stop subsequent rounds'
            if len(tasks)!=len(trace):self.critical=self.critical or 'Replay interrupted before all planned arrivals'
            lags=[r['send_lag_s'] for r in rows if 'send_lag_s' in r]
            timing=dict(max_lag_s=max(lags,default=None),p99_lag_s=quantile(lags,.99),
                predeclared_max_s=1.0,predeclared_p99_s=.5,
                passed=bool(lags) and max(lags)<=1 and quantile(lags,.99)<=.5)
            atomic(folder/'sender_timing_quality.json',timing)
            if name.startswith(('pre_','formal_','sender_acceptance')) and not timing['passed']:
                self.critical=self.critical or 'Actual replay timing exceeds common predeclared tolerance; preserve round and diagnose'
            atomic(folder/'summary.json',summary(rows));atomic(folder/'measurement_rows.json',rows)
            atomic(folder/'validity.json',dict(valid=not self.critical,error=self.critical,sent=len(tasks),planned=len(trace),pending=len(pending)))
            self.completed_rounds.append(name);self.beat('round_finished',valid=not self.critical)
            if self.critical:
                # Keep SSE tasks/services alive for diagnosis; no further rounds.
                atomic(self.root/'BLOCKED.json',dict(round=name,error=self.critical,states=[self.state(i) for i in range(2)]))
                while pending:
                    done_now,pending=await asyncio.wait(pending,timeout=30)
                raise RuntimeError(self.critical)
        self.current=None;return folder,rows


def calibration(folder,rows):
    samples=base.readrows(folder/'states.jsonl');samples=[r for r in samples if 200<=r['elapsed_s']<=600]
    diagnostics=[]
    for i in range(2):
        pairs=[(r['elapsed_s'],r['engines'][i]['waiting']) for r in samples]
        mean_x=statistics.mean(x for x,y in pairs);mean_y=statistics.mean(y for x,y in pairs)
        slope=sum((x-mean_x)*(y-mean_y) for x,y in pairs)/sum((x-mean_x)**2 for x,y in pairs)
        first=statistics.mean(y for x,y in pairs if x<400);last=statistics.mean(y for x,y in pairs if x>=400)
        diagnostics.append(dict(engine=i,queue_slope_per_s=slope,late_minus_early_waiting=last-first,
            mean_waiting=mean_y,max_waiting=max(y for x,y in pairs)))
    early=[r['e2e_s'] for r in rows if r.get('completed') and 200<=r['planned_offset_s']<400]
    late=[r['e2e_s'] for r in rows if r.get('completed') and 400<=r['planned_offset_s']<600]
    latency_ok=bool(early and late and statistics.median(late)<=max(statistics.median(early)*1.25,statistics.median(early)+1))
    preemptions={}
    for sample in samples:
        for state in sample['engines']:
            for req in state['requests']:preemptions[req['request_id']]=max(preemptions.get(req['request_id'],0),req['preemptions'])
    # Compare completed throughput to observed arrivals within a late 200s
    # window; allow a bounded 10% transient, but also require low queue growth.
    timeline=json.loads((folder/'timeline.json').read_text());start=timeline['start_monotonic_s']
    arrived=sum(400<=r['planned_offset_s']<600 for r in rows)
    finished=sum(r.get('completed') and 400<=r['finish_monotonic_s']-start<600 for r in rows)
    throughput_ok=finished>=.9*arrived
    ok=all(d['queue_slope_per_s']<=.01 and d['late_minus_early_waiting']<=2 for d in diagnostics) and latency_ok and throughput_ok and all(r.get('completed') for r in rows) and not sum(preemptions.values())
    value=dict(stable=ok,queue=diagnostics,late_latency_median_s=statistics.median(late) if late else None,
        early_latency_median_s=statistics.median(early) if early else None,latency_trend_pass=latency_ok,
        late_arrivals=arrived,late_completions=finished,throughput_pass=throughput_ok,preemptions=sum(preemptions.values()))
    atomic(folder/'capacity_assessment.json',value);return value


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',default=str(ROOT));parser.add_argument('--resume-services',action='store_true')
    parser.add_argument('--calibration-rates',default='2,3,4');parser.add_argument('--calibration-seed-base',type=int,default=6100)
    parser.add_argument('--fixed-profiles',action='store_true',help='Validate exactly two already selected rates; do not automatically lower them')
    parser.add_argument('--transport-probe',action='store_true',help='One distinct logical request using the previously failed prompt, outside measured rounds')
    parser.add_argument('--reuse-calibration',help='Reference two closed corrected-transport calibration profiles, with unchanged workload/model/rates')
    parser.add_argument('--sender-acceptance',action='store_true',help='60s real open replay concurrent with harmless core snapshots before pretrials')
    args=parser.parse_args()
    h=Incremental(args.root,attach=args.resume_services);h.beat('reattaching_owned_services' if args.resume_services else 'deploying')
    try:
        if not args.resume_services:
            await h.deploy(h.service,'kv');await h.correctness()
        elif not (h.root/'correctness_01/result.json').exists():raise RuntimeError('Independent correctness qualification missing')
        await h.start_gateway()
        if args.transport_probe:
            folder=h.root/'http_real_acceptance_01';folder.mkdir(exist_ok=False);h.current=folder
            await h.configure('A');index=1178;prompt=h.prompts[index];nonce=0
            while True:
                rid=f'http_transport_probe_p1178_n{nonce}'
                if choose_route(rid,[h.state(i) for i in range(2)],[0,0])==1:break
                nonce+=1
            plan=dict(request_id=rid,prompt_index=index,input_token_ids=prompt['token_ids'],input_sha256=prompt['input_sha256'],
                planned_offset_s=0,workload_seed=None,generation=dict(max_tokens=4096,temperature=0,n=1,ignore_eos=False))
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
                row=await h.send(session,plan,time.monotonic(),folder)
            atomic(folder/'result.json',dict(passed=row['completed'],request=row,distinct_logical_request=True,
                original_failed_id='cal_lower_s6201_r000627',automatic_retry=False,counts_as_measurement_round=False))
            if not row['completed']:raise RuntimeError('Real gateway transport acceptance failed')
            h.current=None
        if not any(label=='nvml' and p.poll() is None for label,p,_ in h.processes):
            h.start('nvml',f'{base.sys.executable} -u tools/two_gpu_live_nvml.py --gpus 0,1 --output {h.service}/power.csv',h.service)
        atomic(h.root/'candidate_policy.json',asdict(h.cfg))
        rates=[float(x) for x in args.calibration_rates.split(',')]
        if not rates or any(x<=0 for x in rates):raise ValueError('Positive calibration rates required')
        if args.fixed_profiles and len(set(rates))!=2:raise ValueError('Fixed profile validation requires exactly two rates')
        stable=[];calibrations=[]
        if args.reuse_calibration:
            origin=Path(args.reuse_calibration).resolve()
            if not origin.is_relative_to(base.P/'results'):raise ValueError('Calibration reference outside project results')
            reference=json.loads((origin/'frozen_pretrial.json').read_text())
            if reference['policy']!=asdict(h.cfg):raise RuntimeError('Referenced policy changed')
            calibrations=json.loads((origin/'capacity_calibration.json').read_text())
            for point in calibrations:
                if not point['stable']:raise RuntimeError('Referenced calibration point not qualified')
            stable=[x['rate'] for x in calibrations]
            if sorted(stable)!=sorted(rates):raise RuntimeError('Referenced rates do not match unchanged fixed profiles')
            for folder in origin.glob('calibration_*'):
                validity=json.loads((folder/'validity.json').read_text());result=json.loads((folder/'summary.json').read_text())
                if not validity['valid'] or result['completed']!=result['requests']:raise RuntimeError('Calibration reference contains invalid/incomplete requests')
            atomic(h.root/'calibration_reference.json',dict(origin=str(origin),loads=reference['loads'],
                reason='Same corrected HTTP transport, models, policies and rates; only replace full historical log checks with incremental checks',
                initial_calibration_lag_retained=True,stability_window_s=[200,600],not_a_new_capacity_measurement=True))
            rates=[]
        for index,rate in enumerate(rates):
            trace=poisson(h.prompts,rate,args.calibration_seed_base+index,600,'cal')
            folder,rows=await h.round(f'calibration_{index+1:02d}_r{rate:g}','A',trace)
            result=calibration(folder,rows);calibrations.append(dict(rate=rate,**result))
            if result['stable']:stable.append(rate)
            else:break
        if len(stable)<2 and not args.fixed_profiles:
            # Capacity calibration may lower both profiles together; these are
            # independent trajectories, never a favorable formal-round retry.
            for index,rate in enumerate([.5,1.,1.5]):
                if rate in stable:continue
                folder,rows=await h.round(f'calibration_lower_{index+1:02d}_r{rate:g}','A',poisson(h.prompts,rate,6200+index,600,'cal_lower'))
                result=calibration(folder,rows);calibrations.append(dict(rate=rate,**result))
                if result['stable']:stable.append(rate)
                else:break
        atomic(h.root/'capacity_calibration.json',calibrations)
        if len(set(stable))<2:raise RuntimeError('Two stable distinct load profiles not established; retain calibration and stop')
        medium=min(stable);high=max(stable)
        if args.sender_acceptance:
            async def snapshot_stress():
                expected=h.root/'sender_acceptance_01'
                while h.current!=expected or not (expected/'timeline.json').exists():await asyncio.sleep(.05)
                for k in range(24):
                    await h.require(k%2,'snapshot');await asyncio.sleep(.1)
            stress=asyncio.create_task(snapshot_stress())
            await h.round('sender_acceptance_01','A',poisson(h.prompts,high,6400,60,'sender_acceptance'),duration=60)
            await stress
        atomic(h.root/'frozen_pretrial.json',dict(loads=dict(medium=medium,high=high),policy=asdict(h.cfg),
            parameters_fixed=True,seeds=[7101,7102],formal_seeds=[8101,8102,8103,8104,8105],
            duration_s=600,tail_s=600,capacity_boundary_observed=any(not x['stable'] for x in calibrations)))
        order=[]
        for load,rate,seed,schemes in [('medium',medium,7101,'ABC'),('high',high,7102,'BCA')]:
            trace=poisson(h.prompts,rate,seed,600,'pre_'+load)
            atomic(h.root/'inputs'/f'pre_{load}.json',trace)
            for mode in schemes:order.append(dict(name=f'pre_{load}_{mode}',load=load,rate=rate,seed=seed,mode=mode))
        atomic(h.root/'pretrial_order.json',order)
        for row in order:
            trace=json.loads((h.root/'inputs'/f"pre_{row['load']}.json").read_text())
            await h.round(row['name'],row['mode'],trace)
        from pretrial_gate import evaluate
        gate=evaluate(h.root)
        if gate['decision']=='invalid':raise RuntimeError('Pretrial evidence invalid: '+str(gate['reasons']))
        if gate['decision']=='formal':
            # The same warm model processes continue; no intervening unload or
            # native teardown, and no tuning from formal performance results.
            source=h.root/'execution_sources'
            atomic(h.root/'formal_frozen_code.json',{f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in source.glob('*.py')})
            formal_order=[];permutations=['ABC','BCA','CAB','ACB','CBA','BAC']
            for k,seed in enumerate([8101,8102,8103,8104,8105]):
                for load in (['medium','high'] if k%2==0 else ['high','medium']):
                    rate=medium if load=='medium' else high
                    path=h.root/'inputs'/f'formal_{load}_s{seed}.json'
                    atomic(path,poisson(h.prompts,rate,seed,600,'formal_'+load))
                    for mode in permutations[(k+(load=='high'))%len(permutations)]:
                        formal_order.append(dict(name=f'formal_{load}_s{seed}_{mode}',mode=mode,load=load,seed=seed,rate=rate,trace=str(path)))
            assert len(formal_order)==30;atomic(h.root/'formal_order.json',formal_order)
            h.beat('formal_frozen',planned_rounds=30)
            for row in formal_order:await h.round(row['name'],row['mode'],json.loads(Path(row['trace']).read_text()))
            h.beat('formal_measurements_complete',planned_rounds=30)
        else:
            h.beat('pretrials_complete_no_running_actions',reason='First deliver observed imbalance/candidate/capacity evidence; no automatic search for favorable parameters')
        await h.gateway_runner.cleanup();await h.proxy_session.close()
        await h.cleanup(h.service)
    except BaseException as error:
        atomic(h.root/'run_failure.json',dict(error=repr(error),traceback=traceback.format_exc(),wall_ns=time.time_ns(),
            states=[h.state(i) for i in range(2)],owned=h.identities,preserve_scene=True))
        h.beat('blocked',error=repr(error));raise

if __name__=='__main__':asyncio.run(main())
