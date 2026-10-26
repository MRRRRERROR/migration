"""Chinese service-alive acceptance report; retain raw and strict counters."""
import argparse
import collections
import csv
import html
import json
from pathlib import Path
import re
import statistics
import math

def load(path):
    return [json.loads(line) for line in path.open() if line.strip()] if path.exists() else []
def obj(path):return json.loads(path.read_text()) if path.exists() else None
def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def stats(x):
    x=sorted(v for v in x if v is not None)
    return dict(n=len(x),mean=statistics.mean(x) if x else None,p95=x[int(.95*(len(x)-1))] if x else None,max=max(x) if x else None)
def mean_window(rows,start,end,role):
    pts=sorted((float(r['monotonic_s']),float(r['power_w'])) for r in rows if r['role']==role and r['power_w'] and start<=float(r['monotonic_s'])<end)
    if len(pts)<3 or pts[0][0]-start>.4 or end-pts[-1][0]>.4 or any(b[0]-a[0]>.6 for a,b in zip(pts,pts[1:])):return None
    return statistics.mean(v for _,v in pts)
def energy(rows,start,end,role):
    pts=sorted((float(r['monotonic_s']),float(r['power_w'])) for r in rows if r['role']==role and r['power_w'])
    total=covered=0.
    for (a,p),(b,q) in zip(pts,pts[1:]):
        lo,hi=max(a,start),min(b,end)
        if hi<=lo or b-a>.6:continue
        v=p+(q-p)*(lo-a)/(b-a);w=p+(q-p)*(hi-a)/(b-a)
        total+=(v+w)/2*(hi-lo);covered+=hi-lo
    return dict(joules=total if covered>=.98*(end-start) else None,covered_s=covered,requested_s=end-start)

def power(rows,t0,start,cutoff,measurement_end=None):
    result=dict(P0_w=mean_window(rows,t0-60,t0,'S'),points={},thresholds={},energy={})
    p0=result['P0_w']
    for offset in (1,5,10,30,60,120):
        val=mean_window(rows,t0+offset-1,t0+offset,'S')
        result['points'][str(offset)]=dict(source_w=val,drop_w=p0-val if p0 is not None and val is not None else None,
             drop_pct=100*(p0-val)/p0 if p0 and val is not None else None)
    for ratio in (.1,.25,.5):
        count=0;found=None
        for i in range(1,int((measurement_end or cutoff)-t0)+1):
            val=mean_window(rows,t0+i-1,t0+i,'S')
            count=count+1 if p0 and val is not None and p0-val>=p0*ratio else 0
            if count==5:found=dict(first_window_start_s=i-5,confirmed_s=i);break
        result['thresholds'][str(ratio)]=found
    for label,end,begin in (('post60',t0+60,t0),('post120',t0+120,t0),('sending',cutoff,start)):
        item={r:energy(rows,begin,end,r) for r in ('S','D')}
        item['sum_joules']=sum(item[r]['joules'] for r in ('S','D')) if all(item[r]['joules'] is not None for r in ('S','D')) else None
        result['energy'][label]=item
    return result

def first_difference(a,b):
    return next((i for i,(x,y) in enumerate(zip(a,b)) if x!=y),min(len(a),len(b)) if len(a)!=len(b) else None)

def analyze(folder):
    summary=obj(folder/'summary.json');timeline=obj(folder/'measurement_cutoff.json') or obj(folder/'timeline.json') or {}
    clients=load(folder/'request_results.jsonl');events=load(folder/'token_events.jsonl');kv=load(folder/'kv_events.jsonl')
    samples=load(folder/'instance_samples.jsonl');raw='\n'.join(p.read_text(errors='replace') for p in folder.glob('vllm_*.log'))
    runtime='\n'.join(l for l in raw.split('\n') if ' prompt: ' not in l and '[entrypoints/logger.py:' not in l)
    try:
        import run_migration_policy_baselines as historic
        strict=historic.count_patterns(folder)
    except Exception as exc:strict=dict(unavailable=repr(exc))
    strict_keys=('abort_count','KVTResp_code_404_count','KVTResp_code_500_count','EngineCore_fatal_count','Traceback_count')
    raw_counts=dict(kvt404=raw.count('KVTResp(code=404'),kvt500=raw.count('KVTResp(code=500'),
        tracebacks=raw.count('Traceback (most recent call last)'),abort_lines=sum('abort req' in l.lower() or 'AbortReq' in l for l in raw.split('\n')))
    sequences=collections.defaultdict(list);locations=collections.defaultdict(list)
    core=sorted([r for p in folder.glob('outputs_*.jsonl') for r in load(p)],key=lambda x:x['time_ns'])
    for row in core:
        if row['tokens']:sequences[row['request_id']].extend(row['tokens']);locations[row['request_id']].append(dict(pid=row['pid'],time_ns=row['time_ns']))
    semantic=[]
    for row in clients:
        rid=(row.get('response_id') or '')+'-0';seq=sequences.get(rid,[]);tokens=row.get('token_ids',[])
        semantic.append(dict(request_id=row['request_id'],engine_request_id=rid,completed=row.get('completed'),
            core_api_first_mismatch=first_difference(seq,tokens),core_length=len(seq),api_length=len(tokens),budget_ok=len(tokens)<=4096,
            sse_done=row.get('sse_done'),finish_reason=row.get('finish_reason'),error=row.get('error')))
    attempts=[];groups=collections.defaultdict(list)
    for event in events:
        if event.get('attempt_id'):groups[event['attempt_id']].append(event)
    for key,rows in groups.items():
        rows.sort(key=lambda x:x['monotonic_s']);names=[e['event'] for e in rows]
        taken=next((e for e in rows if e['event']=='taken_over'),None)
        state='completed' if 'target_completed' in names else 'taken_over' if taken else 'natural_completion_or_cancel' if 'source_completed_or_cancelled' in names else 'timeout' if any('timeout' in str(e).lower() for e in rows if e['event']=='migration_failed') else 'cancelled' if 'user_cancel' in names else 'failed' if 'migration_failed' in names else 'unclassified'
        ready=[e for e in rows if e['event']=='target_ready'];pause=[e for e in rows if e['event']=='source_paused']
        record=dict(attempt_id=key,request_id=rows[0]['request_id'],requested_mode=rows[0]['requested_mode'],executed_mode=rows[0]['executed_mode'],state=state,
            token_payload_bytes=sum(e.get('token_payload_bytes',0) for e in rows if e['event']=='wire_send'),kv_payload_bytes=0,
            control_application_frame_bytes=sum(e.get('control_bytes',0) for e in rows if e['event'] in ('wire_send','wire_receive')),
            application_frame_count_complete=not any(e['event']=='wire_send_incomplete' for e in rows),
            additional_zmq_route_control_bytes=None,network_headers_bytes=None,
            background_rounds=names.count('background_round'),snapshots=[e for e in rows if e['event']=='snapshot'],
            initial_rebuild_tokens=ready[-1].get('initial_rebuild_tokens') if ready else None,
            incremental_recompute_tokens=ready[-1].get('incremental_recompute_tokens') if ready else None,
            recompute_active_s=ready[-1].get('recompute_active_s') if ready else None,
            queue_s=next((e.get('queue_s') for e in rows if e['event']=='replay_started'),None),
            final_pause_monotonic_s=pause[-1]['monotonic_s'] if pause else None,
            takeover_monotonic_s=taken['monotonic_s'] if taken else None,
            first_new_token_monotonic_s=next((e['monotonic_s'] for e in rows if e['event']=='target_first_new_token'),None),
            source_release_monotonic_s=next((e['monotonic_s'] for e in rows if e['event']=='source_released'),None),
            events=rows)
        attempts.append(record)
    # Native summaries are batched and per-layer; never attribute the full batch to each request.
    native_bytes=[]
    for path in folder.glob('vllm_*.log'):
        lines=path.read_text(errors='replace').split('\n')
        members=collections.defaultdict(list)
        for line in lines:
            m=re.search(r'StepIdx=([^,]+),State=([12]),FinishedReq=([^,\s]+)',line)
            if m:members[m[1]].append(dict(request_id=m[3],native_state=m[2]))
        for line in lines:
            if 'SendStubMetrics' not in line:continue
            fields=dict(re.findall(r'(\w+)=([^,\s]+)',line));step=fields.get('StepIdx')
            if 'SbSizeTotal' not in fields or 'InflyWrite' not in fields:continue
            per_layer=int(fields['SbSizeTotal']);layers=int(fields['InflyWrite'])
            frame_sizes=set()
            for init in lines:
                if 'TCPChannel::do_init: alloc cuda host buffer' not in init:continue
                details=dict(re.findall(r'(\w+)=(\d+)',init))
                if (details.get('tensor_bytes')==str(per_layer) and details.get('dst_layer_num')==str(layers)
                    and details.get('metadata_size')==fields.get('MergedSbNum')):
                    frame_sizes.add((int(details['header_bytes'])+int(details['metadata_bytes']))*layers)
            native_bytes.append(dict(step=step,request_members=members[step],sb_size_total_per_layer=per_layer,
                completed_layer_writes=layers,batch_kv_tensor_payload_bytes=per_layer*layers,
                kv_data_packet_frame_control_bytes=next(iter(frame_sizes)) if len(frame_sizes)==1 else None,
                per_request_payload_bytes=None,control_bytes=None,network_headers_bytes=None,
                basis='Same cached transfer plan per layer; conversion disabled; completed native layer futures. No proportional per-request allocation.',
                evidence=line,source_log=path.name))
    t0=timeline.get('t0_monotonic_s');metrics=None;drain={};inflight=[];power_quality={}
    if t0:
        snap=obj(folder/'t0_snapshot.json') or {};inflight=(snap.get('S') or {}).get('requests',[])
        after=[s for s in samples if s['monotonic_s']>=t0 and s.get('S')]
        empty=next((s for s in after if not s['S']['requests']),None)
        computable_empty=next((s for s in after if not any(not r.get('paused',False) for r in s['S']['requests'])),None)
        drain['source_compute_requests_empty_s']=computable_empty['monotonic_s']-t0 if computable_empty else None
        drain['source_request_state_empty_s']=empty['monotonic_s']-t0 if empty else None
        last=max((s['S'].get('last_compute_monotonic_s') or 0 for s in samples if s.get('S')),default=0)
        drain['last_source_compute_s']=last-t0 if last else None
        ends=[r.get('finish_monotonic_s') for r in clients if r.get('route_role')=='S']
        drain['last_original_source_api_stream_end_s']=max(ends)-t0 if ends and all(x is not None for x in ends) else None
        if (folder/'power.csv').exists():
            with (folder/'power.csv').open() as f:readings=list(csv.DictReader(f))
            for role in 'SD':
                rr=[r for r in readings if r['role']==role];ts=sorted(float(r['monotonic_s']) for r in rr)
                power_quality[role]=dict(samples=len(rr),uuids=sorted({r['gpu_uuid'] for r in rr}),
                    memory_api=sorted({r['nvml_memory_api'] for r in rr}),missing_power=sum(not r['power_w'] for r in rr),
                    samples_without_compute_process=sum(not json.loads(r['compute_pids']) for r in rr),
                    used_memory_mib=stats([float(r['memory_mib']) for r in rr if r['memory_mib']]),
                    max_read_interval_s=max((b-a for a,b in zip(ts,ts[1:])),default=None),
                    gaps_over_600ms=sum(b-a>.6 for a,b in zip(ts,ts[1:])),
                    interval_s=stats([b-a for a,b in zip(ts,ts[1:])]))
            if timeline.get('send_cutoff_monotonic_s') is not None:
                metrics=power(readings,t0,timeline['start_monotonic_s'],timeline['send_cutoff_monotonic_s'],timeline.get('measurement_cutoff_monotonic_s'))
    class_metrics={}
    for kind,predicate in (
        ('背景请求',lambda r:r['initial_role']=='D'),
        ('重定向新请求',lambda r:r['initial_role']=='S' and r.get('route_role')=='D'),
        ('源API旧请求',lambda r:r.get('route_role')=='S')):
        selected=[r for r in clients if predicate(r)]
        class_metrics[kind]=dict(count=len(selected),ttft_s=stats([r.get('ttft_s') for r in selected]),completion_s=stats([r.get('e2e_s') for r in selected]))
    client_by_engine={(r.get('response_id') or '')+'-0':r for r in clients}
    for attempt in attempts:
        client=client_by_engine.get(attempt['request_id']);pause=attempt['final_pause_monotonic_s']
        if attempt['state']=='natural_completion_or_cancel':
            attempt['state']='natural_completion' if client and client.get('completed') else 'unclassified'
        gaps=[]
        if client and pause:
            chunks=client['chunks'];gaps=[b['time']-a['time'] for a,b in zip(chunks,chunks[1:]) if a['time']<=pause<=b['time']]
        attempt['client_switch_chunk_gap_s']=gaps[0] if gaps else None
    kv_attempts=[];native_trace=load(folder/'native_attempt_trace.jsonl')
    for begin in (e for e in kv if e.get('event_name')=='migration_initiated'):
        rid=begin['request_id'];related=[e for e in kv if e.get('request_id')==rid]
        native=[e for e in native_trace if e.get('request_id')==rid]
        client=client_by_engine.get(rid);names={e['event_name'] for e in related}
        def event_time(name):return next((e.get('timestamp_ns') for e in related if e['event_name']==name),None)
        pause=event_time('final_suspend_begin')
        pause_mono=(pause-timeline['start_wall_ns'])/1e9+timeline['start_monotonic_s'] if pause else None
        gaps=[b['time']-a['time'] for a,b in zip(client['chunks'],client['chunks'][1:]) if pause_mono is not None and a['time']<=pause_mono<=b['time']] if client else []
        state='completed' if 'dst_resume' in names and client and client.get('completed') else 'taken_over' if 'dst_resume' in names else 'natural_completion' if 'migration_attempt_cancelled' in names and client and client.get('completed') else 'cancelled' if 'migration_attempt_cancelled' in names else 'unclassified'
        kv_attempts.append(dict(request_id=rid,attempt_ids=sorted({e['attempt_id'] for e in native if e.get('attempt_id')}),
            requested_mode=begin.get('requested_mode'),executed_mode=begin.get('executed_mode'),state=state,
            input_tokens=begin.get('prompt_len'),source_computed_tokens=next((e.get('computed_tokens') for e in native if e.get('stage')=='PREQMETA_CREATED'),None),
            source_generated_tokens=next((e.get('live_output_tokens') for e in native if e.get('stage')=='PREQMETA_CREATED'),None),
            per_request_kv_payload_bytes=None,token_payload_bytes=None,control_bytes=None,
            native_batch_steps=[b['step'] for b in native_bytes if rid in {x['request_id'] for x in b['request_members']}],
            final_pause_timestamp_ns=pause,takeover_timestamp_ns=event_time('dst_resume'),source_release_timestamp_ns=event_time('source_release'),
            client_switch_chunk_gap_s=gaps[0] if gaps else None,raw_404_lines=[l for l in runtime.split('\n') if rid in l and 'KVTResp(code=404' in l],
            raw_abort_lines=[l for l in runtime.split('\n') if rid in l and ('abort req' in l.lower() or 'AbortReq' in l)],
            events=related,native_evidence=native))
    source_pids={json.loads(p.read_text())['pid'] for p in (folder/'state_S').glob('*.json')}
    target_pids={json.loads(p.read_text())['pid'] for p in (folder/'state_D').glob('*.json')}
    ownership=[]
    for rid,rows in locations.items():
        src=[x['time_ns'] for x in rows if x['pid'] in source_pids];dst=[x['time_ns'] for x in rows if x['pid'] in target_pids]
        if src and dst:ownership.append(dict(request_id=rid,last_source_output_ns=max(src),first_target_output_ns=min(dst),source_after_target=max(src)>min(dst)))
    exit_evidence=[l for l in runtime.split('\n') if re.search(r'AssertFailed|Segfault encountered|Fatal Python error',l)]
    status=dict(summary=summary,timing=timeline,inflight_count=len(inflight),inflight=inflight,
        t0_source_running=sum(r['state']=='running' for r in inflight),t0_source_waiting=sum(r['state']=='waiting' for r in inflight),
        t0_source_decoding=sum(r['generated_tokens']>0 for r in inflight),
        generated_history_distribution=stats([r['generated_tokens'] for r in inflight]),
        real_token_takeovers=sum(e['event']=='taken_over' for e in events),
        real_kv_resume_events=sum(e.get('event_name') in ('dst_resume','migration_done') for e in kv),
        unique_kv_takeovers=len({e.get('request_id') for e in kv if e.get('event_name') in ('dst_resume','migration_done') and e.get('request_id')}),
        raw_counts=raw_counts,original_strict_counters=strict,
        original_strict_validity='unavailable' if 'unavailable' in strict else 'invalid' if any(strict.get(k,0) for k in strict_keys) else 'valid',
        runtime_kvt500=runtime.count('KVTResp(code=500'),client_semantics=semantic,attempts=attempts,
        token_attempt_states=dict(collections.Counter(a['state'] for a in attempts)),kv_events=kv,native_kv_bytes=native_bytes,
        kv_attempts=kv_attempts,kv_attempt_states=dict(collections.Counter(a['state'] for a in kv_attempts)),output_ownership=ownership,
        power=metrics,power_quality=power_quality,drain=drain,performance_by_class=class_metrics,
        migration_selection_order=[dict(request_id=a['request_id'],attempt_id=a.get('attempt_id') or a.get('attempt_ids')) for a in attempts+kv_attempts],
        exit_fault=obj(folder/'exit_fault.json') or (dict(native_log_evidence=exit_evidence,measurement_critical_list=timeline.get('critical'),
                  scope='Check raw shutdown timestamps; exit evidence retained independently of measurement') if exit_evidence else None))
    guard=obj(folder/'semantic_guard.json')
    status['functional_checks']=dict(
        all_planned_sent=bool(summary and summary['sent']==summary['records']),
        all_requests_complete=bool(summary and summary['completed']==summary['sent'] and not summary['errors']),
        core_api_budget_sse=all(r['core_api_first_mismatch'] is None and r['budget_ok'] and r['sse_done'] for r in semantic),
        no_migration_reference=guard is not None and not guard['errors'],
        resources_restored=bool(summary and summary['cleanup_safe'] and summary['kv_restored']),
        measurement_uncontaminated=bool(summary and not summary['critical']),
        no_source_output_after_target=not any(x['source_after_target'] for x in ownership),
        power_points_complete=bool(metrics and metrics['P0_w'] is not None and all(p['source_w'] is not None for p in metrics['points'].values())))
    status['functional_checks']['power_energy_complete']=bool(metrics and all(v['sum_joules'] is not None for v in metrics['energy'].values()))
    status['functional_checks']['model_processes_alive']=bool(power_quality and all(x['samples_without_compute_process']==0 for x in power_quality.values()))
    status['migration_outcome_counts']={name:sum(a['state']==name for a in attempts+kv_attempts) for name in ('completed','taken_over','natural_completion','cancelled','failed','timeout','unclassified')}
    save(folder/'analysis.json',status);save(folder/'migration_attempt_metrics.json',attempts+kv_attempts)
    return status

def fmt(x):
    if x is None:return '缺失'
    if isinstance(x,bool):return '是' if x else '否'
    if isinstance(x,float):return f'{x:.3f}'
    if isinstance(x,dict):return '；'.join(str(k)+'='+fmt(v) for k,v in x.items())
    if isinstance(x,(list,tuple)):return ' / '.join(fmt(v) for v in x)
    return str(x)

def table(headers,rows):
    return '<table><thead><tr>'+''.join('<th>'+html.escape(str(x))+'</th>' for x in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(fmt(x))+'</td>' for x in row)+'</tr>' for row in rows)+'</tbody></table>'

def report(root,reuse_analysis=False):
    rounds={p.name:(obj(p/'analysis.json') if reuse_analysis and (p/'analysis.json').exists() else analyze(p)) for p in root.iterdir() if p.is_dir() and (p/'round_config.json').exists()}
    refs={}
    for scope in ('main','coverage'):
        baseline=load(root/(scope+'_B')/'request_results.jsonl');byid={r['request_id']:r for r in baseline};comparisons=[]
        for mode in 'CDE':
            for row in load(root/(scope+'_'+mode)/'request_results.jsonl'):
                b=byid.get(row['request_id'])
                if b and b.get('completed') and row.get('completed'):
                    mismatch=first_difference(b['token_ids'],row['token_ids'])
                    comparisons.append(dict(mode=mode,request_id=row['request_id'],first_token_mismatch=mismatch,
                        baseline_length=len(b['token_ids']),length=len(row['token_ids']),text_equal=b['text']==row['text'],finish_equal=b['finish_reason']==row['finish_reason'],
                        baseline_token=b['token_ids'][mismatch] if mismatch is not None and mismatch<len(b['token_ids']) else None,
                        actual_token=row['token_ids'][mismatch] if mismatch is not None and mismatch<len(row['token_ids']) else None))
        refs[scope]=comparisons
    save(root/'no_migration_comparisons.json',refs)
    state=obj(root/'independent_acceptance_state.json') or {}
    state['status']='blocked' if obj(root/'blocked.json') else 'measurements_completed_pending_review' if obj(root/'completed.json') else 'running'
    audit=obj(root/'final_acceptance_audit.json')
    if audit and all(audit.get(k) for k in ('selected_gpus_idle','old_protections_unchanged','all_replays_identical','all_request_input_hashes_match','all_functional_checks_passed','no_owned_services_remaining','native_hashes_consistent')):
        state['status']='functional_acceptance_passed_with_coverage_and_telemetry_limits'
    state['normal_unload_qualified']=False
    save(root/'independent_acceptance_state.json',state)
    config=obj(root/'effective_common_config.json') or {};frozen=obj(root/'frozen_load.json') or {};workload=obj(root/'workload_manifest.json') or {}
    pieces=['<!doctype html><meta charset="utf-8"><title>两卡服务存活排空验收</title><style>body{font:16px system-ui;max-width:1250px;margin:36px auto;color:#192435;padding:0 24px}table{border-collapse:collapse;width:100%;margin:18px 0}td,th{border:1px solid #d4dce6;padding:9px;text-align:left}th{background:#eaf0f6}img{max-width:100%}pre{white-space:pre-wrap}a{color:#1765a3}</style>',
        '<h1>两卡服务存活时的排空与功率响应验收</h1>',
        '<p>本报告测量两张本地GPU的服务存活排空。测量期间模型和两端API/引擎保持存活；测量截止后的退出功率不计入收益。旧冻结撤销保护保留，本次独立验收不授予正常卸载资格。没有执行正式五种子25轮实验。</p>',
        '<h2>有效配置与版本</h2>',table(['项目','值'],[['交付基线','delivery_20261009_route_01'],['源/目标物理GPU',config.get('gpus')],['模型',config.get('model')],['精度',config.get('dtype')],['上下文上限/输出预算',[config.get('context_length'),config.get('max_tokens')]],['temperature / n',[0,1]],['批次不变性',config.get('batch_invariant')],['异步调度',config.get('async_scheduling')],['忽略EOS',config.get('ignore_eos')],['故障注入',config.get('fault_injection')],['token_live后台轮数上限/差距/期限秒',[3,16,30]],['方案顺序',obj(root/'scheme_order.json')],['源/背景到达率请求每秒',[frozen.get('source_rate'),frozen.get('background_rate')]],['主轨迹种子/发送秒/t0秒',[101,600,300]]]),
        '<p>物理GPU 1、3沿用本聊天此前“任意两张空闲卡”的明确授权。完整输入来源、去重及模板后长度分布见 <a href="workload_manifest.json">请求池清单</a>；原始文本与模型模板生成的token IDs固定复用。运行文件及哈希见 <a href="version_manifest.json">版本清单</a>，每轮 launch.jsonl 保存最终启动命令，native_loaded.json 保存实际加载库。</p>',
        '<h2>请求池与固定回放</h2>',table(['指标','值'],[['有效去重提示池',workload.get('pool_size')],['2048～4096 token提示数',workload.get('long_pool_size')],['模板后输入长度最小/中位数/p95/最大',[workload.get('length_distribution',{}).get(k) for k in ('min','median','p95','max')]],['主轨迹请求数',len(obj(root/'frozen_main_trace.json') or [])],['固定覆盖源请求数',16],['原始数据文件',workload.get('source_file')],['原始数据SHA256',workload.get('source_sha256')]]),
        '<p>取ShareGPT原始首条human文本，按去除首尾空白后的文本SHA256去重，使用本模型实际聊天模板恰好一次。提示从合格池中有放回均匀抽样；源与背景为独立开环泊松到达。模型输入采用已生成token IDs，无二次聊天模板。来源沿用项目保存的原始数据文件，缺少可核实的首次下载URL，不补造来源链接。</p>',
        '<h2>容量校准</h2>']
    cal=obj(root/'capacity_calibration.json') or []
    pieces.append(table(['候选','源/背景请求每秒','队列斜率','早/晚队列均值','晚段完成率','抢占','稳定'],[
        [c['candidate'],[c['source_rate'],c['background_rate']],c['capacity'].get('queue_slope_per_s'),[c['capacity'].get('early_queue_mean'),c['capacity'].get('late_queue_mean')],c['capacity'].get('late_completion_rate'),c['capacity'].get('preemptions'),c['capacity']['stable']] for c in cal]))
    pieces.append('<p>稳定判定同时检查队列增长、完成率及早晚TTFT/完成延迟p95，不以最终全部完成替代容量判定。完整判据及每秒队列在校准原始记录中。</p>')
    pieces.append('<h2>主验收对照</h2>')
    comparison=[]
    for mode in 'BCDE':
        v=rounds.get('main_'+mode);p=(v or {}).get('power')
        if not v or not p:continue
        threshold=p['thresholds']['0.5']
        comparison.append([{'B':'B 自然排空','C':'C KV迁移','D':'D token_stop','E':'E token_live'}[mode],
            v['drain'].get('source_compute_requests_empty_s'),p['P0_w'],p['points']['10']['source_w'],
            [threshold['first_window_start_s'],threshold['confirmed_s']] if threshold else '未达标',
            p['energy']['post120']['sum_joules']/1000 if p['energy']['post120']['sum_joules'] is not None else None,
            p['energy']['sending']['sum_joules']/1000 if p['energy']['sending']['sum_joules'] is not None else None])
    pieces.append(table(['方案','源计算请求排空秒','P0 W','t0后10秒源功率W','下降50%首次/确认秒','t0后120秒两卡能耗kJ','发送窗口两卡能耗kJ'],comparison))
    pieces.append('<p>上述数据是单次固定轨迹的本地观察。各轮P0、发送滞后和调度瞬时状态有差异；不依据这一轮排序宣称某模式在生产环境更优。源计算排空后原API连接仍可能持续转发目标输出，连接结束时间在分项表中另列。</p>')
    figures=[]
    try:
        import matplotlib;matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        for scope in ('main','coverage'):
            fig,axes=plt.subplots(3,1,figsize=(12,9),sharex=True)
            for mode in 'BCDE':
                path=root/(scope+'_'+mode);timeline=obj(path/'timeline.json') or {};t0=timeline.get('t0_monotonic_s')
                if not t0 or not (path/'power.csv').exists():continue
                with (path/'power.csv').open() as f:rows=list(csv.DictReader(f))
                bins=collections.defaultdict(lambda:collections.defaultdict(list))
                for row in rows:
                    offset=math.floor(float(row['monotonic_s'])-t0)
                    if row['power_w']:bins[offset][row['role']].append(float(row['power_w']))
                for axis,role in zip(axes,('S','D','total')):
                    pts=[]
                    for offset,item in sorted(bins.items()):
                        if role=='total':
                            if item['S'] and item['D']:pts.append((offset,sum(statistics.mean(item[r]) for r in ('S','D'))))
                        elif item[role]:pts.append((offset,statistics.mean(item[role])))
                    axis.plot([x for x,y in pts],[y for x,y in pts],label=mode,lw=1)
                    axis.set_ylabel(role+' power (W)');axis.axvline(0,color='grey',ls='--');axis.grid(alpha=.2)
            axes[0].legend();axes[-1].set_xlabel('Seconds from actual control action t0');fig.tight_layout()
            output=root/(scope+'_power.png');fig.savefig(output,dpi=150);plt.close(fig);figures.append(output.name)
    except Exception as exc:
        figures=[name for name in ('main_power.png','coverage_power.png') if (root/name).exists()]
        if not figures:pieces.append('<p>图表待在本地CPU绘制：'+html.escape(repr(exc))+'</p>')
    for scope,title in (('main','主验收'),('coverage','固定长历史覆盖')):
        pieces.append('<h2>'+title+'</h2>')
        if scope=='coverage':pieces.append('<p>16条固定长输入同时提交，至少8条已开始decode时触发，最长等待60秒，背景按冻结速率运行180秒。提交前另采65秒存活服务的闲置功率，保证完整P0窗口。实际P0仍取t0前60秒，以闲置为主并包含提交到触发之间的prefill/decode，与主验收持续负载基线不同，功率下降比例不混合解释。</p>')
        pieces.append(table(['方案','发送/完成','t0在途','真实token接管','KV接管请求数','资源安全/恢复','旧strict validity','退出故障'],[
            [m,f"{(rounds.get(scope+'_'+m,{}).get('summary') or {}).get('sent','缺失')}/{(rounds.get(scope+'_'+m,{}).get('summary') or {}).get('completed','缺失')}",
             rounds.get(scope+'_'+m,{}).get('inflight_count'),rounds.get(scope+'_'+m,{}).get('real_token_takeovers'),rounds.get(scope+'_'+m,{}).get('unique_kv_takeovers'),
             f"{(rounds.get(scope+'_'+m,{}).get('summary') or {}).get('cleanup_safe')}/{(rounds.get(scope+'_'+m,{}).get('summary') or {}).get('kv_restored')}",
             rounds.get(scope+'_'+m,{}).get('original_strict_validity'),bool(rounds.get(scope+'_'+m,{}).get('exit_fault'))] for m in 'BCDE']))
        if scope+'_power.png' in figures:pieces.append('<img src="'+scope+'_power.png" alt="源、目标及两卡合计功率对齐曲线">')
        for mode in 'BCDE':
            value=rounds.get(scope+'_'+mode)
            if not value:continue
            pieces.append('<h3>方案 '+mode+'</h3>')
            pieces.append(table(['t0源状态','数量'],[['运行中',value['t0_source_running']],['等待中',value['t0_source_waiting']],['已产生生成token',value['t0_source_decoding']]]))
            labels={'all_planned_sent':'计划请求均发送','all_requests_complete':'请求全部完整结束','core_api_budget_sse':'Core/API序列、预算与SSE','no_migration_reference':'无迁移参照一致','resources_restored':'迁移资源恢复','measurement_uncontaminated':'测量无关键故障','power_points_complete':'规定功率点完整','power_energy_complete':'双卡规定能耗窗口完整','no_source_output_after_target':'目标首个输出后源端无新输出','model_processes_alive':'采集期间双卡模型进程存活'}
            pieces.append(table(['功能检查','状态'],[[labels[k],'通过' if v else '未通过或缺失'] for k,v in value['functional_checks'].items()]))
            pieces.append(table(['原始日志计数','次数'],[['KVT 404',value['raw_counts']['kvt404']],['KVT 500',value['raw_counts']['kvt500']],['abort行',value['raw_counts']['abort_lines']],['Traceback',value['raw_counts']['tracebacks']]]))
            if value['raw_counts']['kvt404'] or value['raw_counts']['abort_lines']:
                pieces.append('<p>原始404和abort行不删除；旧strict可能据此标为invalid。逐请求证据将迁移取消、源自然完成、接管后资源释放与真正的客户端错误区分。分项验收通过不改写旧strict结果。</p>')
            exit_audit=obj(root/(scope+'_'+mode)/'exit_phase_audit.json')
            if exit_audit:
                pieces.append('<p>退出故障：native日志出现Segfault标记，API父进程返回0不能据此认定正常卸载。相邻native退出事件已晚于测量截止，功率采集在进程收尾前结束。详见 <a href="'+scope+'_'+mode+'/exit_phase_audit.json">退出时序证据</a>；本次不继续Barex修复。</p>')
                pieces.append(table(['退出时序指标','值'],[['测量截止后至相邻native退出事件秒',[e['preceding_shutdown_after_cutoff_s'] for e in exit_audit['events']]],['精确崩溃时刻','未记录'],['正常卸载资格',False]]))
            real=value['unique_kv_takeovers'] if mode=='C' else value['real_token_takeovers']
            if mode!='B':pieces.append('<p>真实接管覆盖：'+('已覆盖' if real else '不足')+'；后台追赶轮数（不含初始重建与最终补齐）：'+fmt([a['background_rounds'] for a in value['attempts']])+'。</p>')
            switch_gaps=stats([a.get('client_switch_chunk_gap_s') for a in value['attempts']+value['kv_attempts']])
            pieces.append(table(['迁移输出指标','值'],[['切换chunk间隔样本数',switch_gaps['n']],['切换chunk间隔均值/p95/最大秒',[switch_gaps['mean'],switch_gaps['p95'],switch_gaps['max']]]]))
            p=value['power']
            if p:
                pieces.append(table(['时间秒','源功率W','下降W','下降%'],[[i,v['source_w'],v['drop_w'],v['drop_pct']] for i,v in p['points'].items()]))
                pieces.append(table(['指标','值/状态'],[['P0 W',p['P0_w']],*[[f'下降{float(k)*100:g}%首次/确认秒',v or '未达标'] for k,v in p['thresholds'].items()],
                    *[[{'post60':'t0后60秒','post120':'t0后120秒','sending':'整个发送窗口'}[label]+' 能耗J（源/目标/合计）',[x['S']['joules'],x['D']['joules'],x['sum_joules']]] for label,x in p['energy'].items()],
                    *[[{'source_compute_requests_empty_s':'源计算请求排空秒','source_request_state_empty_s':'源请求状态清空秒','last_source_compute_s':'最后源模型执行步秒','last_original_source_api_stream_end_s':'最后原源API输出连接结束秒'}[k],v] for k,v in value['drain'].items()]]))
            pieces.append(table(['请求类别','数量','TTFT均值/p95秒','完成延迟均值/p95秒'],[[k,v['count'],[v['ttft_s']['mean'],v['ttft_s']['p95']],[v['completion_s']['mean'],v['completion_s']['p95']]] for k,v in value['performance_by_class'].items()]))
            pieces.append(table(['迁移终态','次数'],[[{'completed':'接管并完成','taken_over':'已接管未结束','natural_completion':'源自然完成取消副本','cancelled':'取消','failed':'失败','timeout':'超时','unclassified':'未分类'}[k],v] for k,v in value['migration_outcome_counts'].items()]))
            pieces.append('<p>历史长度分布、实际选择顺序、每次模式、字节、排队/重算/追赶、暂停/接管/源释放见 <a href="'+scope+'_'+mode+'/analysis.json">逐请求分析</a>。未发生的失败或超时不代表本负载已覆盖对应故障场景。</p>')
        mismatches=[r for r in refs[scope] if r['first_token_mismatch'] is not None or not r['text_equal'] or not r['finish_equal']]
        pieces.append('<p>与自然排空参照的可比较请求：'+str(len(refs[scope]))+'；差异：'+str(len(mismatches))+'。首个token差异保存在 no_migration_comparisons.json；如有差异，必须继续定位，不能自动解释为浮点误差。</p>')
    pieces.append('<h2>功能、覆盖与限制</h2><p>主验收和覆盖场景独立统计。没有真实接管的模式标为覆盖不足。t0在途字段来自带原始时间戳的附近调度边界快照。计算请求清空排除明确标记的token暂停持有，源请求状态完全清空另列。最后源计算时间为最后一次实际模型执行步结束的边界记录，不能等同所有异步KV复制CUDA kernel的结束时间。NVML每200ms请求读取，实际硬件更新频率未知；显存字段注明v2或fallback。客户端chunk间隔不是逐token ITL。token应用payload和控制帧分别计量；新增路由ZMQ封装字节及网络头缺失时不称为总网络流量。KV字节仅取实际native SendStubMetrics，无法关联到attempt时保持缺失。历史strict计数保留并列，不覆盖原始失败记录。</p>')
    pieces.append('<p>现有token后端按当前running请求顺序串行迁移；KV后端使用原策略并可聚合多个请求传输。token路径没有选中的等待请求仍在源端正常完成，实际选择顺序和t0状态单独保存。两种后端内部并行度与选择范围不同，不将其性能差异全部归因为状态恢复机制。TTFT取首个含文本或token IDs的SSE帧。当前KV诊断配置保留lease recheck额外D2H核查，未开启故障注入。</p>')
    pieces.append('<p>native批次每层SbSizeTotal结合已完成层写入数得到tensor payload；合批后缺少逐请求精确分摊，控制帧及网络头也有缺失，不用逻辑KV大小或平均分摊替代。四方案只有单次固定种子观察，P0本身存在差异，正式重复实验之前不做统计显著性或生产收益结论。</p>')
    blocked=obj(root/'blocked.json')
    pieces.append('<h2>验收状态与正式实验剩余条件</h2>')
    if audit:
        pieces.append(table(['最终检查','状态'],[['功能分项',audit['all_functional_checks_passed']],['相同到达轨迹',audit['all_replays_identical']],['逐请求输入哈希',audit['all_request_input_hashes_match']],['native哈希一致',audit['native_hashes_consistent']],['旧撤销保护未变',audit['old_protections_unchanged']],['本实验服务已退出',audit['no_owned_services_remaining']],['两张选定GPU空闲复核',audit['selected_gpus_idle']],['正常卸载资格',False],['正式五种子实验',False]]))
    pieces.append('<p>完整复核、运行命令和回退说明见 <a href="REPRODUCIBILITY.md">复现说明</a>；最终逐项证据见 <a href="final_acceptance_audit.json">验收审计</a>。后台多轮追赶未在本次阈值下发生；固定长输入场景触发时目标背景尚未在途；逐请求KV分摊及部分控制/网络字节缺失。这些限制必须在正式实验设计中继续说明或补齐。</p>')
    if blocked:pieces.append('<pre>'+html.escape(json.dumps(blocked,ensure_ascii=False,indent=2))+'</pre>')
    pieces.append('<p>本次结果仅支持两卡本地功率响应观察，生产规模、跨地域及全系统节能仍未验证。正式实验须在功能/所有权/清理和数据完整性通过后另行授权。</p>')
    (root/'report.html').write_text(''.join(pieces),encoding='utf-8');save(root/'analysis_index.json',{k:{x:v.get(x) for x in ('inflight_count','real_token_takeovers','real_kv_resume_events','drain','power','raw_counts')} for k,v in rounds.items()})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--reuse-analysis',action='store_true');a=p.parse_args();report(Path(a.root),a.reuse_analysis)
