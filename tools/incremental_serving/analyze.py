"""Auditable whole-request and paired-run analysis, never request-level inference."""
import argparse
from collections import Counter,defaultdict
import csv
import html
import json
import math
from pathlib import Path
import statistics
import re
import hashlib


def read(path):return json.loads(path.read_text(encoding='utf-8'))
def lines(path):
    if not path.exists():return []
    return [json.loads(l) for l in path.read_text(encoding='utf-8').split('\n') if l.strip()]
def write(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
def quantile(a,q):
    a=sorted(a);return a[math.ceil(len(a)*q)-1] if a else None
def mean(a):return statistics.mean(a) if a else None


def csvfile(path,rows):
    keys=list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(rows)


def export_round(folder):
    """Derived flat tables; leave original records, clocks and identities intact."""
    measured=read(folder/'measurement_rows.json');timeline=read(folder/'timeline.json')
    plans={r['request_id']:r for r in read(folder/'replay.json')}
    def wall(t):
        return timeline['start_wall_ns']+round((t-timeline['start_monotonic_s'])*1e9) if t is not None else None
    requests=[]
    for r in measured:
        plan=plans.get(r['request_id'],{});first=r.get('chunks',[{}])[0].get('time') if r.get('chunks') else None
        finish=r.get('finish_monotonic_s') if r.get('completed') else None
        end=r.get('finish_monotonic_s');usage=r.get('usage') or {}
        requests.append(dict(round=folder.name,request_id=r['request_id'],workload_seed=plan.get('workload_seed'),
            prompt_index=plan.get('prompt_index'),input_sha256=plan.get('input_sha256'),
            planned_offset_s=plan.get('planned_offset_s'),planned_monotonic_s=r.get('planned_monotonic_s'),
            actual_send_monotonic_s=r.get('actual_send_monotonic_s'),send_lag_s=r.get('send_lag_s'),
            planned_wall_ns=wall(r.get('planned_monotonic_s')),actual_send_wall_ns=wall(r.get('actual_send_monotonic_s')),
            initial_engine=r.get('initial_engine'),sampled_executing_engines=json.dumps(r.get('execution_engines_observed',[])),
            execution_location_basis='sampled ever_scheduled state; final execution_locations.csv uses actual emitted tokens',
            input_tokens=r.get('input_tokens',len(plan.get('input_token_ids',[]))),
            received_output_token_ids=len(r.get('token_ids',[])),completion_tokens=usage.get('completion_tokens'),
            usage_prompt_tokens=usage.get('prompt_tokens'),usage_count_matches_received_ids=usage.get('completion_tokens')==len(r.get('token_ids',[])) if usage else None,
            first_nonempty_output_block_monotonic_s=first,completed_monotonic_s=finish,client_attempt_end_monotonic_s=end,
            completed_wall_ns=wall(finish),completed=bool(r.get('completed')),unfinished=bool(r.get('unfinished')),
            e2e_s=r.get('e2e_s') if r.get('completed') else None,client_attempt_duration_s=r.get('e2e_s'),
            ttft_s=r.get('ttft_s'),client_mean_tpot_s=r.get('tpot_s'),
            max_output_block_gap_s=r.get('max_output_block_gap_s'),finish_reason=r.get('finish_reason'),
            error=r.get('error'),http_status=r.get('http_status'),output_sha256=r.get('output_sha256'),
            generation_json=json.dumps(plan.get('generation',{}),sort_keys=True)))
    csvfile(folder/'request_metrics.csv',requests)
    clients={r['request_id']:r for r in measured}
    decisions=lines(folder/'decisions.jsonl');sessions=[];decision_rows=[]
    for d in decisions:
        candidate=d.get('candidate') or {};req=candidate.get('request') or {};terminal=d.get('terminal') or {};rpc=d.get('rpc_result') or {}
        states=d.get('states') or []
        decision_rows.append(dict(monotonic_s=d['monotonic_s'],mode=d.get('mode'),action=d.get('action'),reason=d.get('reason'),
            waiting_reason=d.get('waiting_reason'),source=candidate.get('source'),destination=candidate.get('destination'),
            request_id=req.get('request_id'),logical_id=req.get('logical_id'),persistent_checks=d.get('persistent_checks'),
            engine0_running=states[0].get('running') if len(states)>0 else None,
            engine1_running=states[1].get('running') if len(states)>1 else None,
            engine0_waiting=states[0].get('waiting') if len(states)>0 else None,
            engine1_waiting=states[1].get('waiting') if len(states)>1 else None,
            engine0_kv_usage=states[0].get('kv_usage') if len(states)>0 else None,
            engine1_kv_usage=states[1].get('kv_usage') if len(states)>1 else None,
            result=terminal.get('classification'),source_lease_balanced=terminal.get('source_lease_balanced')))
        if d.get('action')!='running_kv_migration':continue
        events=terminal.get('events',[]);native=[e for e in events if e.get('event_name')=='migration_done']
        start=rpc.get('started',d['monotonic_s']);end=terminal.get('confirmed_monotonic_s')
        client=clients.get(req.get('logical_id'),{})
        sessions.append(dict(round=folder.name,session_index=len(sessions)+1,request_id=req.get('request_id'),logical_id=req.get('logical_id'),
            attempt_identity_basis='round + session_index; request_id alone may have multiple attempts',
            source=candidate.get('source'),destination=candidate.get('destination'),start_monotonic_s=start,
            confirmed_monotonic_s=end,controller_session_s=end-start if end is not None else None,
            classification=terminal.get('classification'),source_lease_balanced=terminal.get('source_lease_balanced'),
            candidate_generated_tokens=req.get('generated_tokens'),candidate_computed_tokens=req.get('computed_tokens'),
            native_total_migration_ms=native[-1].get('total_migration_time_ms') if native else None,
            native_event_counts=json.dumps(Counter(e.get('event_name') for e in events),sort_keys=True),
            native_kvt_codes=json.dumps(sorted(set(e['kvt_code'] for e in events if 'kvt_code' in e),key=str)),
            native_kvt_code_counts=json.dumps(Counter(str(e['kvt_code']) for e in events if 'kvt_code' in e),sort_keys=True),
            cancellation_scopes=json.dumps(sorted(set(e['cancellation_scope'] for e in events if e.get('cancellation_scope')))),
            client_completed=client.get('completed'),client_finish_reason=client.get('finish_reason'),
            client_completion_tokens=(client.get('usage') or {}).get('completion_tokens'),client_error=client.get('error'),
            measured_transfer_bytes=None,transfer_bytes_status='unavailable; log-read bytes and KV estimates are not network measurements',
            evidence_log_bytes_read=terminal.get('evidence_log_bytes_read')))
    csvfile(folder/'controller_decisions.csv',decision_rows);csvfile(folder/'migration_sessions.csv',sessions)
    events=lines(folder/'waiting_transfers.jsonl');txs={}
    for event in events:
        tx=event['tx'];entry=txs.setdefault(tx,dict(round=folder.name,tx=tx,request_id=event.get('request_id')))
        entry[event['event']+'_monotonic_s']=event.get('monotonic_s')
        entry['terminal_event']=event['event']
        if event['event']=='begin':entry.update(source=event.get('source'),destination=event.get('destination'))
        if event.get('reply'):entry['reply_json']=json.dumps(event['reply'],ensure_ascii=False)
    csvfile(folder/'waiting_transfer_transactions.csv',list(txs.values()))
    return requests,sessions


def integrity_audit(root):
    """Check observable identities, plans and action permissions without repairing raw data."""
    round_rows=[];by_stem=defaultdict(dict)
    for folder in sorted(root.iterdir()):
        if not folder.is_dir() or not (folder/'summary.json').exists():continue
        mode=read(folder/'config.json')['mode'];plans=read(folder/'replay.json');rows=read(folder/'measurement_rows.json')
        sends=lines(folder/'sends.jsonl');decisions=lines(folder/'decisions.jsonl')
        plan_ids=[r['request_id'] for r in plans];result_ids=[r['request_id'] for r in rows];sent_ids=[r['request_id'] for r in sends]
        digest=hashlib.sha256(json.dumps(plans,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        actions=Counter(d.get('action') for d in decisions if d.get('action'))
        permission_ok=(mode!='A' or not actions) and (mode!='B' or not actions['running_kv_migration'])
        audit=dict(round=folder.name,mode=mode,planned=len(plans),sent=len(sends),result_records=len(rows),
            duplicate_plan_ids=len(plan_ids)-len(set(plan_ids)),duplicate_send_ids=len(sent_ids)-len(set(sent_ids)),
            duplicate_result_ids=len(result_ids)-len(set(result_ids)),missing_results=len(set(plan_ids)-set(result_ids)),
            unplanned_results=len(set(result_ids)-set(plan_ids)),unsent_plans=len(set(plan_ids)-set(sent_ids)),
            action_permission_valid=permission_ok,actions=json.dumps(actions,sort_keys=True),canonical_trace_sha256=digest,
            output_hash_mismatches=sum(r.get('output_sha256')!=hashlib.sha256(json.dumps(r.get('token_ids',[])).encode()).hexdigest() for r in rows if 'output_sha256' in r),
            usage_count_mismatches=sum((r.get('usage') or {}).get('completion_tokens')!=len(r.get('token_ids',[])) for r in rows if r.get('completed')),
            send_lag_max_s=max([s['send_lag_s'] for s in sends],default=None),
            send_lag_p99_s=quantile([s['send_lag_s'] for s in sends],.99))
        round_rows.append(audit)
        if mode in 'ABC' and (folder.name.startswith('pre_') or folder.name.startswith('formal_')):
            by_stem[folder.name[:-2]][mode]=(digest,folder)
    output_rows=[];trace_rows=[]
    for stem,modes in by_stem.items():
        trace_rows.append(dict(comparison=stem,available_modes=''.join(sorted(modes)),
            all_ABC_present=all(m in modes for m in 'ABC'),
            available_traces_identical=len({v[0] for v in modes.values()})==1))
        for first,second in [('A','B'),('B','C')]:
            if first not in modes or second not in modes:continue
            a={r['request_id']:r for r in read(modes[first][1]/'measurement_rows.json')}
            b={r['request_id']:r for r in read(modes[second][1]/'measurement_rows.json')}
            for rid in sorted(a.keys()|b.keys()):
                x=a.get(rid,{});y=b.get(rid,{})
                comparable=x.get('completed') and y.get('completed')
                output_rows.append(dict(comparison=stem,first_mode=first,second_mode=second,request_id=rid,
                    both_complete=bool(comparable),first_completed=bool(x.get('completed')),second_completed=bool(y.get('completed')),
                    first_tokens=len(x.get('token_ids',[])),second_tokens=len(y.get('token_ids',[])),
                    token_ids_equal=x.get('token_ids')==y.get('token_ids') if comparable else None,
                    finish_reasons_equal=x.get('finish_reason')==y.get('finish_reason') if comparable else None))
    csvfile(root/'round_integrity.csv',round_rows);csvfile(root/'trace_pair_integrity.csv',trace_rows)
    csvfile(root/'output_comparisons.csv',output_rows)
    write(root/'integrity_summary.json',dict(closed_rounds=len(round_rows),
        observable_identity_issues=sum(sum(r[k] for k in ['duplicate_plan_ids','duplicate_send_ids','duplicate_result_ids','missing_results','unplanned_results','unsent_plans']) for r in round_rows),
        permission_violations=sum(not r['action_permission_valid'] for r in round_rows),
        trace_mismatches=sum(not r['available_traces_identical'] for r in trace_rows),
        comparable_outputs=sum(r['both_complete'] for r in output_rows),
        output_mismatches=sum(r['token_ids_equal'] is False or r['finish_reasons_equal'] is False for r in output_rows),
        note='Observable records only; atomic race safety also depends on EngineCore correctness gates and raw transaction evidence'))


def paired_seed_plot(root,contrasts,stats):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(11,4.5),sharey=True)
    for axis,load in zip(axes,['medium','high']):
        points=[c for c in contrasts if c['comparison'].startswith('formal_'+load+'_s') and c['valid']]
        for offset,metric,color,label in [(-.08,'B_minus_A_p95_s','#2878a7','B minus A'),(.08,'C_minus_B_p95_s','#ad395a','C minus B')]:
            available=[(int(c['comparison'].rsplit('_s',1)[1]),c[metric]) for c in points if c[metric] is not None]
            if available:axis.plot([x+offset for x,y in available],[y for x,y in available],marker='o',color=color,label=label,linewidth=.8)
            stat=next((s for s in stats if s['load']==load and s['metric']==metric),None)
            if stat and stat['mean_difference_s'] is not None:
                axis.errorbar(8106+offset,stat['mean_difference_s'],
                    yerr=[[stat['mean_difference_s']-stat['ci95_low_s']],[stat['ci95_high_s']-stat['mean_difference_s']]],
                    fmt='s',capsize=4,color=color)
        axis.axhline(0,color='black',linewidth=.8);axis.grid(alpha=.2);axis.set_title(load)
        axis.set_xticks([8101,8102,8103,8104,8105,8106],['8101','8102','8103','8104','8105','mean / CI'])
        axis.set_xlim(8100.7,8106.55)
        axis.set_xlabel('workload seed; aggregate only with all five valid pairs')
        if axis.get_legend_handles_labels()[0]:axis.legend()
    axes[0].set_ylabel('paired E2E p95 difference (seconds; negative is faster)')
    fig.suptitle('Local two-GPU experiment / seed-level paired results')
    fig.tight_layout();fig.savefig(root/'paired_seed_p95.png',dpi=160);plt.close(fig)


def findings(root,rows,contrasts,formal_stats,pairs):
    """Short conclusions separated from the per-round technical audit report."""
    formal=[r for r in rows if r['phase']=='formal'];n=len(formal)
    out=['# 运行中迁移增量收益：结果摘要',
        f'本地同机双 RTX5090，Qwen2.5-7B-Instruct bf16。正式已闭合 {n}/30 轮；'+
        ('以下为完整服务期测量记录，不代表SDK正常卸载验收通过。' if n==30 else '正式尚未完成，以下仅为进度审计，不形成五种子总体结论。'),
        '共同配置：context9216、max_tokens4096、temperature0、n1，允许正常EOS。输入128～4096 token（含目标模型聊天模板），1815条去重ShareGPT文本均匀有放回抽样。medium/high为总到达率0.5/2请求每秒，冻结泊松轨迹开放发送600秒，再观察至完成或固定600秒尾部上限。模型常驻，轮间清空请求/事务和前缀缓存并统一预热，不声称完全重置CUDA分配器及通信缓存。',
        '1. **实际执行的动作。** A仅动态路由；B追加从未开始执行的等待请求原子转移；C完整复用B的路由和等待规则，再追加周期运行中KV迁移。每轮计数见summary.csv；被抢占后重新等待的请求不属于B的无状态转移对象。']
    def fmt(v):return '未形成有限估计' if v is None else f'{v:+.3f}'
    for load in ['medium','high']:
        selected=[r for r in formal if r['round'].startswith('formal_'+load+'_')]
        if not selected:continue
        counts=[]
        for mode in 'ABC':
            rs=[r for r in selected if r['scheme']==mode]
            counts.append(f"{mode}：等待尝试/完成={sum(r['waiting_attempts'] for r in rs)}/{sum(r['waiting_committed'] for r in rs)}，运行迁移尝试/完成/取消/失败={sum(r['running_attempts'] for r in rs)}/{sum(r['kv_completed'] for r in rs)}/{sum(r['kv_cancelled'] for r in rs)}/{sum(r['kv_failed'] for r in rs)}")
        out.append(load+'：'+'；'.join(counts)+'。')
    for index,metric,question in [(2,'B_minus_A_p95_s','B相对A有什么变化'),(3,'C_minus_B_p95_s','C相对B有无增量收益')]:
        out.append(f'{index}. **{question}。** 以同一负载、同一种子的整轮全部请求E2E p95配对，负差表示更快。')
        for stat in [s for s in formal_stats if s['metric']==metric]:
            values=[c[metric] for c in contrasts if c['comparison'].startswith('formal_'+stat['load']+'_') and c['valid'] and c[metric] is not None]
            out.append(f"{stat['load']}：{stat['finite_pairs']}/5个有限有效配对，平均差 {fmt(stat['mean_difference_s'])} 秒，95%配对t区间 [{fmt(stat['ci95_low_s'])}, {fmt(stat['ci95_high_s'])}] 秒；已有配对中{sum(v<0 for v in values)}次更快、{sum(v>0 for v in values)}次更慢。只有5个种子，区间依赖近似分布，不能把各轮请求当独立重复。")
            if metric=='C_minus_B_p95_s' and stat['mean_difference_s'] is not None:
                if stat['ci95_low_s']<=0<=stat['ci95_high_s']:out.append(stat['load']+'：该区间包含0，本次未获得稳定降低主指标p95的证据；不能据此证明等效或所有运行迁移无价值。')
                elif stat['ci95_high_s']<0:out.append(stat['load']+'：本次五种子配对观测到p95降低，结论只限这里的负载和冻结规则。')
                else:out.append(stat['load']+'：本次五种子配对观测到p95增加，不能据此推广所有运行迁移均无价值。')
    secondary_path=root/'paired_secondary_summary.csv'
    if secondary_path.exists():
        with secondary_path.open(encoding='utf-8-sig',newline='') as inp:secondary=list(csv.DictReader(inp))
        for load in ['medium','high']:
            values=[s for s in secondary if s['load']==load and s['contrast']=='C_minus_B' and s.get('mean_difference_s')]
            if values:out.append(load+'的次要指标C−B均值（同种子配对）：'+'，'.join(s['metric']+' '+fmt(float(s['mean_difference_s']))+'秒' for s in values)+'；完整逐种子结果与区间见paired_secondary_differences.csv/paired_secondary_summary.csv，不据此改主指标或寻找有利阈值。')
    if formal and not any(r['waiting_committed'] for r in formal):
        out.append('正式轮次没有成功等待转移，B−A不能解释为成功等待转移带来的收益；应连同等待候选、原子竞态拒绝和路由状态记录阅读。')
    out.append('4. **迁移对象与其他请求受到什么影响。** group_impacts.csv逐种子给出C选择对象、实际完成迁移对象、尝试时源端其他请求、目标端已有请求的同ID配对结果；分组依赖C决策，可能重叠，不构成独立随机对照。')
    out.append('源/目标成员依据C决策时最近一次EngineCore快照（准入年龄上限2秒），不冒充精确物理瞬时状态；同一逻辑请求在B中可能由不同实例执行。')
    group_rows=[]
    for load in ['medium','high']:
        names=sorted({g['group'] for p in pairs if p['comparison'].startswith('formal_'+load+'_') for g in p['groups']})
        for name in names:
            selected=[g for p in pairs if p['comparison'].startswith('formal_'+load+'_') for g in p['groups'] if g['group']==name]
            values=[g['mean_C_minus_B_e2e_s'] for g in selected if g['mean_C_minus_B_e2e_s'] is not None]
            row=dict(load=load,group=name,seeds_with_group=len(values),mean_of_seed_group_e2e_differences_s=mean(values),
                group_request_round_pairs=sum(g['count'] for g in selected),output_mismatches=sum(g['output_mismatches'] for g in selected),
                interpretation='descriptive posthoc C-selected groups; groups may overlap; no independent causal estimate')
            group_rows.append(row)
            out.append(f"{load}，{name}：有组的{len(values)}个种子中，先计算每种子同ID平均E2E差，再平均为 {fmt(row['mean_of_seed_group_e2e_differences_s'])} 秒；组内共{row['group_request_round_pairs']}个请求-轮次配对。")
    csvfile(root/'group_seed_descriptives.csv',group_rows)
    out.append('5. **是否有运行中迁移机会。** 实际运行迁移尝试及终态是已实施动作的证据；opportunity_summary.json另给两端运行数差、差异持续片段与所有拒绝原因。零次动作本身不能证明无机会。正式机会只按冻结的差3、连续3次检查、容量余量、当前上下文候选规则判断，不读取未来回答长度。')
    out.append('6. **性能变化的解释与证据。** state_timeline.png对齐运行数、等待数、KV占用、近期活跃引擎步耗时和迁移开始时间；migration_request_effects.csv对照控制器会话、原生迁移耗时、客户端输出块停顿及B同输出位置区间。时间线支持机制解释，不能将所有整轮差值孤立归因于一次迁移。网络传输字节未实测，保持缺失。SSE块间隔不称逐token ITL。')
    for load in ['medium','high']:
        rs=[r for r in formal if r['round'].startswith('formal_'+load+'_') and r['scheme']=='C']
        effects=[e for p in pairs if p['comparison'].startswith('formal_'+load+'_') for e in p['attempt_effects']]
        native=[e['native_migration_ms'] for e in effects if e['native_migration_ms'] is not None]
        gaps=[e['C_session_max_output_block_gap_s'] for e in effects if e['C_session_max_output_block_gap_s'] is not None]
        if rs:out.append(f"{load}的C运行尝试：600秒到达期内{sum(r['running_attempts_during_arrivals'] for r in rs)}次、尾部{sum(r['running_attempts_in_tail'] for r in rs)}次。"+
            (f"已完成会话的原生耗时范围{min(native):.3f}～{max(native):.3f}ms。" if native else '没有可用的已完成原生会话耗时。')+
            (f"迁移控制会话覆盖的客户端最大输出块间隔为{max(gaps):.3f}秒，B的同输出位置区间逐项对照见CSV。" if gaps else '')+
            '这些会话级范围是描述性数据，不把各会话视为独立随机重复。')
    out.append('迁移取消、原生404返回值与用户失败分别计数。migration_sessions.csv逐次关联客户端完成、finish_reason和输出数，保留各原生事件返回码计数；不能把两个阶段各记一次404误当两个请求失败。候选已执行资格来自EngineCore快照，迁移DTO视角的generated_len=0不能用来认定它是无状态等待转移。')
    checks=read(root/'integrity_summary.json') if (root/'integrity_summary.json').exists() else {}
    out.append(f"输出与身份核对：已闭合ABC轮次的A/B和B/C共{checks.get('comparable_outputs',0)}对完整输出，{checks.get('output_mismatches',0)}对token IDs或finish_reason不一致；观测到的身份问题{checks.get('observable_identity_issues',0)}，动作权限违规{checks.get('permission_violations',0)}。有不一致时必须考虑工作量差异。")
    completed=sum(r['completed'] for r in formal);requests=sum(r['requests'] for r in formal)
    out.append(f"正式请求：{completed}/{requests}完成，失败{sum(r['failed'] for r in formal)}，截止未完成{sum(r['unfinished'] for r in formal)}。主分位以全部发送请求为分母，TTFT另标条件分布。max_tokens=4096，允许EOS，但finish_reason=length代表达到预算截断，不是完全无限制自然生成。")
    out.append('7. **结论边界。** 仅回答这一模型、两张本地GPU、这两个负载和冻结简单规则下的增量表现。简单规则无收益不能推广为所有运行迁移无价值；无迁移机会不能冒充性能负结论。不宣称跨数据中心有效、最优调度、服务器总能耗收益或统计普适性。容量校准的2rps是本次保守零抢占条件下已观测通过的高档；3rps完成但发生17次抢占，不等同于证明持续发散或测得全局最大容量。')
    release_path=root/'resource_release_classified.json'
    if not release_path.exists():release_path=root/'resource_release.json'
    if release_path.exists():
        release=read(release_path)
        out.append('资源收尾：'+('本实验GPU资源已释放。' if release.get('gpu_released') else '本实验GPU释放仍待确认。')+
            '自有残留进程='+str(release.get('owned_residual_pids'))+'；退出证据分类='+str(release.get('exit_evidence_classification'))+'。详见'+release_path.name+'和原始shutdown/服务日志；进程已退出不等于SDK正常卸载严格验收已通过。')
        if release.get('normal_unload_qualified') is False:
            out.append('退出限制：D在Asio doAccept回调路径发生segfault，S提示corrupted double-linked list，均属于测量结束后的停止阶段；正常卸载验收未通过，根因尚未证实。日志和生命周期事件保留，不能仅凭这些栈断定重复释放或最先失效的对象。本报告只分析客户端服务期观测，不宣称平台已达到生产可靠性。')
        if release.get('shutdown_checker_error_scope')=='after_all_30_measurements_in_cleanup_gpu_check':
            out.append('原progress中的blocked和run_failure保留：其Python调用栈位于所有30轮闭合后的cleanup.gpu_check。首次读数无compute PID、显存约1MiB但利用率为100，后续三次独立采样为0且显存2MiB。该停机后空闲门未通过与上述真实native退出错误分别记录，不能相互掩盖；不修改原始轮次validity或放宽安全检查。')
    if (root/'run_failure.json').exists():out.append('另有run_failure.json，需按时间和调用栈区分测量阶段与收尾阶段。原始轮次validity保留；不能只凭退出文件名宣称native崩溃或正常退出通过。')
    (root/'FINDINGS.md').write_text('\n\n'.join(out)+'\n',encoding='utf-8')
    return out


def opportunity(folder):
    states=lines(folder/'states.jsonl');decisions=lines(folder/'decisions.jsonl');durations=Counter();last=None
    max_gap=0;waiting_time=Counter();arrival_gaps=Counter();arrival_waiting=0.;gap_episodes=[];episode=None
    for row in states:
        t=row['monotonic_s'];ss=row['engines'];gap=abs(ss[0]['running']-ss[1]['running']);max_gap=max(max_gap,gap)
        if last is not None:
            dt=min(1.,t-last[0]);durations[last[1]]+=dt
            waiting_time['any_waiting_s']+=dt*last[2]
            waiting_time['zero_waiting_s']+=dt*(not last[2])
            overlap=max(0,min(600,row['elapsed_s'])-max(0,last[3]))
            overlap=min(dt,overlap);arrival_gaps[last[1]]+=overlap;arrival_waiting+=overlap*last[2]
        source=0 if ss[0]['running']>ss[1]['running'] else 1
        if gap>=3:
            if episode is None:episode=dict(source=source,start=t,end=t)
            elif source==episode['source']:episode['end']=t
            else:gap_episodes.append(episode);episode=dict(source=source,start=t,end=t)
        elif episode is not None:gap_episodes.append(episode);episode=None
        last=(t,gap,any(s['waiting'] for s in ss),row['elapsed_s'])
    if episode:gap_episodes.append(episode)
    for e in gap_episodes:e['duration_s']=e['end']-e['start']
    spacings=[b['monotonic_s']-a['monotonic_s'] for a,b in zip(states,states[1:])]
    return dict(max_running_gap=max_gap,running_gap_duration_s=dict(durations),waiting=waiting_time,
        fixed_600s_arrival_window=dict(running_gap_duration_s=dict(arrival_gaps),
            gap_at_least_3_s=sum(v for k,v in arrival_gaps.items() if k>=3),any_waiting_s=arrival_waiting,
            observed_duration_s=sum(arrival_gaps.values())),
        state_sampling=dict(observations=len(states),median_interval_s=statistics.median(spacings) if spacings else None,
            max_interval_s=max(spacings,default=None),unaccounted_duration_due_to_gaps_s=sum(max(0,x-1) for x in spacings),
            duration_basis='left-observation integration capped at 1s; sampled evidence, not continuous-state proof; includes tail'),
        gap_at_least_3_episodes=gap_episodes,max_gap_episode_s=max([e['duration_s'] for e in gap_episodes],default=0),
        decision_reasons=Counter(d.get('reason') for d in decisions),
        waiting_attempts=sum(d.get('action')=='waiting_transfer' for d in decisions),
        running_attempts=sum(d.get('action')=='running_kv_migration' for d in decisions),
        observed_core_step_s_mean=[mean([x for row in states for x in row['engines'][i]['active_step_s']]) for i in range(2)])


def pair(bfolder,cfolder,label):
    b={r['request_id']:r for r in read(bfolder/'measurement_rows.json')};c={r['request_id']:r for r in read(cfolder/'measurement_rows.json')}
    ids=sorted(b.keys()|c.keys());rows=[];groups=defaultdict(set);attempt_effects=[]
    def block_pairs(row):
        total=0;points=[]
        for block in row.get('chunks',[]):
            total+=block.get('token_count',0);points.append((block['time'],total))
        return [dict(start=a[0],end=z[0],left_output_count=a[1],right_output_count=z[1],gap_s=z[0]-a[0]) for a,z in zip(points,points[1:])]
    for d in lines(cfolder/'decisions.jsonl'):
        if d.get('action')=='running_kv_migration':
            rid=d['candidate']['request']['logical_id'];groups['C选择的运行候选（含取消）'].add(rid)
            terminal=d.get('terminal',{});classification=terminal.get('classification','unresolved')
            if classification=='completed':groups['C实际完成运行迁移的请求'].add(rid)
            groups['C尝试时源端其他请求'].update(d.get('source_other_ids',[]))
            groups['C尝试时目标端已有请求'].update(d.get('destination_existing_ids',[]))
            x=b.get(rid,{});y=c.get(rid,{})
            start=d.get('rpc_result',{}).get('started',d['monotonic_s']);end=terminal.get('confirmed_monotonic_s',d.get('ended',start))
            affected=[p for p in block_pairs(y) if p['start']<=end and p['end']>=start]
            lo=min([p['left_output_count'] for p in affected],default=None);hi=max([p['right_output_count'] for p in affected],default=None)
            baseline=[p for p in block_pairs(x) if lo is not None and p['left_output_count']<=hi and p['right_output_count']>=lo]
            native=[e.get('total_migration_time_ms') for e in terminal.get('events',[]) if e.get('event_name')=='migration_done']
            attempt_effects.append(dict(request_id=rid,source=d['candidate']['source'],destination=d['candidate']['destination'],
                classification=classification,controller_session_s=end-start,native_migration_ms=native[-1] if native else None,
                C_session_max_output_block_gap_s=max([p['gap_s'] for p in affected],default=None),
                B_same_output_range_max_block_gap_s=max([p['gap_s'] for p in baseline],default=None),
                output_position_range=str((lo,hi)),
                C_minus_B_e2e_s=y['e2e_s']-x['e2e_s'] if x.get('completed') and y.get('completed') else None,
                output_equal=x.get('token_ids')==y.get('token_ids') if x.get('completed') and y.get('completed') else None,
                C_completed=y.get('completed'),B_completed=x.get('completed'),
                user_metric='client SSE output-block interval; not per-token ITL; output-range comparison is posthoc'))
    for rid in ids:
        x=b.get(rid,{});y=c.get(rid,{})
        equal=x.get('token_ids')==y.get('token_ids') and x.get('finish_reason')==y.get('finish_reason') if x.get('completed') and y.get('completed') else None
        delta=y['e2e_s']-x['e2e_s'] if x.get('completed') and y.get('completed') else None
        rows.append(dict(request_id=rid,comparison=label,B_completed=x.get('completed'),C_completed=y.get('completed'),
            output_equal=equal,B_completion_tokens=len(x.get('token_ids',[])),C_completion_tokens=len(y.get('token_ids',[])),
            B_e2e_s=x.get('e2e_s'),C_e2e_s=y.get('e2e_s'),C_minus_B_e2e_s=delta))
    group_rows=[]
    for name,selected in groups.items():
        subset=[r for r in rows if r['request_id'] in selected]
        def group_p95(mode):
            values=sorted(r[mode+'_e2e_s'] for r in subset if r[mode+'_completed'])
            rank=math.ceil(.95*len(selected))
            return values[rank-1] if rank and len(values)>=rank else None
        group_rows.append(dict(group=name,count=len(selected),matched=sum(rid in b and rid in c for rid in selected),
            paired_complete=sum(r['C_minus_B_e2e_s'] is not None for r in subset),
            B_completed=sum(bool(r['B_completed']) for r in subset),C_completed=sum(bool(r['C_completed']) for r in subset),
            mean_C_minus_B_e2e_s=mean([r['C_minus_B_e2e_s'] for r in subset if r['C_minus_B_e2e_s'] is not None]),
            B_p95_s=group_p95('B'),C_p95_s=group_p95('C'),
            output_mismatches=sum(r['output_equal'] is False for r in subset),
            group_basis='依赖C决策的事后分组；同ID匹配；不作为独立随机对照'))
    csvfile(cfolder/'paired_B_requests.csv',rows);csvfile(cfolder/'paired_B_groups.csv',group_rows)
    csvfile(cfolder/'migration_request_effects.csv',attempt_effects)
    return dict(comparison=label,matched=sum(rid in b and rid in c for rid in ids),
        missing_identity=sum(rid not in b or rid not in c for rid in ids),output_mismatches=sum(r['output_equal'] is False for r in rows),
        completed_output_pairs=sum(r['output_equal'] is not None for r in rows),
        B_received_completion_tokens=sum(len(x.get('token_ids',[])) for x in b.values()),
        C_received_completion_tokens=sum(len(x.get('token_ids',[])) for x in c.values()),groups=group_rows,attempt_effects=attempt_effects)


def plots(folder):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    samples=lines(folder/'states.jsonl')
    if not samples:return
    fig,axes=plt.subplots(4,1,figsize=(11,10),sharex=True)
    for i in range(2):
        x=[s['elapsed_s'] for s in samples]
        for axis,key in zip(axes,['running','waiting','kv_usage','active_step_s']):
            y=[(statistics.median(s['engines'][i][key]) if s['engines'][i][key] and s['engines'][i]['running'] else None) if key=='active_step_s' else s['engines'][i][key] for s in samples]
            axis.plot(x,y,label=f'GPU {i}',linewidth=.9);axis.set_ylabel(key);axis.grid(alpha=.2)
    for a in axes:a.legend();a.axvline(600,color='black',linestyle='--',linewidth=.7)
    axes[-1].set_xlabel('seconds since replay start; dashed = end of arrivals')
    timeline=read(folder/'timeline.json')
    for attempt in lines(folder/'running_attempts.jsonl'):
        for axis in axes:axis.axvline(attempt['started']-timeline['start_monotonic_s'],color='#ab3050',alpha=.4,linewidth=.7)
    fig.suptitle(folder.name);fig.tight_layout();fig.savefig(folder/'state_timeline.png',dpi=150);plt.close(fig)
    if (folder/'power.csv').exists():
        with (folder/'power.csv').open(encoding='utf-8-sig',newline='') as f:power=list(csv.DictReader(f))
        fig,axes=plt.subplots(3,1,figsize=(11,8),sharex=True);quality=[]
        for i in range(2):
            rows=[r for r in power if int(r['gpu_index'])==i];x=[float(r['monotonic_s'])-timeline['start_monotonic_s'] for r in rows]
            for axis,key,label in zip(axes,['power_w','utilization_pct','memory_mib'],['GPU power (W)','GPU utilization (%)','GPU memory (MiB)']):
                y=[float(r[key]) if r.get(key) not in ('',None) else None for r in rows]
                axis.plot(x,y,label=f'GPU {i}',linewidth=.8);axis.set_ylabel(label)
            readings=[r['power_w'] for r in rows if r.get('power_w') not in ('',None)]
            quality.append(dict(gpu_index=i,samples=len(rows),power_available=len(readings),
                median_read_interval_s=statistics.median([b-a for a,b in zip(x,x[1:])]) if len(x)>1 else None,
                repeated_adjacent_power_fraction=sum(a==b for a,b in zip(readings,readings[1:]))/max(1,len(readings)-1),
                device_measurement_cadence='not directly measured; repeated readings do not uniquely identify sensor cadence'))
        for axis in axes:axis.legend();axis.grid(alpha=.2);axis.axvline(600,color='black',linestyle='--',linewidth=.7)
        axes[-1].set_xlabel('seconds since replay start; dashed = end of arrivals')
        fig.suptitle(folder.name+' / measured NVML readings (0.2s requested)');fig.tight_layout();fig.savefig(folder/'gpu_power.png',dpi=150);plt.close(fig)
        write(folder/'power_read_quality.json',quality)


def execution_locations(root):
    """Final pass over per-instance native output evidence, not queue presence."""
    windows={};owners=defaultdict(list);results={}
    for f in root.iterdir():
        if not f.is_dir() or not (f/'measurement_rows.json').exists():continue
        timeline=read(f/'timeline.json');rows=read(f/'measurement_rows.json')
        start=timeline['start_wall_ns'];end_mono=max([x.get('finish_monotonic_s',timeline['start_monotonic_s']+1200) for x in rows],default=timeline['start_monotonic_s']+600)
        end=start+(end_mono-timeline['start_monotonic_s'])*1e9
        windows[f.name]=(start,end)
        for row in rows:
            rid=row['request_id'];owners['cmpl-'+rid+'-0'].append(f.name)
            results[(f.name,rid)]=dict(round=f.name,request_id=rid,initial_engine=row.get('initial_engine'),
                completed=row.get('completed'),engine0_nonempty_output_events=0,engine1_nonempty_output_events=0)
    for i,role in enumerate('SD'):
        path=root/'services_01'/f'outputs_{role}.jsonl'
        if not path.exists():continue
        with path.open(encoding='utf-8') as inp:
            for line in inp:
                if not line.strip():continue
                event=json.loads(line);engine_id=event['request_id']
                if not event.get('tokens'):continue
                for name in owners.get(engine_id,[]):
                    lo,hi=windows[name]
                    if lo<=event['time_ns']<=hi:
                        rid=engine_id.removeprefix('cmpl-')[:-2]
                        results[(name,rid)][f'engine{i}_nonempty_output_events']+=1
    for row in results.values():
        row['engines_with_output_evidence']=','.join(str(i) for i in range(2) if row[f'engine{i}_nonempty_output_events'])
        row['missing_output_location']=bool(row['completed'] and not row['engines_with_output_evidence'])
    csvfile(root/'execution_locations.csv',list(results.values()))
    write(root/'execution_location_coverage.json',dict(requests=len(results),completed_missing=sum(x['missing_output_location'] for x in results.values()),
        basis='nonempty per-instance EngineCore emitted-token events; original queue presence is not execution',
        token_event_counts_are_not_transfer_bytes=True))


def main():
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('--include-core-locations',action='store_true');a=p.parse_args();root=Path(a.root)
    rows=[];opportunities={};pairs=[];flat_requests=[];flat_sessions=[]
    quality={};quality_path=root/'final_audit/native_and_resource_audit.csv'
    if quality_path.exists():
        with quality_path.open(encoding='utf-8-sig',newline='') as inp:quality={r['round']:r for r in csv.DictReader(inp)}
    for folder in sorted(root.iterdir()):
        if not folder.is_dir() or not (folder/'summary.json').exists():continue
        config=read(folder/'config.json');result=read(folder/'summary.json');valid=read(folder/'validity.json')
        opp=opportunity(folder);opportunities[folder.name]=opp;write(folder/'opportunity.json',opp)
        terminal_counts=Counter(t['classification'] for t in lines(folder/'migration_terminals.jsonl'))
        waiting_events=Counter(t['event'] for t in lines(folder/'waiting_transfers.jsonl'))
        measured=read(folder/'measurement_rows.json')
        request_table,session_table=export_round(folder)
        flat_requests.extend(request_table);flat_sessions.extend(session_table)
        terminal_records=lines(folder/'migration_terminals.jsonl')
        native_durations=[e['total_migration_time_ms'] for t in terminal_records for e in t.get('events',[]) if e.get('event_name')=='migration_done' and isinstance(e.get('total_migration_time_ms'),(int,float))]
        q=quality.get(folder.name);reasons=[]
        if not valid['valid']:reasons.append('original_validity_failed')
        if result['failed'] or result['unfinished']:reasons.append('request_failures_require_diagnosis')
        if q:
            for field in ['foreign_gpu_samples','unbalanced_terminal_leases','completed_missing_acquire_evidence','duplicate_lease_events']:
                if int(q.get(field) or 0):reasons.append(field)
            if q.get('native_session_accounting_explained')!='True':reasons.append('unexplained_native_session_accounting')
            if not q.get('control_metrics_available_fraction') or float(q['control_metrics_available_fraction'])<.99:reasons.append('control_metrics_unavailable_over_1_percent')
        row=dict(round=folder.name,phase='formal' if folder.name.startswith('formal_') else 'pretrial' if folder.name.startswith('pre_') else 'calibration',
            scheme=config['mode'],valid=not reasons,original_validity=valid['valid'],independent_native_resource_audit=bool(q),
            analysis_exclusion_reasons=json.dumps(reasons),**{k:v for k,v in result.items() if not isinstance(v,dict)},
            stop_fraction=result['finish_reasons']['stop']/result['requests'] if result['requests'] else None,
            length_fraction=result['finish_reasons']['length']/result['requests'] if result['requests'] else None,
            waiting_attempts=opp['waiting_attempts'],running_attempts=opp['running_attempts'],max_running_gap=opp['max_running_gap'],
            waiting_committed=waiting_events['committed'],waiting_atomic_rejected=waiting_events['atomic_claim_rejected'],
            kv_completed=terminal_counts['completed'],kv_cancelled=terminal_counts['cancelled'],kv_failed=terminal_counts['failed'],
            kv_not_dispatched=terminal_counts['not_dispatched'],
            running_gap_ge3_s=sum(v for k,v in opp['running_gap_duration_s'].items() if int(k)>=3),
            max_gap_episode_s=opp['max_gap_episode_s'])
        row['short_answer_le256_count']=sum(r.get('completed') and len(r.get('token_ids',[]))<=256 for r in measured)
        row['failure_rate']=result['failed']/result['requests'] if result['requests'] else None
        row['running_gap_ge3_during_arrivals_s']=opp['fixed_600s_arrival_window']['gap_at_least_3_s']
        start=read(folder/'timeline.json')['start_monotonic_s']
        row['running_attempts_during_arrivals']=sum(d.get('action')=='running_kv_migration' and d['monotonic_s']-start<600 for d in lines(folder/'decisions.jsonl'))
        row['running_attempts_in_tail']=opp['running_attempts']-row['running_attempts_during_arrivals']
        row['native_events_with_404_code']=sum(e.get('code')==404 or e.get('kvt_code')==404 for t in terminal_records for e in t.get('events',[]))
        row['sessions_with_404_code']=sum(any(e.get('code')==404 or e.get('kvt_code')==404 for e in t.get('events',[])) for t in terminal_records)
        row['long_answer_ge1024_count']=sum(r.get('completed') and len(r.get('token_ids',[]))>=1024 for r in measured)
        row['native_migration_mean_ms']=mean(native_durations)
        row['native_migration_p95_ms']=quantile(native_durations,.95)
        row['max_client_output_block_gap_s']=max([r['max_output_block_gap_s'] for r in measured if r.get('max_output_block_gap_s') is not None],default=None)
        rows.append(row)
        try:plots(folder)
        except ImportError:write(folder/'plot_unavailable.json',dict(reason='matplotlib not installed; raw time series retained'))
    csvfile(root/'summary.csv',rows)
    csvfile(root/'request_metrics.csv',flat_requests);csvfile(root/'migration_sessions.csv',flat_sessions)
    integrity_audit(root)
    if a.include_core_locations:execution_locations(root)
    for c in [*root.glob('pre_*_C'),*root.glob('formal_*_C')]:
        b=c.with_name(c.name[:-1]+'B')
        if (c/'measurement_rows.json').exists() and (b/'measurement_rows.json').exists():pairs.append(pair(b,c,c.name))
    write(root/'paired_output_checks.json',pairs);write(root/'opportunity_summary.json',opportunities)
    all_groups=[dict(comparison=p['comparison'],**g) for p in pairs for g in p['groups']]
    csvfile(root/'group_impacts.csv',all_groups)
    csvfile(root/'migration_request_effects.csv',[dict(comparison=p['comparison'],**effect) for p in pairs for effect in p['attempt_effects']])
    # Seed-level p95 differences. No pooled request significance test.
    byname={r['round']:r for r in rows};contrasts=[]
    for name,r in byname.items():
        if not name.endswith('_C'):continue
        b=byname.get(name[:-1]+'B');a=byname.get(name[:-1]+'A')
        if a and b:
            contrasts.append(dict(comparison=name[:-2],valid=a['valid'] and b['valid'] and r['valid'],
                A_p95_s=a['e2e_p95_s'],B_p95_s=b['e2e_p95_s'],C_p95_s=r['e2e_p95_s'],
                B_waiting_committed=b['waiting_committed'],C_waiting_committed=r['waiting_committed'],
                C_running_attempts=r['running_attempts'],C_kv_completed=r['kv_completed'],C_kv_cancelled=r['kv_cancelled'],
                C_minus_B_p95_s=r['e2e_p95_s']-b['e2e_p95_s'] if r['e2e_p95_s'] is not None and b['e2e_p95_s'] is not None else None,
                B_minus_A_p95_s=b['e2e_p95_s']-a['e2e_p95_s'] if b['e2e_p95_s'] is not None and a['e2e_p95_s'] is not None else None))
    csvfile(root/'paired_round_differences.csv',contrasts)
    secondary=[];secondary_stats=[]
    for c in contrasts:
        a,b,z=[byname[c['comparison']+'_'+m] for m in 'ABC']
        for metric in ['e2e_p50_s','e2e_p99_s','ttft_p50_s','ttft_p95_s']:
            for label,first,second in [('B_minus_A',a,b),('C_minus_B',b,z)]:
                delta=second.get(metric)-first.get(metric) if first.get(metric) is not None and second.get(metric) is not None else None
                secondary.append(dict(comparison=c['comparison'],valid=c['valid'],metric=metric,contrast=label,difference_s=delta))
    csvfile(root/'paired_secondary_differences.csv',secondary)
    for load in ['medium','high']:
        for metric in ['e2e_p50_s','e2e_p99_s','ttft_p50_s','ttft_p95_s']:
            for label in ['B_minus_A','C_minus_B']:
                selected=[s for s in secondary if s['comparison'].startswith('formal_'+load+'_s') and s['metric']==metric and s['contrast']==label]
                values=[s['difference_s'] for s in selected if s['valid'] and s['difference_s'] is not None]
                full=len(selected)==5 and len(values)==5;delta=mean(values) if full else None
                half=2.7764451051977987*statistics.stdev(values)/math.sqrt(5) if full else None
                secondary_stats.append(dict(load=load,metric=metric,contrast=label,finite_pairs=len(values),
                    mean_difference_s=delta,ci95_low_s=delta-half if full else None,ci95_high_s=delta+half if full else None,
                    method='secondary descriptive paired-seed t interval df4; no multiple-outcome significance claim'))
    csvfile(root/'paired_secondary_summary.csv',secondary_stats)
    formal_stats=[]
    for load in ['medium','high']:
        selected=[c for c in contrasts if c['comparison'].startswith('formal_'+load+'_s')]
        if not selected:continue
        for metric in ['C_minus_B_p95_s','B_minus_A_p95_s']:
            values=[c[metric] for c in selected if c['valid'] and c[metric] is not None]
            all_five=len(selected)==5 and len(values)==5
            delta=mean(values) if all_five else None
            half=2.7764451051977987*statistics.stdev(values)/math.sqrt(5) if all_five else None
            formal_stats.append(dict(load=load,metric=metric,planned_pairs=5,observed_pairs=len(selected),finite_pairs=len(values),
                mean_difference_s=delta,ci95_low_s=delta-half if all_five else None,ci95_high_s=delta+half if all_five else None,
                method='5 independent workload-seed pairs, Student t df=4; small-sample descriptive uncertainty; no request-level independence assumption',
                missing_pair_policy='No aggregate estimate if any planned seed pair missing/nonfinite'))
    csvfile(root/'paired_seed_uncertainty.csv',formal_stats)
    if any(c['comparison'].startswith('formal_') for c in contrasts):
        try:paired_seed_plot(root,contrasts,formal_stats)
        except ImportError:pass
    write(root/'analysis_version.json',dict(source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        source_path=str(Path(__file__).resolve()),statistical_unit='workload seed, separately for each load',
        original_frozen_operational_source_modified=False))
    concise=findings(root,rows,contrasts,formal_stats,pairs)
    gate_path=root/'correctness_01/result.json'
    passed=gate_path.exists() and read(gate_path).get('passed') is True
    failure=read(root/'run_failure.json') if (root/'run_failure.json').exists() else None
    report=['# 运行中迁移的增量收益实验',
        '本实验为同机两张 GPU 的本地实验，比较运行中 KV 迁移相对于相同动态路由和无状态等待转移规则的增量。',
        f'服务期请求转移正确性验收：{"通过" if passed else "尚未通过"}。已形成完整计量数据的轮次：{len(rows)}。正常卸载另见资源证据，不包含在该准入验收中。']
    if (root/'progress.json').exists():
        state=read(root/'progress.json')
        report.append(f"生成报告时状态：{state.get('phase')}；当前目录 {state.get('current')}。校准/预实验/正式完整记录数分别为 {sum(r['phase']=='calibration' for r in rows)}/{sum(r['phase']=='pretrial' for r in rows)}/{sum(r['phase']=='formal' for r in rows)}；未完成阶段不能当作已有结果。")
    if failure:report.append('另有故障记录：'+failure['error']+'。失败现场与原始日志保留，需按调用栈和时间区分测量与收尾；不自动重写各轮原始validity。')
    for c in contrasts:
        cr=byname[c['comparison']+'_C'];br=byname[c['comparison']+'_B'];ar=byname[c['comparison']+'_A']
        def fmt(x):return f'{x:.3f}' if x is not None else '无法形成有限估计'
        report.append(f"{c['comparison']}：A/B/C实际等待转移完成 {ar['waiting_committed']}/{br['waiting_committed']}/{cr['waiting_committed']} 次；运行中KV迁移完成 {ar['kv_completed']}/{br['kv_completed']}/{cr['kv_completed']} 次。B−A的E2E p95差值 {fmt(c['B_minus_A_p95_s'])} 秒，C−B差值 {fmt(c['C_minus_B_p95_s'])} 秒；负值代表延迟下降。有效性：{c['valid']}。")
        if cr['running_attempts']==0:
            opp=opportunities[cr['round']]
            report.append(f"{cr['round']} 未发起运行中迁移。最大运行数差 {opp['max_running_gap']}，差至少3的最长同方向连续观测片段 {opp['max_gap_episode_s']:.3f} 秒；逐次拒绝计数 {dict(opp['decision_reasons'])}。没有运行中动作时，该次C−B差异不能归因于运行中迁移收益。")
    for pair_result in pairs:
        report.append(f"{pair_result['comparison']} 同ID输出核对：{pair_result['completed_output_pairs']} 对完整输出中，{pair_result['output_mismatches']} 对不一致；B/C实际收到输出token合计 {pair_result['B_received_completion_tokens']}/{pair_result['C_received_completion_tokens']}。"+
            ('C没有运行中迁移对象，无法估计迁移对象、源端其他请求与目标端已有请求三组的迁移影响。' if not pair_result['groups'] else '三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。'))
    if formal_stats:
        report.append('正式配对结果：'+json.dumps(formal_stats,ensure_ascii=False)+'。仅5个种子，区间依赖种子层面差值的近似分布；不能据此宣称广泛普适性。')
    report += ['A 只动态路由；B 额外允许在 EngineCore 边界确认从未执行的等待请求转移；C 在完整 B 上追加周期性运行中 KV 迁移。实际动作次数及完成/取消/失败/未发起分类见 summary.csv、migration_terminals.jsonl 和 decisions.jsonl。没有实际等待转移时，不能把 B−A 的运行波动视为等待转移的贡献。',
        'B−A 与 C−B 的逐轮差值见 paired_round_differences.csv。负差表示延迟下降。预实验每个负载只有一次配对，不能据此声称稳定收益或统计显著。正式五种子结果才允许按种子计算均值和不确定区间。',
        'C 选择的迁移对象、当时源端其他请求、目标端已有请求均按同 request_id 提取 B 对应结果；这是依赖 C 决策的事后分组，不是独立随机对照。见 paired_B_groups.csv。',
        '是否存在机会：见 opportunity_summary.json 中运行数差的持续时间、持续差异片段和逐次拒绝原因。零次迁移本身不能证明没有机会，也不能证明迁移无价值。',
        '状态曲线同时展示 running、waiting、KV 占用和近期 EngineCore 活跃步耗时。步耗时包含引擎执行与输出处理，不等同于纯 GPU kernel 时间。传输量若原日志未实测则保持缺失，不用 KV 大小冒充网络传输量。',
        '主 E2E 分位以所有已发送请求为分母；失败和超时不删除，有限完成时间覆盖不足相应分位时填空。TTFT 表是完成请求的条件分布，完成率需同时阅读。TPOT=(客户端完成时间−首个非空输出块时间)/(输出token数−1)，是客户端均值口径。SSE 间隔称输出块间隔。',
        '有输出 token IDs 或 finish_reason 不一致时，同时报告输出工作量差异，不能将全部延迟差归于迁移。GPU 功率仅作辅助观测，不代表服务器或机房总用电。',
        '结论范围：只适用于这里的模型、两张 GPU、负载和冻结的简单控制规则，不能外推所有运行中迁移或跨数据中心系统。']
    (root/'REPORT.md').write_text('\n\n'.join(report)+'\n',encoding='utf-8')
    labels=dict(round='轮次',phase='阶段',scheme='方案',valid='服务期有效',requests='请求数',completed='完成数',completion_rate='完成率',failure_rate='失败率',failed='失败',unfinished='未完成',e2e_p50_s='E2E p50秒',e2e_p95_s='E2E p95秒',e2e_p99_s='E2E p99秒',ttft_p95_s='TTFT p95秒',waiting_committed='等待转移完成',kv_completed='KV迁移完成',kv_cancelled='KV取消',kv_failed='KV失败',kv_not_dispatched='KV未发起',max_running_gap='最大运行数差')
    def cell(key,value):
        if value is None:return '缺失'
        if key in ('completion_rate','failure_rate'):return f'{value:.1%}'
        if isinstance(value,float):return f'{value:.3f}'
        return str(value)
    headers=list(labels)
    table='<tr>'+''.join('<th>'+html.escape(labels[k])+'</th>' for k in headers)+'</tr>'
    for row in rows:table+='<tr>'+''.join('<td>'+html.escape(cell(k,row.get(k,'')))+'</td>' for k in headers)+'</tr>'
    content='<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>运行中迁移增量收益</title><style>body{font:15px system-ui;margin:24px;color:#152133}table{border-collapse:collapse;font-size:13px}th,td{border:1px solid #bdc8d8;padding:7px}th{background:#eaf0f9}p{max-width:1100px;line-height:1.65}img{max-width:1100px;width:100%}</style><h1>运行中迁移增量收益实验</h1>'
    content+=''.join('<p>'+re.sub(r'\*\*(.*?)\*\*',r'<strong>\1</strong>',html.escape(p))+'</p>' for p in concise[1:])
    content+='<p><a href="summary.csv">逐轮CSV</a> · <a href="paired_round_differences.csv">种子配对</a> · <a href="request_metrics.csv">请求记录</a> · <a href="migration_sessions.csv">迁移会话</a> · <a href="FINDINGS.md">文字摘要</a></p><div style="overflow:auto"><table>'+table+'</table></div>'
    if all_groups:
        columns=dict(comparison='配对',group='C决策后分组',count='组内请求数',paired_complete='完整配对数',mean_C_minus_B_e2e_s='平均C−B秒',B_p95_s='B组p95秒',C_p95_s='C组p95秒',output_mismatches='输出不一致数')
        content+='<h2>同ID事后分组（可重叠，不作为独立随机对照）</h2><table><tr>'+''.join('<th>'+v+'</th>' for v in columns.values())+'</tr>'
        for group in all_groups:content+='<tr>'+''.join('<td>'+html.escape(cell(k,group.get(k,'')))+'</td>' for k in columns)+'</tr>'
        content+='</table>'
    if (root/'paired_seed_p95.png').exists():content+='<h2>正式种子配对</h2><img src="paired_seed_p95.png">'
    for folder in sorted(root.iterdir()):
        if folder.is_dir() and (folder/'state_timeline.png').exists():
            content+='<details><summary>'+html.escape(folder.name)+'：状态与辅助GPU功率</summary><img loading="lazy" src="'+folder.name+'/state_timeline.png">'
            if (folder/'gpu_power.png').exists():content+='<img loading="lazy" src="'+folder.name+'/gpu_power.png">'
            content+='</details>'
    (root/'report.html').write_text(content+'</html>',encoding='utf-8')
    print(json.dumps(dict(rounds=len(rows),correctness_passed=passed,failure=bool(failure))))

if __name__=='__main__':main()
