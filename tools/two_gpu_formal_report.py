"""Report the frozen A/L/H experiment, preserving failed/missing cells."""
import argparse
import collections
import csv
import json
from pathlib import Path
import statistics
from two_gpu_live_report import analyze, obj, load, save, table, fmt, energy, stats
from two_gpu_live_exit_audit import main as audit_exit

LABELS={'A':'A 常规混合','L':'L 长输入','H':'H 目标较忙'}
MODES={'B':'自然排空','C':'KV迁移','D':'token_stop','E':'token_live'}

def enrich(folder,value):
    clients=load(folder/'request_results.jsonl');timeline=value['timing'];t0=timeline.get('t0_monotonic_s')
    if not t0:return value
    with (folder/'power.csv').open() as f:power=list(csv.DictReader(f))
    end=timeline.get('work_completion_monotonic_s')
    value['whole_work_energy']={r:energy(power,timeline['start_monotonic_s'],end,r) for r in 'SD'} if end else None
    value['whole_work_duration_s']=end-timeline['start_monotonic_s'] if end else None
    if value['whole_work_energy']:
        x=value['whole_work_energy'];value['whole_work_energy']['sum_joules']=sum(x[r]['joules'] for r in 'SD') if all(x[r]['joules'] is not None for r in 'SD') else None
    snap=obj(folder/'t0_snapshot.json') or {};target=(snap.get('D') or {}).get('requests',[])
    engine_to_client={(r.get('response_id') or '')+'-0':r for r in clients}
    active={r['request_id'] for r in target if r['state']=='running'}
    original=[engine_to_client[rid] for rid in active if rid in engine_to_client and engine_to_client[rid]['initial_role']=='D']
    later=[r for r in clients if r['initial_role']=='D' and r['actual_send_monotonic_s']>=t0]
    def metrics(rows):
        return dict(count=len(rows),request_ids=[r['request_id'] for r in rows],ttft=stats([r['ttft_s'] for r in rows]),
            completion=stats([r['e2e_s'] for r in rows]),remaining_after_t0=stats([r['finish_monotonic_s']-t0 for r in rows]))
    value['target_background_cohorts']=dict(existing_at_t0=metrics(original),arrived_after_t0=metrics(later))
    samples=load(folder/'instance_samples.jsonl');before=[s for s in samples if t0-60<=s['monotonic_s']<t0 and s.get('D')]
    after=[s for s in samples if s['monotonic_s']>=t0 and s.get('S')]
    last_active=max((i for i,s in enumerate(after) if any(not r.get('paused',False) for r in s['S']['requests'])),default=-1)
    sustained=after[last_active+1] if last_active+1<len(after) else None
    value['drain']['source_compute_requests_empty_s']=sustained['monotonic_s']-t0 if sustained else None
    value['source_drain_definition']='First observed compute-empty boundary after the final nonempty source observation; execution boundary separately retained'
    output=[r for r in load(folder/'outputs_D.jsonl') if timeline['start_wall_ns']+(t0-timeline['start_monotonic_s']-60)*1e9<=r['time_ns']<timeline['start_wall_ns']+(t0-timeline['start_monotonic_s'])*1e9]
    value['target_pre_t0_load']=dict(running_mean=statistics.mean(s['D']['running'] for s in before) if before else None,
        waiting_mean=statistics.mean(s['D']['waiting'] for s in before) if before else None,
        generated_tokens_per_s=sum(len(r['tokens']) for r in output)/60,
        power_w=statistics.mean(float(r['power_w']) for r in power if r['role']=='D' and r['power_w'] and t0-60<=float(r['monotonic_s'])<t0))
    value['coverage']=obj(folder/'timeline.json').get('coverage')
    value['t0_total_history_distribution']=stats([r['input_tokens']+r['generated_tokens'] for r in value['inflight']])
    value['actual_output_length_distribution']=stats([len(r['token_ids']) for r in clients])
    value['maximum_observed_migration_tasks']=max((s['S'].get('migration_tasks',0) for s in samples if s.get('S')),default=None)
    value['exit_phase_audit']=obj(folder/'exit_phase_audit.json')
    # Core ownership timestamps remain available; SSE does not identify each chunk's engine.
    value['exact_last_source_to_first_target_client_chunk_gap']=None
    value['client_gap_scope']='Existing pause-boundary chunk-gap estimate only; exact engine attribution missing'
    save(folder/'formal_analysis.json',value)
    return value

def report(root,reuse=False):
    html=['<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>三种负载排空与功率实验</title><style>body{font:16px system-ui;max-width:1300px;margin:35px auto;padding:0 24px;color:#1c293b}table{border-collapse:collapse;width:100%;margin:18px 0}td,th{padding:8px;border:1px solid #ccd5df;overflow-wrap:anywhere}th{background:#eef3f8}img{max-width:100%}pre{white-space:pre-wrap}</style>',
        '<h1>两卡、三种负载场景的排空与功率响应</h1>',
        '<p>按冻结方案执行A/L/H，每场景每方案一次，计划12轮。模型和服务在测量期间保持存活；源API保留原连接并转发输出。已有Barex退出故障单独记录，本次不授予正常卸载资格。旧先导结果不计入正式轮次。</p>']
    all_results={};completed=[]
    for scenario in 'ALH':
        base=root/scenario;html.append('<h2>'+LABELS[scenario]+'</h2>')
        if not base.exists():html.append('<p>尚未运行。</p>');continue
        audit_exit(base)
        frozen=obj(base/'frozen_load.json');cal=obj(base/'capacity_calibration.json') or []
        html.append(table(['容量候选','源请求每秒','背景请求每秒','队列斜率','晚段完成率','抢占','通过'],[
            [r['candidate'],r['source_rate'],r['background_rate'],r['capacity'].get('queue_slope_per_s'),r['capacity'].get('late_completion_rate'),r['capacity'].get('preemptions'),r['capacity']['stable']] for r in cal]))
        if obj(base/'capacity_blocked.json'):html.append('<p>三个冻结候选均未得到容量配置；该场景未运行正式轮次，保留全部校准证据。</p>');continue
        if frozen:html.append(table(['冻结指标','值'],[['源/背景请求每秒',[frozen['source_rate'],frozen['background_rate']]],['发送秒/t0秒',[600,300]],['实际轨迹请求数',len(obj(base/'frozen_main_trace.json') or [])],['顺序',obj(base/'scheme_order.json')]]))
        cells={}
        for mode in 'BCDE':
            folder=base/('main_'+mode)
            if not (folder/'summary.json').exists():continue
            value=obj(folder/'formal_analysis.json') if reuse and (folder/'formal_analysis.json').exists() else enrich(folder,analyze(folder))
            cells[mode]=value;completed.append(scenario+'_'+mode)
        all_results[scenario]=cells
        b=cells.get('B');rows=[]
        for m in 'BCDE':
            a=cells.get(m)
            if not a:rows.append([MODES[m],'未完成',None,None,None,None,None]);continue
            p=a['power'];whole=(a.get('whole_work_energy') or {}).get('sum_joules')
            rows.append([MODES[m],[a['summary']['sent'],a['summary']['completed']],a['drain'].get('source_compute_requests_empty_s'),
                a['real_token_takeovers'] if m in 'DE' else a['unique_kv_takeovers'] if m=='C' else '—',
                p['energy']['post120']['sum_joules']/1000 if p and p['energy']['post120']['sum_joules'] is not None else None,
                whole/1000 if whole is not None else None,a.get('whole_work_duration_s')])
        html.append(table(['方案','发送/完成','源计算排空秒','真实接管','后120秒两卡能耗kJ','整批能耗kJ','整批时长秒'],rows))
        if (base/'main_power.png').exists():html.append('<img src="'+scenario+'/main_power.png" alt="源卡、目标卡与两卡合计功率">')
        if b:
            differences=[]
            for m in 'CDE':
                a=cells.get(m)
                if not a:continue
                def diff(x,y):return x-y if x is not None and y is not None else None
                differences.append([MODES[m],diff(a['drain'].get('source_compute_requests_empty_s'),b['drain'].get('source_compute_requests_empty_s')),
                    diff(a['power']['energy']['post120']['sum_joules'],b['power']['energy']['post120']['sum_joules']),
                    diff((a.get('whole_work_energy') or {}).get('sum_joules'),(b.get('whole_work_energy') or {}).get('sum_joules'))])
            html.append(table(['相对本场景B','排空时间差秒','后120秒能耗差J','整批能耗差J'],differences))
        for mode,a in cells.items():
            html.append('<h3>'+MODES[mode]+'</h3>')
            p=a['power'];cov=a.get('coverage') or {}
            html.append(table(['t0覆盖指标','值'],[['源在途',a['inflight_count']],['源已decode',a['t0_source_decoding']],['目标running',cov.get('target_running')],['覆盖满足冻结条件',cov.get('passed')],['已生成长度分布',a['generated_history_distribution']],['输入加已生成历史长度分布',a['t0_total_history_distribution']],['实际输出长度分布',a['actual_output_length_distribution']],['观测最大前端迁移任务数',a['maximum_observed_migration_tasks']],['迁移终态',a['migration_outcome_counts']]]))
            html.append(table(['目标t0前60秒负载','值'],list(a.get('target_pre_t0_load',{}).items())))
            html.append(table(['背景请求类别','数量','TTFT均值/p95秒','完成延迟均值/p95秒','t0后剩余时间均值/p95秒'],[
                [name,v['count'],[v['ttft']['mean'],v['ttft']['p95']],[v['completion']['mean'],v['completion']['p95']],[v['remaining_after_t0']['mean'],v['remaining_after_t0']['p95']]] for name,v in a['target_background_cohorts'].items()]))
            html.append(table(['用户请求类别','数量','TTFT均值/p95秒','完成延迟均值/p95秒'],[[k,v['count'],[v['ttft_s']['mean'],v['ttft_s']['p95']],[v['completion_s']['mean'],v['completion_s']['p95']]] for k,v in a['performance_by_class'].items()]))
            if p:
                html.append(table(['t0后秒','源功率W','下降W','下降%'],[[k,v['source_w'],v['drop_w'],v['drop_pct']] for k,v in p['points'].items()]))
                html.append(table(['功率/能耗指标','值'],[['P0 W',p['P0_w']],*[[f'下降{float(k)*100:g}%首次/确认',v or '未达标'] for k,v in p['thresholds'].items()],
                    *[[label+'能耗J（源/目标/合计）',[x['S']['joules'],x['D']['joules'],x['sum_joules']]] for label,x in p['energy'].items()]]))
            html.append(table(['资源/有效性指标','值'],[['资源清空',a['summary']['cleanup_safe']],['KV恢复基线',a['summary']['kv_restored']],['旧strict validity',a['original_strict_validity']],['原始404',a['raw_counts']['kvt404']],['原始abort行',a['raw_counts']['abort_lines']],['测量关键错误',len(a['summary']['critical'])],['退出native故障',bool(a.get('exit_phase_audit'))],['正常卸载资格',False]]))
            html.append('<p>逐请求输出、迁移与缺失指标见 <a href="'+scenario+'/main_'+mode+'/formal_analysis.json">原始分析</a>。载荷字节不代表完整网络字节。客户端chunk并不直接标识输出引擎，现有暂停边界chunk间隔另存，精确最后源chunk到首个目标chunk间隔缺失。</p>')
            if a.get('exit_phase_audit'):html.append('<p>native退出故障证据见 <a href="'+scenario+'/main_'+mode+'/exit_phase_audit.json">退出时序</a>。父进程返回0不等于native正常退出；已知退出故障未继续修复。</p>')
    html.append('<h2>执行状态与记录</h2>'+table(['项目','值'],[['计划正式轮次',12],['已完成正式轮次',len(completed)],['完成列表',completed],['五种子追加实验',False],['正常卸载资格',False]]))
    html.append('<p>各场景独立展示，不平均为总体收益。后台增量追赶为0轮时如实保留；初始重建和最终补齐不计后台轮数。整批能耗使用各方案自身完成时刻，并同时报告时长；固定120秒能耗不能冒充整批节能。结论限于本地两卡及当前实现，不外推生产集群或跨地域。</p>')
    html.append('<p>冻结配置、派生种子、完整清单及启动命令按场景保存。旧未分类attempt的既有日志补审计见 <a href="prior_unclassified_audit.json">旧记录审计</a>，证据不足仍为无法确认。</p>')
    (root/'report.html').write_text(''.join(html),encoding='utf-8');save(root/'formal_analysis_index.json',dict(completed=completed,scenarios={k:{m:dict(drain=a['drain'],whole_work_energy=a.get('whole_work_energy'),power=a['power'],coverage=a.get('coverage')) for m,a in v.items()} for k,v in all_results.items()}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--reuse-analysis',action='store_true');a=p.parse_args();report(Path(a.root),a.reuse_analysis)
