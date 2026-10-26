"""CPU-only L-v2 report and integrity/ownership audit after measurement ends."""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import socket
import tarfile
import time
import psutil
from two_gpu_live_report import analyze, load, obj, save, table
from two_gpu_formal_report import enrich
from two_gpu_live_collect import collect
from two_gpu_live_exit_audit import main as audit_exit
from two_gpu_formal_finalize import scoped_trace

LABELS = {'B': '自然排空', 'C': 'KV迁移', 'D': 'token_stop', 'E': 'token_live'}


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def finalize(root, archive=None):
    complete = obj(root / 'completed.json')
    if not complete and not (root / 'blocked.json').exists():
        raise RuntimeError('Experiment still running')
    base = root / 'L'
    collect(base)
    audit_exit(base)
    page = ['<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>L-v2长输入补充实验</title><style>body{font:16px system-ui;max-width:1350px;margin:30px auto;padding:0 24px;color:#1c293b}table{border-collapse:collapse;width:100%;margin:18px 0}td,th{padding:8px;border:1px solid #ccd5df;overflow-wrap:anywhere}th{background:#eef3f8}img{max-width:100%}pre{white-space:pre-wrap}</style><h1>L-v2长输入负载：排空与功率响应</h1>']
    plan = obj(root / 'supplement_plan.json')
    page.append('<p>原L三个候选均容量不足。本补充版本源候选0.35、0.25、0.15请求/秒，背景保持0.3。原A/H结果和L失败证据保留，补充结果独立报告。模型、4GiB KV、temperature=0、正常EOS和4096输出预算沿用上一轮；每单元一次，主发送600秒、t0=300秒、最多600秒尾部等待。测量期间源/目标模型及API保持存活，测量截止后额外30秒观察和退出功率不计收益。</p>')
    page.append(table(['版本指标', '值'], [[k, v] for k, v in plan.items() if k != 'conditional_coverage']))
    page.append(table(['共同配置', '值'], [[k, v] for k, v in (obj(root / 'effective_common_config.json') or {}).items() if k != 'gpu_authorization']))
    cal = obj(base / 'capacity_calibration.json') or []
    calibration_summaries = [obj(base / ('calibration_' + str(r['candidate'])) / 'summary.json') or {} for r in cal]
    breakdown = []
    for r in cal:
        c = r['capacity']
        latency_checks = {}
        for key in ('ttft_s', 'e2e_s'):
            v = c.get('latency', {}).get(key, {})
            early = v.get('early', {}).get('p95')
            late = v.get('late', {}).get('p95')
            threshold = max(early * 2, early + 1) if early is not None else None
            latency_checks[key] = dict(early_p95=early, late_p95=late, threshold=threshold,
                passed=late is not None and threshold is not None and late <= threshold)
        breakdown.append(dict(candidate=r['candidate'],
            queue_slope_passed=c.get('queue_slope_per_s', float('inf')) <= .01,
            queue_mean_passed=c.get('late_queue_mean', float('inf')) - c.get('early_queue_mean', 0) <= 2,
            completion_threshold=c.get('offered_rate', 0) * .85,
            completion_passed=c.get('late_completion_rate', 0) >= c.get('offered_rate', 0) * .85,
            zero_preemptions=c.get('preemptions') == 0, latency=latency_checks))
    save(root / 'capacity_rule_breakdown.json', breakdown)
    page.append('<h2>容量校准</h2>')
    page.append(table(['候选', '发送/完成', '错误数', '请求清理', 'KV恢复'], [[r['candidate'], [s.get('sent'), s.get('completed')], len(s.get('errors', [])), s.get('cleanup_safe'), s.get('kv_restored')] for r, s in zip(cal, calibration_summaries)]))
    page.append(table(['候选', '源/背景请求每秒', 'waiting斜率每秒', '早/晚waiting均值', '晚完成率/到达率', '抢占次数', '通过'], [[r['candidate'], [r['source_rate'], r['background_rate']], r['capacity'].get('queue_slope_per_s'), [r['capacity'].get('early_queue_mean'), r['capacity'].get('late_queue_mean')], [r['capacity'].get('late_completion_rate'), r['capacity'].get('offered_rate')], r['capacity'].get('preemptions'), r['capacity']['stable']] for r in cal]))
    page.append(table(['候选', '延迟', '早段p95秒', '晚段p95秒'], [[r['candidate'], label, r['capacity'].get('latency', {}).get(key, {}).get('early', {}).get('p95'), r['capacity'].get('latency', {}).get(key, {}).get('late', {}).get('p95')] for r in cal for key, label in [('ttft_s', 'TTFT'), ('e2e_s', '完成延迟')]]))
    page.append(table(['候选', '队列斜率达标', '队列均值达标', '完成率门槛/达标', 'TTFT增长达标', '完成延迟增长达标', '无抢占'], [[x['candidate'], x['queue_slope_passed'], x['queue_mean_passed'], [x['completion_threshold'], x['completion_passed']], x['latency']['ttft_s']['passed'], x['latency']['e2e_s']['passed'], x['zero_preemptions']] for x in breakdown]))
    page.append('<p>稳定性沿用60—180秒/180—300秒窗口：队列斜率≤0.01请求/秒、均值增长≤2、晚完成率≥到达率85%、晚p95≤max(早p95×2,早p95+1秒)，无错误与抢占。不是仅等待全部请求完成。选定首个通过候选后冻结完整开环轨迹，四方案共用请求标识、输入token IDs、计划到达时刻及生成配置。</p>')
    checks, unclassified, input_checks, mappings, coverage_acceptance = [], [], [], [], []
    total = paired = 0
    prompts = load(root / 'inputs/prompts.jsonl')
    for prefix, title, trace_name, comparison_name in [('main_', '600秒主实验', 'frozen_main_trace.json', 'paired_output_comparisons.json'), ('coverage_', '独立固定16条长历史覆盖', 'frozen_coverage_trace.json', 'coverage_paired_output_comparisons.json')]:
        folders = [base / (prefix + mode) for mode in 'BCDE' if (base / (prefix + mode) / 'summary.json').exists()]
        if not folders:
            continue
        page.append('<h2>' + title + '</h2>')
        if prefix == 'coverage_':
            page.append('<p>16条固定2048—4096 token原提示词同时发S，D继续0.3请求/秒背景；至少8条源请求decode才触发，最多等60秒。此场景与主实验分开统计，不强制输出长度，不关闭EOS。先运行自然排空验证触发可执行。固定批量触发条件要求源至少8条decode；继承的主L判据还包含目标至少4条running，原值保留，独立批量场景按自己的触发规则另报。</p>')
        trace = obj(base / trace_name) or []
        expected = {r['request_id']: r for r in trace}
        comparison = obj(base / comparison_name) or {}
        rows = comparison.get('comparisons', [])
        paired += len(rows)
        values = {}
        for folder in folders:
            scoped_trace(folder)
            a = enrich(folder, analyze(folder))
            if prefix == 'coverage_':
                observed = a.get('coverage') or {}
                coverage_acceptance.append(dict(round=folder.name, trigger_observed=bool(a['timing'].get('t0_monotonic_s')),
                    source_decoding=a['t0_source_decoding'], target_running=observed.get('target_running'),
                    independent_fixed_batch_passed=bool(a['timing'].get('t0_monotonic_s') and a['t0_source_decoding'] >= 8),
                    inherited_main_L_coverage=observed,
                    criterion='fixed batch trigger requires >=8 source decoding within 60s; target background .3/s remains unchanged'))
            values[folder.name[-1]] = a
            clients = load(folder / 'request_results.jsonl')
            total += sum(r.get('completed', False) for r in clients)
            mode = folder.name[-1]
            mapping = {r['role']: r for r in obj(folder / 'gpu_mapping.json') or []}
            config = dict(obj(root / 'effective_common_config.json'), gpus=[mapping[r]['gpu_index'] for r in 'SD'], gpu_uuid_by_role={r: mapping[r]['gpu_uuid'] for r in 'SD'}, migration_mode=obj(folder / 'round_config.json')['migration_mode'])
            save(folder / 'effective_round_config.json', config)
            mappings.append(dict(round=folder.name, gpus=config['gpus'], uuids=config['gpu_uuid_by_role']))
            for c in clients:
                planned = expected.get(c['request_id'])
                prompt = prompts[c['prompt_index']]
                input_checks.append(dict(round=folder.name, request_id=c['request_id'], present=planned is not None, prompt_index=bool(planned and planned['prompt_index'] == c['prompt_index']), input_sha=c['input_sha256'] == prompt['input_sha256'], input_length=c['input_tokens'] == prompt['input_tokens'], budget=len(c['token_ids']) <= 4096, context=prompt['input_tokens'] + 4096 <= 9216))
            matched = [r for r in rows if r['mode'] == mode]
            checks.append(dict(round=folder.name, planned=len(trace), recorded=len(clients), ids_exact=collections.Counter(r['request_id'] for r in clients) == collections.Counter(expected.keys()), completed=sum(r.get('completed', False) for r in clients), sse_complete=all(r.get('sse_done') for r in clients), paired=len(matched), paired_errors=sum(r['first_token'] is not None or not all(r[k] for k in ('text_equal', 'finish_equal', 'input_equal')) for r in matched), functional=a['functional_checks'], coverage=a.get('coverage'), exit_fault=bool(a.get('exit_phase_audit'))))
            for attempt in a['attempts'] + a['kv_attempts']:
                if attempt['state'] == 'unclassified':
                    unclassified.append(dict(round=folder.name, request_id=attempt['request_id'], attempt_id=attempt.get('attempt_id') or attempt.get('attempt_ids'), events=attempt['events'], classification='unconfirmed; client completion does not prove migration terminal state'))
        def kj(value):
            return value / 1000 if value is not None else None
        page.append(table(['方案', '发送/完成', '源排空秒', '真实接管', '覆盖达标', 't0源decode/目标running', '后120秒两卡能耗kJ', '整批两卡能耗kJ'], [[LABELS[m], [a['summary']['sent'], a['summary']['completed']], a['drain'].get('source_compute_requests_empty_s'), a['real_token_takeovers'] if m in 'DE' else a['unique_kv_takeovers'] if m == 'C' else 0, (a.get('coverage') or {}).get('passed'), [a['t0_source_decoding'], (a.get('coverage') or {}).get('target_running')], kj((a.get('power') or {}).get('energy', {}).get('post120', {}).get('sum_joules')), kj((a.get('whole_work_energy') or {}).get('sum_joules'))] for m, a in values.items()]))
        image = 'main_power.png' if prefix == 'main_' else 'coverage_power.png'
        if (base / image).exists():
            page.append('<img src="L/' + image + '" alt="源卡、目标卡及两卡合计功率">')
        page.append(table(['方案', '对照数', '输出差异数', 'Core/API/预算/SSE', '资源恢复', '迁移终态计数', '旧strict validity'], [[LABELS[m], len([r for r in rows if r['mode'] == m]), len([r for r in comparison.get('errors', []) if r['mode'] == m]), a['functional_checks'].get('core_api_budget_sse'), a['functional_checks'].get('resources_restored'), a['migration_outcome_counts'], a['original_strict_validity']] for m, a in values.items()]))
        for m, a in values.items():
            page.append('<h3>' + LABELS[m] + '</h3>')
            page.append(table(['t0源request_id', '原输入tokens', '已生成tokens', '已计算KV tokens', 'KV块', '状态'], [[x['request_id'], x.get('input_tokens'), x.get('generated_tokens'), x.get('computed_tokens'), x.get('kv_blocks'), x.get('state')] for x in a['inflight']]))
            page.append('<p>输入、生成历史和已计算KV长度分别记录；已生成的最后一个token通常尚未作为后续模型输入计算KV，不能用输入加生成长度代替已计算KV长度。迁移各阶段目标覆盖进度、暂停、接管及源释放证据保留在本单元formal_analysis.json及原始事件中。</p>')
            power = a.get('power') or {}
            baseline_begin = a['timing'].get('t0_monotonic_s')
            baseline_full = bool(baseline_begin and baseline_begin - a['timing']['start_monotonic_s'] >= 60)
            if prefix == 'coverage_' and not baseline_full:
                page.append('<p>本覆盖场景触发早于60秒，t0前完整60秒功率基线缺失；P0和相对下降指标仅为已有样本观察，不能用于完整60秒基线比较。此限制不修改原始数据，主实验功率基线另行核查。</p>')
            page.append(table(['功率指标', '值'], [['P0源功率W', power.get('P0_w')], ['功率采集质量', a['power_quality']]]))
            page.append(table(['t0后秒', '源功率W', '下降W', '下降百分比'], [[k, v['source_w'], v['drop_w'], v['drop_pct']] for k, v in power.get('points', {}).items()]))
            page.append(table(['下降阈值', '首次窗口开始秒', '连续5秒确认时刻秒'], [[str(float(k)*100)+'%', v.get('first_window_start_s') if v else '未达标', v.get('confirmed_s') if v else '未达标'] for k, v in power.get('thresholds', {}).items()]))
            page.append(table(['能耗窗口', '源J', '目标J', '两卡J'], [[k, v['S']['joules'], v['D']['joules'], v['sum_joules']] for k, v in power.get('energy', {}).items()]))
            page.append(table(['其他指标', '值'], [['整批工作时长秒', a.get('whole_work_duration_s')], ['源排空及API流结束', a['drain']], ['发送滞后', a['summary']['send_lag']], ['t0在途数量', a['inflight_count']], ['输入加生成历史长度分布', a.get('t0_total_history_distribution')], ['目标t0前60秒负载', a.get('target_pre_t0_load')], ['各请求类性能', a['performance_by_class']], ['背景请求分组', a.get('target_background_cohorts')], ['原始404及取消计数', a['raw_counts']], ['退出native故障', bool(a.get('exit_phase_audit'))]]))
            if a.get('exit_phase_audit'):
                page.append('<p><a href="L/' + prefix + m + '/exit_phase_audit.json">本轮原始退出故障及测量截止关联证据</a>单独保存；不授予正常卸载资格。</p>')
            page.append(table(['attempt_id', '模式/状态', 'token载荷B', '发送端控制帧B', 'KV载荷B', '排队/重算秒', '首次/增量重算tokens', '后台轮数', '最终暂停/接管/首新token/源释放时刻'], [[t['attempt_id'], [t['requested_mode'], t['executed_mode'], t['state']], t['token_payload_bytes'], sum(ev.get('control_bytes', 0) for ev in t['events'] if ev['event'] == 'wire_send'), t['kv_payload_bytes'], [t['queue_s'], t['recompute_active_s']], [t['initial_rebuild_tokens'], t['incremental_recompute_tokens']], t['background_rounds'], [t['final_pause_monotonic_s'], t['takeover_monotonic_s'], t['first_new_token_monotonic_s'], t['source_release_monotonic_s']]] for t in a['attempts']]))
            page.append(table(['KV批次指标', '值'], [['实际native张量载荷B', sum(t['batch_kv_tensor_payload_bytes'] for t in a['native_kv_bytes'])], ['迁移选择与执行顺序', a['migration_selection_order']], ['暂停边界客户端chunk间隔', [t.get('client_switch_chunk_gap_s') for t in a['attempts'] + a['kv_attempts']]]]))
    save(root / 'formal_delivery_checks.json', checks)
    save(root / 'fixed_batch_coverage_acceptance.json', coverage_acceptance)
    save(root / 'actual_round_gpu_mapping.json', mappings)
    save(root / 'current_unclassified_attempt_audit.json', unclassified)
    save(root / 'input_identity_checks.json', input_checks)
    input_bad = [r for r in input_checks if not all(r[k] for k in ('present', 'prompt_index', 'input_sha', 'input_length', 'budget', 'context'))]
    save(root / 'input_identity_summary.json', dict(checked=len(input_checks), errors=input_bad))
    baseline = obj(root / 'minimal_preflight.json')
    integrity = [dict(path=r['path'], expected=r['sha256'], actual=sha(Path(r['path']))) for r in baseline['checks']]
    save(root / 'serving_native_integrity_final.json', integrity)
    model_integrity = [dict(path='/data/models/Qwen2.5-7B-Instruct/' + name, expected=expected,
        actual=sha(Path('/data/models/Qwen2.5-7B-Instruct') / name))
        for name, expected in obj(root / 'model_configuration_hashes.json').items()]
    save(root / 'model_configuration_integrity_final.json', model_integrity)
    if input_bad or any(r['expected'] != r['actual'] for r in integrity):
        raise RuntimeError('Final input or serving/native integrity mismatch')
    if any(r['expected'] != r['actual'] for r in model_integrity):
        raise RuntimeError('Model/tokenizer configuration changed')
    ports = []
    for port in obj(root / 'effective_common_config.json')['ports']:
        try:
            with socket.socket() as s:
                s.bind(('127.0.0.1', port))
            ports.append(dict(port=port, available=True))
        except OSError as exc:
            ports.append(dict(port=port, available=False, error=repr(exc)))
    owned = []
    for p in root.rglob('owned_processes.json'):
        owned.extend(obj(p))
    for row in load(root / 'owned_tree_identity_samples.jsonl'):
        owned.extend(row['processes'])
    owned.extend(obj(root / 'helper_identities.json') or [])
    live = {}
    for identity in owned:
        try:
            p = psutil.Process(identity['pid'])
            if p.pid != os.getpid() and p.create_time() == identity['created'] and p.status() != psutil.STATUS_ZOMBIE:
                live[p.pid] = dict(pid=p.pid, created=p.create_time(), command=p.cmdline())
        except psutil.NoSuchProcess:
            pass
    cleanup = dict(verified_live_owned=list(live.values()), ports=ports, old_finish_audit=obj(root / 'finish_audit.json'), normal_unload_qualified=False, wall_ns=time.time_ns())
    save(root / 'final_cleanup_verification.json', cleanup)
    if coverage_acceptance:
        page.append('<h2>固定批量覆盖的独立判定</h2>' + table(['单元', '触发观测', '源decode', '目标running', '固定批量触发达标', '继承主L覆盖'], [[x['round'], x['trigger_observed'], x['source_decoding'], x['target_running'], x['independent_fixed_batch_passed'], x['inherited_main_L_coverage'].get('passed')] for x in coverage_acceptance]))
    page.append('<h2>释放、版本与限制</h2>' + table(['清理指标', '值'], [['本实验残留', cleanup['verified_live_owned']], ['端口检查', ports], ['正常卸载资格', False], ['未分类尝试数', len(unclassified)], ['输入核查数/错误数', [len(input_checks), len(input_bad)]], ['完成正式单元', (complete or {}).get('completed_formal_rounds')], ['累计请求完成/输出对照数（主与覆盖分开详见上表）', [total, paired]]]))
    page.append('<p>请求、迁移会话、源记录和临时KV清理以及空闲块恢复基线均保留逐次记录。终态ledger不按临时请求判断泄漏。GPU释放证据见finish_audit.json，辅助进程身份见helper_identities.json。只管理本实验PID+create_time+cmdline核实的进程；未终止其他任务。旧FREEZE_MIGRATION_VALIDATION_REVOKED.json保留，正常卸载资格仍为否。native退出故障与完整测量分开记录，父进程返回0不能证明native正常退出。</p>')
    page.append('<p>200ms是NVML读取请求周期，硬件刷新周期不由此推定。used/reserved显存采用NVML v2。源排空、最后计算和原API输出流结束分别记录，TCP FIN时刻缺失。token实际应用载荷与发送端控制帧分别统计，KV张量批次载荷不按每个请求重复分摊；KV控制消息字节、额外ZMQ路由帧、网络头及总网络流量缺失。客户端chunk间隔不能称逐token ITL。后台轮数为0时不宣称多轮追赶已覆盖。所有权和终态不明保留未分类。各方案不同物理卡组合的差异不能只归因迁移方式。</p>')
    interruptions = []
    for file in (root / 'interruptions').rglob('summary.json'):
        summary = obj(file)
        rows = load(file.parent / 'request_results.jsonl')
        interruptions.append(dict(path=str(file.parent.relative_to(root)), valid=False, planned=summary['records'],
            sent=summary['sent'], completed=summary['completed'],
            protected_cancels=sum(r.get('error') == 'user_cancel_after_measurement_cutoff' for r in rows),
            errors=dict(collections.Counter(r.get('error') for r in rows if r.get('error'))),
            critical=summary['critical'], cleanup_safe=summary['cleanup_safe'], kv_restored=summary['kv_restored']))
    save(root / 'excluded_interrupted_attempts.json', interruptions)
    if interruptions:
        page.append('<h2>污染尝试：保留并排除</h2>' + table(['尝试目录', '有效', '计划/发送/完成', '保护取消', '原因', '请求清理', 'KV恢复'], [[x['path'], x['valid'], [x['planned'], x['sent'], x['completed']], x['protected_cancels'], [c['kind'] for c in x['critical']], x['cleanup_safe'], x['kv_restored']] for x in interruptions]))
        page.append('<p>外部计算进入GPU后，先保存测量截止，再显式取消未完成请求并只清理已确认身份的本实验进程；原始错误/取消/未发送记录完整保留。污染轮次不计成功，空闲复核后以同一冻结轨迹重跑。未终止外部进程。即使部分早期窗口发生在干扰前，也不将整轮污染数据计为有效四方案结果。</p>')
    repair = obj(root / 'guard_reader_repair.json')
    if repair:
        page.append('<h2>错误监视器文件轮换修复</h2>' + table(['记录指标', '值'], [['原监视器PID', repair['old_identity']['pid']], ['原日志', repair['old_log']], ['接续监视器源码', repair['new_source']], ['源码SHA256', repair['sha256']], ['测量进程重启', repair['measurement_processes_restarted']], ['请求轨迹改变', repair['workload_changed']]]))
        page.append('<p>污染尝试归档后，同名请求日志重建，原错误监视器按路径保留旧读取偏移，在解析行时退出。修复版按inode/文件长度重置偏移，保留未完成行和待确认失败，仅跟踪当前尝试，启动后重新扫描现有请求日志。主控制器、NVML与引擎观察一直运行，原始请求数据和Core/API核查保留；该监视器修复不改变到达、输出或迁移参数。原异常与新PID/create_time/命令完整保留。</p>')
    page.append('<p>实际代码与依赖哈希见execution_sources_manifest.json、version_manifest.json、dependencies.json和serving_native_integrity_final.json。各轮启动命令/环境见launch.jsonl，真实native加载路径见native_loaded.json。回退只需停用本补充控制脚本；服务/native未修改，无须重装。旧结果不覆盖。结论只限本地两卡当前实现，不宣称生产规模、跨地域、全系统节能或完整ServerlessLLM复现。</p></html>')
    (root / 'report.html').write_text(''.join(page), encoding='utf-8')
    main_checks = [r for r in checks if r['round'].startswith('main_')]
    main_coverage_passed = len(main_checks) == 4 and all((r.get('coverage') or {}).get('passed') for r in main_checks)
    status = 'completed' if complete and complete['fully_completed'] else 'capacity_limited' if complete else 'blocked'
    if status == 'completed' and not main_coverage_passed:
        status = 'execution_completed_with_main_coverage_limits'
    save(root / 'delivery_status.json', dict(status=status, completed=(complete or {}).get('completed_formal_rounds', []), completed_requests=total,
        calibration_completed_requests=sum(s.get('completed', 0) for s in calibration_summaries),
        calibration_candidates_evaluated=len(cal), main_coverage_passed=main_coverage_passed,
        fixed_batch_coverage_passed=all(r['independent_fixed_batch_passed'] for r in coverage_acceptance) if coverage_acceptance else None,
        paired_comparisons=paired, unclassified=len(unclassified), serving_native_unchanged=True, models_released=not live, normal_unload_qualified=False))
    if archive:
        if archive.exists():
            raise RuntimeError('Archive exists; preserve it')
        manifest = [dict(path=str(p.relative_to(root)), bytes=p.stat().st_size, sha256=sha(p)) for p in sorted(root.rglob('*')) if p.is_file() and not p.is_symlink() and '__pycache__' not in p.parts and p.name != 'artifact_manifest.json' and p.suffix != '.gz']
        save(root / 'artifact_manifest.json', manifest)
        with tarfile.open(archive, 'w:gz', compresslevel=3) as tar:
            tar.add(root / 'artifact_manifest.json', arcname=root.name + '/artifact_manifest.json')
            for row in manifest:
                tar.add(root / row['path'], arcname=root.name + '/' + row['path'], recursive=False)
        Path(str(archive) + '.sha256').write_text(sha(archive) + '  ' + archive.name + '\n')
    print(json.dumps(obj(root / 'delivery_status.json'), ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--archive')
    args = parser.parse_args()
    finalize(Path(args.root), Path(args.archive) if args.archive else None)
