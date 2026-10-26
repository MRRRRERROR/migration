"""CPU-only final evidence presentation after the finite frozen run has ended."""
import argparse
import collections
import hashlib
import html
import json
from pathlib import Path
import shutil
from two_gpu_live_report import load, obj, save, table, stats
from two_gpu_formal_report import report, enrich
from two_gpu_live_report import analyze


def scoped_trace(folder):
    """PID reuse must not attach older native records to a new round."""
    path = folder / 'native_attempt_trace.jsonl'
    timing = obj(folder / 'measurement_cutoff.json') or obj(folder / 'timeline.json')
    if not path.exists() or not timing:
        return False
    rows = load(path)
    begin = timing['start_wall_ns']
    end = begin + (timing['measurement_cutoff_monotonic_s'] - timing['start_monotonic_s']) * 1e9
    retained, dropped, unscoped = [], [], []
    for row in rows:
        stamp = row.get('timestamp_ns')
        if stamp is None and row.get('timestamp') is not None:
            stamp = row['timestamp'] * 1e9
        if stamp is None:
            unscoped.append(row)
            retained.append(row)
        elif begin <= stamp <= end:
            retained.append(row)
        else:
            dropped.append(row)
    if dropped:
        original = folder / 'native_attempt_trace_pid_only.jsonl'
        if not original.exists():
            shutil.copy2(path, original)
        path.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in retained), encoding='utf-8')
    save(folder / 'native_time_scope_audit.json', dict(
        begin_wall_ns=begin, end_wall_ns=end, retained=len(retained),
        excluded_pid_reuse_or_outside_window=len(dropped), missing_timestamp=len(unscoped),
        excluded_records=dropped, global_native_trace_modified=False))
    return bool(dropped)


def finalize(root):
    if not (root / 'completed.json').exists() and not (root / 'blocked.json').exists():
        raise RuntimeError('The experiment is still running; finalization is CPU-only after cutoff')
    mappings, details = [], []
    common = obj(root / 'effective_common_config.json') or {}
    for scenario in 'ALH':
        paired = obj(root / scenario / 'paired_output_comparisons.json') or {}
        comparisons = paired.get('comparisons', [])
        for mode in 'BCDE':
            folder = root / scenario / ('main_' + mode)
            if not (folder / 'summary.json').exists():
                continue
            if scoped_trace(folder):
                enrich(folder, analyze(folder))
            value = obj(folder / 'formal_analysis.json')
            mapping = obj(folder / 'gpu_mapping.json') or []
            by_role = {r['role']: r for r in mapping}
            effective = dict(common, gpus=[by_role[r]['gpu_index'] for r in 'SD'],
                gpu_uuid_by_role={r: by_role[r]['gpu_uuid'] for r in 'SD'},
                migration_mode=obj(folder / 'round_config.json')['migration_mode'],
                gpu_authorization='Initial any-two-idle authorization; later explicit human authorization for 0,2; actual mapping retained per round',
                effective_gpu_basis='actual per-round mapping; human authorization retained')
            save(folder / 'effective_round_config.json', effective)
            mappings.append(dict(round=f'{scenario}_{mode}', gpus=effective['gpus'], uuid=effective['gpu_uuid_by_role']))
            requests = load(folder / 'request_results.jsonl')
            expected = len(obj(root / scenario / 'frozen_main_trace.json') or [])
            rows = [r for r in comparisons if r['mode'] == mode]
            comparison_pass = (len(rows) == expected and not any(
                r['first_token'] is not None or not all(r[k] for k in ('text_equal', 'finish_equal', 'input_equal')) for r in rows)) if mode != 'B' else None
            attempts = value['attempts'] + value['kv_attempts']
            token_bytes = sum(r['token_payload_bytes'] for r in value['attempts'])
            control_observed = sum(r['control_application_frame_bytes'] for r in value['attempts'])
            control_bytes = sum(e.get('control_bytes', 0) for r in value['attempts'] for e in r['events'] if e['event'] == 'wire_send')
            native_bytes = sum(r['batch_kv_tensor_payload_bytes'] for r in value['native_kv_bytes'])
            actual_ids = [r['request_id'] for r in requests]
            planned_ids = [r['request_id'] for r in obj(root / scenario / 'frozen_main_trace.json') or []]
            item = dict(round=f'{scenario}_{mode}', planned=expected, recorded=len(requests),
                planned_ids_exact=collections.Counter(actual_ids) == collections.Counter(planned_ids),
                paired_comparisons=len(rows), paired_output_pass=comparison_pass,
                all_sse_done=all(r.get('sse_done') for r in requests),
                finish_reasons=dict(collections.Counter(r.get('finish_reason') for r in requests)),
                errors=dict(collections.Counter(str(r.get('error')) for r in requests if r.get('error'))),
                token_application_payload_bytes=token_bytes,
                token_control_application_frame_bytes=control_bytes,
                token_control_send_receive_observation_bytes=control_observed,
                kv_batch_tensor_payload_bytes=native_bytes,
                token_mode_kv_payload_bytes=0 if mode in 'DE' else None,
                kv_per_request_payload_bytes=None, total_network_bytes=None,
                background_rounds=dict(collections.Counter(r['background_rounds'] for r in value['attempts'])),
                final_pause_chunk_gap_s=stats([r.get('client_switch_chunk_gap_s') for r in attempts]),
                migration_outcomes=value['migration_outcome_counts'],
                functional_checks=value['functional_checks'],
                source_drain=value['drain'], power_quality=value['power_quality'],
                send_lag_s=value['summary']['send_lag'],
                exit_fault=bool(value.get('exit_phase_audit')), normal_unload_qualified=False)
            details.append(item)
            save(folder / 'formal_delivery_checks.json', item)
    save(root / 'actual_round_gpu_mapping.json', mappings)
    save(root / 'formal_delivery_checks.json', details)
    report(root, reuse=True)
    page = (root / 'report.html').read_text(encoding='utf-8')
    config_labels = dict(model='模型', dtype='精度', context_length='上下文token上限', max_tokens='输出token上限',
        temperature='temperature', n='序列数n', batch_invariant='批次不变性', async_scheduling='异步调度',
        ignore_eos='ignore_eos', max_rounds='token_live后台最多轮数', final_gap_tokens='最终差距token阈值',
        timeout='单次token迁移超时秒', kv_cache_memory_bytes='每实例KV缓存字节',
        automatic_scaling='自动扩缩容', automatic_migration='其他自动迁移', fault_injection='故障注入')
    workload = obj(root / 'workload_manifest.json') or {}
    configuration = '<h2>冻结配置与请求池</h2>' + table(['配置项', '值'], [[label, common.get(key)] for key, label in config_labels.items()])
    configuration += table(['请求池指标', '值'], [[key, workload.get(name)] for key, name in (
        ('原数据SHA256', 'source_sha256'), ('完整池唯一提示数', 'pool_size'), ('长输入子池唯一提示数', 'long_pool_size'),
        ('实际模板后长度分布', 'length_distribution'), ('去重规则', 'dedup'), ('采样规则', 'sampling'))])
    configuration += ('<p>ShareGPT原始首条human文本使用当前Qwen聊天模板恰好一次，按最终token长度筛选；'
        '实际发送原token IDs，不再次套模板。输入与4096输出预算之和小于9216上下文上限。'
        '派生seed、角色独立开环泊松清单、实际输入哈希与计划/实际发送时刻完整保存。'
        '保留KVT_LEASE_TRACE=1、KVT_LEASE_RECHECK=1及既有物理lease/引用保护，KV包含额外D2H字节复核；比较的是当前整体实现。'
        'KV批量处理和token串行流程沿用原策略，没有为排名更改迁移实现。'
        '各轮命令和环境见launch.jsonl，实际加载native库及哈希见native_loaded.json；'
        '初始common配置卡号保持原记录，实际卡号以effective_round_config.json为准。'
        '<a href="minimal_preflight.json">唯一必要预检与实际导入路径</a>；<a href="dependencies.json">依赖版本</a>；'
        '<a href="version_manifest.json">代码版本</a>。</p>')
    page = page.replace('<h2>A 常规混合</h2>', configuration + '<h2>A 常规混合</h2>', 1)
    hardware = '<h2>实际GPU映射与污染尝试</h2>' + table(
        ['正式单元', '源物理GPU', '目标物理GPU', '源UUID', '目标UUID'],
        [[r['round'], *r['gpus'], r['uuid']['S'], r['uuid']['D']] for r in mappings])
    if (root / 'hardware_transition_01.json').exists():
        hardware += ('<p>A/B、A/E、A/D使用1、3。首个A/C尝试被外部计算进入GPU1污染，原始记录独立保留，未计作有效正式轮次。'
            '随后依据用户“1，3被占了你就用另外两个”的授权，使用0、2重跑同一A/C冻结轨迹，L/H沿用0、2。'
            'A/C相对A/B的功率差同时包含物理卡组合变化，不能仅归因迁移模式。未终止其他用户任务。</p>'
            '<p><a href="hardware_transition_01.json">换卡授权与复核</a>；'
            '<a href="interruptions/A_C_gpu_interference/main_C/measurement_cutoff.json">污染尝试截止证据</a>。'
            '污染后保护取消的请求与有效轮次分开统计。</p>')
    page = page.replace('<h2>A 常规混合</h2>', hardware + '<h2>A 常规混合</h2>', 1)
    interruptions = []
    interrupted_paths = list((root / 'interruptions').glob('*/*'))
    if (root / 'blocked.json').exists() and (root / 'L/calibration_2/summary.json').exists():
        interrupted_paths.append(root / 'L/calibration_2')
    for folder in interrupted_paths:
        cutoff = obj(folder / 'measurement_cutoff.json')
        if not cutoff or not cutoff.get('critical'):
            continue
        summary = obj(folder / 'summary.json') or {}
        requests = load(folder / 'request_results.jsonl')
        critical = cutoff['critical']
        item = dict(path=str(folder.relative_to(root)), valid=False,
            first_interference_s=critical[0]['gpu']['monotonic_s'] - cutoff['start_monotonic_s'] if critical[0].get('gpu') else None,
            cutoff_s=cutoff['measurement_cutoff_monotonic_s'] - cutoff['start_monotonic_s'],
            cause=sorted({c['kind'] for c in critical}),
            sent=summary.get('sent', sum(r.get('actual_send_monotonic_s') is not None for r in requests)),
            completed=summary.get('completed', sum(bool(r.get('completed')) for r in requests)),
            protected_cancels=sum(r.get('error') == 'user_cancel_after_measurement_cutoff' for r in requests),
            unfinished_at_cutoff=len(cutoff.get('unfinished_at_cutoff', [])),
            errors=dict(collections.Counter(str(r.get('error')) for r in requests if r.get('error'))))
        interruptions.append(item)
    save(root / 'excluded_interrupted_attempts.json', interruptions)
    if (root / 'blocked.json').exists():
        note = ('<p><strong>本次执行因外部GPU计算反复插入而停止：有效正式轮次4/12。'
            'A四方案完成；L候选1容量不通过，候选2两次被污染，未取得有效结果；L候选3及H尚未运行。'
            '未将污染尝试计为容量不通过、有效轮次或迁移用户失败。后续需连续不被其他计算插入的两卡时间窗口，从同一L候选2清单接续。</strong></p>')
        page = page.replace('<h1>两卡、三种负载场景的排空与功率响应</h1>', '<h1>两卡、三种负载场景的排空与功率响应</h1>' + note, 1)
    sections = ['<h2>无效尝试与硬阻塞记录</h2>']
    sections.append(table(['尝试目录', '有效', '首次干扰秒', '测量截止秒', '原因', '发送/完成', '保护取消', '截止时未完'],
        [[r['path'], r['valid'], r['first_interference_s'], r['cutoff_s'], r['cause'],
          [r['sent'], r['completed']], r['protected_cancels'], r['unfinished_at_cutoff']] for r in interruptions]))
    sections.append('<p>外部进程与本实验进程的PID身份分别核对。仅管理本实验进程，未终止外部计算；'
        '保护取消发生在污染截止后，不用于制造成功完成率。全部原始结果、未完成清单、控制器异常、GPU进程证据及清理记录均保留。'
        '外部短任务再次离开GPU并不能消除已经发生的测量污染。</p><h2>逐请求对照、载荷与观测完整性</h2>')
    sections.append(table(['单元', '计划/记录', '清单一致', '对照请求数', '逐token/文本/finish对照', 'SSE完整', '发送滞后均值/p95秒'],
        [[r['round'], [r['planned'], r['recorded']], r['planned_ids_exact'], r['paired_comparisons'],
          r['paired_output_pass'] if r['paired_output_pass'] is not None else '参照', r['all_sse_done'],
          [r['send_lag_s']['mean'], r['send_lag_s']['p95']]] for r in details]))
    sections.append(table(['单元', 'token应用载荷B', '发送端token控制帧B', 'KV批次张量载荷B', '后台轮数分布', '暂停边界chunk间隔均值/p95秒'],
        [[r['round'], r['token_application_payload_bytes'], r['token_control_application_frame_bytes'],
          r['kv_batch_tensor_payload_bytes'], r['background_rounds'],
          [r['final_pause_chunk_gap_s']['mean'], r['final_pause_chunk_gap_s']['p95']]] for r in details]))
    sections.append('<p>token模式不传KV张量；上表KV是已完成native批次的实际张量载荷，不能按请求重复分摊。'
        'token控制帧不含额外ZMQ路由帧和网络头部，总网络流量缺失。后台轮数仅统计background_round，初次重建及最终补齐不计入。'
        '控制帧表采用wire_send计数；旧分析中的收发端观测字节含wire_receive，保留原值并分别保存，不能重复算为发送流量。'
        '暂停边界chunk间隔是客户端观测，不是逐token ITL，也不是精确源/目标chunk归属间隔。选择顺序和每attempt状态、时间、排队及重算记录保存在各单元formal_analysis.json与migration_attempt_metrics.json。</p>')
    sections.append('<p>A/token_stop有3个、A/token_live有5个仅包含wire收发的尝试，关联客户端均正常完成，'
        '但缺少迁移终态证据，保留为未分类，不推断其接管或取消成功。'
        'A/token_live成功接管26条，后台增量追赶均为0轮；已观察初始重建与最终补齐，本批负载没有覆盖多轮后台追赶。'
        '现有已部署增量追赶实现未被修改，也未为制造追赶轮数调整参数。'
        '<a href="A/current_unclassified_attempt_audit.json">既有日志逐请求关联</a>。</p>')
    sections.append(table(['单元', '源持续计算排空秒', '源最后计算秒', '原API最后连接结束秒', 'NVML最大间隔S/D秒', '缺失功率S/D', '退出native故障'],
        [[r['round'], r['source_drain'].get('source_compute_requests_empty_s'), r['source_drain'].get('last_source_compute_s'),
          r['source_drain'].get('last_original_source_api_stream_end_s'),
          [r['power_quality'][x]['max_read_interval_s'] for x in 'SD'],
          [r['power_quality'][x]['missing_power'] for x in 'SD'], r['exit_fault']] for r in details]))
    sections.append('<p>200ms为NVML请求读取周期，不表示硬件功率传感器更新周期。测量截止后额外30秒服务存活观察和退出阶段不计入整批工作能耗。'
        '源计算排空、最后计算活动与旧API连接结束采用不同口径。请求列表清空不等于所有native lease均已直接观测清空，'
        '完整lease状态清空时刻缺失；保留请求、会话、临时KV及空闲块恢复基线的证据。'
        '终态ledger与临时请求/KV分别处理。退出故障不会被父进程返回0掩盖；本次正常卸载资格仍为否。</p>')
    for scenario in 'LH':
        cal = obj(root / scenario / 'capacity_calibration.json') or []
        if not cal:
            continue
        sections.append('<h3>' + scenario + '容量判定依据</h3>')
        rows = []
        for r in cal:
            c = r['capacity']
            for key, label in [('ttft_s', 'TTFT'), ('e2e_s', '完成延迟')]:
                v = c.get('latency', {}).get(key, {})
                rows.append([r['candidate'], label, v.get('early', {}).get('p95'), v.get('late', {}).get('p95'),
                    c.get('early_queue_mean'), c.get('late_queue_mean'), c.get('offered_rate'), c.get('stable')])
        sections.append(table(['候选', '延迟指标', '早段p95秒', '晚段p95秒', '早段waiting均值', '晚段waiting均值', '合计到达率', '通过'], rows))
        sections.append('<p>按冻结判据检查60—180秒与180—300秒窗口，队列斜率、均值增长、晚段完成率、延迟增长及错误/抢占共同决定容量是否通过；等待全部完成不单独构成容量通过。</p>')
    sections.append('<p><a href="formal_delivery_checks.json">交付分项状态</a>；<a href="finish_audit.json">两卡释放与旧保护校验</a>。'
        '本次每场景每方案仅一次，不具备多种子统计置信区间；不追加五种子实验。结论限于两卡本地功率响应。</p>')
    (root / 'report.html').write_text(page + ''.join(sections), encoding='utf-8')
    print(json.dumps(dict(formal_cells=len(details), mappings=mappings), ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    finalize(Path(args.root))
