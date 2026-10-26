"""Summarize every planned seed without silently dropping missing outcomes."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def summarize(rows, seeds, metric):
    values = [r[metric] for r in rows]
    complete = sorted(r['seed'] for r in rows) == sorted(seeds) and all(v is not None for v in values)
    return {'expected_n': len(seeds), 'observed_n': len(rows),
            'missing_n': sum(v is None for v in values),
            'mean': statistics.mean(values) if complete else None,
            'min': min(values) if complete else None, 'max': max(values) if complete else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    args = parser.parse_args()
    root = args.input
    cfg = json.loads((root/'experiment.json').read_text())
    rows = json.loads((root/'comparisons.json').read_text())
    main_rows = [r for r in rows if r['phase'] != 'smoke']  # Smoke is a duplicate, not a fourth seed.
    metrics = ['candidate_delta_mean_ms', 'source_other_delta_mean_ms', 'target_existing_delta_mean_ms',
               'source_new_delta_mean_ms', 'delta_makespan_ms', 'delta_window_max_gap_ms',
               'migration_duration_ms', 'transfer_bytes', 'replay_active_batch_ms']
    aggregates = []
    for load in cfg['target_loads']:
        for bw in cfg['bandwidth_bytes_per_ms']:
            for mode in ('kv', 'token_stop', 'token_live'):
                subset = [r for r in main_rows if (r['load'],r['bandwidth_bytes_per_ms'],r['mode']) == (load,bw,mode)]
                aggregates.append(dict(load=load, bandwidth_bytes_per_ms=bw, mode=mode,
                                  metrics={m:summarize(subset,cfg['seeds'],m) for m in metrics}))
    (root/'aggregates.json').write_text(json.dumps(aggregates,indent=2),encoding='utf-8')
    failed = [r for r in rows if r['error'] or r['unfinished'] or r['timeout_count'] or
              r['migration_state'] not in ('completed','not_requested')]
    audits = [json.loads(p.read_text()) for p in root.rglob('audit.json')]
    checks = [json.loads(p.read_text()) for p in root.rglob('checks.json')]
    lines = ['# 最小迁移对照实验', '',
             '结果口径：客户端 TPOT 派生 decode 近似 + 示例 prefill/replay + 固定可靠链路的敏感性分析。',
             '不能解释为 5090/WAN 实测性能或完整 vLLM 复现。所有延迟差值为迁移组减去不迁移组，负数改善。', '',
             f'- 正式矩阵 {len(main_rows)} 次运行（9 条件 × 3 种子 × 4 模式），另有 {len(rows)-len(main_rows)} 次独立首组验收。',
             f'- 请求失败/未完成/迁移非完成/超时条目：{len(failed)}，逐项见 failures.json。',
             f'- 输出完整且 KV 归零：{sum(a["exact_outputs"] and a["kv_freed"] for a in audits)}/{len(audits)} 次。',
             f'- 按条件验收检查全通过：{sum(all(c.values()) for c in checks)}/{len(checks)} 组。',
             '- 首组、筛查、重复分别存于 smoke/screen/repeat；首组不重复计入三种子汇总。',
             '- averages 不丢弃缺失值：任一种子缺失则对应均值为 null；不只统计成功迁移。',
             '', '## 被迁移请求完成延迟差值（3 种子均值，ms）', '',
             '|目标原有并发请求|带宽 Gbit/s|KV|token_stop|token_live|',
             '|---:|---:|---:|---:|---:|']
    for load in cfg['target_loads']:
        for bw in cfg['bandwidth_bytes_per_ms']:
            cells = [a['metrics']['candidate_delta_mean_ms']['mean'] for a in aggregates
                     if a['load']==load and a['bandwidth_bytes_per_ms']==bw]
            lines.append(f'|{load}|{bw*8/1e6:g}|'+'|'.join('未知' if v is None else f'{v:+.3f}' for v in cells)+'|')
    lines += ['', '## 其他指标范围', '',
              '下表为全部正式条件与种子中指标的最小/最大值；组延迟先对同一请求集合做配对差，再取组均值。', '',
              '|指标|KV|token_stop|token_live|', '|---|---:|---:|---:|']
    for metric in metrics[1:]:
        cells = []
        for mode in ('kv','token_stop','token_live'):
            values = [r[metric] for r in main_rows if r['mode']==mode]
            cells.append(f'{min(values):.6f} … {max(values):.6f}' if all(v is not None for v in values) else '含缺失值，见原始结果')
        lines.append('|'+metric+'|'+'|'.join(cells)+'|')
    migration_rows = [r for r in main_rows if r['mode'] != 'none']
    improved = sum(r['candidate_delta_mean_ms'] is not None and r['candidate_delta_mean_ms'] < 0 for r in migration_rows)
    improved_sets = sum(r['delta_makespan_ms'] is not None and r['delta_makespan_ms'] < 0 for r in migration_rows)
    missing = sum(r['candidate_delta_mean_ms'] is None for r in migration_rows)
    lines += ['', '## 判断与边界', '',
              f'候选请求完成延迟改善 {improved}/{len(migration_rows)} 次；全请求集合完成时间改善 {improved_sets}/{len(migration_rows)} 次；候选配对差缺失 {missing} 次。带宽和负载的影响须结合上表与原始种子结果判断。',
              '源其他请求若出现接近本地输出序列化量级（64 bytes / 1000000 bytes/ms）的微小负差，不能据此推断有意义的算力收益。',
              '测量派生 TPOT 网格的低 batch 区间近似平坦，迁走一个请求可能不会降低源端 batch 耗时；全部完成时间还取决于晚到请求。此配置结果不支持迁移普遍无收益，也不支持 token 普遍优于 KV。',
              'prefill/replay 使用示例仿射模型且候选上下文较短，不能将此处重建耗时用于预测长上下文真实重建性能。',
              '缺少完整排队/抢占，因此不能评价过载时等待尾延迟、抢占重算、OOM 避免或真实容量提升；当前 KV 充足、无抢占模型。功率曲线未接入，未报告能耗。', '',
              '指标定义：migration_window_max_gap_ms 为与 [触发,迁移终态] 相交的完整相邻客户端输出间隔最大值，包含跨边界间隔；基线使用同一绝对时间窗口。没有相邻输出时记 null。',
              'replay_active_batch_ms 是重建批次实际占用的虚拟设备时间之和，不包含网络/排队，不等于整个恢复历时。迁移耗时包括协议往返、等待、传输和重建，传输量包括双向控制和 payload，输出转发另见 summary.network。',
              '原始每请求完成延迟与差值见 comparisons.json 的 requests；逐 token 时间戳、迁移字节分项、事件和全部配置保存在各模式目录。']
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    (root/'analysis_manifest.json').write_text(json.dumps({
        'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'comparisons_sha256':hashlib.sha256((root/'comparisons.json').read_bytes()).hexdigest()},indent=2))


if __name__ == '__main__':
    main()
