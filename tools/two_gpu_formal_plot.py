"""Chinese plots of recorded formal measurements; CPU only, no new trials."""
import argparse
import collections
import csv
import json
import math
from pathlib import Path
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot(root):
    plt.rcParams['font.family'] = ['Microsoft YaHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    colors = {'B': '#64748b', 'C': '#2563eb', 'D': '#d97706', 'E': '#059669'}
    names = {'B': 'B 自然排空', 'C': 'C KV迁移', 'D': 'D token_stop', 'E': 'E token_live'}
    titles = {'A': 'A 常规混合', 'L': 'L 长输入', 'H': 'H 目标较忙'}
    provenance = []
    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    plotted = False
    for mode in 'BCDE':
        folder = root / ('main_' + mode)
        if not (folder / 'timeline.json').exists() or not (folder / 'power.csv').exists():
            continue
        timeline = json.loads((folder / 'timeline.json').read_text(encoding='utf-8'))
        t0 = timeline.get('t0_monotonic_s')
        cutoff = timeline.get('measurement_cutoff_monotonic_s')
        if t0 is None or cutoff is None:
            continue
        bins = collections.defaultdict(lambda: collections.defaultdict(list))
        with (folder / 'power.csv').open(encoding='utf-8', newline='') as stream:
            for row in csv.DictReader(stream):
                stamp = float(row['monotonic_s'])
                if row['power_w'] and timeline['start_monotonic_s'] <= stamp <= cutoff:
                    bins[math.floor(stamp - t0)][row['role']].append(float(row['power_w']))
        for axis, role in zip(axes, ('S', 'D', 'total')):
            points = []
            for offset, data in sorted(bins.items()):
                if role == 'total' and data['S'] and data['D']:
                    points.append((offset, sum(statistics.mean(data[r]) for r in 'SD')))
                elif role != 'total' and data[role]:
                    points.append((offset, statistics.mean(data[role])))
            axis.plot([p[0] for p in points], [p[1] for p in points], label=names[mode], color=colors[mode], linewidth=1.2)
        provenance.append(dict(mode=mode, start=timeline['start_monotonic_s'], t0=t0, cutoff=cutoff))
        plotted = True
    if not plotted:
        plt.close(fig)
        return
    for axis, ylabel in zip(axes, ('源卡功率（W）', '目标卡功率（W）', '两卡合计功率（W）')):
        axis.set_ylabel(ylabel)
        axis.axvline(0, color='#94a3b8', linestyle='--')
        axis.grid(alpha=.2)
    axes[0].legend(ncol=4)
    axes[-1].set_xlabel('相对实际排空控制时刻 t0 的时间（秒）')
    fig.suptitle(titles.get(root.name, root.name) + ' · 服务存活时每秒平均功率')
    note = 'A/B、A/D、A/E使用GPU 1、3；A/C使用GPU 0、2，跨卡差异不能只归因迁移方式。' if root.name == 'A' else '本场景各方案使用GPU 0、2；曲线截止于各轮测量截止，退出功率不计入。'
    fig.text(.5, .008, note, ha='center', fontsize=9)
    fig.tight_layout(rect=(0, .025, 1, .965))
    fig.savefig(root / 'main_power.png', dpi=160)
    plt.close(fig)
    (root / 'plot_environment.json').write_text(json.dumps(dict(
        matplotlib=matplotlib.__version__, scope='local CPU plotting of frozen NVML measurements',
        aggregation='one-second bins aligned at actual t0; raw timestamps retained',
        cutoff='measurement cutoff; extra alive observation and shutdown excluded',
        rounds=provenance), ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    plot(Path(args.root))
