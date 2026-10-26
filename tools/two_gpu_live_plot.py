"""Plot frozen raw NVML data locally; no changes to the inference environment."""
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

def main(root):
    plt.rcParams['font.family']=['Microsoft YaHei','DejaVu Sans']
    plt.rcParams['axes.unicode_minus']=False
    colors={'B':'#64748b','C':'#2563eb','D':'#d97706','E':'#059669'}
    labels={'B':'B 自然排空','C':'C KV迁移','D':'D token_stop','E':'E token_live'}
    for scope in ('main','coverage'):
        fig,axes=plt.subplots(3,1,figsize=(12,9),sharex=True)
        plotted=False
        for mode in 'BCDE':
            folder=root/(scope+'_'+mode)
            if not (folder/'timeline.json').exists() or not (folder/'power.csv').exists():continue
            timeline=json.loads((folder/'timeline.json').read_text());t0=timeline.get('t0_monotonic_s')
            if t0 is None:continue
            bins=collections.defaultdict(lambda:collections.defaultdict(list))
            with (folder/'power.csv').open() as f:
                for row in csv.DictReader(f):
                    if row['power_w']:bins[math.floor(float(row['monotonic_s'])-t0)][row['role']].append(float(row['power_w']))
            for axis,role in zip(axes,('S','D','total')):
                pts=[]
                for offset,item in sorted(bins.items()):
                    if role=='total':
                        if item['S'] and item['D']:pts.append((offset,sum(statistics.mean(item[r]) for r in 'SD')))
                    elif item[role]:pts.append((offset,statistics.mean(item[role])))
                axis.plot([x for x,y in pts],[y for x,y in pts],label=labels[mode],color=colors[mode],lw=1.2)
                axis.set_ylabel({'S':'源卡功率（W）','D':'目标卡功率（W）','total':'两卡合计功率（W）'}[role])
            plotted=True
        for axis in axes:axis.axvline(0,color='#94a3b8',ls='--');axis.grid(alpha=.2)
        if plotted:axes[0].legend(ncol=4)
        axes[-1].set_xlabel('相对实际排空控制时刻 t0 的时间（秒）')
        fig.suptitle('服务存活主验收 · 每秒平均功率' if scope=='main' else '固定长输入覆盖 · 请求提交前为闲置服务基线')
        fig.tight_layout();fig.savefig(root/(scope+'_power.png'),dpi=160);plt.close(fig)
    (root/'plot_environment.json').write_text(json.dumps(dict(matplotlib=matplotlib.__version__,scope='local CPU plotting of recorded CSV',aggregation='one-second bins aligned at actual t0; raw timestamps retained'),indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();main(Path(a.root))
