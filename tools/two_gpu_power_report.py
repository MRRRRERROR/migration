"""A single Chinese results table; plots and concise paired summary are separate."""
import argparse
import csv
import html
import json
from pathlib import Path
import statistics
import sys
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
except ImportError:
    plt=None

def f(value):return '未达到/缺失' if value is None else f'{value:.3f}' if isinstance(value,(float,int)) else str(value)
def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();root=Path(a.root)
    summaries=[]
    from two_gpu_power_enrich import enrich
    for path in sorted(root.glob('formal_*/summary.json')):
        if not (path.parent/'enriched_summary.json').exists():enrich(path.parent)
        summaries.append(json.loads((path.parent/'enriched_summary.json').read_text()))
    headers=['轮次','方案','种子','P0(W)']+[f'{t}s源下降(W)' for t in (1,5,10,30,60,120)]+['10%开始/确认(s)','25%开始/确认(s)','50%开始/确认(s)',
        '60–300s源平均下降(W)','源60s能量(J)','源120s能量(J)','目标60s增功率(W)','两卡60s总功率变化(W)',
        '源请求清空(s)','安全退出准备(s)','源进程退出(s)','显存恢复空闲(s)','计划/发送/完成','900s窗口完成率','最终完成率',
        '源在途后续完成p95(s)','最大输出块间隔(s)','目标背景TTFT p95(s)','转入TTFT p95(s)','目标最大waiting','KVT500','404','原始取消','错误/未完成','原始strict validity','stop/length','迁移证据分类']
    body=[];plots=root/'plots';plots.mkdir(exist_ok=True);paired={}
    for s in summaries:
        power=s['power'];timing=s['timing'];inflight=s.get('t0_S_inflight',{})
        vals=[s['name'],s['mode'],s['seed'],power['source_p0_w']]+[power['points'][str(t)]['source_drop_w'] for t in (1,5,10,30,60,120)]
        for ratio in ('0.1','0.25','0.5'):
            threshold=power['thresholds'][ratio];vals.append(f"{threshold['start_s']}/{threshold['confirmed_s']}" if threshold else None)
        waiting=[]
        for r in (root/s['name']/'instance_samples.jsonl').read_text().splitlines():
            row=json.loads(r);st=row.get('D')
            if st:waiting.append(st['waiting'])
        vals.extend([-power['S_mean_change_60_300_w'] if power['S_mean_change_60_300_w'] is not None else None,
            power['S_energy_60']['joules'],power['S_energy_120']['joules'],power['points']['60']['destination_increase_w'],
            power['points']['60']['total_power_change_w']])
        vals.extend(timing.get(k) for k in ('source_requests_empty_s','source_safe_ready_s','source_process_exit_s','source_memory_idle_s'))
        vals.extend([f"{s['planned']}/{s['sent']}/{s['completed']}",s['completed_in_900s']/s['planned'],s['completion_rate'],
            inflight.get('post_t0_completion_s',{}).get('p95'),s['max_output_block_gap_s']['max'],
            s['D_background']['ttft_s']['p95'],s['new_transferred']['ttft_s']['p95'],max(waiting,default=None),
            s['raw_counts']['kvt500'],s['raw_counts']['kvt404'],s['raw_counts']['cancel'],s['unfinished'],s.get('original_raw_strict_validity',s['raw_strict_validity']),
            f"{s['finish_reasons'].get('stop',0)}/{s['finish_reasons'].get('length',0)}",s.get('migration_evidence_classes',{})])
        body.append('<tr>'+''.join('<td>'+html.escape(f(v))+'</td>' for v in vals)+'</tr>')
        rows=list(csv.DictReader((root/s['name']/'power.csv').open()))
        for role in ('S','D'):
            if plt is None:continue
            points=[(float(r['monotonic_s'])-timing['t0_monotonic_s'],float(r['power_w'])) for r in rows if r['role']==role and r['power_w']]
            fig,ax=plt.subplots(figsize=(11,4));ax.plot([x for x,y in points],[y for x,y in points],lw=.6)
            ax.axvline(0,color='black',ls='--',label='t0');ax.set(xlabel='Time relative to t0 (s)',ylabel=f'GPU {role} power (W)',title=s['name']);ax.legend();fig.tight_layout();fig.savefig(plots/(s['name']+'_'+role+'.png'),dpi=140);plt.close(fig)
        paired[(s['seed'],s['mode'])]=s
    document='<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>两卡逻辑双域 GPU 迁移降负荷实验</title><style>body{font-family:Arial,"Microsoft YaHei",sans-serif;margin:16px}table{border-collapse:collapse;font-size:13px}td,th{border:1px solid #bbb;padding:7px;white-space:nowrap}th{background:#edf3f8;position:sticky;top:0}tr:nth-child(even){background:#f7f7f7}</style><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join(body)+'</tbody></table></html>'
    (root/'report.html').write_text(document,encoding='utf-8')
    comparisons=[]
    for seed in (101,102,103,104,105):
        if (seed,'B') not in paired or (seed,'C') not in paired:continue
        b,c=paired[(seed,'B')],paired[(seed,'C')]
        diff={str(t):c['power']['points'][str(t)]['source_drop_w']-b['power']['points'][str(t)]['source_drop_w'] if c['power']['points'][str(t)]['source_drop_w'] is not None and b['power']['points'][str(t)]['source_drop_w'] is not None else None for t in (1,5,10,30,60,120)}
        comparisons.append({'seed':seed,'extra_source_drop_w_C_minus_B':diff,
            'background_ttft_p95_change_s':c['D_background']['ttft_s']['p95']-b['D_background']['ttft_s']['p95'],
            'source_inflight_e2e_p95_B':b['t0_S_inflight']['e2e_s']['p95'],'source_inflight_e2e_p95_C':c['t0_S_inflight']['e2e_s']['p95']})
        if plt is not None:
            fig,axes=plt.subplots(2,1,figsize=(11,7),sharex=True)
            for s in (b,c):
                rows=list(csv.DictReader((root/s['name']/'power.csv').open()))
                for ax,role in zip(axes,('S','D')):
                    points=[(float(r['monotonic_s'])-s['timing']['t0_monotonic_s'],float(r['power_w'])) for r in rows if r['role']==role and r['power_w']]
                    ax.plot([x for x,y in points],[y for x,y in points],lw=.7,label=s['mode']);ax.set_ylabel(f'GPU {role} power (W)')
            for ax in axes:ax.axvline(0,color='black',ls='--');ax.legend()
            axes[-1].set_xlabel('Time relative to t0 (s)');axes[0].set_title(f'Paired seed {seed}: B versus C');fig.tight_layout();fig.savefig(plots/f'paired_seed_{seed}.png',dpi=140);plt.close(fig)
    (root/'paired_comparison.json').write_text(json.dumps(comparisons,ensure_ascii=False,indent=2))
    if plt is None:
        (root/'plots_render_pending.json').write_text(json.dumps({'reason':'Serving environment has no matplotlib; raw power and table are available.',
            'render_command':'python tools/two_gpu_power_report.py --root <downloaded-result-directory>',
            'required_environment':'CPU-only Python environment with matplotlib; no changes to frozen serving environment'},indent=2))
    if comparisons:
        averages={str(t):statistics.mean(r['extra_source_drop_w_C_minus_B'][str(t)] for r in comparisons if r['extra_source_drop_w_C_minus_B'][str(t)] is not None) for t in (1,5,10,30,60,120)}
        summary=f"已完成 {len(comparisons)} 个配对种子。C 相比 B 在 t0 后 1/5/10/30/60/120 秒的源 GPU 额外功率下降均值(W)：{[round(averages[str(t)],3) for t in (1,5,10,30,60,120)]}。目标原有背景请求 TTFT p95 的 C−B 均值(s)：{statistics.mean(r['background_ttft_p95_change_s'] for r in comparisons):.3f}。逐轮目标功率增加、两卡总功率变化和用户输出块停顿见原始表格；源功率下降不能单独推出整体节能。输出最多4096 token，finish_reason=length 的请求仍受长度限制。\n"
        (root/'summary_zh.txt').write_text(summary,encoding='utf-8')

if __name__=='__main__':main()
