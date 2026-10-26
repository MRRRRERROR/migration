"""Table and measured-power plots for finite recalibration/B-C acceptance."""
import argparse
import csv
import html
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from two_gpu_power_enrich import enrich


def load(path):
    return json.loads(path.read_text(encoding='utf-8'))


def fmt(value):
    if value is None:
        return '缺失/未达到'
    return f'{value:.3f}' if isinstance(value,float) else str(value)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    args=parser.parse_args()
    root=Path(args.root)
    config=load(root/'final_config.json')
    cal=load(root/'capacity_calibration.json')
    summaries=[]
    for mode in 'BC':
        folder=root/f'acceptance_{mode}'
        # Force UTF-8 on Windows for logs and JSON created on the Linux host.
        if (folder/'blocked_enriched_observations.json').exists():
            summaries.append(load(folder/'blocked_enriched_observations.json'))
        else:
            summaries.append(load(folder/'enriched_summary.json') if (folder/'enriched_summary.json').exists() else enrich(folder))
    assert (root/'acceptance_B/replay.jsonl').read_bytes()==(root/'acceptance_C/replay.jsonl').read_bytes(), 'B/C request plans differ'
    verification=[]
    for s in summaries:
        folder=root/s['name']
        requests=[json.loads(line) for line in (folder/'request_results.jsonl').read_text(encoding='utf-8').splitlines()]
        token_mismatches=[r['request_id'] for r in requests if r.get('server_input_tokens')!=r['input_tokens']]
        errors=[r['request_id'] for r in requests if r.get('error') or not r.get('completed')]
        assert not token_mismatches and not errors, (s['name'],token_mismatches,errors)
        verification.append({'mode':s['mode'],'planned':s['planned'],'sent':s['sent'],'completed':s['completed'],
                             'input_token_mismatches':token_mismatches,'errors':errors,'max_send_delay_s':s['send_delay_s']['max']})
    (root/'acceptance_client_verification.json').write_text(json.dumps({'identical_BC_replays':True,'cases':verification},indent=2),encoding='utf-8')
    headers=['方案','验收状态','源/背景到达率(rps)','窗口/t0(s)','计划/发送/完成','最终完成率','stop/length',
             't0源在途','P0源(W)']+[f'{t}s源下降(W)' for t in (1,5,10,30,60,120)]+[
             '源清空(s)','安全就绪(s)','进程退出(s)','显存空闲(s)','最大输出块间隔(s)',
             '目标背景TTFT p95(s)','转入TTFT p95(s)','目标最大waiting','KVT500/404/取消',
             '迁移完成/取消/未分类','原始strict validity','已归还/获取block lease']
    body=[]
    plots=root/'plots'
    plots.mkdir(exist_ok=True)
    for s in summaries:
        folder=root/s['name']
        power=s['power']
        timing=s['timing']
        samples=[json.loads(line) for line in (folder/'instance_samples.jsonl').read_text(encoding='utf-8').splitlines()]
        waiting=[row['D']['waiting'] for row in samples if row.get('D')]
        lease=load(folder/'final_leases.json')
        classes=s['migration_attempt_classification']
        values=[s['mode'],'未通过：目标native退出segfault' if s.get('acceptance_passed') is False else '通过',f"{config['source_rate']}/{config['destination_rate']}",'180/60',
                f"{s['planned']}/{s['sent']}/{s['completed']}",s['completion_rate'],
                f"{s['finish_reasons'].get('stop',0)}/{s['finish_reasons'].get('length',0)}",
                s['t0_S_inflight']['n'],power['source_p0_w']]
        values.extend(power['points'][str(t)]['source_drop_w'] for t in (1,5,10,30,60,120))
        values.extend(timing.get(k) for k in ('source_requests_empty_s','source_safe_ready_s','source_process_exit_s','source_memory_idle_s'))
        values.extend([s['max_output_block_gap_s']['max'],s['D_background']['ttft_s']['p95'],
                       s['new_transferred']['ttft_s']['p95'],max(waiting,default=None),
                       f"{s['raw_counts']['kvt500']}/{s['raw_counts']['kvt404']}/{s['raw_counts']['cancel']}",
                       f"{classes.get('committed',0)}/{classes.get('cancelled',0)}/{classes.get('unresolved',0)}",
                       s['original_raw_strict_validity'],f"{lease['returned']}/{lease['acquired']}"])
        body.append('<tr>'+''.join('<td>'+html.escape(fmt(v))+'</td>' for v in values)+'</tr>')
        with (folder/'power.csv').open(encoding='utf-8') as stream:
            rows=list(csv.DictReader(stream))
        rows=[row for row in rows if timing['start_monotonic_s']<=float(row['monotonic_s'])<=timing['start_monotonic_s']+180]
        for role in ('S','D'):
            points=[(float(row['monotonic_s'])-timing['t0_monotonic_s'],float(row['power_w'])) for row in rows if row['role']==role and row['power_w']]
            fig,ax=plt.subplots(figsize=(11,4))
            ax.plot([x for x,y in points],[y for x,y in points],lw=.7)
            ax.axvline(0,color='black',ls='--',label='t0')
            ax.set(xlabel='Time relative to t0 (s); fixed 180 s replay window',ylabel=f'GPU {role} power (W)',title=f'Acceptance {s["mode"]}, seed 999'+(' (target teardown failed)' if s.get('acceptance_passed') is False else ''))
            ax.legend();fig.tight_layout();fig.savefig(plots/f'{s["name"]}_{role}.png',dpi=140);plt.close(fig)
    (root/'acceptance_report.html').write_text('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>退出修复后B/C验收</title><style>body{font-family:Arial,"Microsoft YaHei";margin:12px}table{border-collapse:collapse;font-size:13px}td,th{border:1px solid #aaa;padding:7px;white-space:nowrap}th{background:#edf3f8}</style><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join(body)+'</tbody></table></html>',encoding='utf-8')
    b,c=summaries
    extra={str(t):c['power']['points'][str(t)]['source_drop_w']-b['power']['points'][str(t)]['source_drop_w']
           if None not in (c['power']['points'][str(t)]['source_drop_w'],b['power']['points'][str(t)]['source_drop_w']) else None
           for t in (1,5,10,30,60,120)}
    (root/'acceptance_pair.json').write_text(json.dumps({'seed':999,'scope':'one short pair with C target native teardown failure; not 5-seed formal measurement',
         'full_BC_acceptance_passed':not any(s.get('acceptance_passed') is False for s in summaries),
         'extra_source_drop_w_C_minus_B':extra,'source_memory_idle_s':{s['mode']:s['timing']['source_memory_idle_s'] for s in summaries},
         'length_limited_requests':{s['mode']:s['finish_reasons'].get('length',0) for s in summaries}},ensure_ascii=False,indent=2),encoding='utf-8')
    fig,axes=plt.subplots(2,1,figsize=(11,7),sharex=True)
    for s in summaries:
        with (root/s['name']/'power.csv').open(encoding='utf-8') as stream:
            rows=list(csv.DictReader(stream))
        rows=[row for row in rows if s['timing']['start_monotonic_s']<=float(row['monotonic_s'])<=s['timing']['start_monotonic_s']+180]
        for ax,role in zip(axes,('S','D')):
            points=[(float(row['monotonic_s'])-s['timing']['t0_monotonic_s'],float(row['power_w'])) for row in rows if row['role']==role and row['power_w']]
            ax.plot([x for x,y in points],[y for x,y in points],lw=.7,label=s['mode']);ax.set_ylabel(f'GPU {role} power (W)')
    for ax in axes:
        ax.axvline(0,color='black',ls='--');ax.legend()
    axes[-1].set_xlabel('Time relative to t0 (s); fixed 180 s replay window');axes[0].set_title('Short pair: seed 999, B/C; C target teardown failed')
    fig.tight_layout();fig.savefig(plots/'paired_acceptance_seed999.png',dpi=140);plt.close(fig)
    summary=f"短流程验收：B通过；C目标端native退出segfault，整体验收未通过，正式轮次0/15。B/C客户端各完成{s['planned']}条请求，不能用客户端完成代替资源退出验收。最终到达率S={config['source_rate']}、D背景={config['destination_rate']}请求/秒。提示词池实际1581条，原始文本去重且实际模板输入128～4096 token，均匀有放回抽样。\n" if c.get('acceptance_passed') is False else f"B/C短流程验收通过，各{s['planned']}条请求，非正式15轮。\n"
    summary+=f"B/C源进程退出时间分别为{b['timing']['source_process_exit_s']:.3f}/{c['timing']['source_process_exit_s']:.3f}秒，显存恢复空闲为{b['timing']['source_memory_idle_s']:.3f}/{c['timing']['source_memory_idle_s']:.3f}秒。\n"
    summary+=f"C相比B在1/5/10/30/60/120秒的源GPU额外下降W：{[round(extra[str(t)],3) if extra[str(t)] is not None else None for t in (1,5,10,30,60,120)]}。仅一组短验收，不能作为正式迁移收益结论。\n"
    summary+="允许正常EOS，但设置4096输出上限；任何finish_reason=length仍受长度限制，不能视为完全自然生成。TPOT=(最后非空输出块时刻－首个非空输出块时刻)/(completion_tokens－1)，SSE间隔是输出块间隔。原始strict validity和取消/404计数保留，按迁移及请求完成证据另行分类。\n"
    (root/'acceptance_summary_zh.txt').write_text(summary,encoding='utf-8')
    print(json.dumps({'observed_rounds':[s['name'] for s in summaries],'full_BC_acceptance_passed':not any(s.get('acceptance_passed') is False for s in summaries),'extra_drop_w':extra}))


if __name__=='__main__':
    main()
