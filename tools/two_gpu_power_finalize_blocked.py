"""Preserve a blocked acceptance as blocked; never manufacture 15-round results."""
import argparse
import csv
import hashlib
import html
import json
from pathlib import Path
import statistics
import sys
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from two_gpu_power_metrics import power_metrics

def read(path):
    if not path.exists():return []
    with path.open() as f:return [json.loads(s) for s in f if s.strip()]
def dump(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
def csvwrite(path,data):
    if not data:return
    keys=list(dict.fromkeys(k for r in data for k in r))
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader()
        for row in data:w.writerow({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(list,dict)) else v for k,v in row.items()})
def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();root=Path(a.root)
    b=root/'acceptance_B';requests=read(b/'request_results.jsonl');plan=read(b/'replay.jsonl');timeline=json.loads((b/'timeline.json').read_text())
    power=list(csv.DictReader((b/'power.csv').open()));metrics=power_metrics(power,timeline['t0_monotonic_s'])
    csvwrite(b/'request_metrics.csv',requests)
    samples=read(b/'instance_samples.jsonl');csvwrite(b/'queue_metrics.csv',[{'monotonic_s':r['monotonic_s'],'role':role,
        'running':(r.get(role) or {}).get('running'),'waiting':(r.get(role) or {}).get('waiting')} for r in samples for role in ('S','D')])
    result={'status':'blocked_acceptance','not_formal':True,'formal_rounds_completed':0,
        'planned':len(plan),'sent':len(requests),'completed':sum(r['completed'] for r in requests),
        'errors':sum(bool(r['error']) for r in requests),'power':metrics,'window_s':180,'t0_offset_s':60,
        'primary_blocker':'Native teardown segfault and BarexMRGuard buffer deregistration assertion; API-only SIGTERM probe also reproduced it.',
        'controller_secondary_issue':'NVML v1 used includes 498 MiB device-reserved memory: idle measured 502 MiB, not <=64 MiB. Fixed future memory acquisition to NVML v2 and baseline-relative detection; raw readings preserved.',
        'finish_reason_counts':{reason:sum(r['finish_reason']==reason for r in requests) for reason in ('stop','length','abort')},
        'two_gpu_total_energy_claim':'No formal B/C pair exists; no migration gain or whole-system energy saving conclusion is available.'}
    dump(b/'blocked_acceptance_summary.json',result)
    plots=root/'plots';plots.mkdir(exist_ok=True)
    for role in ('S','D'):
        pts=[(float(r['monotonic_s'])-timeline['t0_monotonic_s'],float(r['power_w'])) for r in power if r['role']==role and r['power_w']]
        fig,axes=plt.subplots(2,1,figsize=(11,7))
        for ax in axes:
            ax.plot([x for x,y in pts],[y for x,y in pts],lw=.8);ax.axvline(0,color='black',ls='--',label='t0');ax.set_ylabel(f'GPU {role} power (W)')
        axes[0].set_xlim(-60,120);axes[0].set_title('Acceptance B: 180 s replay - blocked by native teardown');axes[0].legend()
        axes[1].axvline(120,color='gray',ls=':',label='Replay end');axes[1].set_title('Complete monitor record, including fault diagnosis and idle GPU');axes[1].legend();axes[1].set_xlabel('Time relative to t0 (s)')
        fig.tight_layout();fig.savefig(plots/f'blocked_acceptance_B_{role}.png',dpi=150);plt.close(fig)
    characteristics={}
    for role in ('S','D'):
        selected=[r for r in power if r['role']==role];valid=[r for r in selected if r['power_w']]
        intervals=[float(b['monotonic_s'])-float(a['monotonic_s']) for a,b in zip(selected,selected[1:])]
        characteristics[role]={'requested_interval_s':.2,'observed_interval_median_s':statistics.median(intervals),
            'observed_interval_max_s':max(intervals),'missing_power_readings':len(selected)-len(valid),
            'identical_adjacent_reading_fraction':sum(a['power_w']==b['power_w'] for a,b in zip(valid,valid[1:]))/max(1,len(valid)-1),
            'measurement_update_rate':'not inferred from polling rate; repeated values and acquisition gaps preserved'}
    dump(root/'power_readout_characteristics.json',characteristics)
    headers=['阶段/轮次','方案','种子','状态','计划/发送/完成','完成率','KVT500','源P0(W)']+[f'{t}s源下降(W)' for t in (1,5,10,30,60,120)]+['退出','stop/length','备注']
    table=[]
    for role in ('S','D'):
        s=json.loads((root/f'calibration_0_{role}/summary.json').read_text())
        vals=[f'容量短测{role}','A',700,'请求完成；退出断言失败',f"{s['planned']}/{s['sent']}/{s['completed']}",f"{s['completion_rate']:.2%}",s['raw_counts']['kvt500'],s['power']['source_p0_w']]+['不适用']*6+['native断言失败',f"{s['finish_reasons'].get('stop',0)}/{s['finish_reasons'].get('length',0)}",'每个候选点300秒；非最终有效校准']
        table.append(vals)
    vals=['短流程验收','B',999,'阻塞',f"{result['planned']}/{result['sent']}/{result['completed']}",f"{result['completed']/result['planned']:.2%}",0,metrics['source_p0_w']]+[metrics['points'][str(t)]['source_drop_w'] for t in (1,5,10,30,60,120)]+['native崩溃',f"{result['finish_reason_counts']['stop']}/{result['finish_reason_counts']['length']}",'180秒验收窗口；不可作为900秒正式结果']
    table.append(vals)
    for i,row in enumerate(json.loads((root/'formal_order.json').read_text()),1):table.append([f'正式{i}',row['mode'],row['seed'],'未执行（验收阻塞）','—','—','—','—']+['—']*6+['—','—','未生成测量结果'])
    def cell(v):return '缺失' if v is None else f'{v:.3f}' if isinstance(v,float) else str(v)
    content='<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>两卡逻辑双域迁移降负荷实验：验收阻塞</title><style>body{font-family:"Microsoft YaHei",Arial;margin:16px}table{border-collapse:collapse;font-size:13px}th,td{border:1px solid #bbb;padding:7px;white-space:nowrap}th{background:#edf3f8}tr:nth-child(even){background:#fafafa}</style><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(cell(v))+'</td>' for v in r)+'</tr>' for r in table)+'</tbody></table></html>'
    (root/'report.html').write_text(content,encoding='utf-8')
    text=f"正式实验0/15轮。S=1.0请求/秒和D=1.3请求/秒两项300秒容量短测共750/750请求完成，队列无增长，但退出检查失败，不能作为最终有效校准。B验收{result['completed']}/{result['planned']}请求完成，正常退出路径出现native segfault/Barex内存注销断言，API单独SIGTERM也重现，故按关键错误规则暂停。输出length={result['finish_reason_counts']['length']}，仍受4096 token上限限制。缺少C验收与正式配对，不能给出KV迁移额外降功率或用户代价的结论。\n"
    (root/'summary_zh.txt').write_text(text,encoding='utf-8')
    source=Path(__file__).resolve().parent;freeze=root/'experiment_sources';freeze.mkdir(exist_ok=True)
    files=list(source.glob('two_gpu_power*.py'))+[source/'two_gpu_nvml_monitor.py',source/'power_drain_observer/sitecustomize.py',source/'start_two_gpu_power_drain.sh']
    manifest=[]
    for f in files:
        target=freeze/f.name;target.write_bytes(f.read_bytes());manifest.append({'source':str(f),'sha256':hashlib.sha256(f.read_bytes()).hexdigest()})
    dump(root/'experiment_source_manifest.json',manifest)
    inventory=[]
    for log in root.glob('*/vllm_*.log'):
        for number,line in enumerate(log.read_text(errors='replace').splitlines(),1):
            if '[entrypoints/logger.py:' in line:continue
            if 'AssertFailed' in line or 'Segfault encountered' in line:inventory.append({'file':str(log),'line':number,'evidence':line})
    dump(root/'native_teardown_fault_inventory.json',inventory)
    dump(root/'config_acceptance_status.json',{'formal_completed':0,'final_config_is_candidate_only':True,
        'capacity_observations_completed':True,'normal_exit_accepted':False,'final_capacity_accepted':False,
        'new_workload_preparation_and_recalibration_required':True,'native_teardown_errors':len(inventory)})
    (root/'REPRODUCE.md').write_text('本次因关键退出错误阻塞，正式0/15。冻结的服务代码未改动。\n\n数据来源：https://huggingface.co/datasets/shibing624/sharegpt_gpt4/resolve/main/sharegpt_gpt4.jsonl\n\n提示词数量1582，按原始文本128～4096 token筛选；聊天模板增加额外输入，实际最大4119，已保存两种计数，上下文9216覆盖输入及4096输出预算。采用有放回抽样。\n\n复现启动（须先解决并验证正常退出，再新建目录；旧目录保留）：\n\n```bash\nbash tools/prepare_two_gpu_power_drain.sh --root results/<new-directory> --source <original-sharegpt-jsonl>\nbash tools/start_two_gpu_power_drain.sh --root results/<new-directory>\n```\n\nfinal_config.json、capacity_calibration.json、formal_order.json、replays/及version_manifest.json保存配置、选择依据、顺序、回放和固定KVT版本。B验收GPU显存原始CSV使用NVML v1，含设备reserved显存，空闲基线502 MiB；nvidia-smi空闲used为4 MiB。后续采集已修正为NVML v2。TPOT为(最后非空SSE输出块时间−首非空输出块时间)/(completion_tokens−1)；输出块间隔不是逐token ITL。\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
