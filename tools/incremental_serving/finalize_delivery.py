"""Final CPU-only cross-check and compact addendum; never changes raw records."""
import argparse
from collections import Counter
import csv
import hashlib
import html
import json
from pathlib import Path
import re


def load(path):
    return json.loads(path.read_text(encoding='utf-8'))


def table(path):
    with path.open(encoding='utf-8-sig',newline='') as inp:
        return list(csv.DictReader(inp))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('root');args=parser.parse_args()
    root=Path(args.root);summary=table(root/'summary.csv')
    formal=[r for r in summary if r['phase']=='formal']
    integrity=load(root/'integrity_summary.json');native=load(root/'final_audit/result.json')
    locations=load(root/'execution_location_coverage.json');checks=table(root/'round_integrity.csv')
    prom=table(root/'prometheus_preemption_review.csv')
    sessions=[r for r in table(root/'migration_sessions.csv') if r['round'].startswith('formal_')]
    waiting=[];proofs=[]
    for folder in sorted(root.glob('formal_*')):
        path=folder/'waiting_transfers.jsonl'
        if not folder.is_dir() or not path.exists():continue
        for line in path.read_text(encoding='utf-8').splitlines():
            if not line.strip():continue
            event=json.loads(line);waiting.append(dict(round=folder.name,**event))
            if event['event']=='atomic_claim_rejected':
                error=event['reply']['error'];decoder=json.JSONDecoder()
                proof,_=decoder.raw_decode(error[error.index('{'):])
                proofs.append(dict(round=folder.name,tx=event['tx'],**proof))
    if proofs:
        with (root/'waiting_atomic_rejection_proofs.csv').open('w',encoding='utf-8-sig',newline='') as out:
            writer=csv.DictWriter(out,fieldnames=list(proofs[0]));writer.writeheader();writer.writerows(proofs)
    counts=Counter(r['classification'] for r in sessions)
    review=dict(formal_rounds=len(formal),requests=sum(int(r['requests']) for r in formal),
        completed=sum(int(r['completed']) for r in formal),failed=sum(int(r['failed']) for r in formal),
        unfinished=sum(int(r['unfinished']) for r in formal),service_valid_rounds=sum(r['valid']=='True' for r in formal),
        selected_running_sessions=len(sessions),running_terminals=dict(counts),
        native_initiated_formal=sum(int(r['native_initiated']) for r in table(root/'final_audit/native_and_resource_audit.csv') if r['round'].startswith('formal_')),
        waiting_events=dict(Counter(e['event'] for e in waiting)),
        waiting_rejection_proofs=len(proofs),waiting_rejected_already_running=sum(p['status']=='RUNNING' and p['ever_scheduled'] for p in proofs),
        completed_output_comparisons=integrity['comparable_outputs'],output_mismatches=integrity['output_mismatches'],
        usage_count_mismatches=sum(int(r['usage_count_mismatches']) for r in checks),
        output_hash_mismatches=sum(int(r['output_hash_mismatches']) for r in checks),
        send_lag_max_s=max(float(r['send_lag_max_s']) for r in checks),
        prometheus_errors_inside_closed_rounds=sum(int(r['errors_inside']) for r in prom),
        bracketing_preemption_counter_changes=sorted(set(r['observed_counter_change'] for r in prom)),
        native_audit=native,execution_coverage=locations,
        finish_length_formal=sum(round(float(r['length_fraction'])*int(r['requests'])) for r in formal),
        note='Derived audit. Original progress, validity, failures and all raw logs preserved. Normal native unload failed; GPU resources released independently.')
    (root/'final_review.json').write_text(json.dumps(review,ensure_ascii=False,indent=2),encoding='utf-8')
    notes=[
        '30轮正式测量与6轮有效预实验均已闭合。正式22953/22953请求完成，失败0、截止未完成0；正式服务期独立审计30/30有效。SDK正常卸载验收未通过：测量结束后D出现Asio回调segfault、S出现堆损坏，详见上层OPEN_ISSUES.md。两张GPU和全部自有辅助服务已释放，未用SIGKILL或关闭安全检查。',
        '迁移计数闭合：正式C共39次控制器选中会话，37次完成、1次取消、1次未发起、0次失败。中档13=12完成+1取消；高档26=25完成+1未发起。原生migration_initiated共38次。未发起发生于formal_high_s8105_r000608，EngineCore返回找不到预期请求，未产生原生发起事件；该逻辑请求最终正常stop，不把它冒充传输失败。',
        '取消与404对应：formal_medium_s8101_r000277的同一会话，在stage_copy_end、final_copy_end、migration_attempt_cancelled各记录一次404，共3个原生事件、1个取消会话、0个用户失败。该请求正常stop，源lease已配平。详见migration_sessions.csv及该轮migration_terminals.jsonl。',
        f"等待转移：正式B尝试7次、C尝试10次，17次均在EngineCore原子复核时拒绝、0次提交；{review['waiting_rejected_already_running']}/17条拒绝证明显示请求已进入RUNNING且ever_scheduled=true。拒绝保留源请求继续执行，不能把已执行状态当无状态搬走。逐次tx和资格证明见waiting_atomic_rejection_proofs.csv。等待很短、调度竞态拒绝是真实结果，不为制造成功转移削弱调度。",
        '时间线解释：C的600秒到达期内，running差至少3的累计观测时间，中档各种子为7.55～16.94秒，高档35.51～68.06秒；差异多数时间不足阈值。39次选中中22次发生在到达期、17次在尾部，实际完成迁移37个请求只占C全部7651个请求的0.484%。结合短等待队列、稀疏运行差异与小迁移比例，整轮p95变化较小是可解释的，但这些关联不能证明单一因果机制。',
        '负载结论边界：2请求/秒是测试中符合保守零抢占判据的高档，并未测得完整容量边界；3请求/秒因17次抢占被保守排除，并非已证明不稳定。故“高负载”是本轮相对档位，不能宣称已充分覆盖真实饱和/容量极限。1815条独立提示词有放回回放、每档仅5个种子，也限制外推。',
        f"记录覆盖：27606个预实验/正式请求全部有实际EngineCore非空输出的位置证据；18404对A/B、B/C完整输出一致。usage计数和输出哈希异常均0。最大实际发送滞后{review['send_lag_max_s']:.3f}秒。Prometheus测量窗口采集错误0、各窗口前后累计抢占计数变化0；停止后98次连接拒绝另存，不伪装成服务期错误。NVML采样未记录外来GPU进程，但采样间隙不构成连续无干扰证明。",
        f"长度限制：正式{review['finish_length_formal']}个请求finish_reason=length，比例{review['finish_length_formal']/review['requests']:.3%}。允许EOS且输出预算4096，但这些请求确实受到长度限制，不能称为完全无限制自然生成；三个方案的同ID输出与终止原因一致。",
    ]
    (root/'FINAL_REVIEW.md').write_text('# 最终核对补充\n\n'+'\n\n'.join(notes)+'\n',encoding='utf-8')
    report=root/'report.html';text=report.read_text(encoding='utf-8')
    text=re.sub(r'<!-- FINAL_REVIEW_BEGIN -->.*?<!-- FINAL_REVIEW_END -->','',text,flags=re.S)
    block='<!-- FINAL_REVIEW_BEGIN --><details open><summary>最终核对与退出限制</summary>'+''.join('<p>'+html.escape(n)+'</p>' for n in notes)+'</details><!-- FINAL_REVIEW_END -->'
    text=text.replace('<h1>运行中迁移增量收益实验</h1>','<h1>运行中迁移增量收益实验</h1>'+block)
    report.write_text(text,encoding='utf-8')
    missing=[]
    for url in re.findall(r'(?:src|href)="([^"]+)"',text):
        if '://' not in url and not url.startswith('#') and not (root/url).exists():missing.append(url)
    (root/'html_link_audit.json').write_text(json.dumps(dict(missing_links=missing),indent=2),encoding='utf-8')
    if missing:raise RuntimeError('Missing report artifacts: '+str(missing))
    print(json.dumps({k:review[k] for k in ['formal_rounds','requests','completed','running_terminals','waiting_events','waiting_rejected_already_running','finish_length_formal']}))


if __name__=='__main__':main()
