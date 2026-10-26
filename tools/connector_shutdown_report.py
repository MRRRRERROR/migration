"""Produce a reviewable evidence bundle from real connector exit cases."""
import argparse
import difflib
import hashlib
import html
import json
from pathlib import Path
import re
import shutil


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--staged-code', required=True)
    args = parser.parse_args()
    root, staged = Path(args.root), Path(args.staged_code)
    code = root / 'code'
    code.mkdir(exist_ok=True)
    for name in ('original', 'final', 'frozen_accepted'):
        shutil.copytree(staged / name, code / name, dirs_exist_ok=True)
    for path in staged.glob('*build.log'):
        shutil.copy2(path, code / path.name)
    patches = []
    for name in ('barex_protocol.cpp', 'barex_protocol.h', 'utils.py', 'hybrid_modules.py'):
        original = staged / ('original' if name.startswith('barex') else 'fix1/python_original') / name
        final = staged / 'final' / name
        patches.extend(difflib.unified_diff(original.read_text(encoding='utf-8').splitlines(True), final.read_text(encoding='utf-8').splitlines(True),
                                           fromfile='a/' + name, tofile='b/' + name))
        shutil.copy2(original, code / ('original_' + name))
    (code / 'shutdown_fix.patch').write_text(''.join(patches), encoding='utf-8')
    failures = []
    for folder in sorted(root.glob('diag_*')):
        for log in folder.glob('vllm_*.log'):
            lines = log.read_text(encoding='utf-8',errors='replace').splitlines()
            releases = [line for line in lines if 'event=guard_release_result' in line and 'result=10 ' in line]
            for release in releases:
                fields = dict(re.findall(r'(\w+)=([^\s]+)', release))
                identity = fields['buf']
                aliases = [line for line in lines if 'event=tcp_layer_tensor_alias ' in line and 'buf=' + identity + ' ' in line]
                prior = [line for line in lines[:lines.index(release)] if 'event=guard_release_result ' in line and 'buf=' + identity + ' ' in line]
                failures.append({'case':folder.name, 'source_log':str(log.relative_to(root)), 'line':lines.index(release)+1,
                                 'operation':fields['operation'], 'return_code':int(fields['result']),
                                 'return_name':'BAREX_ERR_ADDRESS', 'identity':fields, 'release_evidence':release,
                                 'tensor_alias_creation':aliases, 'prior_release_count':len(prior)})
    cases = []
    for path in sorted(root.glob('final_*/result.json')):
        result = json.loads(path.read_text(encoding='utf-8'))
        result['case'] = path.parent.name
        result['exits'] = []
        for i in range(result['instances']):
            exit_data = json.loads((path.parent / f'exit_{i}.json').read_text(encoding='utf-8'))
            text = (path.parent / f'vllm_{i}.log').read_text(encoding='utf-8',errors='replace')
            lifecycle = [json.loads(line.split('KVT_PY_SHUTDOWN ',1)[1]) for line in text.splitlines() if 'KVT_PY_SHUTDOWN {' in line]
            ended = [row for row in lifecycle if row['event']=='core_shutdown_end']
            assert ended and all(not loop['running'] and loop['closed'] for row in ended for loop in row.get('loops',[]))
            assert not exit_data['remaining_processes'] and not exit_data['fatal_markers']
            result['exits'].append({'instance':i, 'shell_returncode':exit_data['shell_returncode'],
                                    'shutdown_s':exit_data['end_monotonic_s']-exit_data['start_monotonic_s'],
                                    'memory_mib':exit_data['gpu']['memory_mib'], 'remaining_processes':exit_data['remaining_processes'],
                                    'compute_pids':exit_data['gpu']['compute_pids'], 'fatal_markers':exit_data['fatal_markers'],
                                    'nccl_unexplicit_destroy_warnings':text.count('destroy_process_group() was not called before program exit'),
                                    'core_shutdown_end':ended, 'thread_join_logs':[line for line in text.splitlines() if 'KVT_THREAD_SHUTDOWN joined' in line],
                                    'native_shutdown_logs':[line for line in text.splitlines() if 'event=barex_context_' in line]})
        assert result['clean']
        cases.append(result)
    assert len(cases)==6 and len(failures)==3
    binary = next((code/'frozen_accepted').glob('*.so'))
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    evidence = {'scope':'Two RTX5090 GPUs on one host; TP1 HybridConnector TCP; not RDMA teardown validation',
                'native_base_commit':'6ebc08a193e10f9f809859edda929a031804da62',
                'serving_base_commit':'aa9097c899fd2f290e031f8a88f854afe4f0b415',
                'frozen_binary_sha256':digest, 'failed_release_cases':failures, 'final_cases':cases,
                'sigkill_used_for_normal_exit':False}
    (root/'shutdown_evidence.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')
    headers = ['场景','实例数','每实例请求数','停止路径','通过','退出耗时(s)','shell返回码','显存(MiB)','残留计算进程','native异常','connector线程','NCCL未显式destroy警告']
    body = []
    for case in cases:
        exits = case['exits']
        values = [case['case'],case['instances'],case['requests_per_instance'],case['stop'],'是',
                  '/'.join(f"{e['shutdown_s']:.3f}" for e in exits),'/'.join(str(e['shell_returncode']) for e in exits),
                  '/'.join(str(e['memory_mib']) for e in exits),'0','0','已join，loop已关闭',
                  '/'.join(str(e['nccl_unexplicit_destroy_warnings']) for e in exits)]
        body.append('<tr>'+''.join('<td>'+html.escape(str(value))+'</td>' for value in values)+'</tr>')
    (root/'shutdown_acceptance.html').write_text('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>Connector退出验收</title><style>body{font-family:Arial,"Microsoft YaHei"}table{border-collapse:collapse}td,th{border:1px solid #aaa;padding:8px}</style><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join(body)+'</tbody></table></html>',encoding='utf-8')
    report = f'''退出修复与证据（2026-10-07）

已复现3次Barex断言：无请求直接SIGTERM、3条请求完成后直接SIGTERM、3条请求完成后项目测试辅助入口的SIGTERM阶段。
失败操作均为ReleaseBuffer，返回10（BAREX_ERR_ADDRESS）。MR的native_mr、released_flag、allocator为空，adopted=0。
同一buf能关联到tcp_layer_tensor_alias的NoAllocNoRegistration创建记录，失败前没有该buf的释放记录。
失败时cuCtxGetCurrent成功，primary_active=1；Barex WaitStop与Shutdown返回0。
因此这次native断言的证据支持所有权错误：PyTorch KV张量视图不属于Barex内存池，却被ReleaseGuard交给池释放。
现有记录不支持这次错误是重复释放或CUDA上下文已经销毁；不把这些假设写成既定原因。

独立的生命周期问题：旧版本hybridsched、hybridworker在core_shutdown_end/atexit仍在运行。
修复后在正常shutdown中收尾任务、关闭RPC监听/连接、停止并join线程、关闭loop。
不能仅凭此证据把所有历史segfault都单独归因于线程；本次验收证明修复组合下退出无异常。

内存修复：增加明确的BorrowedTensorView所有权类别及分类断言；借用视图不调用Barex释放。
所有真实Barex分配/注册对象的ReleaseAndDeregBuffer、DeregUserMr、ReleaseBuffer返回值检查及原断言保留。
新增Shutdown/WaitStop真实返回值检查，不忽略返回值，不使用SIGKILL作为退出流程。

停止入口核对：docs/source/design/llumlet/llumlet_and_llumlet_proxy.md建议SIGTERM正常退出。
tests/local/utils.py::kill_process_tree会对子进程和父进程发SIGTERM，随后有5秒SIGKILL兜底。
对照试验只复现其SIGTERM阶段，未执行兜底。正式控制器对vLLM API入口发SIGTERM，由引擎完成正常收尾。
旧Ray部署的tools/real_migration/stop_llumnix.sh先停止服务再ray stop；未用于当前vLLM进程部署。

最终版本6/6真实验收通过：单实例0/3请求×两种停止路径，双实例0/3请求依次退出。
均无native断言、segfault、残留进程；NVML v2 used显存恢复4MiB、计算进程为空，两个connector loop已关闭。
v1显存统计包含498MiB设备reserved；验收使用v2 used与真实空闲基线，未把reserved误作泄漏。
PyTorch仍有NCCL未显式destroy的警告，原样保留；进程、GPU used显存和PID命名Barex共享内存核对均无残留。
这证明当前场景下退出后的可观测资源清理，不等同于证明所有内部对象均显式析构。只读共享内存核对见post_exit_resource_audit.json。
project_tree的shell返回-15是测试主动SIGTERM的结果，api_sigterm的shell返回0；不是native崩溃。
范围仅涵盖当前两张RTX5090、TP1、TCP connector，不声称完成RDMA或其他硬件的验收。

冻结native SHA256：{digest}
冻结完整源码及SHA256SUMS：code/frozen_accepted；修复差异：code/shutdown_fix.patch。
逐对象日志和逐场景证据：shutdown_evidence.json；原始日志保留在各diag_*/final_*目录。
之前实验与失败/重跑记录均未覆盖。新校准/B/C目录：../two_gpu_power_drain_20261007_shutdown_fixed_01。
'''
    if (root/'FREEZE_MIGRATION_VALIDATION_REVOKED.json').exists():
        report='【更新：完整迁移退出验收未通过，禁止用于正式实验】\n基础6项退出场景通过后，C验收的目标端在BarexCtx::Shutdown内segfault。\nGDB最小复现确认XContextTcpImpl::Shutdown→XChannelTcpImpl::Close→ToString，发生于静态析构；最小复现CUDA查询rc=4、context为空。\n显式清理候选lifecycle2失败：Shutdown/WaitStop返回0后，主线程析构Asio scheduler，native io_loop_func_server_仍在运行；malloc报告double free or corruption。\n该malloc诊断不能自动认定同一个MR重复释放；具体alloc/free配对尚未定位。C原轮次CUDA查询rc=0，不能把最小复现中的CUDA失效推广为C的已证实原因。\n已保存GDB线程栈及11个关联native共享内存文件，候选未通过、已恢复基础版本。正式0/15；最终冻结不可宣称完成。\n\n'+report
    (root/'diagnosis_zh.txt').write_text(report,encoding='utf-8')
    print(json.dumps({'final_cases':len(cases),'reproduced_failures':len(failures),'sha256':digest}))


if __name__=='__main__':
    main()
