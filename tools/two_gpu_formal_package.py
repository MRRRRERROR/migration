"""Package partial or complete frozen-run evidence; never launch inference."""
import argparse
import collections
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
from two_gpu_live_report import obj, load, save


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def package(root, archive):
    project = Path('/data/home/zylu/llumnix-v018-main')
    integrity = []
    for row in obj(root / 'minimal_preflight.json')['checks']:
        actual = digest(Path(row['path']))
        integrity.append(dict(path=row['path'], expected=row['sha256'], actual=actual, unchanged=actual == row['sha256']))
    save(root / 'serving_native_integrity_final.json', integrity)
    if not all(r['unchanged'] for r in integrity):
        raise RuntimeError('Serving/native integrity differs; preserve evidence and do not claim unchanged environment')
    prompt_pool = load(root / 'inputs/prompts.jsonl')
    checks = []
    for scenario in 'ALH':
        frozen = obj(root / scenario / 'frozen_main_trace.json')
        if not frozen:
            continue
        by_id = {r['request_id']: r for r in frozen}
        for folder in sorted((root / scenario).glob('main_*')):
            if not (folder / 'summary.json').exists():
                continue
            actual = {r['request_id']: r for r in load(folder / 'request_results.jsonl')}
            rows = []
            for planned in obj(folder / 'replay.json'):
                reference = by_id[planned['request_id']]
                pool = prompt_pool[planned['prompt_index']]
                result = actual.get(planned['request_id'], {})
                rows.append(dict(request_id=planned['request_id'],
                    input_token_ids_equal=planned['token_ids'] == reference['token_ids'] == pool['token_ids'],
                    input_sha256_equal=planned['input_sha256'] == reference['input_sha256'] == pool['input_sha256'] == result.get('input_sha256'),
                    generation_equal=planned['generation'] == reference['generation'],
                    planned_arrival_equal=planned['planned_offset_s'] == reference['planned_offset_s'],
                    role_equal=planned['initial_role'] == reference['initial_role']))
            save(folder / 'input_replay_identity_checks.json', dict(
                scope='Saved pre-send replay, unchanged actual sender prompt pool, frozen trace, and recorded client input hash',
                server_all_request_input_ids_observation='not separately captured for all requests; original token-ID payload construction retained in launch source',
                rows=rows, passed=all(all(r[k] for k in r if k != 'request_id') for r in rows)))
            checks.append(dict(round=str(folder.relative_to(root)), requests=len(rows), passed=all(all(r[k] for k in r if k != 'request_id') for r in rows)))
    save(root / 'input_identity_summary.json', checks)
    if not all(r['passed'] for r in checks):
        raise RuntimeError('Input replay identity differs; preserve per-request evidence')
    status = dict(status='blocked' if (root / 'blocked.json').exists() else 'completed',
        planned_formal_cells=12, completed_formal_cells=len(obj(root / 'formal_delivery_checks.json') or []),
        completed=[r['round'] for r in obj(root / 'formal_delivery_checks.json') or []],
        reason='Repeated transient foreign GPU computations invalidated L candidate 2; no further rounds launched',
        calibration=dict(L='candidate1 failed capacity; candidate2 interrupted twice; candidate3 not run', H='not run'),
        next_checkpoint='L candidate2, same frozen 313-request calibration_trace_2.json',
        required_condition='Continuous two-GPU window without foreign compute processes entering selected 0,2',
        models_released=True, evidence=obj(root / 'finish_audit.json'),
        serving_native_unchanged=all(r['unchanged'] for r in integrity), normal_unload_qualified=False,
        formal_five_seed_experiment=False)
    save(root / 'delivery_status.json', status)
    sources = root / 'final_available_sources'
    sources.mkdir(exist_ok=True)
    names = ('two_gpu_formal_load.py', 'two_gpu_formal_resume.py', 'two_gpu_formal_continue.py',
        'two_gpu_formal_failure_guard.py', 'two_gpu_formal_finish.py', 'two_gpu_formal_report.py',
        'two_gpu_formal_finalize.py', 'two_gpu_formal_package.py')
    patches = []
    for name in names:
        p = project / 'tools' / name
        shutil.copy2(p, sources / name)
        original = subprocess.run(['git', 'show', 'HEAD:tools/' + name], cwd=project, text=True, capture_output=True)
        old = original.stdout if original.returncode == 0 else ''
        patches.extend(difflib.unified_diff(old.splitlines(keepends=True), p.read_text().splitlines(keepends=True),
            fromfile='a/tools/' + name if old else '/dev/null', tofile='b/tools/' + name))
    (root / 'experiment_scripts.patch').write_text(''.join(patches), encoding='utf-8')
    save(root / 'final_available_sources_manifest.json', [dict(file=p.name, sha256=digest(p)) for p in sources.glob('*.py')])
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=project, text=True, capture_output=True)
    save(root / 'git_head_final.json', dict(head=head.stdout.strip(), code_scope='Experiment tooling only; delivery serving/native files unchanged'))
    delivery = '''# 本次正式执行交付（因外部GPU干扰中断）

有效正式单元4/12：A/B、A/E、A/D、A/C，各772/772请求。C/D/E共2316个逐请求对照均与B的token序列、文本和finish_reason一致；四轮SSE完整，输出预算保持4096，759条stop、13条length。实际输入重放核对见input_identity_summary.json。

源计算排空：B 135.180秒、C 8.758秒、D 10.240秒、E 12.087秒。源30秒窗口功率分别285.498W、16.431W、12.332W、45.784W；下降分别33.185W、307.924W、307.170W、272.405W。源计算排空后服务和模型仍存活，不代表显存释放或GPU可重新分配。

后120秒两卡能耗：B 73.251kJ、C 41.898kJ、D 42.115kJ、E 42.229kJ。整批工作能耗：B 355.151kJ、C 316.932kJ、D 320.609kJ、E 320.278kJ；各自窗口约744.2—745.2秒。固定窗口与整批工作分开比较，不平均不同场景。

t0原有目标背景9条请求：完成延迟均值B 33.691秒、C 34.566秒、D 44.124秒、E 44.812秒；t0之后剩余均值B 14.984秒、C 15.849秒、D 25.427秒、E 26.064秒。token方式初次重算在当前目标资源下会影响旧背景请求，不宣称无代价。

实际接管C 27、D 28、E 26。D有3、E有5个只有wire收发的尝试，客户端正常完成但迁移终态证据不足，保留未分类。E的本批成功接管均为0轮后台追赶；初次重建及最终补齐真实发生，未覆盖多轮后台增量追赶。

A/B/E/D使用物理1、3；第一A/C因外部进程插入GPU1污染。按用户“1，3被占了你就用另外两个”改为S=0、D=2，A/C同清单有效重跑；A/C相对B的功率差包含卡组合变化，不作纯模式归因。UUID与初始温度/时钟见actual_round_gpu_mapping.json及各轮gpu_mapping.json。

L候选1（源1、背景.3）完成380/380但容量不通过：waiting斜率.746/s、晚段均值140.435、早段52.259、晚段完成.567/s、抢占200。L候选2（源.75、背景.3，冻结313条）两次被外部计算进入0、2污染，分别约254秒及149秒停止，266发送/156完成/110保护取消及154发送/97完成/57保护取消。污染尝试不判容量通过或不通过。候选3及H未运行；没有把未运行场景称为正式覆盖。

请求与临时迁移资源恢复，KV空闲块恢复4680基线。本实验进程及子进程均已退出，两卡无本实验残留，未终止其他用户任务。A/C测量后约34.586秒记录native退出segfault，位于额外30秒服务存活观察之后；保留证据，不继续Barex修复、不授予正常卸载资格。旧FREEZE_MIGRATION_VALIDATION_REVOKED.json哈希保持不变。

报告：report.html。有效轮次A/main_*，原始日志、power.csv、client JSONL、token/KV事件、replay.json、launch.jsonl、native_loaded.json、cleanup_observations.json和shutdown.json均保存。三次无效尝试见excluded_interrupted_attempts.json及对应目录；未发送、未完成和保护取消原始清单均保留。

只修改实验驱动、接续、观测报告及打包工具，没有修改本次交付的22个服务文件和4个native库；结束哈希见serving_native_integrity_final.json。运行环境仍是llumnix5090_vllm018，项目Git HEAD及文件manifest保存。实际执行源码快照分别在frozen_execution_sources、resume_execution_sources；最终可用源码在final_available_sources，补丁experiment_scripts.patch只涉及实验工具。

后续从同一L候选2开始，保留L候选1和A四轮。不应直接重跑旧one-shot启动脚本；下一次唯一归档标签的命令见RESUME_UNFINISHED.sh。该脚本尚未执行，运行前需保证0、2能够连续不被其他计算插入。每次仍做GPU双次10秒复核，忙卡不启动。

回退不需要重新安装依赖或恢复native库。已结束的实验不再有模型进程；如需回退工具，使用保存的实际执行源码快照并先核对其manifest，保留全部results记录。不要覆盖交付后端、删除旧保护或使用全局ray stop/pkill。

精确最后源chunk到首个目标chunk间隔、逐请求KV字节、完整网络封装字节、全部native lease清空时刻均标缺失。现有暂停边界chunk间隔不是逐token ITL。结论限于本地两卡当前实现，不外推生产规模或跨地域，也不宣称完整ServerlessLLM复现。未追加五种子实验。
'''
    (root / 'DELIVERY.md').write_text(delivery, encoding='utf-8')
    ready = obj(root / 'report_ready.json') or {}
    ready.update(plots_pending_local_cpu=False, partial_delivery=True, completed_formal_cells=4)
    save(root / 'report_ready.json', ready)
    paths = [p for p in sorted(root.rglob('*')) if p.is_file() and not p.is_symlink()
        and p.suffix != '.gz' and p.name != 'artifact_manifest.json' and '__pycache__' not in p.parts]
    manifest = [dict(path=str(p.relative_to(root)), bytes=p.stat().st_size, sha256=digest(p)) for p in paths]
    save(root / 'artifact_manifest.json', manifest)
    if archive.exists():
        raise RuntimeError('Archive target already exists; evidence must not be overwritten')
    with tarfile.open(archive, 'w:gz', compresslevel=3) as tar:
        for p in paths + [root / 'artifact_manifest.json']:
            tar.add(p, arcname=root.name + '/' + str(p.relative_to(root)), recursive=False)
    archive.with_suffix(archive.suffix + '.sha256').write_text(digest(archive) + '  ' + archive.name + '\n')
    print(json.dumps(dict(archive=str(archive), size=archive.stat().st_size, sha256=digest(archive),
        files=len(manifest), serving_native_unchanged=all(r['unchanged'] for r in integrity)), ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--archive', required=True)
    args = parser.parse_args()
    package(Path(args.root), Path(args.archive))
