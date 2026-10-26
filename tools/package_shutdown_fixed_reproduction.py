"""Record the exact tools and finite reproduction commands in the new result."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    args=parser.parse_args()
    root=Path(args.root)
    source=Path(__file__).resolve().parent
    target=root/'experiment_code'
    target.mkdir(exist_ok=True)
    names=['two_gpu_power_drain.py','two_gpu_power_prepare.py','two_gpu_power_metrics.py',
           'two_gpu_power_enrich.py','two_gpu_nvml_monitor.py','two_gpu_power_progress.py',
           'two_gpu_power_version_info.py','shutdown_fixed_bc_report.py','start_two_gpu_power_drain.sh',
           'freeze_and_recalibrate_shutdown_fixed.sh','connector_shutdown_cases.py',
           'build_connector_shutdown_stage.sh','run_connector_shutdown_acceptance.sh',
           'run_connector_shutdown_cases.sh','audit_shutdown_resources.py']
    manifest=[]
    for name in names:
        path=source/name
        shutil.copy2(path,target/name)
        manifest.append({'file':name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    for name in ('power_drain_observer','shutdown_lifecycle_observer'):
        shutil.copytree(source/name,target/name,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__'))
        for path in (source/name).rglob('*.py'):
            manifest.append({'file':str(path.relative_to(source)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    (target/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    text='''本次范围：6项基础退出验收通过、两个300秒容量点通过、B通过，C目标native退出失败。正式0/15。
整体验收未通过，冻结版本已撤销迁移适用资格，禁止继续正式实验。显式native收尾候选lifecycle2在最小GDB复现中也失败，已回退。
物理GPU仅0/1，每卡一个实例；同机两卡逻辑双域。未设置抢空闲卡任务，未使用SIGKILL作正常退出。
修复冻结路径：/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01/frozen_accepted。
修复证据与完整冻结内容：../connector_shutdown_repair_20261007_01。
模型Qwen2.5-7B-Instruct，BF16，上下文9216，batch token预算8192，每实例KV cache 4GiB。
最终到达率S=1.0、D背景=0.3rps；入口切换后D=1.3rps。输出最多4096，temperature=0，正常EOS。
提示词原始ShareGPT文本去重，实际模板输入也筛选128～4096 token；池1581条，均匀有放回抽样。
输出达到上限的length请求仍受长度限制，不能称完全自然生成。表格及配对图仅短流程验收，不是15轮统计结论。

以下为原校准/B/C运行命令的复现记录；当前该版本被验证撤销标记阻断。只有native生命周期问题解决并重新验收后才能运行正式流程。
在两张已获准GPU可用且端口空闲时（把NEW_RESULT替换为一个未使用的新目录）：
cd /data/home/zylu/llumnix-v018-main
PYTHON=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python
NEW_RESULT=/data/home/zylu/llumnix-v018-main/results/<new-unique-directory>
"$PYTHON" tools/two_gpu_power_prepare.py --root "$NEW_RESULT" --source /data/home/zylu/llumnix-v018-main/results/two_gpu_power_drain_20261006_2119/inputs/sharegpt_gpt4.jsonl
export LLUMNIX_SERVING_FROZEN_DIR=/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01/frozen_accepted
bash tools/start_two_gpu_power_drain.sh --root "$NEW_RESULT" --stop-after-acceptance

重新运行退出矩阵：每次必须使用新root，不能覆盖当前final_*目录。
bash tools/run_connector_shutdown_cases.sh --root <new-case-directory> --instances 1 --requests 0 --stop api_sigterm
bash tools/run_connector_shutdown_cases.sh --root <new-case-directory> --instances 1 --requests 3 --stop api_sigterm
bash tools/run_connector_shutdown_cases.sh --root <new-case-directory> --instances 1 --requests 0 --stop project_tree
bash tools/run_connector_shutdown_cases.sh --root <new-case-directory> --instances 1 --requests 3 --stop project_tree
bash tools/run_connector_shutdown_cases.sh --root <new-case-directory> --instances 2 --requests 0 --stop api_sigterm
bash tools/run_connector_shutdown_cases.sh --root <new-case-directory> --instances 2 --requests 3 --stop api_sigterm

结果后处理：在Linux部署环境使用tools/two_gpu_power_enrich.py --round <acceptance_B/C目录>保存原始计数。
下载新结果后在有matplotlib的CPU环境运行：python tools/shutdown_fixed_bc_report.py --root <新结果目录>。
代码与日志路径、运行时包版本、模型权重hash、generation config内容分别见experiment_code、version_manifest.json、runtime_and_model_versions.json。
本目录中的正式种子101～105回放文件和15轮顺序为准备记录；未把短验收算入正式轮次。
'''
    (root/'reproduction_zh.txt').write_text(text,encoding='utf-8')
    print(json.dumps({'packaged_files':len(manifest)}))


if __name__=='__main__':
    main()
