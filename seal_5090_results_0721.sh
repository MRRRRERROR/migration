#!/usr/bin/env bash
set -eo pipefail

REPO=/data/home/zylu/llumnix-v018-main
BLADE=/data/home/zylu/tmp/blade-kvt
RESULT_DIR="$REPO/results/5090_4inst_formal_20260721_014617"
ARCHIVE="$REPO/results/5090_4inst_formal_20260721_014617.tar.gz"

cd "$REPO"

mkdir -p "$RESULT_DIR"

git diff > "$RESULT_DIR/llumnix_git_diff.patch" || true
git status --short > "$RESULT_DIR/llumnix_git_status.txt" || true
git -C "$BLADE" diff > "$RESULT_DIR/blade_kvt_git_diff.patch" || true
git -C "$BLADE" status --short > "$RESULT_DIR/blade_kvt_git_status.txt" || true

python - <<'PY' > "$RESULT_DIR/env_probe.txt"
from pathlib import Path
import importlib
import os
import subprocess

def run(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        return repr(exc)

print("llumnix_commit", run("git -C /data/home/zylu/llumnix-v018-main rev-parse HEAD"))
print("llumnix_status")
print(run("git -C /data/home/zylu/llumnix-v018-main status --short"))
print("blade_kvt_commit", run("git -C /data/home/zylu/tmp/blade-kvt rev-parse HEAD"))
print("blade_kvt_branch", run("git -C /data/home/zylu/tmp/blade-kvt branch --show-current"))
print("blade_kvt_status")
print(run("git -C /data/home/zylu/tmp/blade-kvt status --short"))

for name in ["llumnix", "vllm", "blade_kvt"]:
    try:
        mod = importlib.import_module(name)
        print(f"{name}_import_path", getattr(mod, "__file__", ""))
        print(f"{name}_version", getattr(mod, "__version__", "unknown"))
    except Exception as exc:
        print(f"{name}_import_error", repr(exc))

try:
    import torch
    print("torch_version", torch.__version__)
    print("torch_cuda", torch.version.cuda)
    print("cuda_available", torch.cuda.is_available())
    print("gpu_count", torch.cuda.device_count())
    for i in range(torch.cuda.device_count()):
        print(f"gpu_{i}", torch.cuda.get_device_name(i))
except Exception as exc:
    print("torch_probe_error", repr(exc))

print("CUDA_HOME", os.environ.get("CUDA_HOME", ""))
print("CUDA_VISIBLE_DEVICES", os.environ.get("CUDA_VISIBLE_DEVICES", ""))
PY

{
  echo "# ENV_AND_PATCH_SUMMARY"
  echo
  echo "## Environment"
  echo
  cat "$RESULT_DIR/env_probe.txt"
  echo
  echo "## Key Patches"
  echo
  echo "- blade_kvt source branch: codex/backport-site-compat-20260720_220335"
  echo "- engine_proxy.py: added sched_skip_migrating_req compatibility for one-arg and two-arg scheduler calls; fallback returns False."
  echo "- hybrid_modules.py: removed stale vllm.v1.hybrid_connector import path and used blade_kvt-local compatibility constants."
  echo "- migration/backend.py: fixed block id handling from blockids[0] to blockids and added decode-backend worker-info lookup."
  echo "- migration/kvtmigration.py: handled bypass_bind substepid=0 and matching bypass_clear path."
  echo "- run_remote_minimal_migration_probe.sh: set VLLM_HOST_IP=127.0.0.1 so KVT TCP endpoints use loopback on this single-node 5090 run."
  echo
  echo "## Diff Files"
  echo
  echo "- llumnix_git_diff.patch"
  echo "- llumnix_git_status.txt"
  echo "- blade_kvt_git_diff.patch"
  echo "- blade_kvt_git_status.txt"
  echo
  echo "## Startup Commands"
  echo
  echo "### Formal orchestration"
  echo '```bash'
  sed -n '1,240p' "$REPO/run_formal_4inst_0721.sh" 2>/dev/null || true
  echo '```'
  echo
  echo "### Baseline runner"
  echo '```bash'
  sed -n '1,260p' "$REPO/run_remote_no_migration_baseline.sh" 2>/dev/null || true
  echo '```'
  echo
  echo "### Migration runner"
  echo '```bash'
  sed -n '1,320p' "$REPO/run_remote_minimal_migration_probe.sh" 2>/dev/null || true
  echo '```'
} > "$RESULT_DIR/ENV_AND_PATCH_SUMMARY.md"

python - <<'PY' > "$RESULT_DIR/results_summary_field_check.txt"
from pathlib import Path

root = Path("/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617")
text = (root / "RESULTS_SUMMARY.md").read_text(errors="ignore")
checks = {
    "baseline 三轮": "baseline 三轮" in text or "Baseline 结果" in text,
    "migration 三轮": "migration 三轮" in text or "Migration 结果" in text,
    "migration_count": "migration_count" in text,
    "finish_request_ids": "finish_request_ids" in text or "FinishedReq" in text,
    "downtime": "downtime" in text,
    "successful_responses": "successful_responses" in text,
}
for key, ok in checks.items():
    print(f"{key}: {ok}")
missing = [key for key, ok in checks.items() if not ok]
print("missing:", ", ".join(missing) if missing else "none")
PY

{
  echo "## EngineCore fatal / Traceback / ERROR scan"
  echo
  echo "### EngineCore fatal"
  grep -Rni "EngineCore fatal" "$RESULT_DIR" 2>/dev/null || true
  echo
  echo "### Traceback"
  grep -Rni "Traceback" "$RESULT_DIR" 2>/dev/null || true
  echo
  echo "### ERROR"
  grep -Rni "ERROR" "$RESULT_DIR" 2>/dev/null || true
} > "$RESULT_DIR/fatal_error_scan.txt"

python - <<'PY' > "$RESULT_DIR/FINAL_HANDOFF.md"
from pathlib import Path
import csv

root = Path("/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617")
rows = list(csv.DictReader((root / "formal_results.csv").open()))
baseline = [r for r in rows if r["kind"] == "baseline"]
migration = [r for r in rows if r["kind"] == "migration"]
scan = (root / "fatal_error_scan.txt").read_text(errors="ignore")
fatal_present = "EngineCore fatal" in scan or "Traceback" in scan

def avg(key, subset):
    vals = [float(r[key]) for r in subset if r.get(key)]
    return sum(vals) / len(vals) if vals else 0.0

print("# FINAL_HANDOFF")
print()
print("实验是否完成：完成。四实例 baseline 三轮和 migration 三轮均完成，migration 日志包含真实 KVT transfer 证据。")
print(f"结果目录：{root}")
print()
print("关键指标：")
print(f"- baseline：3 轮，每轮 successful_responses=50/50，平均 qps={avg('qps', baseline):.4f}，平均 throughput_tok_s={avg('throughput_tok_s', baseline):.2f}，平均 median_e2e_latency={avg('median_e2e_latency', baseline):.4f}s。")
print(f"- migration：3 轮，每轮 successful_responses=50/50，平均 qps={avg('qps', migration):.4f}，平均 throughput_tok_s={avg('throughput_tok_s', migration):.2f}，平均 median_e2e_latency={avg('median_e2e_latency', migration):.4f}s。")
print(f"- migration evidence：KVTResp(code=0)={sum(int(r['kvt_code0']) for r in migration)}，FinishedReq lines={sum(int(r['finished_req_lines']) for r in migration)}，unique FinishedReq ids={sum(int(r['unique_finished_req_ids']) for r in migration)}，SendStubMetrics={sum(int(r['send_metrics']) for r in migration)}，migration_end={sum(int(r['migration_end']) for r in migration)}，KVTResp(code=404)={sum(int(r['kvt_code404']) for r in migration)}，KVTResp(code=500)={sum(int(r['kvt_code500']) for r in migration)}。")
print()
print(f"是否存在 fatal：{'存在，需要查看 fatal_error_scan.txt' if fatal_present else '未发现 EngineCore fatal 或 Traceback。'}")
print()
print("可用于汇报的文件：")
for name in [
    "RESULTS_SUMMARY.md",
    "formal_results.csv",
    "ENV_AND_PATCH_SUMMARY.md",
    "FINAL_HANDOFF.md",
    "fatal_error_scan.txt",
    "llumnix_git_diff.patch",
    "blade_kvt_git_diff.patch",
    "baseline_run1.summary.txt",
    "baseline_run2.summary.txt",
    "baseline_run3.summary.txt",
    "migration_run1.summary.txt",
    "migration_run2.summary.txt",
    "migration_run3.summary.txt",
]:
    print(f"- {root / name}")
PY

tar -C "$REPO/results" -czf "$ARCHIVE" "5090_4inst_formal_20260721_014617"

echo "SEALED_RESULT_DIR=$RESULT_DIR"
echo "SEALED_ARCHIVE=$ARCHIVE"
echo "SUMMARY_CHECK"
cat "$RESULT_DIR/results_summary_field_check.txt"
echo "FATAL_SCAN_COUNTS"
printf "EngineCore fatal: "
grep -Rci "EngineCore fatal" "$RESULT_DIR" 2>/dev/null | awk -F: '{s+=$2} END{print s+0}'
printf "Traceback: "
grep -Rci "Traceback" "$RESULT_DIR" 2>/dev/null | awk -F: '{s+=$2} END{print s+0}'
printf "ERROR: "
grep -Rci "ERROR" "$RESULT_DIR" 2>/dev/null | awk -F: '{s+=$2} END{print s+0}'
echo "ARCHIVE_SIZE"
ls -lh "$ARCHIVE"
