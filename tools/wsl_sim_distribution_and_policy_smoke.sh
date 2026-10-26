#!/usr/bin/env bash
set -euo pipefail

source "$HOME/miniforge3/etc/profile.d/conda.sh"
conda activate llumnix-wsl-sim

cd /mnt/d/llumnix-ray-main

profile="/mnt/d/llumnix-ray-main/_simulator_profile_test/simulator_approx_profile_5090/qwen2p5_7b_5090_profile_approx_numpy126.pkl"
model="/mnt/d/llumnix-ray-main/_simulator_model_stub/Qwen2.5-7B-Instruct"

start_server() {
  local out_dir="$1"
  local dispatch_policy="$2"
  local migration_enabled="$3"
  local request_policy="${4:-SR}"

  mkdir -p "$out_dir"
  ray stop --force >/dev/null 2>&1 || true
  python -u -m llumnix.entrypoints.vllm.api_server \
    --config-file configs/vllm.yml \
    --port 1234 \
    --ray-cluster-port 6389 \
    --initial-instances 4 \
    --model "$model" \
    --trust-remote-code \
    --max-model-len 1024 \
    --max-num-batched-tokens 2048 \
    --gpu-memory-utilization 0.5 \
    --worker-use-ray \
    --device cpu \
    --dispatch-policy "$dispatch_policy" \
    --topk-random-dispatch 1 \
    --simulator-mode \
    --profiling-result-file-path "$profile" \
    --disable-async-output-proc \
    --log-instance-info \
    --log-filename "$out_dir/wsl_sim_instance.log" \
    ${migration_enabled:+--enable-routine-migration --pair-migration-frequency 1 --pair-migration-policy defrag --migrate-out-threshold 3300 --migration-load-metric remaining_steps --request-migration-policy "$request_policy" --migration-backend rayrpc --migration-buffer-blocks 32} \
    > "$out_dir/server_console.log" 2>&1 &
  echo "$!" > "$out_dir/server.pid"
}

wait_ready() {
  local out_dir="$1"
  local pid
  pid="$(cat "$out_dir/server.pid")"
  for _ in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:1234/is_ready > "$out_dir/is_ready.json" 2>/dev/null; then
      echo "ready=true" | tee -a "$out_dir/run_status.txt"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server_exited_before_ready" | tee -a "$out_dir/run_status.txt"
      return 1
    fi
    sleep 2
  done
  echo "ready=false" | tee -a "$out_dir/run_status.txt"
  return 1
}

stop_server() {
  local out_dir="$1"
  if [ -f "$out_dir/server.pid" ]; then
    kill "$(cat "$out_dir/server.pid")" >/dev/null 2>&1 || true
  fi
  ray stop --force >/dev/null 2>&1 || true
}

scan_logs() {
  local out_dir="$1"
  grep -RniE "Traceback|ERROR|RuntimeError|ActorDiedError|Error in engine loop|EngineCore" "$out_dir/server_console.log" "$out_dir/client_console.log" > "$out_dir/error_scan.txt" 2>/dev/null || true
  grep -RniE "dispatch request|Engine finished request|Migrate|migrate|migration|successful_responses|throughput_tok_s" "$out_dir/server_console.log" "$out_dir/client_console.log" > "$out_dir/key_events.txt" 2>/dev/null || true
}

count_instances() {
  local out_dir="$1"
  python - "$out_dir" <<'PY'
import json
import re
import sys
from collections import Counter
from pathlib import Path

out = Path(sys.argv[1])
text = (out / "server_console.log").read_text(errors="replace")
patterns = [r"global_scheduler.py:\d+\] dispatch request [0-9a-f]{8,} to instance ([0-9a-f]{8,})"]
counts = Counter()
for pattern in patterns:
    for match in re.finditer(pattern, text):
        counts[match.group(1)] += 1
data = {
    "dispatch_counts": dict(counts),
    "total_dispatch_records": sum(counts.values()),
    "instance_count_seen": len(counts),
}
(out / "instance_distribution.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
print(json.dumps(data, indent=2))
PY
}

write_distribution_report() {
  local out_dir="$1"
  python - "$out_dir" <<'PY'
import json
from pathlib import Path
import sys

out = Path(sys.argv[1])
dist = json.loads((out / "instance_distribution.json").read_text())
bench = json.loads((out / "concurrent_benchmark.json").read_text())
counts = dist["dispatch_counts"]
lines = [
    "# SIM_4INST_DISTRIBUTION",
    "",
    f"- 结果目录: `{out}`",
    "- 配置: 4 instances, 100 requests, concurrency=20, stable_short, no_migration",
    "- 调度策略: `load`",
    f"- 成功请求: {bench['successful_responses']}/{bench['requests']}",
    f"- QPS: {bench['qps']:.4f}",
    f"- 吞吐: {bench['throughput_tok_s']:.4f} tok/s",
    f"- median E2E: {bench['median_e2e_s']}",
    "",
    "## 实例分布",
    "",
    "| instance_id | 请求数 |",
    "| --- | ---: |",
]
for key, value in sorted(counts.items()):
    lines.append(f"| `{key}` | {value} |")
lines += [
    "",
    "## 判断",
    "",
    "未集中到单实例。" if len(counts) > 1 and max(counts.values(), default=0) < bench["requests"] else "仍然集中到单实例。",
]
(out / "SIM_4INST_DISTRIBUTION.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(out / "SIM_4INST_DISTRIBUTION.md")
PY
}

run_one() {
  local out_dir="$1"
  local dispatch_policy="$2"
  local migration_enabled="$3"
  local request_policy="${4:-SR}"

  echo "out_dir=$out_dir" | tee "$out_dir/run_status.txt"
  echo "dispatch_policy=$dispatch_policy" | tee -a "$out_dir/run_status.txt"
  echo "migration_enabled=${migration_enabled:-false}" | tee -a "$out_dir/run_status.txt"

  start_server "$out_dir" "$dispatch_policy" "$migration_enabled" "$request_policy"
  wait_ready "$out_dir"
  python tools/sim_concurrent_benchmark.py \
    --requests 100 \
    --concurrency 20 \
    --max-tokens 32 \
    --output "$out_dir/concurrent_benchmark.json" \
    2>&1 | tee "$out_dir/client_console.log"
  scan_logs "$out_dir"
  count_instances "$out_dir" 2>&1 | tee "$out_dir/instance_distribution_console.log"
  stop_server "$out_dir"
}

main() {
  local root="results/wsl_sim_validation_$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$root"

  python tools/sim_profile_sanity_check.py \
    --profile "$profile" \
    --result-dir "results/wsl_sim_4inst_20260831_200717" \
    --input-len 64 \
    --output-len 32 \
    --decode-batch-size 8

  local dist_dir="$root/distribution_no_migration"
  mkdir -p "$dist_dir"
  run_one "$dist_dir" "load" "" "SR"
  write_distribution_report "$dist_dir"

  local summary="$root/SIM_POLICY_SMOKE_SUMMARY.md"
  {
    echo "# SIM_POLICY_SMOKE_SUMMARY"
    echo
    echo "- 根目录: \`$root\`"
    echo "- 配置: 4 instances, 100 requests, concurrency=20, stable_short"
    echo
  } > "$summary"

  for policy in no_migration load_sr tpot_tail composite_proposed; do
    out_dir="$root/policy_${policy}"
    mkdir -p "$out_dir"
    case "$policy" in
      no_migration)
        run_one "$out_dir" "load" "" "SR"
        ;;
      load_sr)
        run_one "$out_dir" "flood" "1" "SR"
        ;;
      tpot_tail|composite_proposed)
        {
          echo "policy=$policy"
          echo "status=not_run"
          echo "reason=current simulator entrypoint exposes request-migration-policy choices LCR/SR/LR/FCW/FCWSR only; TPOT/composite policy is implemented in the separate Go gateway/scheduler path, not this local vLLM simulator entrypoint."
        } | tee "$out_dir/run_status.txt"
        ;;
    esac
    scan_logs "$out_dir" || true
    count_instances "$out_dir" > "$out_dir/instance_distribution_console.log" 2>&1 || true
    python - "$policy" "$out_dir" "$summary" <<'PY'
import json
import sys
from pathlib import Path

policy = sys.argv[1]
out = Path(sys.argv[2])
summary = Path(sys.argv[3])
bench_path = out / "concurrent_benchmark.json"
status = "未运行"
details = ""
if bench_path.exists():
    data = json.loads(bench_path.read_text())
    status = "完成" if data["successful_responses"] == data["requests"] else "请求未全成功"
    details = (
        f"成功 {data['successful_responses']}/{data['requests']}，"
        f"QPS {data['qps']:.4f}，吞吐 {data['throughput_tok_s']:.4f} tok/s"
    )
else:
    details = (out / "run_status.txt").read_text(errors="replace").strip() if (out / "run_status.txt").exists() else "无记录"
with summary.open("a", encoding="utf-8") as f:
    f.write(f"\n## {policy}\n\n")
    f.write(f"- 状态: {status}\n")
    f.write(f"- 结果目录: `{out}`\n")
    f.write(f"- 摘要: {details}\n")
    if (out / "key_events.txt").exists():
        text = (out / "key_events.txt").read_text(errors="replace")
        f.write(f"- migration 关键词行数: {sum(1 for line in text.splitlines() if 'migrat' in line.lower())}\n")
    if (out / "error_scan.txt").exists():
        err = (out / "error_scan.txt").read_text(errors="replace").strip()
        f.write(f"- error_scan 行数: {len(err.splitlines()) if err else 0}\n")
PY
  done
  echo "$root"
  echo "$summary"
}

main "$@"
