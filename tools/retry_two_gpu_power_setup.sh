#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
old=$project/results/two_gpu_power_drain_20261006_2119
root=$old/attempt_02
mkdir "$root"
mkdir "$root/inputs"
cp "$old/inputs/prompts.jsonl" "$root/inputs/"
cp "$old/workload_manifest.json" "$old/formal_order.json" "$root/"
cat > "$root/retry_reason.json" <<'JSON'
{"attempt":2,"previous_root":"..","reason":"Experiment observer read scheduler before EngineCore initialization completed; failed startup, zero replay requests. Fixed observer to wait for scheduler without changing frozen KVT sources.","previous_logs_preserved":true,"not_a_formal_rerun":true}
JSON
# Only processes whose exact PIDs/commands were read from this failed attempt.
python=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python
"$python" - <<'PY'
import os,psutil,signal,json,time
expected={4045298:'redis-server --port 6379',4045307:'4cbe7b02-a085-4617-abbc-c82d4b720340',4045310:'02a97d2c-70eb-499c-848e-d01758cccd07',4032013:'two_gpu_power_preflight.sh'}
for pid,text in expected.items():
    if not psutil.pid_exists(pid):continue
    p=psutil.Process(pid)
    if text not in ' '.join(p.cmdline()) or p.uids().real!=os.getuid():raise RuntimeError('Ownership mismatch')
    targets=p.children(recursive=True)+[p]
    for child in targets:child.terminate()
    gone,alive=psutil.wait_procs(targets,timeout=15)
    if alive:raise RuntimeError('Own helper did not exit gracefully')
print('OWN_FAILED_START_HELPERS_CLEANED')
PY
nohup bash "$project/tools/start_two_gpu_power_drain.sh" --root "$root" > "$root/controller.log" 2>&1 < /dev/null &
echo "$!"
