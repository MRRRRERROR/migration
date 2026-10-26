#!/usr/bin/env bash
set -euo pipefail
root=/data/home/zylu/llumnix-v018-main/results/two_gpu_power_drain_20261006_2119
printf 'CONTROLLER\n'
tail -n 10 "$root/controller.log" 2>/dev/null || true
printf 'FILES\n'
find "$root" -maxdepth 2 -name summary.json -o -name experiment_blocked.json -o -name blocked.json -o -name completed.json
printf 'GPU\n'
nvidia-smi -i 0,1 --query-gpu=index,uuid,memory.used,utilization.gpu,power.draw --format=csv
nvidia-smi -i 0,1 --query-compute-apps=pid,process_name,used_memory --format=csv
printf 'STATE\n'
for path in "$root"/calibration_*/state_S/*.json "$root"/calibration_*/state_D/*.json; do
    if [[ -f "$path" ]]; then head -c 600 "$path"; printf '\n'; fi
done
printf 'LATEST_LOG\n'
find "$root" -maxdepth 2 -name 'vllm_*.log' -printf '%T@ %p\n' | sort -nr | head -2 | cut -d' ' -f2- | while IFS= read -r path; do tail -n 4 "$path"; done
