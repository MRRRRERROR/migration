#!/usr/bin/env bash
set -euo pipefail

source "$HOME/miniforge3/etc/profile.d/conda.sh"
conda activate llumnix-wsl-sim

cd /mnt/d/llumnix-ray-main

ray stop --force >/dev/null 2>&1 || true

log_name="wsl_sim_smoke_$(date +%Y%m%d_%H%M%S).log"
profile="/mnt/d/llumnix-ray-main/_simulator_profile_test/simulator_approx_profile_5090/qwen2p5_7b_5090_profile_approx_numpy126.pkl"

python -u -m llumnix.entrypoints.vllm.api_server \
  --config-file configs/vllm.yml \
  --port 1234 \
  --ray-cluster-port 6389 \
  --initial-instances 1 \
  --model /mnt/d/llumnix-ray-main/_simulator_model_stub/Qwen2.5-7B-Instruct \
  --trust-remote-code \
  --max-model-len 1024 \
  --max-num-batched-tokens 2048 \
  --gpu-memory-utilization 0.5 \
  --worker-use-ray \
  --device cpu \
  --dispatch-policy flood \
  --simulator-mode \
  --profiling-result-file-path "$profile" \
  --disable-async-output-proc \
  --log-instance-info \
  --log-filename wsl_sim_smoke_instance.log \
  > "$log_name" 2>&1 &

pid=$!
echo "server_pid=$pid"
echo "log=$log_name"

ready=0
for _ in $(seq 1 90); do
  if curl -fsS http://127.0.0.1:1234/is_ready >/tmp/llumnix_ready.out 2>/dev/null; then
    cat /tmp/llumnix_ready.out
    ready=1
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "server_exited"
    break
  fi
  sleep 2
done

if [ "$ready" = 1 ]; then
  echo
  echo "=== generate ==="
  curl -sS -w "\nHTTP_STATUS:%{http_code}\n" http://127.0.0.1:1234/generate_benchmark \
    -H "Content-Type: application/json" \
    -d '{"prompt":"Hello","n":1,"temperature":0.0,"max_tokens":8,"ignore_eos":true}'
fi

echo
printf "=== log tail: %s ===\n" "$log_name"
tail -n 160 "$log_name" || true

kill "$pid" >/dev/null 2>&1 || true
ray stop --force >/dev/null 2>&1 || true
