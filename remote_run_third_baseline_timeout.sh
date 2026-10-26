#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/llumnix-ray-main"
source "$HOME/venvs/llumnix/bin/activate"

BASE_EXP_DIR="$HOME/llumnix_results/baseline_sim_opt-1.3b_20260701_192646"
MODEL_PATH="$HOME/models/opt-1.3b"
SOURCE_EXP="$HOME/llumnix_results/opt-1.3b_20260628_153807"
NAME="load_migration_defrag"

export HEAD_NODE=1
export HEAD_NODE_IP=127.0.0.1

SERVICE_PID=""
cleanup() {
  set +e
  if [[ -n "${SERVICE_PID:-}" ]]; then
    kill "$SERVICE_PID" >/dev/null 2>&1 || true
    wait "$SERVICE_PID" >/dev/null 2>&1 || true
    SERVICE_PID=""
  fi
  ray stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 60); do
    if python - <<'PY' >/tmp/llumnix_ready_third.out 2>/tmp/llumnix_ready_third.err
import ray, sys
try:
    ray.init(address="127.0.0.1:6389", namespace="llumnix", ignore_reinit_error=True, log_to_driver=False)
    names = [a["name"] for a in ray.util.list_named_actors(all_namespaces=True) if a["namespace"] == "llumnix"]
    inst = [n for n in names if n.startswith("instance_")]
    print(names)
    sys.exit(0 if len(inst) >= 2 else 2)
except Exception as e:
    print(repr(e))
    sys.exit(1)
PY
    then
      cat /tmp/llumnix_ready_third.out >> "$BASE_EXP_DIR/${NAME}_retry_ready.log"
      return 0
    fi
    sleep 2
  done
  echo "service not ready" | tee -a "$BASE_EXP_DIR/${NAME}_retry_ready.log"
  cat /tmp/llumnix_ready_third.out /tmp/llumnix_ready_third.err >> "$BASE_EXP_DIR/${NAME}_retry_ready.log" 2>/dev/null || true
  return 1
}

wait_port() {
  for _ in $(seq 1 120); do
    if python - <<'PY' >/dev/null 2>&1
import socket
s = socket.socket()
s.settimeout(1)
try:
    s.connect(("127.0.0.1", 1234))
finally:
    s.close()
PY
    then
      echo "port 1234 ready at $(date)" >> "$BASE_EXP_DIR/${NAME}_retry_ready.log"
      sleep 3
      return 0
    fi
    sleep 1
  done
  echo "port 1234 not ready" | tee -a "$BASE_EXP_DIR/${NAME}_retry_ready.log"
  return 1
}

mkdir -p "$BASE_EXP_DIR/$NAME"
rm -f "$BASE_EXP_DIR/$NAME/benchmark.log" "$BASE_EXP_DIR/$NAME/migration.log" \
      "$BASE_EXP_DIR/$NAME/service.log" "$BASE_EXP_DIR/$NAME/server_instance.csv"

echo "===== RETRY $NAME $(date) =====" | tee -a "$BASE_EXP_DIR/run.log"
cleanup

python -u -m llumnix.entrypoints.vllm.api_server \
  --config-file configs/vllm.yml \
  --host 127.0.0.1 \
  --port 1234 \
  --ray-cluster-port 6389 \
  --initial-instances 2 \
  --launch-ray-cluster \
  --model "$MODEL_PATH" \
  --worker-use-ray \
  --max-model-len 2048 \
  --max-num-batched-tokens 4096 \
  --gpu-memory-utilization 0.7 \
  --dispatch-policy load \
  --migrate-out-threshold 100000 \
  --migration-backend rayrpc \
  --migration-buffer-blocks 32 \
  --simulator-mode \
  --profiling-result-file-path "$SOURCE_EXP/profiling.pkl" \
  --disable-async-output-proc \
  --log-instance-info \
  --log-filename "$BASE_EXP_DIR/$NAME/server" \
  --max-units 2 \
  --enable-routine-migration \
  --request-migration-policy SR \
  --enable-defrag True \
  > "$BASE_EXP_DIR/$NAME/service.log" 2>&1 &
SERVICE_PID=$!

wait_ready
wait_port

python benchmark/benchmark_serving.py \
  --ip_ports 127.0.0.1:1234 \
  --tokenizer "$MODEL_PATH" \
  --backend vLLM \
  --gen_random_prompts \
  --random_prompt_count 300 \
  --random_prompt_lens_mean 512 \
  --random_prompt_lens_range 128 \
  --variable_prompt_lens_distribution uniform \
  --allow_variable_generation_length \
  --variable_response_lens_mean 512 \
  --variable_response_lens_range 128 \
  --variable_response_lens_distribution uniform \
  --qps 8 \
  --distribution poisson \
  --log_latencies \
  --fail_on_response_failure \
  > "$BASE_EXP_DIR/$NAME/benchmark.log" 2>&1

grep -RniE "begin migrate|migrate done|migration status|migrate out|migrate in|total_kv_cache_size|speed:|Error in instance migrate_out|Pending migrate in" /tmp/ray/session_latest/logs \
  > "$BASE_EXP_DIR/$NAME/migration.log" 2>/dev/null || true

echo "===== RETRY DONE $NAME $(date) =====" | tee -a "$BASE_EXP_DIR/run.log"
