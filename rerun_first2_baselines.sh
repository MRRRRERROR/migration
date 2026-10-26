#!/usr/bin/env bash
set -uo pipefail

cd "$HOME/llumnix-ray-main" || exit 1
source "$HOME/venvs/llumnix/bin/activate"

EXP_DIR="$HOME/llumnix_results/baseline_first2_retry_$(date +%Y%m%d_%H%M%S)"
MODEL_PATH="$HOME/models/opt-1.3b"
SOURCE_EXP="$HOME/llumnix_results/opt-1.3b_20260628_153807"

export HEAD_NODE=1
export HEAD_NODE_IP=127.0.0.1

mkdir -p "$EXP_DIR"

{
  echo "created_at: $(date)"
  echo "host: $(hostname)"
  echo "repo: $HOME/llumnix-ray-main"
  echo "model_path: $MODEL_PATH"
  echo "source_profiling: $SOURCE_EXP/profiling.pkl"
  echo "mode: simulator"
  echo
  python --version
  pip show llumnix vllm ray torch transformers tokenizers huggingface-hub click 2>/dev/null
} > "$EXP_DIR/env.txt"

cleanup() {
  set +e
  if [[ -n "${SERVICE_PID:-}" ]]; then
    kill "$SERVICE_PID" >/dev/null 2>&1 || true
    wait "$SERVICE_PID" >/dev/null 2>&1 || true
    SERVICE_PID=""
  fi
  ray stop >/dev/null 2>&1 || true
}

wait_instances() {
  local name="$1"
  for _ in $(seq 1 120); do
    if python - <<'PY' > "$EXP_DIR/$name/ready_instances.tmp" 2>&1
import ray, sys
try:
    ray.init(address="127.0.0.1:6389", namespace="llumnix", ignore_reinit_error=True, log_to_driver=False)
    names = [a["name"] for a in ray.util.list_named_actors(all_namespaces=True) if a["namespace"] == "llumnix"]
    inst = [n for n in names if n.startswith("instance_")]
    print(names)
    print("instances:", inst)
    sys.exit(0 if len(inst) >= 2 else 2)
except Exception as e:
    print(repr(e))
    sys.exit(1)
PY
    then
      cat "$EXP_DIR/$name/ready_instances.tmp" >> "$EXP_DIR/$name/ready.log"
      return 0
    fi
    sleep 2
  done
  echo "instances not ready" | tee -a "$EXP_DIR/$name/ready.log"
  cat "$EXP_DIR/$name/ready_instances.tmp" >> "$EXP_DIR/$name/ready.log" 2>/dev/null || true
  return 1
}

wait_port() {
  local name="$1"
  for _ in $(seq 1 180); do
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
      echo "port 1234 ready at $(date)" >> "$EXP_DIR/$name/ready.log"
      sleep 5
      return 0
    fi
    if [[ -n "${SERVICE_PID:-}" ]] && ! kill -0 "$SERVICE_PID" >/dev/null 2>&1; then
      echo "service process exited before port ready" | tee -a "$EXP_DIR/$name/ready.log"
      return 1
    fi
    sleep 1
  done
  echo "port 1234 not ready" | tee -a "$EXP_DIR/$name/ready.log"
  return 1
}

run_case() {
  local name="$1"
  local dispatch="$2"
  SERVICE_PID=""

  mkdir -p "$EXP_DIR/$name"
  echo "===== RUN $name dispatch=$dispatch $(date) =====" | tee -a "$EXP_DIR/run.log"

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
    --dispatch-policy "$dispatch" \
    --migrate-out-threshold 100000 \
    --migration-backend rayrpc \
    --migration-buffer-blocks 32 \
    --simulator-mode \
    --profiling-result-file-path "$SOURCE_EXP/profiling.pkl" \
    --disable-async-output-proc \
    --log-instance-info \
    --log-filename "$EXP_DIR/$name/server" \
    --max-units 2 \
    --enable-defrag False \
    > "$EXP_DIR/$name/service.log" 2>&1 &

  SERVICE_PID=$!

  if ! wait_instances "$name"; then
    echo "FAILED_READY_INSTANCES $name" | tee -a "$EXP_DIR/run.log"
    cleanup
    return 1
  fi

  if ! wait_port "$name"; then
    echo "FAILED_READY_PORT $name" | tee -a "$EXP_DIR/run.log"
    cleanup
    return 1
  fi

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
    > "$EXP_DIR/$name/benchmark.log" 2>&1

  local bench_code=$?

  grep -RniE "begin migrate|migrate done|migration status|migrate out|migrate in|total_kv_cache_size|speed:|Error in instance migrate_out|Pending migrate in" /tmp/ray/session_latest/logs \
    > "$EXP_DIR/$name/migration.log" 2>/dev/null || true

  cleanup

  if [[ "$bench_code" -eq 0 ]]; then
    echo "===== DONE $name $(date) =====" | tee -a "$EXP_DIR/run.log"
  else
    echo "===== FAILED $name benchmark_exit=$bench_code $(date) =====" | tee -a "$EXP_DIR/run.log"
  fi

  return "$bench_code"
}

overall=0
run_case "rr_no_migration" "rr" || overall=1
run_case "load_no_migration" "load" || overall=1

cd "$HOME/llumnix_results" || exit 1
tar_name="$(basename "$EXP_DIR").tar.gz"
tar -czf "$tar_name" "$(basename "$EXP_DIR")"

echo
echo "EXP_DIR=$EXP_DIR"
echo "TARBALL=$HOME/llumnix_results/$tar_name"
echo "overall_exit=$overall"
exit "$overall"
