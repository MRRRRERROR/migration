#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/llumnix-ray-main"
source "$HOME/venvs/llumnix/bin/activate"

MODEL_PATH="$HOME/models/opt-1.3b"
SOURCE_EXP="$HOME/llumnix_results/opt-1.3b_20260628_153807"
BASE_EXP_DIR="$HOME/llumnix_results/baseline_sim_opt-1.3b_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BASE_EXP_DIR"

export HEAD_NODE=1
export HEAD_NODE_IP=127.0.0.1

cat > "$BASE_EXP_DIR/env.txt" <<EOF
created_at: $(date)
host: $(hostname)
repo: $HOME/llumnix-ray-main
model_path: $MODEL_PATH
source_exp: $SOURCE_EXP
profiling: $SOURCE_EXP/profiling.pkl
mode: simulator baseline
EOF
python --version >> "$BASE_EXP_DIR/env.txt" 2>&1 || true
pip show llumnix vllm ray torch transformers tokenizers huggingface-hub click >> "$BASE_EXP_DIR/env.txt" 2>&1 || true
nvidia-smi >> "$BASE_EXP_DIR/env.txt" 2>&1 || true

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
  local name="$1"
  for _ in $(seq 1 90); do
    if python - <<'PY' >/tmp/llumnix_ready.out 2>/tmp/llumnix_ready.err
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
      cat /tmp/llumnix_ready.out >> "$BASE_EXP_DIR/${name}_ready.log"
      return 0
    fi
    sleep 2
  done
  echo "service $name not ready" | tee -a "$BASE_EXP_DIR/${name}_ready.log"
  cat /tmp/llumnix_ready.out /tmp/llumnix_ready.err >> "$BASE_EXP_DIR/${name}_ready.log" 2>/dev/null || true
  return 1
}

run_case() {
  local name="$1"
  local dispatch="$2"
  local migrate="$3"
  local defrag="$4"

  echo "===== RUN $name dispatch=$dispatch migrate=$migrate defrag=$defrag =====" | tee -a "$BASE_EXP_DIR/run.log"
  cleanup
  mkdir -p "$BASE_EXP_DIR/$name"

  local migration_args=()
  if [[ "$migrate" == "1" ]]; then
    migration_args+=(--enable-routine-migration --request-migration-policy SR)
  fi
  if [[ "$defrag" == "1" ]]; then
    migration_args+=(--enable-defrag)
  fi

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
    --log-filename "$BASE_EXP_DIR/$name/server" \
    --max-units 2 \
    "${migration_args[@]}" \
    > "$BASE_EXP_DIR/$name/service.log" 2>&1 &
  SERVICE_PID=$!

  wait_ready "$name"

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
    > "$BASE_EXP_DIR/$name/benchmark.log" 2>&1

  grep -RniE "begin migrate|migrate done|migration status|migrate out|migrate in|total_kv_cache_size|speed:|Error in instance migrate_out|Pending migrate in" /tmp/ray/session_latest/logs \
    > "$BASE_EXP_DIR/$name/migration.log" 2>/dev/null || true

  cleanup
  echo "===== DONE $name =====" | tee -a "$BASE_EXP_DIR/run.log"
}

run_case rr_no_migration rr 0 0
run_case load_no_migration load 0 0
run_case load_migration_defrag load 1 1

echo "$BASE_EXP_DIR" > "$HOME/llumnix_results/latest_baseline_sim_path.txt"
echo "BASE_EXP_DIR=$BASE_EXP_DIR"
