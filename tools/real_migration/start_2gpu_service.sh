#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/real_migration/llumnix_experiment_env.sh
source "$SCRIPT_DIR/llumnix_experiment_env.sh"

mkdir -p "$EXP_DIR"

if [[ ! -d "$LLUMNIX_REPO" ]]; then
  echo "Repo not found: $LLUMNIX_REPO"
  exit 1
fi

if [[ ! -f "$LLUMNIX_VENV/bin/activate" ]]; then
  echo "Virtualenv not found: $LLUMNIX_VENV"
  exit 1
fi

if [[ ! -d "$MODEL_PATH" ]]; then
  echo "Model path not found: $MODEL_PATH"
  exit 1
fi

echo "Experiment directory: $EXP_DIR"
echo "Service log: $SERVICE_LOG"
echo "Using GPUs: $CUDA_VISIBLE_DEVICES"
echo
nvidia-smi
echo

source "$LLUMNIX_VENV/bin/activate"
cd "$LLUMNIX_REPO"

echo "Stopping existing Ray processes visible to this user..."
ray stop || true

echo "Starting Llumnix service..."
nohup python -u -m llumnix.entrypoints.vllm.api_server \
  --config-file configs/vllm.yml \
  --host "$HOST" \
  --port "$PORT" \
  --ray-cluster-port "$RAY_PORT" \
  --initial-instances "$INITIAL_INSTANCES" \
  --launch-ray-cluster \
  --model "$MODEL_PATH" \
  --worker-use-ray \
  --max-model-len "$MAX_MODEL_LEN" \
  --max-num-batched-tokens "$MAX_NUM_BATCHED_TOKENS" \
  --gpu-memory-utilization "$GPU_MEMORY_UTILIZATION" \
  --migration-backend "$MIGRATION_BACKEND" \
  --migration-buffer-blocks "$MIGRATION_BUFFER_BLOCKS" \
  --enable-routine-migration \
  --request-migration-policy SR \
  --log-instance-info \
  --log-filename "$EXP_DIR/server.log" \
  > "$SERVICE_LOG" 2>&1 &

echo "$!" > "$EXP_DIR/service.pid"
echo "Service PID: $(cat "$EXP_DIR/service.pid")"
echo "Waiting for manager and instances..."

python - <<'PY'
import os
import time
import ray

addr = f"{os.environ['HOST']}:{os.environ['RAY_PORT']}"
expected = int(os.environ["INITIAL_INSTANCES"])
deadline = time.time() + 240
last = []

while time.time() < deadline:
    try:
        ray.init(address=addr, namespace="llumnix", ignore_reinit_error=True, log_to_driver=False)
        actors = ray.util.list_named_actors(all_namespaces=True)
        names = [a["name"] for a in actors if a.get("namespace") == "llumnix"]
        instances = [n for n in names if n.startswith("instance_")]
        last = names
        if "manager" in names and len(instances) >= expected:
            print("Ready actors:", names)
            raise SystemExit(0)
    except Exception as exc:
        last = [f"waiting: {exc!r}"]
    time.sleep(3)

print("Timed out waiting for Llumnix actors. Last seen:", last)
raise SystemExit(1)
PY

echo
echo "Service is ready."
echo "Run benchmark with:"
echo "  EXP_DIR=\"$EXP_DIR\" bash \"$SCRIPT_DIR/run_migration_benchmark.sh\""
