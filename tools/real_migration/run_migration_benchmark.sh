#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/real_migration/llumnix_experiment_env.sh
source "$SCRIPT_DIR/llumnix_experiment_env.sh"

source "$LLUMNIX_VENV/bin/activate"
cd "$LLUMNIX_REPO"

mkdir -p "$EXP_DIR"

PROMPT_COUNT="${PROMPT_COUNT:-300}"
PROMPT_MEAN="${PROMPT_MEAN:-512}"
PROMPT_RANGE="${PROMPT_RANGE:-128}"
RESPONSE_MEAN="${RESPONSE_MEAN:-512}"
RESPONSE_RANGE="${RESPONSE_RANGE:-128}"
QPS="${QPS:-8}"

echo "Benchmark log: $BENCH_LOG"
echo "Prompts: $PROMPT_COUNT, QPS: $QPS"

python benchmark/benchmark_serving.py \
  --ip_ports "$HOST:$PORT" \
  --tokenizer "$MODEL_PATH" \
  --backend vLLM \
  --gen_random_prompts \
  --random_prompt_count "$PROMPT_COUNT" \
  --random_prompt_lens_mean "$PROMPT_MEAN" \
  --random_prompt_lens_range "$PROMPT_RANGE" \
  --variable_prompt_lens_distribution uniform \
  --allow_variable_generation_length \
  --variable_response_lens_mean "$RESPONSE_MEAN" \
  --variable_response_lens_range "$RESPONSE_RANGE" \
  --variable_response_lens_distribution uniform \
  --qps "$QPS" \
  --distribution poisson \
  --log_latencies \
  --fail_on_response_failure \
  2>&1 | tee "$BENCH_LOG"

echo
echo "Migration log snippets:"
grep -RniE "begin migrate|migrate done|migration status|migrate out|total_kv_cache_size|speed:" "$EXP_DIR" /tmp/ray 2>/dev/null | tail -n 120 || true

