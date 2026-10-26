set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

EXP_DIR=/data/home/zylu/llumnix-ray-main/5090_results_0719_migration_thr3300
mkdir -p "$EXP_DIR"

BENCH_LOG="$EXP_DIR/benchmark_len768_out256_qps4_count64_prerpc_loaded.log"
EVENT_LOG="$EXP_DIR/migration_v1_events_prerpc_loaded.log"

python benchmark/benchmark_serving.py \
  --ip_ports 127.0.0.1:1234 \
  --tokenizer /data/models/Qwen2.5-7B-Instruct \
  --backend vLLM \
  --gen_random_prompts \
  --random_prompt_count 64 \
  --random_prompt_lens_mean 768 \
  --random_prompt_lens_range 1 \
  --variable_prompt_lens_distribution uniform \
  --allow_variable_generation_length \
  --variable_response_lens_mean 256 \
  --variable_response_lens_range 1 \
  --variable_response_lens_distribution uniform \
  --qps 4 \
  --distribution poisson \
  --log_latencies \
  --fail_on_response_failure \
  2>&1 | tee "$BENCH_LOG"

grep -R "MIGRATION_EVENT_V1\|migration failed\|frontend_migration" /tmp/ray/session_latest/logs > "$EVENT_LOG" || true

echo
echo "===== benchmark summary ====="
grep -E "successful_responses|throughput_tok_s|backend GenerationBackend|median_|AssertionError|Traceback|error_type|ERROR" "$BENCH_LOG" | tail -n 80 || true

echo
echo "===== event counts ====="
grep -oE "event=[a-zA-Z0-9_]+" "$EVENT_LOG" | sort | uniq -c | sort -nr || true

echo
echo "===== migration trace tail ====="
grep -E "migrate_out_selected|frontend_migrate_start|frontend_migrate_enter|frontend_core_update|frontend_encode|frontend_migrate_rpc_try|frontend_rpc|frontend_migrate_done|frontend_migrate_failed|migration failed|TimeoutError|RuntimeError|migrate_out_done" "$EVENT_LOG" | tail -n 260 || true

echo
echo "===== files ====="
echo "$BENCH_LOG"
echo "$EVENT_LOG"
