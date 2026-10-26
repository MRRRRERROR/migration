#!/usr/bin/env bash
set -euo pipefail

source "$HOME/miniforge3/etc/profile.d/conda.sh"
conda activate llumnix-wsl-sim

cd /mnt/d/llumnix-ray-main

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="results/wsl_sim_4inst_${ts}"
mkdir -p "$out_dir"

profile="/mnt/d/llumnix-ray-main/_simulator_profile_test/simulator_approx_profile_5090/qwen2p5_7b_5090_profile_approx_numpy126.pkl"
model="/mnt/d/llumnix-ray-main/_simulator_model_stub/Qwen2.5-7B-Instruct"
server_log="$out_dir/server_console.log"
bench_log="$out_dir/benchmark_console.log"

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
  --dispatch-policy flood \
  --simulator-mode \
  --profiling-result-file-path "$profile" \
  --disable-async-output-proc \
  --log-instance-info \
  --log-filename "$out_dir/wsl_sim_4inst_instance.log" \
  > "$server_log" 2>&1 &

pid=$!
echo "server_pid=$pid" | tee "$out_dir/run_status.txt"
echo "out_dir=$out_dir" | tee -a "$out_dir/run_status.txt"

ready=0
for _ in $(seq 1 120); do
  if curl -fsS http://127.0.0.1:1234/is_ready >/tmp/llumnix_4inst_ready.out 2>/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "server_exited_before_ready" | tee -a "$out_dir/run_status.txt"
    break
  fi
  sleep 2
done

if [ "$ready" != 1 ]; then
  echo "ready=false" | tee -a "$out_dir/run_status.txt"
  tail -n 240 "$server_log" > "$out_dir/server_tail_on_failure.log" || true
  kill "$pid" >/dev/null 2>&1 || true
  ray stop --force >/dev/null 2>&1 || true
  exit 1
fi

echo "ready=true" | tee -a "$out_dir/run_status.txt"

python -u ./benchmark/benchmark_serving.py \
  --ip_ports 127.0.0.1:1234 \
  --backend vLLM \
  --tokenizer "$model" \
  --trust_remote_code \
  --log_filename "$out_dir/benchmark_4inst.log" \
  --gen_random_prompts \
  --random_prompt_count 20 \
  --random_prompt_lens_mean 64 \
  --random_prompt_lens_range 0 \
  --variable_prompt_lens_distribution uniform \
  --allow_variable_generation_length \
  --variable_response_lens_mean 32 \
  --variable_response_lens_range 0 \
  --variable_response_lens_distribution uniform \
  --max_request_len 1024 \
  --qps 4 \
  --distribution uniform \
  --log_latencies \
  --fail_on_response_failure \
  2>&1 | tee "$bench_log"

echo "benchmark_done=true" | tee -a "$out_dir/run_status.txt"

grep -RniE "Traceback|ERROR|RuntimeError|ActorDiedError|Error in engine loop" "$out_dir" > "$out_dir/error_scan.txt" || true
grep -RniE "Init Llumnix components done|dispatch request|Engine finished request|successful_responses|throughput_tok_s|backend vLLM" "$out_dir" > "$out_dir/key_events.txt" || true

tail -n 240 "$server_log" > "$out_dir/server_tail.log" || true
tail -n 200 "$bench_log" > "$out_dir/benchmark_tail.log" || true

kill "$pid" >/dev/null 2>&1 || true
ray stop --force >/dev/null 2>&1 || true

echo "out_dir=$out_dir"
echo "error_count=$(wc -l < "$out_dir/error_scan.txt")"
