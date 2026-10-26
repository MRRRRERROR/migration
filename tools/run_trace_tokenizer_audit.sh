#!/usr/bin/env bash
set -euo pipefail

repo=/data/home/zylu/llumnix-v018-main
trace=/data/huxin/docker_data/eassy-method2/LLMServingSim/workloads/sharegpt-qwen3-32b-300-sps10.jsonl
stamp=$(date +%Y%m%d_%H%M%S)
output="$repo/results/tokenizer_audit_$stamp"
echo "$output" > "$repo/results/tokenizer_audit_latest_path.txt"
cd "$repo"
export HF_ENDPOINT="${HF_ENDPOINT:-https://hf-mirror.com}"
export HF_HUB_ETAG_TIMEOUT=15
export HF_HUB_DOWNLOAD_TIMEOUT=60
/data/home/zylu/venvs/llumnix5090/bin/python tools/audit_trace_tokenizer_compatibility.py \
  --trace "$trace" \
  --source-model /data/home/zylu/tokenizers/Qwen3-32B \
  --target-model /data/models/Qwen2.5-7B-Instruct \
  --sample-size 100 \
  --seed 42 \
  --generation-script /data/huxin/docker_data/eassy-method2/LLMServingSim/workloads/examples/gen-qwen3-32b.sh \
  --generation-command "python3 -m workloads.generators sharegpt --model Qwen/Qwen3-32B --source shibing624/sharegpt_gpt4 --num-reqs 300 --sps 10 --seed 42 --min-input-toks 256 --min-output-toks 512" \
  --output-dir "$output"
