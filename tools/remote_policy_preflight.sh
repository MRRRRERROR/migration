#!/usr/bin/env bash
set -eo pipefail
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main
echo PROCESSES
ps -ef | grep -E 'run_multi_baseline|run_migration_policy|bin/gateway|bin/scheduler|vllm serve|redis-server|discovery.discovery' | grep -v grep || true
echo PORTS
ss -ltnp | grep -E '6379|810[0-3]|18088|18089|362(18|26|34|42)' || true
echo GPU_PROCESSES
nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader
echo GPU_SUMMARY
nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu --format=csv,noheader
