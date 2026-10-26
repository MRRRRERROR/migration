#!/usr/bin/env bash
set -eo pipefail
cd /data/home/zylu/llumnix-v018-main
for p in 8100 8101 8102 8103; do
  echo "port=$p"
  curl -s -m 2 -w ' status=%{http_code}\n' "http://localhost:$p/health" || true
done

echo "logs dir:"
ls -lh logs 2>/dev/null || true
for f in logs/vllm_0_migration.log logs/vllm_1_migration.log logs/vllm_2_migration.log logs/vllm_3_migration.log logs/gateway_migration.log logs/scheduler_migration.log; do
  echo "====$f===="
  tail -n 35 "$f" 2>/dev/null || true
done
