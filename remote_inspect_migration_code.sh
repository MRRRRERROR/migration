source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

echo "===== patched markers ====="
grep -RniE "frontend_migrate_enter|frontend_core_update_start|frontend_encode_start|frontend_migrate_start" \
  llumnix/backends/vllm_v1 llumnix/llumlet llumnix/entrypoints | head -n 160 || true

echo
echo "===== running api server ====="
ps -ef | grep "llumnix.entrypoints.vllm_v1.api_server" | grep -v grep || true

echo
echo "===== migration_frontend migrate snippet ====="
nl -ba llumnix/backends/vllm_v1/migration_frontend.py | sed -n '50,120p'
