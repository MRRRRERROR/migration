cd /data/home/zylu/llumnix-ray-main

echo "===== grep callback/frontend ====="
grep -RniE "def migration_done_callback|migration_done_callback|migration_frontend =|hybrid_connector|MIGRATION_EVENT_V1 event=frontend_migrate" \
  llumnix/backends/vllm_v1/core.py | head -n 120 || true

echo
echo "===== callback snippet ====="
nl -ba llumnix/backends/vllm_v1/core.py | sed -n '1080,1210p'

echo
echo "===== init frontend snippet ====="
nl -ba llumnix/backends/vllm_v1/core.py | sed -n '880,935p'
