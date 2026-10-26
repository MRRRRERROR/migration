cd /data/home/zylu/llumnix-ray-main

echo "===== migration methods in backends ====="
grep -RniE "async def migrate|def migrate|migrate_out|migrate_in|send_cache|recv_cache|migration_backend" \
  llumnix/backends llumnix/llumlet | head -n 240 || true

echo
echo "===== vllm non-v1 files ====="
find llumnix/backends -maxdepth 3 -type f | sort | grep -E "vllm|migration" | head -n 200
