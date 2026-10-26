#!/usr/bin/env bash
set -euo pipefail

echo "=== nvidia-smi ==="
nvidia-smi

echo
echo "=== compute processes ==="
if ! nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader; then
  echo "Could not query compute processes."
  exit 1
fi

echo
echo "If only Xorg appears in the process table and GPU-Util is near 0%, the cards are ready."

