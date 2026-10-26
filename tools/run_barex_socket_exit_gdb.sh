#!/usr/bin/env bash
set -uo pipefail
project=/data/home/zylu/llumnix-v018-main
root=$project/results/connector_shutdown_repair_20261007_01
export PATH=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin:$PATH
export PYTHONPATH=/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay:$project/python:$project
export CUDA_VISIBLE_DEVICES=1
export CUDA_DEVICE_ORDER=PCI_BUS_ID
export VLLM_HOST_IP=127.0.0.1
export BLLM_KVTRANS_PORT_BASE=36400
cd "$project"
variant=${1:-late}
extra=()
if [[ "$variant" == explicit ]]; then extra=(--explicit); fi
gdb --batch -ex 'set pagination off' -ex 'handle SIGSEGV stop print pass' -ex run -ex 'thread apply all bt' -ex continue --args python tools/barex_socket_exit_probe.py --root "$root/native_socket_probe_${variant}_gdb" --socket "${extra[@]}" > "$root/native_socket_probe_${variant}_gdb.log" 2>&1
echo "GDB_COMPLETE rc=$?"
