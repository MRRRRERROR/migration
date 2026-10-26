#!/usr/bin/env bash
set -euo pipefail
root=/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01
repo=/data/home/zylu/tmp/blade-kvt
overlay=/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay
stage=${1:-diagnostics}
if [[ "$stage" == diagnostics ]]; then
  mkdir -p "$root/original" "$root/diagnostics"
  test ! -f "$root/original/kvtransfer_ops.cpython-312-x86_64-linux-gnu.so"
  cp -p "$repo/kvtransfer/src/barex_protocol.cpp" "$repo/kvtransfer/include/protocol/barex_protocol.h" "$root/original/"
  cp -p "$overlay/blade_kvt/kvtransfer_ops.cpython-312-x86_64-linux-gnu.so" "$root/original/"
  git -C "$repo" diff > "$root/original/preserved_worktree.diff"
  cp "$root/diagnostics/barex_protocol.cpp" "$repo/kvtransfer/src/barex_protocol.cpp"
  cp "$root/diagnostics/barex_protocol.h" "$repo/kvtransfer/include/protocol/barex_protocol.h"
else
  mkdir -p "$root/$stage/python_original"
  cp -p "$overlay/blade_kvt/hybrid_connector/utils.py" "$overlay/blade_kvt/hybrid_connector/hybrid_modules.py" "$root/$stage/python_original/"
  cp "$root/$stage/barex_protocol.cpp" "$repo/kvtransfer/src/barex_protocol.cpp"
  cp "$root/$stage/barex_protocol.h" "$repo/kvtransfer/include/protocol/barex_protocol.h"
  if [[ -f "$root/$stage/kvtransfer_pybind.cpp" ]]; then
    cp -p "$repo/kvtransfer/kvtransfer_pybind.cpp" "$root/$stage/kvtransfer_pybind.before.cpp"
    cp "$root/$stage/kvtransfer_pybind.cpp" "$repo/kvtransfer/kvtransfer_pybind.cpp"
  fi
fi
export PATH=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin:$PATH
timeout 600 cmake --build "$repo/build/temp.linux-x86_64-cpython-312/blade_kvt.kvtransfer_ops" --target kvtransfer_ops --parallel 2
cp "$repo/build/lib.linux-x86_64-cpython-312/blade_kvt/kvtransfer_ops.cpython-312-x86_64-linux-gnu.so" "$root/$stage/"
cp "$root/$stage/kvtransfer_ops.cpython-312-x86_64-linux-gnu.so" "$overlay/blade_kvt/"
if [[ "$stage" != diagnostics ]]; then
  cp "$root/$stage/utils.py" "$root/$stage/hybrid_modules.py" "$overlay/blade_kvt/hybrid_connector/"
  cp "$root/$stage/utils.py" "$root/$stage/hybrid_modules.py" "$repo/blade_kvt/hybrid_connector/"
fi
sha256sum "$overlay/blade_kvt/kvtransfer_ops.cpython-312-x86_64-linux-gnu.so"
