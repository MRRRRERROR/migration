#!/usr/bin/env bash
set -euo pipefail
root=/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01
overlay=/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay/blade_kvt
repo=/data/home/zylu/tmp/blade-kvt
cp "$root/frozen_accepted/"*.so "$overlay/"
cp "$root/frozen_accepted/hybrid_connector/utils.py" "$root/frozen_accepted/hybrid_connector/hybrid_modules.py" "$overlay/hybrid_connector/"
cp "$root/final/barex_protocol.cpp" "$repo/kvtransfer/src/barex_protocol.cpp"
cp "$root/final/barex_protocol.h" "$repo/kvtransfer/include/protocol/barex_protocol.h"
cp "$root/lifecycle2/kvtransfer_pybind.before.cpp" "$repo/kvtransfer/kvtransfer_pybind.cpp"
cp "$root/frozen_accepted/hybrid_connector/utils.py" "$root/frozen_accepted/hybrid_connector/hybrid_modules.py" "$repo/blade_kvt/hybrid_connector/"
sha256sum "$overlay/"*.so > "$root/restored_basic_version_sha256.txt"
cat "$root/restored_basic_version_sha256.txt"
