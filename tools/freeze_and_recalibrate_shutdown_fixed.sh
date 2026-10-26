#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
repair=/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01
frozen=$repair/frozen_accepted
root=$project/results/two_gpu_power_drain_20261007_shutdown_fixed_01
python=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python
"$python" - <<'PY'
import json
from pathlib import Path
root=Path('/data/home/zylu/llumnix-v018-main/results/connector_shutdown_repair_20261007_01')
paths=list(root.glob('final_*/result.json'))
assert len(paths)==6, f'Expected six final cases; found {len(paths)}'
for path in paths:
    result=json.loads(path.read_text());assert result['clean'],str(path)
    for index in range(result['instances']):
        exit=json.loads((path.parent/f'exit_{index}.json').read_text())
        assert not exit['remaining_processes'] and not exit['fatal_markers'] and not exit['connector_loop_lifecycle_failures']
        assert not json.loads(exit['gpu']['compute_pids']) and exit['gpu']['memory_mib']<=64
        log=(path.parent/f'vllm_{index}.log').read_text(errors='replace')
        lifecycle=[json.loads(line.split('KVT_PY_SHUTDOWN ',1)[1]) for line in log.splitlines() if 'KVT_PY_SHUTDOWN {' in line]
        ended=[row for row in lifecycle if row['event']=='core_shutdown_end']
        assert ended and all(not loop['running'] and loop['closed'] for row in ended for loop in row.get('loops',[])), str(path)
        assert 'KVT_THREAD_SHUTDOWN' in log and 'event=barex_context_wait_stopped' in log, str(path)
(root/'FINAL_SHUTDOWN_ACCEPTANCE.json').write_text(json.dumps({'passed':True,'cases':[str(p) for p in paths],
    'sigkill_used':False,'scope':'TP1 HybridConnector TCP; single instance 0/3 requests, API SIGTERM and test-helper SIGTERM phase; two instances sequential API SIGTERM 0/3 requests'},indent=2))
PY
test ! -d "$frozen"
mkdir "$frozen"
cp -a /data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay/blade_kvt/hybrid_connector "$frozen/"
cp /data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay/blade_kvt/kvtransfer_ops*.so "$frozen/"
cp "$repair/final/barex_protocol.cpp" "$repair/final/barex_protocol.h" "$frozen/"
find "$frozen" -type f \( -name '*.py' -o -name '*.cpp' -o -name '*.h' -o -name '*.so' \) -exec sha256sum {} + > "$frozen/SHA256SUMS"
cp "$project/results/connector_shutdown_repair_20261007_01/FINAL_SHUTDOWN_ACCEPTANCE.json" "$root/"
export LLUMNIX_SERVING_FROZEN_DIR="$frozen"
exec bash "$project/tools/start_two_gpu_power_drain.sh" --root "$root" --stop-after-acceptance
