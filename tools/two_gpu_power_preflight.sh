#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
export PYTHONPATH=/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay:$project/python/discovery:$project/python:$project
cd "$project"
python=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python
"$python" - <<'PY'
import inspect, json, os, pathlib
from tests.local import utils
import blade_kvt.hybrid_connector.kvtbackend as k
import blade_kvt.hybrid_connector.migration.kvtmigration as m
import ctypes; print("NVML",ctypes.CDLL("libnvidia-ml.so.1"))
print('UTILS',utils.__file__)
for name in ('get_vllm_command','get_redis_command','get_discovery_command','start_process','cleanup_processes'):
    print(inspect.getsource(getattr(utils,name)))
print('BACKEND',k.__file__,'MIGRATION',m.__file__)
print('MODEL CONFIG',pathlib.Path('/data/models/Qwen2.5-7B-Instruct/generation_config.json').read_text())
print('RPC PROTO')
from llumnix.server.proto import llumlet_server_pb2 as p
for n,d in p.DESCRIPTOR.message_types_by_name.items():
    print(n,[(f.name,f.type) for f in d.fields])
print('DATA')
for root in ('/data/home/zylu','/data/huxin/docker_data/eassy-method2/LLMServingSim','/data/datasets'):
    for folder,dirs,files in os.walk(root):
        dirs[:]=[d for d in dirs if d not in ('results','.git','site-packages','node_modules','models','.conda','venvs','repair_20261002','tmp')]
        for f in files:
            if 'sharegpt' in f.lower() and f.endswith(('.json','.jsonl','.parquet')): print(os.path.join(folder,f))
PY
