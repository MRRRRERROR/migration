#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ROOT=/data/home/zylu/llumnix-v018-main/results/5090_migration_policy_baselines_20260819_201922

python -m py_compile \
  python/llumnix/llumnix/utils.py \
  python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py \
  run_migration_policy_baselines.py

python - <<'PY'
from types import SimpleNamespace

from llumnix.utils import RequestMigrationPolicy
from llumnix.migration_frontend.vllm_v1.kvt_migration_frontend import KVTMigrationFrontend

print("policies", [policy.value for policy in RequestMigrationPolicy])
print("frontend import ok", KVTMigrationFrontend.__name__)

frontend = object.__new__(KVTMigrationFrontend)
frontend._policy_seed = "20260819"
frontend._active_migration_policy = "SR"
frontend._composite_min_remaining = 8
frontend._composite_max_context = 4096

def req(request_id, prompt, generated, maximum):
    core = SimpleNamespace(
        request_id=request_id,
        prompt_token_ids=list(range(prompt)),
        sampling_params=SimpleNamespace(max_tokens=maximum),
    )
    return core, generated

requests = [req("a", 20, 2, 32), req("b", 40, 10, 256), req("c", 10, 4, 48)]
frontend._log_policy_decision = lambda *args, **kwargs: None
remaining = frontend._get_sorted_indices(RequestMigrationPolicy.REMAINING_LONGEST, requests)
composite = frontend._get_sorted_indices(RequestMigrationPolicy.COMPOSITE, requests)
random_order = frontend._get_sorted_indices(RequestMigrationPolicy.RANDOM, requests)
print("remaining order", remaining)
print("composite order", composite)
print("random order", random_order)
assert remaining == [1, 2, 0]
assert composite == [1, 2, 0]
assert sorted(random_order) == [0, 1, 2]
PY

diff -u "$ROOT/00_config/source_backups/utils.py" \
  python/llumnix/llumnix/utils.py > "$ROOT/00_config/utils_policy.diff" || true
diff -u "$ROOT/00_config/source_backups/kvt_migration_frontend.py" \
  python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py \
  > "$ROOT/00_config/kvt_frontend_policy.diff" || true
cp run_migration_policy_baselines.py "$ROOT/00_config/run_migration_policy_baselines.py"
wc -l "$ROOT/00_config/utils_policy.diff" "$ROOT/00_config/kvt_frontend_policy.diff"
