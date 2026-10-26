set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

SRC=/data/home/zylu/tmp/blade-kvt
HANDOFF=/data/home/zylu/llumnix-v018-main/AI_HANDOFF.md
TS=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$SRC/backups/req2corereq_compat_$TS"
DIFF_FILE="/data/home/zylu/llumnix-v018-main/blade_kvt_req2corereq_compat_diff_$TS.patch"
PATCH_LOG="/data/home/zylu/llumnix-v018-main/blade_kvt_req2corereq_compat_$TS.log"

cd "$SRC"
BRANCH=$(git branch --show-current)
mkdir -p "$BACKUP_DIR"
cp blade_kvt/hybrid_connector/engine_proxy.py "$BACKUP_DIR/engine_proxy.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/engine_proxy.py")
s = p.read_text()

old = '''def req2corereq(req: Request) -> EngineCoreRequest:
    # just make mypy happy~
    if req.sampling_params and req.sampling_params.extra_args is not None:
        req.sampling_params.extra_args["kv_transfer_params"] = None
    return EngineCoreRequest(
        request_id=req.request_id,
        prompt_token_ids=req.prompt_token_ids,
        mm_features=req.mm_features,
        sampling_params=req.sampling_params,
        pooling_params=req.pooling_params,
        eos_token_id=req.eos_token_id,
        arrival_time=req.arrival_time,
        lora_request=req.lora_request,
        cache_salt=req.cache_salt,
        data_parallel_rank=None,
        prompt_embeds=req.prompt_embeds,
        priority=req.priority,
        trace_headers=req.trace_headers,
        queue_server_address=req.queue_server_address,
    )
'''

new = '''def req2corereq(req: Request) -> EngineCoreRequest:
    # just make mypy happy~
    if req.sampling_params and req.sampling_params.extra_args is not None:
        req.sampling_params.extra_args["kv_transfer_params"] = None
    kwargs = {
        "request_id": req.request_id,
        "prompt_token_ids": req.prompt_token_ids,
        "mm_features": req.mm_features,
        "sampling_params": req.sampling_params,
        "pooling_params": req.pooling_params,
        "eos_token_id": getattr(req, "eos_token_id", None),
        "arrival_time": req.arrival_time,
        "lora_request": req.lora_request,
        "cache_salt": req.cache_salt,
        "data_parallel_rank": None,
        "prompt_embeds": req.prompt_embeds,
        "prompt_is_token_ids": getattr(req, "prompt_is_token_ids", None),
        "priority": req.priority,
        "trace_headers": req.trace_headers,
        "queue_server_address": getattr(req, "queue_server_address", None),
    }
    params = inspect.signature(EngineCoreRequest).parameters
    return EngineCoreRequest(**{
        key: value for key, value in kwargs.items()
        if key in params
    })
'''

if old in s:
    s = s.replace(old, new, 1)
elif "params = inspect.signature(EngineCoreRequest).parameters" in s:
    print("req2corereq compat already present")
else:
    raise SystemExit("req2corereq target block not found")

p.write_text(s)
PY

python -m py_compile blade_kvt/hybrid_connector/engine_proxy.py
git diff -- blade_kvt/hybrid_connector/engine_proxy.py | tee "$DIFF_FILE"

{
  echo
  echo "## $TS blade_kvt req2corereq 兼容补丁"
  echo "- 分支: $BRANCH"
  echo "- 备份目录: $BACKUP_DIR"
  echo "- 修改文件: blade_kvt/hybrid_connector/engine_proxy.py"
  echo "- 补丁作用: 按当前 vLLM EngineCoreRequest 签名过滤 eos_token_id、queue_server_address 等旧字段，避免构造请求时 EngineCore fatal。"
  echo "- diff 文件: $DIFF_FILE"
} >> "$HANDOFF"

bash /data/home/zylu/llumnix-v018-main/remote_rebuild_blade_kvt.sh 2>&1 | tee "$PATCH_LOG" || true

cd /data/home/zylu/llumnix-v018-main
python - <<'PY'
from pathlib import Path
import importlib.metadata as md
import blade_kvt
from blade_kvt.hybrid_connector import HybridConnector
print("blade_kvt file", Path(blade_kvt.__file__).resolve())
print("blade_kvt version", md.version("blade-kvt"))
print("HybridConnector ok", HybridConnector)
PY

bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh 2>&1 | tee "/data/home/zylu/llumnix-v018-main/minimal_single_request_after_req2corereq_$TS.log"

LATEST_TRACE=$(grep -RniE "Traceback|TypeError|RuntimeError|EngineCore encountered a fatal error|Exception" logs "/data/home/zylu/llumnix-v018-main/minimal_single_request_after_req2corereq_$TS.log" 2>/dev/null | tail -n 120 || true)
RESULT_LINE=$(grep -E "MINIMAL_SINGLE_REQUEST_RESULT|Response received" "/data/home/zylu/llumnix-v018-main/minimal_single_request_after_req2corereq_$TS.log" | tail -n 20 || true)

{
  echo
  echo "### req2corereq 补丁后安装和最小单请求结果"
  echo "- 安装命令: bash /data/home/zylu/llumnix-v018-main/remote_rebuild_blade_kvt.sh"
  echo "- 测试命令: bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh"
  echo "- 测试结果:"
  echo '```text'
  if [ -n "$RESULT_LINE" ]; then
    echo "$RESULT_LINE"
  else
    echo "未抓到 MINIMAL_SINGLE_REQUEST_RESULT。"
  fi
  echo '```'
  echo "- 最新 traceback / fatal 摘要:"
  echo '```text'
  if [ -n "$LATEST_TRACE" ]; then
    echo "$LATEST_TRACE"
  else
    echo "未抓到 Traceback/EngineCore fatal。"
  fi
  echo '```'
} >> "$HANDOFF"

echo "DONE req2corereq patch/test"
echo "backup=$BACKUP_DIR"
echo "diff=$DIFF_FILE"
