from pathlib import Path

p = Path("/data/home/zylu/.conda/envs/llumnix5090_vllm018/lib/python3.12/site-packages/blade_kvt/hybrid_connector/hybrid_modules.py")
s = p.read_text()
bak = p.with_suffix(".py.bak_trace_wrapper_0720")
if not bak.exists():
    bak.write_text(s)

helper = """

class _LlumnixNoopTraceWrapper:
    traceparent = None

    def on_req_kv_start(self, *_args, **_kwargs):
        return None

    def on_req_kv_end(self, *_args, **_kwargs):
        return None


def _ensure_trace_wrapper(req):
    if not hasattr(req, "trace_wrapper"):
        req.trace_wrapper = _LlumnixNoopTraceWrapper()
    return req.trace_wrapper
"""

if "class _LlumnixNoopTraceWrapper" not in s:
    marker = "logger = get_logger(__name__)\n"
    if marker not in s:
        raise SystemExit("logger marker not found")
    s = s.replace(marker, marker + helper, 1)

s = s.replace(
    '        load_count, save_count = self._backend.get_operations(req)\n'
    '        req.trace_wrapper.on_req_kv_start("load")\n'
    '        req.trace_wrapper.on_req_kv_start("save")\n',
    '        load_count, save_count = self._backend.get_operations(req)\n'
    '        trace_wrapper = _ensure_trace_wrapper(req)\n'
    '        trace_wrapper.on_req_kv_start("load")\n'
    '        trace_wrapper.on_req_kv_start("save")\n',
)

s = s.replace(
    '        state._req.trace_wrapper.on_req_kv_end("save")\n',
    '        _ensure_trace_wrapper(state._req).on_req_kv_end("save")\n',
)
s = s.replace(
    '        state._req.trace_wrapper.on_req_kv_end("load")\n',
    '        _ensure_trace_wrapper(state._req).on_req_kv_end("load")\n',
)

p.write_text(s)
print("patched blade_kvt hybrid_modules trace_wrapper fallback")
