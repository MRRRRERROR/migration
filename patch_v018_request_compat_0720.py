from pathlib import Path
import blade_kvt

base = Path(blade_kvt.__file__).resolve().parent
p = base / "hybrid_connector" / "hybrid_modules.py"
s = p.read_text()

bak = p.with_suffix(".py.bak_request_compat_0720")
if not bak.exists():
    bak.write_text(s)

helper = '''
_LLUMNIX_MAX_COMPUTED_TOKENS_ATTR = "_llumnix_max_computed_tokens"

def _max_computed_tokens(req):
    getter = getattr(req, "max_computed_tokens", None)
    if getter is not None:
        return getter()
    return getattr(req, _LLUMNIX_MAX_COMPUTED_TOKENS_ATTR, None)

def _set_max_computed_tokens(req, value):
    setter = getattr(req, "set_max_computed_tokens", None)
    if setter is not None:
        return setter(value)
    setattr(req, _LLUMNIX_MAX_COMPUTED_TOKENS_ATTR, value)
    return None

'''

if "_LLUMNIX_MAX_COMPUTED_TOKENS_ATTR" not in s:
    marker = "def _ensure_trace_wrapper(req):\n"
    idx = s.index(marker)
    s = s[:idx] + helper + s[idx:]

s = s.replace("req.max_computed_tokens()", "_max_computed_tokens(req)")
s = s.replace("req.set_max_computed_tokens(req.num_computed_tokens)", "_set_max_computed_tokens(req, req.num_computed_tokens)")

p.write_text(s)
print("patched request max_computed_tokens compatibility:", p)
