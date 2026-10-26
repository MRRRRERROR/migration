from pathlib import Path

import blade_kvt


p = Path(blade_kvt.__file__).resolve().parent / "hybrid_connector" / "__init__.py"
s = p.read_text()
bak = p.with_suffix(".py.bak_fix_env_defaults_quotes_0720")
if not bak.exists():
    bak.write_text(s)

marker = "# Llumnix 5090/vLLM 0.18 compatibility defaults."
start = s.find(marker)
if start != -1:
    end = s.find("# Re-export public symbols", start)
    if end == -1:
        raise SystemExit("cannot find end marker")
    s = s[:start] + s[end:]

block = '''# Llumnix 5090/vLLM 0.18 compatibility defaults.
import vllm.envs as _llumnix_vllm_envs

for _name, _value in {
    "LLUMNIX_MAX_BLOCK_RATIO_MIG_IN": 1.0,
    "VLLM_ENABLE_BYPASS_SUBSTEP": False,
    "VLLM_V6D_ASYNC_REGISTER": False,
    "VLLM_ZERO_HYBRID_KV_CACHE": False,
}.items():
    if not hasattr(_llumnix_vllm_envs, _name):
        setattr(_llumnix_vllm_envs, _name, _value)

'''

p.write_text(block + s.lstrip())
print("fixed", p)
for i, line in enumerate(p.read_text().splitlines(), 1):
    if i <= 18 or "scheduler_rpc_host" in line:
        print(i, line)
