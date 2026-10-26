from pathlib import Path
import vllm.v1.request as rq

p = Path(rq.__file__).resolve()
s = p.read_text()

s = s.replace(
    "return getattr(self,  _llumnix_max_computed_tokens, None)",
    "return getattr(self, '_llumnix_max_computed_tokens', None)",
)
s = s.replace(
    'return getattr(self,  "_llumnix_max_computed_tokens", None)',
    "return getattr(self, '_llumnix_max_computed_tokens', None)",
)

p.write_text(s)
print("fixed Request max_computed_tokens attribute quoting:", p)
