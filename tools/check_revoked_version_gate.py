"""Exercise the actual early version gate without importing serving or CUDA."""
import ast
import json
from pathlib import Path

project=Path(__file__).resolve().parents[1]
source=project/'tools/two_gpu_power_drain.py'
tree=ast.parse(source.read_text())
function=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='verify_version')
namespace={'FROZEN':Path('/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01/frozen_accepted')}
exec(compile(ast.Module(body=[function],type_ignores=[]),str(source),'exec'),namespace)
try:
    namespace['verify_version']()
except RuntimeError as error:
    assert 'revoked' in str(error)
    result={'blocked_as_required':True,'reason':str(error),'gpu_or_serving_modules_imported':False}
else:
    raise RuntimeError('Rejected migration version unexpectedly allowed')
root=project/'results/two_gpu_power_drain_20261007_shutdown_fixed_01'
(root/'formal_version_gate_check.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
