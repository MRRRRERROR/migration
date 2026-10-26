"""Reverse recorded post-failure edits; explicitly label this as reconstruction."""
import hashlib
import json
from pathlib import Path

source=Path(__file__).with_name('two_gpu_power_drain.py').read_text()
source=source.replace('|Segfault encountered|AssertFailed|Fatal Python error','')
source=source.replace("        if any(not 128<=p['input_tokens']<=4096 for p in self.prompts):raise RuntimeError('Actual model input length outside 128..4096; prepare a new directory and recalibrate')\n",'')
source=source.replace("        self.idle_memory={i:self.nv.row(i,role)['memory_mib'] for i,role in ((0,'S'),(1,'D'))}\n",'')
start=source.index('        for name,owned,log in self.processes:')
end=source.index('    def registry',start)
source=source[:start]+source[end:]
source=source.replace("while self.nv.row(0,'S')['memory_mib']>self.idle_memory[0]+64 or self.nv.processes(self.nv.handle(0)):","while self.nv.row(0,'S')['memory_mib']>64:")
root=Path(__file__).resolve().parents[1]/'results/two_gpu_power_drain_20261006_2119/attempt_02'
target=root/'experiment_sources/executed_controller_after_refresh_reconstructed.py'
target.write_text(source,encoding='utf-8');compile(source,str(target),'exec')
record={'status':'reconstructed by reversing the recorded post-failure edits; no pre-launch controller hash was captured',
    'sha256_of_reconstruction':hashlib.sha256(source.encode()).hexdigest(),
    'memory_timeout_line':next(i for i,line in enumerate(source.splitlines(),1) if 'Source GPU memory release timeout' in line),
    'serving_version_is_exact':'runtime_and_model_versions.json and version_manifest.json; frozen native hash was checked unchanged',
    'delivered_controller_has_post_failure_fixes':True}
(root/'controller_source_reconstruction_notice.json').write_text(json.dumps(record,indent=2))
print(json.dumps(record))
