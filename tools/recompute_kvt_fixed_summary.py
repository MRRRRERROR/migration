#!/usr/bin/env python3
"""Recompute the fixed KVT smoke gate from already captured logs."""

import importlib.util
import json
from pathlib import Path


RUNNER = Path("/tmp/run_llumnix_load_sweep.py")
spec = importlib.util.spec_from_file_location("load_sweep_runner", RUNNER)
runner = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(runner)

run_dir = runner.SMOKE_DIR / "smoke_migration_kvt_fixed"
summary_path = run_dir / "run_summary.json"
summary = json.loads(summary_path.read_text(encoding="utf-8"))
summary.update(runner.count_patterns(run_dir))
summary["first_fatal"] = runner.first_fatal(run_dir)
passed = runner.smoke_passed(summary, True)

summary_json = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
summary_path.write_text(summary_json, encoding="utf-8")
(run_dir / "run.log").write_text(summary_json, encoding="utf-8")
(runner.SMOKE_DIR / "smoke_migration_kvt_fixed.log").write_text(summary_json, encoding="utf-8")

lines = [
    "# Step 03 Migration Smoke KVT Fixed Summary",
    "",
    "- Fault class: B (listener bind/connect host mismatch)",
    "- Correction: `VLLM_HOST_IP=127.0.0.1` plus API/KVT listener readiness gate",
    f"- KVT listener endpoints: {', '.join(summary.get('kvt_listener_endpoints', []))}",
    f"- Successful responses: {summary.get('successful_responses', 0)}/{summary.get('request_num', 0)}",
    f"- HTTP 200 and non-empty text: {summary.get('http_200_text_nonempty', 0)}",
    f"- Abort finish reasons: {summary.get('abort_finish_reason_count', 0)}",
    f"- Failed abort logs: {summary.get('abort_count', 0)}",
    f"- Expected migration suspend logs: {summary.get('migration_suspend_count', 0)}",
    f"- KVTResp(code=0): {summary.get('KVTResp_code_0_count', 0)}",
    f"- KVTResp(code=404): {summary.get('KVTResp_code_404_count', 0)}",
    f"- KVTResp(code=500): {summary.get('KVTResp_code_500_count', 0)}",
    f"- FinishedReq: {summary.get('FinishedReq_lines', 0)}",
    f"- SendStubMetrics: {summary.get('SendStubMetrics_count', 0)}",
    f"- EngineCore fatal: {summary.get('EngineCore_fatal_count', 0)}",
    f"- Traceback: {summary.get('Traceback_count', 0)}",
    f"- ERROR: {summary.get('ERROR_count', 0)}",
    f"- CUDA illegal memory access: {summary.get('CUDA_illegal_memory_access_count', 0)}",
    f"- Smoke passed: **{passed}**",
    "- Formal 54-run matrix started: **False**",
]
(runner.STEP_DIR / "step03_migration_smoke_kvt_fixed_summary.md").write_text(
    "\n".join(lines) + "\n", encoding="utf-8"
)
print(json.dumps({"passed": passed, **runner.count_patterns(run_dir)}, ensure_ascii=False, indent=2))
