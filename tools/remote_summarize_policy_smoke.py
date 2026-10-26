import json
from pathlib import Path

root = Path("/data/home/zylu/llumnix-v018-main/results/5090_migration_policy_baselines_20260819_201922")
summaries = []
for path in sorted((root / "01_smoke").glob("stable_short/*/run_1/run_summary.json")):
    summaries.append(json.loads(path.read_text()))

lines = [
    "# Migration Policy Smoke Summary",
    "",
    "| Policy | Success | KVT 0 | KVT 404 | KVT 500 | FinishedReq | Policy decisions | Fatal | Traceback | ERROR | Finish abort | Contended |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
]
for item in summaries:
    lines.append(
        "| {migration_policy} | {successful_responses}/{request_num} | {KVTResp_code_0_count} | "
        "{KVTResp_code_404_count} | {KVTResp_code_500_count} | {FinishedReq_lines} | "
        "{migration_policy_decision_count} | {EngineCore_fatal_count} | {Traceback_count} | "
        "{ERROR_count} | {finish_reason_abort_count} | {gpu_contention} |".format(**item)
    )
lines += [
    "",
    "- All six policies completed: `{}`".format(len(summaries) == 6),
    "- All requests successful: `{}`".format(all(s.get("successful_responses") == 20 for s in summaries)),
    "- Mandatory failure totals: `{}`".format({
        key: sum(s.get(key, 0) for s in summaries)
        for key in (
            "EngineCore_fatal_count", "Traceback_count", "ERROR_count",
            "CUDA_illegal_memory_access_count", "KVTResp_code_500_count",
            "finish_reason_abort_count",
        )
    }),
    "- Note: TPOT-tail and Composite may legitimately abstain from migration under stable-short when no TPOT/SLO violation exists; their final scheduler commands are recorded in each run summary.",
]
(root / "04_step_summaries").mkdir(parents=True, exist_ok=True)
(root / "04_step_summaries" / "smoke_summary.md").write_text("\n".join(lines) + "\n")
print(json.dumps(summaries, ensure_ascii=False))
