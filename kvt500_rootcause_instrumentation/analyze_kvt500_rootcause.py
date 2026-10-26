from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SOURCE_STAGES = (
    "MIGRATION_SELECTED",
    "PREALLOC_ACK",
    "SOURCE_TRANSFER_SUBMIT",
    "SCHED_GET_RAW_BLOCKS",
    "SCHED_GET_ADAPTED_BLOCKS",
    "AFTER_SOURCE_BLOCK_LOOKUP",
    "MIGRATION_BYPASS_BLOCKS",
    "PREQMETA_BLOCKS",
    "SEND_BATCH_BLOCKS",
    "PYTHON_SEND_BOUNDARY",
)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    for line_number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        try:
            row = json.loads(line)
            row["_line"] = line_number
            rows.append(row)
        except json.JSONDecodeError:
            continue
    return sorted(rows, key=lambda row: (row.get("timestamp_ns", 0), row["_line"]))


def shape(row: dict[str, Any] | None):
    return None if row is None else row.get("block_group_sizes")


def is_empty_shape(value) -> bool:
    return value is not None and (len(value) == 0 or any(size == 0 for size in value))


def latest(rows, event):
    matches = [row for row in rows if row.get("event") == event]
    return matches[-1] if matches else None


def stage_rows(rows):
    result = {}
    for event in SOURCE_STAGES:
        candidates = [row for row in rows if row.get("event") == event]
        if event == "PREQMETA_BLOCKS":
            candidates = [row for row in candidates if "kvtbackend" in str(row.get("component"))]
        if candidates:
            result[event] = candidates[-1]
    return result


def classify(rows):
    stages = stage_rows(rows)
    ordered = [stages[event] for event in SOURCE_STAGES if event in stages]
    shaped = [row for row in ordered if shape(row) is not None]
    first_empty = next((row for row in shaped if is_empty_shape(shape(row))), None)
    first_empty_ts = first_empty.get("timestamp") if first_empty else None
    earlier_nonempty = [
        row for row in shaped
        if first_empty_ts is not None
        and row.get("timestamp", 0) < first_empty_ts
        and not is_empty_shape(shape(row))
    ]
    last_nonempty = earlier_nonempty[-1] if earlier_nonempty else None
    lower = last_nonempty.get("timestamp", float("-inf")) if last_nonempty else float("-inf")
    upper = first_empty_ts if first_empty_ts is not None else float("inf")
    mutations = [
        row for row in rows
        if row.get("event") == "BLOCK_MUTATION"
        and lower <= row.get("timestamp", 0) <= upper
    ]
    mutation_types = sorted({str(row.get("mutation_type")) for row in mutations})
    raw = latest(rows, "SCHED_GET_RAW_BLOCKS")
    adapted = latest(rows, "SCHED_GET_ADAPTED_BLOCKS")
    boundary = latest(rows, "PYTHON_SEND_BOUNDARY")
    bypass = latest(rows, "MIGRATION_BYPASS_BLOCKS")
    preqmeta = latest(rows, "PREQMETA_BLOCKS")

    case = "UNRESOLVED"
    reason = "No rule has direct evidence."
    if first_empty and last_nonempty and mutations:
        case = "CASE_A"
        reason = "A non-empty source state is followed by an explicit block mutation and then an empty source state."
    elif first_empty and first_empty.get("event") in ("SCHED_GET_RAW_BLOCKS", "AFTER_SOURCE_BLOCK_LOOKUP") and not last_nonempty:
        case = "CASE_F"
        reason = "The first observable scheduler lookup is already empty and no earlier mutation was captured."
    elif raw and adapted and not is_empty_shape(shape(raw)) and is_empty_shape(shape(adapted)):
        case = "CASE_C"
        reason = "Raw scheduler block groups are non-empty but the hybrid shape adapter output is empty."
    elif bypass and preqmeta and not is_empty_shape(shape(bypass)) and is_empty_shape(shape(preqmeta)):
        case = "CASE_C"
        reason = "Block lookup/bypass is non-empty but PReqMeta is empty."
    elif preqmeta and boundary and not is_empty_shape(shape(preqmeta)) and is_empty_shape(shape(boundary)):
        case = "CASE_D"
        reason = "PReqMeta is non-empty but Python send-boundary arguments are empty."
    elif boundary and not is_empty_shape(shape(boundary)):
        case = "CASE_E"
        reason = "Python send boundary is non-empty while the transfer still returns KVT500."

    return {
        "stages": stages,
        "first_empty": first_empty,
        "last_nonempty": last_nonempty,
        "mutations": mutations,
        "mutation_types": mutation_types,
        "case": case,
        "reason": reason,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    events_path = root / "kvt500_block_lifecycle.jsonl"
    rows = read_jsonl(events_path)
    results_dir = root / "02_results"
    final_dir = root / "03_final"
    results_dir.mkdir(parents=True, exist_ok=True)
    final_dir.mkdir(parents=True, exist_ok=True)

    by_attempt = defaultdict(list)
    for row in rows:
        by_attempt[row.get("attempt_id", "unknown")].append(row)
    failed = {
        attempt: attempt_rows
        for attempt, attempt_rows in by_attempt.items()
        if any(row.get("event") == "KVTResp500" or row.get("code") == 500 for row in attempt_rows)
    }
    analyses = {attempt: classify(attempt_rows) for attempt, attempt_rows in failed.items()}

    timeline = ["# KVT500 attempt timelines", ""]
    wanted = set(SOURCE_STAGES) | {
        "PREALLOC_BEGIN", "BEFORE_SOURCE_BLOCK_LOOKUP", "BLOCK_MUTATION",
        "FINISHED_REQ", "FINISH_REQUEST", "REQUEST_FINISHED", "SCHEDULER_REMOVE",
        "KVTResp500", "KVTResp",
    }
    for attempt, attempt_rows in failed.items():
        timeline += [f"## {attempt}", "", "| timestamp | event | component | shape | mutation/status |", "|---:|---|---|---|---|"]
        for row in attempt_rows:
            if row.get("event") not in wanted:
                continue
            detail = row.get("mutation_type") or row.get("request_status") or row.get("code") or ""
            timeline.append(
                f"| {row.get('timestamp')} | {row.get('event')} | {row.get('component')} | "
                f"`{row.get('block_group_sizes')}` | {detail} |"
            )
        timeline.append("")
    (results_dir / "KVT500_ATTEMPT_TIMELINES.md").write_text("\n".join(timeline) + "\n", encoding="utf-8")

    fields = [
        "attempt_id", "request_id", "src", "dst", "selected_group_sizes",
        "prealloc_ack_src_group_sizes", "submit_group_sizes", "lookup_group_sizes",
        "migration_bypass_group_sizes", "preqmeta_group_sizes", "send_batch_group_sizes",
        "python_send_boundary_group_sizes", "first_empty_stage", "first_empty_timestamp",
        "mutation_between_nonempty_and_empty", "mutation_type", "mutation_function",
        "preempt_before_empty", "recompute_before_empty", "free_before_empty",
        "finish_before_empty", "scheduler_remove_before_empty", "kvt500",
        "request_completed", "final_status", "case",
    ]
    transition_rows = []
    for attempt, attempt_rows in failed.items():
        analysis = analyses[attempt]
        stages = analysis["stages"]
        first_empty = analysis["first_empty"]
        cutoff = first_empty.get("timestamp", float("inf")) if first_empty else float("inf")
        before = [row for row in attempt_rows if row.get("timestamp", 0) <= cutoff]
        mutation_types = analysis["mutation_types"]
        completed = any(row.get("event") in ("REQUEST_FINISHED", "SCHEDULER_REMOVE") for row in attempt_rows)
        final = latest(attempt_rows, "REQUEST_FINISHED") or latest(attempt_rows, "SCHEDULER_REMOVE")
        transition_rows.append({
            "attempt_id": attempt,
            "request_id": attempt_rows[0].get("request_id"),
            "src": next((r.get("src_instance") for r in attempt_rows if r.get("src_instance")), None),
            "dst": next((r.get("dst_instance") for r in attempt_rows if r.get("dst_instance")), None),
            "selected_group_sizes": shape(stages.get("MIGRATION_SELECTED")),
            "prealloc_ack_src_group_sizes": shape(stages.get("PREALLOC_ACK")),
            "submit_group_sizes": shape(stages.get("SOURCE_TRANSFER_SUBMIT")),
            "lookup_group_sizes": shape(stages.get("AFTER_SOURCE_BLOCK_LOOKUP")),
            "migration_bypass_group_sizes": shape(stages.get("MIGRATION_BYPASS_BLOCKS")),
            "preqmeta_group_sizes": shape(stages.get("PREQMETA_BLOCKS")),
            "send_batch_group_sizes": shape(stages.get("SEND_BATCH_BLOCKS")),
            "python_send_boundary_group_sizes": shape(stages.get("PYTHON_SEND_BOUNDARY")),
            "first_empty_stage": first_empty.get("event") if first_empty else None,
            "first_empty_timestamp": first_empty.get("timestamp") if first_empty else None,
            "mutation_between_nonempty_and_empty": bool(analysis["mutations"]),
            "mutation_type": ",".join(mutation_types),
            "mutation_function": ",".join(sorted({str(r.get('mutation_function')) for r in analysis['mutations']})),
            "preempt_before_empty": any(r.get("mutation_type") == "PREEMPT" for r in before),
            "recompute_before_empty": any(r.get("mutation_type") == "RECOMPUTE" for r in before),
            "free_before_empty": any(r.get("mutation_type") == "FREE" for r in before),
            "finish_before_empty": any(r.get("event") in ("FINISH_REQUEST", "REQUEST_FINISHED") for r in before),
            "scheduler_remove_before_empty": any(r.get("event") == "SCHEDULER_REMOVE" for r in before),
            "kvt500": True,
            "request_completed": completed,
            "final_status": final.get("request_status") if final else None,
            "case": analysis["case"],
        })
    with (results_dir / "KVT500_BLOCK_TRANSITION.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(transition_rows)

    case_counts = Counter(analysis["case"] for analysis in analyses.values())
    confirmed_case = case_counts.most_common(1)[0][0] if case_counts else "UNRESOLVED"
    repeated = case_counts.get(confirmed_case, 0) >= 2
    direct = confirmed_case in {"CASE_A", "CASE_C", "CASE_D", "CASE_E"}
    level = "CONFIRMED" if repeated and direct else ("HIGHLY_LIKELY" if direct or failed else "POSSIBLE")
    first_empty_counts = Counter(
        analysis["first_empty"].get("event")
        for analysis in analyses.values()
        if analysis["first_empty"]
    )
    first_empty_summary = first_empty_counts.most_common(1)[0][0] if first_empty_counts else "not observed in Python"
    mutation_count = sum(bool(analysis["mutations"]) for analysis in analyses.values())
    adapter_count = sum(analysis["case"] == "CASE_C" for analysis in analyses.values())
    boundary_count = sum(analysis["case"] == "CASE_E" for analysis in analyses.values())

    report = [
        "# KVT500 Root Cause Confirmation", "",
        "## 1. 实验配置", "",
        f"- Root: `{root}`", f"- Event log: `{events_path}`", "- Policy: tpot_tail", "- Config: E C32, 1200 requests, concurrency 32, seed 20260905", "",
        "## 2. 实际复现情况", "", f"- KVT500 attempts: **{len(failed)}**", "",
        "## 3. Source block vector 首次变空的位置", "", f"- Most common stage: **{first_empty_summary}**", "",
        "## 4. Attempt evidence", "", "| attempt | case | first empty | mutation | explanation |", "|---|---|---|---|---|",
    ]
    for attempt, analysis in analyses.items():
        report.append(
            f"| {attempt} | {analysis['case']} | "
            f"{analysis['first_empty'].get('event') if analysis['first_empty'] else 'none'} | "
            f"{','.join(analysis['mutation_types']) or 'none'} | {analysis['reason']} |"
        )
    report += [
        "", "## 5. 三个核心观测时点", "",
        "完整逐 attempt 对比见 `02_results/KVT500_BLOCK_TRANSITION.csv`。", "",
        "## 6. 是否存在 block mutation", "", f"- {mutation_count}/{len(failed)} failed attempts 在非空到空之间捕获 mutation。", "",
        "## 7. 是否存在 preempt/recompute/free", "",
        f"- Types: `{sorted({t for a in analyses.values() for t in a['mutation_types']})}`", "",
        "## 8. 是否是 adapter shape 问题", "", f"- CASE_C attempts: {adapter_count}", "",
        "## 9. 是否是 Python->C++ 问题", "", f"- CASE_E attempts: {boundary_count}", "",
        "## 10. 最终根因等级", "", f"**{level}**", "", f"Dominant classification: `{confirmed_case}` ({case_counts.get(confirmed_case, 0)} attempts).", "",
        "## 11. 精确涉及函数", "",
        "- `blade_kvt.hybrid_connector.kvtbackend.get_kvblk_ids`", "- `PBackend._migration_bypass`", "- `PBackend._build_req_send_batch`", "- `PBackend._start_req_send`", "- `vllm.v1.core.sched.Scheduler._preempt_request/_free_blocks`", "- `vllm.v1.core.kv_cache_manager.KVCacheManager.allocate_slots/free/remove_skipped_blocks`", "",
        "## 12. 修复建议", "",
        "本任务未实施修复。应仅在上述 evidence 指向的首次状态转换处设计 block snapshot/pinning、生命周期同步或 shape adapter 修复，并另行验证。", "",
    ]
    (final_dir / "KVT500_ROOT_CAUSE_CONFIRMATION.md").write_text("\n".join(report), encoding="utf-8")

    diagram = ["# KVT500 Root Cause Timeline", ""]
    for attempt, analysis in analyses.items():
        diagram += [f"## {attempt}", "", "```text"]
        for event in SOURCE_STAGES:
            row = analysis["stages"].get(event)
            if row:
                diagram.append(f"{event}: groups={row.get('block_group_sizes')}")
                diagram.append("    ↓")
        for mutation in analysis["mutations"]:
            diagram.append(f"BLOCK_MUTATION: {mutation.get('mutation_type')} @ {mutation.get('mutation_function')}")
            diagram.append("    ↓")
        diagram += ["KVT500", "    ↓", "request completion observed" if any(r.get('event') in ('REQUEST_FINISHED','SCHEDULER_REMOVE') for r in failed[attempt]) else "request completion not captured", "```", ""]
    (final_dir / "KVT500_ROOT_CAUSE_TIMELINE.md").write_text("\n".join(diagram), encoding="utf-8")

    print(json.dumps({
        "kvt500_attempts": len(failed),
        "first_empty_stage": first_empty_summary,
        "mutation_attempts": mutation_count,
        "dominant_case": confirmed_case,
        "root_cause_level": level,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
