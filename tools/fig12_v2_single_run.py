#!/usr/bin/env python3
"""Run and validate one existing Figure 12 v2 Config C experiment round."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, "/data/home/zylu/llumnix-v018-main")
import run_figure12_v2 as runner


def validation_reasons(summary: dict) -> list[str]:
    request_num = int(summary.get("request_num") or 400)
    reasons = []
    for key in ("successful_responses", "http_200_text_nonempty"):
        if int(summary.get(key) or 0) != request_num:
            reasons.append(f"{key}={summary.get(key)}")
    for key in (
        "finish_reason_abort_count",
        "KVTResp_code_500_count",
        "Traceback_count",
        "EngineCore_fatal_count",
        "Scheduler_fatal_count",
        "CUDA_illegal_memory_access_count",
        "actual_service_ERROR_count",
        "sse_read_timeout_count",
    ):
        if int(summary.get(key) or 0) != 0:
            reasons.append(f"{key}={summary.get(key)}")
    if int(summary.get("fragmentation_nonzero_sample_count") or 0) == 0:
        reasons.append("fragmentation never became nonzero")
    if int(summary.get("blocked_HOL_sample_count") or 0) == 0:
        reasons.append("blocked HOL never observed")
    if summary.get("long_input_TTFT_P99_s") is None:
        reasons.append("long-input TTFT unavailable")
    return reasons


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", required=True)
    parser.add_argument("--repeat", required=True, type=int)
    parser.add_argument("--run-group", required=True)
    args = parser.parse_args()

    runner.paper.configure_policies()
    config = dict(runner.PRESSURE_CONFIGS["config_c"])
    config["timeout_seconds"] = 600
    summary = runner.run_config(
        args.policy,
        args.repeat,
        config,
        args.run_group,
        "forced_fragmentation",
        "config_c",
    )
    reasons = validation_reasons(summary)
    summary["strict_validation_reasons"] = reasons
    summary["strict_validation_passed"] = not reasons

    summary_path = (
        runner.V2_DIR
        / args.run_group
        / "forced_fragmentation"
        / "config_c"
        / args.policy
        / f"run_{args.repeat}"
        / "run_summary.json"
    )
    Path(summary_path).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "summary_path": str(summary_path),
                "policy": args.policy,
                "repeat": args.repeat,
                "passed": not reasons,
                "reasons": reasons,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not reasons else 4


if __name__ == "__main__":
    raise SystemExit(main())
