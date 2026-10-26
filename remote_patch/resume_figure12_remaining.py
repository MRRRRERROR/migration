#!/usr/bin/env python3
"""Resume only the five unfinished Figure 12 fragmentation runs."""

import json

import run_paper_supplement_experiments as exp


def main() -> None:
    exp.create_dirs()
    exp.configure_policies()
    exp.copy_profiling()
    requests, demand = exp.make_fig12_workload("fragmentation_medium", smoke=False)
    plan = [
        ("tpot_tail", 2),
        ("tpot_tail", 3),
        ("composite_proposed", 1),
        ("composite_proposed", 2),
        ("composite_proposed", 3),
    ]
    results = []
    for policy, repeat in plan:
        summary = exp.run_custom(
            "figure12",
            "fragmentation_medium",
            policy,
            repeat,
            requests,
            50,
            4096,
            exp.submit_phased,
            False,
            demand,
            {
                "large_request_prompt_tokens": demand,
                "metric_design": "phased occupancy then large arrivals",
                "figure12_resume": True,
            },
        )
        results.append(summary)
        print(json.dumps({
            "policy": policy,
            "repeat": repeat,
            "successful_responses": summary.get("successful_responses"),
            "Traceback_count": summary.get("Traceback_count"),
            "ERROR_count": summary.get("ERROR_count"),
            "KVTResp_code_500_count": summary.get("KVTResp_code_500_count"),
            "abort_count": summary.get("abort_count"),
        }, ensure_ascii=False), flush=True)
    exp.write_json(exp.STEP_DIR / "figure12_remaining_results.json", results)


if __name__ == "__main__":
    main()
