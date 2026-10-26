import json
import sys

sys.path.insert(0, "/data/home/zylu/llumnix-v018-main")

import run_figure12_v2 as fig12


config = dict(fig12.PRESSURE_CONFIGS["config_c"])
config["timeout_seconds"] = 600
targets = [
    ("load_sr", 2),
    ("load_sr", 3),
    ("tpot_tail", 1),
    ("tpot_tail", 2),
    ("tpot_tail", 3),
    ("composite_proposed", 1),
    ("composite_proposed", 2),
    ("composite_proposed", 3),
]


def validation_reasons(summary):
    reasons = list(fig12.strict_reason(summary))
    checks = {
        "successful_responses": summary.get("successful_responses") == 400,
        "http_200_text_nonempty": summary.get("http_200_text_nonempty") == 400,
        "KVTResp_code_500_count": summary.get("KVTResp_code_500_count", 0) == 0,
        "finish_reason_abort_count": summary.get("finish_reason_abort_count", 0) == 0,
        "runtime_Traceback_count": summary.get("runtime_Traceback_count", 0) == 0,
        "EngineCore_fatal_count": summary.get("EngineCore_fatal_count", 0) == 0,
        "CUDA_illegal_memory_access_count": summary.get("CUDA_illegal_memory_access_count", 0) == 0,
        "AssertionError_count": summary.get("AssertionError_count", 0) == 0,
        "sse_read_timeout_count": summary.get("sse_read_timeout_count", 0) == 0,
        "fragmentation_nonzero_sample_count": summary.get("fragmentation_nonzero_sample_count", 0) > 0,
        "blocked_HOL_sample_count": summary.get("blocked_HOL_sample_count", 0) > 0,
        "long_input_TTFT_P99_s": summary.get("long_input_TTFT_P99_s") not in (None, "NA"),
    }
    reasons.extend(name for name, passed in checks.items() if not passed)
    return sorted(set(reasons))

for policy, repeat_id in targets:
    path = (
        fig12.V2_DIR
        / "formal_selected"
        / "forced_fragmentation"
        / "config_c"
        / policy
        / f"run_{repeat_id}"
        / "run_summary.json"
    )
    if path.exists():
        existing = json.loads(path.read_text())
        reasons = validation_reasons(existing)
        if existing.get("usable_for_main_conclusion") and not reasons:
            print(f"SKIP valid {policy}/run_{repeat_id}", flush=True)
            continue

    print(f"START {policy}/run_{repeat_id}", flush=True)
    summary = fig12.run_config(
        policy,
        repeat_id,
        config,
        "formal_selected",
        "forced_fragmentation",
        "config_c",
    )
    reasons = validation_reasons(summary)
    summary["strict_validation_reasons"] = reasons
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(
        json.dumps(
            {
                "policy": policy,
                "repeat": repeat_id,
                "successful_responses": summary.get("successful_responses"),
                "http_200_text_nonempty": summary.get("http_200_text_nonempty"),
                "reasons": reasons,
                "first_error": summary.get("first_error", summary.get("first_fatal")),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if reasons:
        raise SystemExit(4)

print("FIG12_CONFIG_C_REMAINING_COMPLETE", flush=True)
