import json
import sys
from pathlib import Path

sys.path.insert(0, "/data/home/zylu/llumnix-v018-main")

import run_figure12_v2 as fig12


config = dict(fig12.PRESSURE_CONFIGS["config_c"])
config["timeout_seconds"] = 600
summary = fig12.run_config(
    "load_sr",
    1,
    config,
    "retry_load_sr_run1_once",
    "forced_fragmentation",
    "config_c",
)
reasons = fig12.strict_reason(summary)
summary["strict_validation_reasons"] = reasons
summary["retry_scope"] = "single controlled retry of formal Config C load_sr/run_1"
summary["replaces_original_only_if_valid"] = not reasons

path = (
    fig12.V2_DIR
    / "retry_load_sr_run1_once"
    / "forced_fragmentation"
    / "config_c"
    / "load_sr"
    / "run_1"
    / "run_summary.json"
)
path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
print(json.dumps({"summary_path": str(path), "reasons": reasons, "summary": summary}, ensure_ascii=False, indent=2))
raise SystemExit(0 if not reasons else 4)
