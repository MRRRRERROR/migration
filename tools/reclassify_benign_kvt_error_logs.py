#!/usr/bin/env python3
"""Reclassify blade_kvt successful completion messages logged at ERROR level."""

from __future__ import annotations

import argparse
import glob
import json
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()

    updated = 0
    raw_total = 0
    benign_total = 0
    actual_total = 0
    for summary_path in args.root.glob("03_runs/*/*/run_*/run_summary.json"):
        run_dir = summary_path.parent
        lines = []
        for log_path in glob.glob(str(run_dir / "*.log")):
            if Path(log_path).name == "run.log":
                continue
            lines.extend(Path(log_path).read_text(errors="replace").splitlines())
        error_lines = [line for line in lines if re.search(r"\bERROR\b", line)]
        benign = [
            line for line in error_lines
            if "ERROR:blade_kvt.hybrid_connector.kvtbackend:disagg kvt done." in line
        ]
        actual = [line for line in error_lines if line not in benign]

        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["raw_ERROR_log_count"] = len(error_lines)
        summary["benign_kvt_completion_error_level_count"] = len(benign)
        summary["ERROR_count"] = len(actual)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (run_dir / "run.log").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        updated += 1
        raw_total += len(error_lines)
        benign_total += len(benign)
        actual_total += len(actual)

    print(json.dumps({
        "updated_summaries": updated,
        "raw_ERROR_log_count": raw_total,
        "benign_kvt_completion_error_level_count": benign_total,
        "actual_ERROR_count": actual_total,
    }, indent=2))


if __name__ == "__main__":
    main()
