#!/usr/bin/env python3
"""Run one measured-profile balanced migration trigger case."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_existing_simulator_matrix as sim  # noqa: E402


def main() -> None:
    profile = sim.REPO / "profiles/5090_qwen25_7b_measured/qwen2p5_7b_5090_profile_measured.pkl"
    os.environ["SIM_PROFILE_PATH"] = str(profile)
    sim.PROFILE = profile
    sim.WORKLOADS["long_output_migration"]["max_tokens"] = lambda _i: 512

    root = sim.REPO / "results" / f"wsl_sim_measured_balanced_trigger_once_{time.strftime('%Y%m%d_%H%M%S')}"
    root.mkdir(parents=True, exist_ok=True)

    summary = sim.run_case(
        root,
        "balanced_threshold_4096",
        "long_output_migration",
        40,
        40,
        "flood",
        migration=True,
        pair_policy="balanced",
        request_policy="SR",
        migrate_out_threshold="4096",
        polling_interval="0.01",
    )
    manifest = {
        "root": str(root),
        "profile": str(profile),
        "summary": summary,
    }
    (root / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
