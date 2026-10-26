#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/llumnix-ray-main"
source "$HOME/venvs/llumnix/bin/activate"

echo "--- procs ---"
pgrep -af run_sim_baselines || true
pgrep -af benchmark_serving || true
pgrep -af api_server || true

echo "--- ray stop ---"
ray stop || true

echo "--- latest baseline dir ---"
python - <<'PY'
from pathlib import Path
base_root = Path.home() / "llumnix_results"
bases = sorted(base_root.glob("baseline_sim_opt-1.3b_*"), key=lambda p: p.stat().st_mtime, reverse=True)
if not bases:
    print("BASE=")
    raise SystemExit
base = bases[0]
print("BASE=" + str(base))
for p in sorted(base.glob("*")):
    if p.is_dir():
        files = sorted(x.relative_to(p).as_posix() for x in p.rglob("*") if x.is_file())
        print(f"[{p.name}]")
        for f in files:
            size = (p / f).stat().st_size
            print(f"  {f} {size}")
    else:
        print(f"{p.name} {p.stat().st_size}")
rp = base / "run.log"
print("--- run.log ---")
print(rp.read_text() if rp.exists() else "missing")
PY
