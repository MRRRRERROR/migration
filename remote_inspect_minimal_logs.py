from pathlib import Path

patterns = [
    "Traceback",
    "TypeError",
    "RuntimeError",
    "AttributeError",
    "EngineCore failed",
    "EngineCore encountered",
    "AcceleratorError",
    "unsupported PTX",
    "ERROR",
    "ValueError",
    "ImportError",
    "Triton",
    "TRITON",
    "triton",
]

for lp in [
    Path("logs/vllm_0_minimal.log"),
    Path("logs/gateway_minimal.log"),
    Path("logs/runtime_0_minimal.log"),
]:
    print("===", lp, "===")
    if not lp.exists():
        print("missing")
        continue
    hits = []
    for i, line in enumerate(lp.read_text(errors="ignore").splitlines(), 1):
        if any(p in line for p in patterns):
            hits.append((i, line))
    for i, line in hits[-180:]:
        print(f"{i}: {line}")
