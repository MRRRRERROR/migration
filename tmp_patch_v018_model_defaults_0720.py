from pathlib import Path

p = Path("/data/home/zylu/llumnix-v018-main/tests/local/utils.py")
s = p.read_text()
s = s.replace(
    'MODEL_NAME: str = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-7B")',
    'MODEL_NAME: str = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")',
)
s = s.replace(
    'MODEL_PATH: str = os.environ.get("MODEL_PATH", f"/models/{MODEL_NAME}")',
    'MODEL_PATH: str = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")',
)
p.write_text(s)
print("patched tests/local/utils.py MODEL defaults")
