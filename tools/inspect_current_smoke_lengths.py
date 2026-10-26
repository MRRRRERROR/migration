import json
from transformers import AutoTokenizer

path = "/data/home/zylu/llumnix-v018-main/results/5090_load_sweep_20260721_191608/02_workloads/stable_short_low_load.jsonl"
with open(path, encoding="utf-8") as handle:
    rows = [json.loads(line) for line in handle if line.strip()][:10]

tokenizer = AutoTokenizer.from_pretrained(
    "/data/models/Qwen2.5-7B-Instruct", trust_remote_code=True
)
lengths = [len(tokenizer.encode(row["prompt"], add_special_tokens=False)) for row in rows]
print("prompt_token_lengths", lengths)
print("min", min(lengths), "max", max(lengths), "mean", sum(lengths) / len(lengths))
print("max_tokens", sorted({row["max_tokens"] for row in rows}))
print("ignore_eos", sorted({row["ignore_eos"] for row in rows}))
