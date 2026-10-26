import collections
import glob
import json
import os
import re
from pathlib import Path


RUN = Path("results/5090_paper_supplement_20260821_170651/02_fig12_fragmentation_v2/formal_selected/forced_fragmentation/config_c/load_sr/run_1")


summary = json.loads((RUN / "run_summary.json").read_text())
print("=== SUMMARY MIGRATION ===")
for key in sorted(summary):
    if any(term in key.lower() for term in ("migration", "abort", "kvtresp", "stale", "endpoint", "finish_reason")):
        value = summary[key]
        print(key, f"list[{len(value)}]" if isinstance(value, list) else repr(value))

print("=== RESPONSE COLLECTIONS ===")
for key in ("sample_results", "failed_results", "responses", "errors"):
    value = summary.get(key)
    print(key, type(value).__name__, len(value) if isinstance(value, list) else repr(value))
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and (
                item.get("status") != 200 or item.get("error") or item.get("finish_reason") in ("abort", "error")
            ):
                print(json.dumps(item, ensure_ascii=False)[:1800])

print("=== REQUEST METRICS ===")
metrics = json.loads((RUN / "request_metrics.json").read_text())
print(type(metrics).__name__, list(metrics)[:30] if isinstance(metrics, dict) else len(metrics))
items = metrics if isinstance(metrics, list) else metrics.get("requests", metrics.get("records", []))
print("items", len(items))
print("metric_keys", sorted(items[0]) if items else [])
print("request_status_counts", collections.Counter(str(item.get("request_status")) for item in items))
print("status_counts", collections.Counter(str(item.get("status")) for item in items))
print("http_status_counts", collections.Counter(str(item.get("http_status")) for item in items))
print("finish_reason_counts", collections.Counter(str(item.get("finish_reason")) for item in items))
print("text_nonempty_counts", collections.Counter(str(item.get("text_nonempty")) for item in items))
abort_ids = {
    "cmpl-fig12v2_formal_selected_config_c_occupancy_127-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_249-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_250-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_261-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_265-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_271-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_281-0",
    "cmpl-fig12v2_formal_selected_config_c_large_arrival_290-0",
}
for item in items:
    item_id = item.get("request_id") or item.get("id") or item.get("logical_request_id")
    interesting = item_id in abort_ids or any(value in str(item) for value in abort_ids)
    failed = (
        item.get("success") is False
        or item.get("error")
        or item.get("http_status") not in (None, 200)
        or item.get("status_code") not in (None, 200)
        or str(item.get("status", "")).lower() not in ("success", "completed", "200", "")
    )
    if isinstance(item, dict) and (interesting or failed):
        print(json.dumps(item, ensure_ascii=False)[:2000])
for item in items:
    if item.get("request_status") != "success" or not item.get("text_nonempty") or item.get("finish_reason") == "abort":
        print("NON_SUCCESS", json.dumps(item, ensure_ascii=False)[:2400])

print("=== ABORT IDS ===")
abort_pattern = re.compile(r"AbortReq\(reqid='([^']+)'.*?output=(True|False).*?reason='([^']+)'")
aborts = []
for path in glob.glob(str(RUN / "vllm_*.log")):
    for line in open(path, errors="ignore"):
        match = abort_pattern.search(line)
        if match:
            aborts.append((match.group(1), match.group(2), match.group(3), os.path.basename(path)))
print(collections.Counter(item[2] for item in aborts))
for item in aborts:
    if item[2] != "migration.suspend":
        print(item)

print("=== EVENT NAMES ===")
event_counts = collections.Counter()
event_examples = {}
for line in open(RUN / "migration_efficiency_events.jsonl", errors="ignore"):
    try:
        event = json.loads(line)
    except Exception:
        continue
    name = event.get("event_name") or event.get("event") or "?"
    event_counts[name] += 1
    event_examples.setdefault(name, event)
for name, count in event_counts.most_common():
    print(name, count)
for name in event_counts:
    if any(term in name.lower() for term in ("fail", "stale", "invalid", "abort", "500")):
        print("EX", name, json.dumps(event_examples[name], ensure_ascii=False)[:1500])
