from __future__ import annotations

import csv
import re
import subprocess
import time
from collections import Counter
from pathlib import Path

ROOT = Path("/data/home/zylu/llumnix-v018-main")
BLADE = Path("/data/home/zylu/tmp/blade-kvt")
FORMAL = ROOT / "results" / "5090_4inst_formal_20260721_014617"
RESULTS = ROOT / "results"
EFF_DIR = RESULTS / ("5090_migration_efficiency_" + time.strftime("%Y%m%d_%H%M%S"))
EFF_DIR.mkdir(parents=True, exist_ok=False)

KEYWORDS = [
    "migration start", "migration end", "migrate_out_selected", "migrate_out_begin",
    "migrate_in_begin", "pre_alloc", "prealloc", "ack", "abort", "send", "recv",
    "KVTResp", "commit", "release", "finish_request_ids", "FinishedReq", "stage",
    "StepIdx", "block", "bytes", "SbSize", "request_id", "src", "dst", "instance",
    "generated", "computed", "decode", "latency", "first token", "finish",
    "EngineCore", "Traceback", "ERROR", "fatal", "SendStubMetrics",
]
PATTERN = re.compile("|".join(re.escape(k) for k in KEYWORDS), re.I)

SCAN_FILES = [
    BLADE / "blade_kvt" / "hybrid_connector" / "migration" / "backend.py",
    BLADE / "blade_kvt" / "hybrid_connector" / "kvtbackend.py",
    BLADE / "blade_kvt" / "hybrid_connector" / "migration" / "kvtmigration.py",
    BLADE / "blade_kvt" / "hybrid_connector" / "hybrid_modules.py",
    BLADE / "blade_kvt" / "hybrid_connector" / "engine_proxy.py",
    ROOT / "python" / "llumnix" / "llumnix" / "migration_frontend" / "vllm_v1" / "kvt_migration_frontend.py",
    ROOT / "python" / "llumnix" / "llumnix" / "engine_client" / "vllm_v1" / "engine_client.py",
    ROOT / "python" / "llumnix" / "llumnix" / "status_collector" / "vllm_v1" / "status_collector.py",
    ROOT / "python" / "llumnix" / "llumnix" / "status_collector" / "vllm_v1" / "status_updater.py",
]


def sh(cmd: str, cwd: Path = ROOT) -> str:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ).stdout


def write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


env = []
env.append("$ pwd\n" + sh("pwd"))
env.append("$ git rev-parse HEAD\n" + sh("git rev-parse HEAD || true"))
env.append("$ git status --short\n" + sh("git status --short || true"))
env.append("$ git -C /data/home/zylu/tmp/blade-kvt rev-parse HEAD\n" + sh("git -C /data/home/zylu/tmp/blade-kvt rev-parse HEAD || true"))
env.append("$ git -C /data/home/zylu/tmp/blade-kvt status --short\n" + sh("git -C /data/home/zylu/tmp/blade-kvt status --short || true"))
env.append(
    "$ python env imports\n"
    + sh(
        "source /opt/miniconda3/etc/profile.d/conda.sh && "
        "conda activate llumnix5090_vllm018 && "
        "python - <<'PY'\n"
        "import torch, vllm, blade_kvt, pathlib\n"
        "print('torch', torch.__version__, torch.version.cuda)\n"
        "print('cuda_available', torch.cuda.is_available())\n"
        "print('gpu_count', torch.cuda.device_count())\n"
        "for i in range(torch.cuda.device_count()): print(i, torch.cuda.get_device_name(i))\n"
        "print('vllm', vllm.__version__, pathlib.Path(vllm.__file__).resolve())\n"
        "print('blade_kvt', pathlib.Path(blade_kvt.__file__).resolve())\n"
        "PY"
    )
)
write(EFF_DIR / "environment_snapshot.txt", "\n\n".join(env))

source_hits = []
for fp in SCAN_FILES:
    if not fp.exists():
        source_hits.append(f"### MISSING {fp}\n")
        continue
    lines = []
    for i, line in enumerate(fp.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        if PATTERN.search(line):
            lines.append(f"{i}: {line}")
    source_hits.append(f"### {fp}\n" + ("\n".join(lines) if lines else "(no keyword hits)") + "\n")
write(EFF_DIR / "source_metric_keyword_hits.txt", "\n".join(source_hits))

formal_summary = []
formal_csv = FORMAL / "formal_results.csv"
if formal_csv.exists():
    with formal_csv.open(newline="", encoding="utf-8", errors="replace") as f:
        rows = list(csv.DictReader(f))
    formal_summary.append(f"formal_results.csv rows={len(rows)}")
    mode_col = "mode" if rows and "mode" in rows[0] else "kind"
    for mode in sorted(set(r.get(mode_col, "") for r in rows)):
        mr = [r for r in rows if r.get(mode_col) == mode]

        def nums(col: str) -> list[float]:
            out = []
            for r in mr:
                try:
                    out.append(float(r.get(col, "") or 0))
                except Exception:
                    pass
            return out

        qps = nums("qps")
        successful = sum(int(float(r.get("successful_responses", 0) or 0)) for r in mr)
        kvt_code0 = sum(int(float(r.get("kvt_code0", 0) or 0)) for r in mr)
        finished_req = sum(int(float(r.get("finished_req_lines", 0) or 0)) for r in mr)
        formal_summary.append(
            f"mode={mode} runs={len(mr)} successful={successful} "
            f"avg_qps={sum(qps) / len(qps) if qps else 'NA'} "
            f"kvt_code0={kvt_code0} finished_req_lines={finished_req}"
        )
else:
    formal_summary.append("formal_results.csv missing")
write(EFF_DIR / "formal_results_quick_parse.txt", "\n".join(formal_summary))

log_patterns = {
    "migration_events": r"MIGRATION_EVENT|migration end|migrate end|KVTResp|FinishedReq|SendStubMetrics|finish_request_ids|migrate_out_selected|migration_count|downtime",
    "fatal_scan": r"EngineCore.*fatal|Traceback|ERROR|RuntimeError|CUDA illegal|ActorDiedError",
    "decode_scan": r"decode.*step|step.*latency|per_token_latency|first token|finished request|finish_time",
}
for name, pat in log_patterns.items():
    out = sh(f"grep -RniE '{pat}' {FORMAL} 2>/dev/null | head -1000 || true")
    write(EFF_DIR / f"{name}_formal_grep.txt", out)

counts = Counter()
if rows:
    migration_rows = [r for r in rows if r.get("kind") == "migration" or r.get("mode") == "migration"]
    for key, col in [
        ("KVTResp(code=0)", "kvt_code0"),
        ("KVTResp(code=404)", "kvt_code404"),
        ("KVTResp(code=500)", "kvt_code500"),
        ("FinishedReq lines", "finished_req_lines"),
        ("unique FinishedReq ids", "unique_finished_req_ids"),
        ("SendStubMetrics", "send_metrics"),
        ("migration_end", "migration_end"),
        ("traceback column", "traceback"),
    ]:
        total = 0
        for r in migration_rows:
            try:
                total += int(float(r.get(col, 0) or 0))
            except Exception:
                pass
        counts[key] = total
    filtered = FORMAL / "fatal_error_scan_filtered.txt"
    if filtered.exists():
        txt = filtered.read_text(encoding="utf-8", errors="ignore")
        for label in ["EngineCore fatal", "Traceback", "ERROR"]:
            m = re.search(rf"^{re.escape(label)}:\s*(\d+)", txt, re.M)
            counts[f"filtered {label}"] = int(m.group(1)) if m else 0
else:
    counts["formal_results.csv"] = 0
write(EFF_DIR / "formal_log_event_counts.txt", "\n".join(f"{k}: {v}" for k, v in counts.items()))

available = [
    ("request_id", "已有。KVTResp、FinishedReq、migration end/migrate end 日志中能看到 request id。"),
    ("source/destination instance", "部分已有。迁移日志和调度日志包含 source/destination 或 peer/dst 信息，但字段格式需要统一解析。"),
    ("KVTResp code", "已有。可统计 code=0/404/500。正式四实例结果里 code=0 已出现。"),
    ("KV transfer timing", "部分已有。SendStubMetrics 暴露 SendUs、RecvUs、D2H、H2D、TransUs、QueueUs 等。"),
    ("copied bytes", "部分已有。SendStubMetrics 中有 SbSizeTotal/SbSizeMin/SbSizeMax，可作为 KV 传输字节量来源。"),
    ("migration total time", "部分已有。migration end / migrate end 中有 duration/dur_ms，但缺少统一 migration_event_id 和完整 lifecycle 起点。"),
    ("finish evidence", "已有等价证据。当前正式结果用 FinishedReq lines / unique FinishedReq ids 证明完成；literal finish_request_ids 字段不稳定。"),
    ("request success/text/fatal", "已有。benchmark summary/raw response 与 fatal scan 可得到 successful_responses、text_nonempty、EngineCore fatal/Traceback/ERROR。"),
]
missing = [
    ("migration_event_id", "缺失。需要每次迁移入口生成并贯穿 source/destination/KVT logs。"),
    ("stage0_prealloc_begin/ack/abort", "缺失或不统一。需要在 blade_kvt migration backend/prealloc 路径加结构化日志。"),
    ("stage_i_copy_begin/end and stage_count", "缺失或只能弱推断。需要在 KVT copy/send/recv loop 中记录 stage id、blocks、bytes。"),
    ("final_suspend_begin/final_copy/commit/source_release/dst_resume", "缺失。Figure 10 downtime 需要这些精确时间点。"),
    ("dst_first_token_after_resume", "缺失。需要在目标端恢复后首个 token 输出处按 request_id 打点。"),
    ("downtime_commit_ms/downtime_resume_ms", "缺失。不能从现有日志可靠计算，需要上述时间点。"),
    ("generated_len_at_migration/total_context_len_at_migration", "部分缺失。KVTResp 有 computed/output_token_ids，但需要统一到迁移事件字段。"),
    ("normal/source/dst decode step overhead", "缺失。需要 EngineCore decode step 或 status collector 轻量打点。"),
    ("blocking copy real baseline", "未确认存在。需要下一阶段专门查开关；找不到就写 feasibility 或实现真实阻塞路径。"),
    ("recompute baseline", "未确认存在。需要下一阶段专门查开关；无法接管时只能标 estimate，并需人工确认。"),
]
instr = [
    ("/data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/migration/backend.py", "迁移生命周期主打点：migration_event_id、begin/end、prealloc、final suspend、commit、release、resume。"),
    ("/data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/kvtbackend.py", "KVT send/recv、KVTResp、stage id、block/byte count、backend、带宽。"),
    ("/data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/migration/kvtmigration.py", "请求绑定、迁入迁出状态、目标端恢复、FinishedReq 关联。"),
    ("/data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/hybrid_modules.py", "迁移调度状态和 engine hook，适合接入 request state 字段。"),
    ("/data/home/zylu/llumnix-v018-main/python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py", "Llumnix 发起迁移处：source/destination、request_id、mode、sequence config。"),
    ("/data/home/zylu/llumnix-v018-main/python/llumnix/llumnix/engine_client/vllm_v1/engine_client.py", "输出/首 token/finish 与请求成功状态。"),
    ("/data/home/zylu/llumnix-v018-main/python/llumnix/llumnix/status_collector/vllm_v1/status_collector.py", "实例负载、decode step overhead、source/destination running batch 观测。"),
]

md = []
md.append("# Llumnix 迁移效率实验打点审计\n")
md.append(f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
md.append(f"- 审计结果目录: `{EFF_DIR}`")
md.append(f"- 参考正式结果目录: `{FORMAL}`")
md.append("- 本轮没有修改源码，只读取代码和现有日志。\n")
md.append("## 当前已有指标\n")
for k, v in available:
    md.append(f"- `{k}`: {v}")
md.append("\n## 当前缺失或不可靠指标\n")
for k, v in missing:
    md.append(f"- `{k}`: {v}")
md.append("\n## 建议打点位置\n")
for f, v in instr:
    md.append(f"- `{f}`: {v}")
md.append("\n## 可直接从现有日志解析的指标\n")
for k in [
    "request_id",
    "source/destination instance 部分字段",
    "KVTResp code",
    "SendStubMetrics transfer timing",
    "SbSizeTotal copied bytes",
    "FinishedReq 完成证据",
    "benchmark successful_responses/text/fatal scan",
]:
    md.append(f"- {k}")
md.append("\n## 必须补代码才能可靠得到的指标\n")
for k in [
    "migration_event_id",
    "stage0_prealloc_begin/ack",
    "stage_i_copy_begin/end",
    "final_suspend_begin",
    "commit_begin/done",
    "source_release",
    "dst_resume",
    "dst_first_token_after_resume",
    "downtime_commit_ms",
    "downtime_resume_ms",
    "normal/source/dst decode step overhead",
    "blocking_copy real mode",
    "recompute real/estimate mode 标记",
]:
    md.append(f"- {k}")
md.append("\n## 现有正式结果快速计数\n")
md.append("```text")
md.append((EFF_DIR / "formal_log_event_counts.txt").read_text(encoding="utf-8"))
md.append("```\n")
md.append("## 下一步\n")
md.append("1. 先补结构化迁移事件日志，不改变大版本、不重编 vLLM。")
md.append("2. 先做 seq_len=256 live migration smoke，要求 downtime 字段真实非空。")
md.append("3. blocking copy 和 recompute 先做可行性搜索，缺真实路径时输出 BLOCKING_RECOMPUTE_FEASIBILITY.md。")
write(EFF_DIR / "MIGRATION_EFFICIENCY_INSTRUMENTATION_AUDIT.md", "\n".join(md))

handoff = ROOT / "AI_HANDOFF.md"
entry = f"""

## {time.strftime('%Y-%m-%d %H:%M:%S')} 迁移效率实验 Phase 1 审计
- 阶段: Phase 1, 代码/日志指标审计。
- 结果目录: {EFF_DIR}
- 本轮修改文件: 无源码修改。
- 备份路径: 不适用，本轮只读。
- 生成文件: MIGRATION_EFFICIENCY_INSTRUMENTATION_AUDIT.md, environment_snapshot.txt, source_metric_keyword_hits.txt, formal_results_quick_parse.txt, formal_log_event_counts.txt。
- 关键结论: 现有正式日志可解析 KVTResp、FinishedReq、SendStubMetrics、部分迁移时长和成功率；Figure 10 需要的 downtime_resume、commit downtime、stage 细分、first token after resume、decode overhead 仍需补结构化打点。
- 下一步计划: 按第一条缺失链路补 migration_event_id 和 lifecycle timestamps，然后做 seq_len=256 live migration smoke。
- 是否已有 Figure 10 完整实验数据: 否，当前只有四实例真实迁移成功数据，不含完整迁移效率矩阵。
"""
with handoff.open("a", encoding="utf-8") as f:
    f.write(entry)

print(EFF_DIR)
print(EFF_DIR / "MIGRATION_EFFICIENCY_INSTRUMENTATION_AUDIT.md")
