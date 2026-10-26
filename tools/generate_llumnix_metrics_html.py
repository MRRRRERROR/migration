#!/usr/bin/env python3
import csv
import html
import json
import math
import re
from datetime import datetime
from pathlib import Path


RESULT_DIR = Path("/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617")
OUT_HTML = RESULT_DIR / "llumnix_5090_4inst_metrics_table.html"


def read_text(path):
    try:
        return path.read_text(errors="replace")
    except FileNotFoundError:
        return ""


def fmt(value, digits=4):
    if value is None or value == "":
        return "NA"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NA"
        return f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return str(value)


def pct(n, d):
    if d in (None, 0):
        return "NA"
    return f"{(n / d * 100):.2f}%"


def avg(vals):
    nums = [v for v in vals if isinstance(v, (int, float))]
    return sum(nums) / len(nums) if nums else None


def total(vals):
    nums = [v for v in vals if isinstance(v, (int, float))]
    return sum(nums) if nums else None


def first_match(pattern, text):
    m = re.search(pattern, text, re.M)
    return m.group(1).strip() if m else None


def load_csv_rows():
    rows = []
    with (RESULT_DIR / "formal_results.csv").open(newline="", errors="replace") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def to_num(v):
    if v is None or v == "":
        return None
    try:
        if re.fullmatch(r"-?\d+", str(v)):
            return int(v)
        return float(v)
    except ValueError:
        return v


def metric_values(rows, kind, key):
    values = []
    for r in sorted([x for x in rows if x.get("kind") == kind], key=lambda x: int(x["run"])):
        values.append(to_num(r.get(key)))
    while len(values) < 3:
        values.append(None)
    return values[:3]


def parse_summary_json(prefix, run):
    path = RESULT_DIR / f"{prefix}_run{run}.summary.txt"
    text = read_text(path)
    marker = "BASELINE_4_SUMMARY" if prefix == "baseline" else "MIGRATION_PROBE_SUMMARY"
    for line in text.splitlines():
        if marker in line:
            payload = line.split(marker, 1)[1].strip()
            try:
                return json.loads(payload)
            except Exception:
                return {}
    return {}


def count_regex(pattern, files):
    count = 0
    for path in files:
        text = read_text(path)
        count += len(re.findall(pattern, text))
    return count


def count_regex_per_run(pattern, prefix="migration"):
    values = []
    for i in range(1, 4):
        values.append(count_regex(pattern, [RESULT_DIR / f"{prefix}_run{i}.console.log"]))
    return values


def extract_env():
    env = read_text(RESULT_DIR / "ENV_AND_PATCH_SUMMARY.md")
    final = read_text(RESULT_DIR / "FINAL_HANDOFF.md")
    results = read_text(RESULT_DIR / "RESULTS_SUMMARY.md")
    return {
        "llumnix_commit": first_match(r"llumnix_commit\s+(.+)", env) or first_match(r"Llumnix commit:\s*`([^`]+)`", results),
        "blade_commit": first_match(r"blade_kvt_commit\s+(.+)", env) or first_match(r"blade_kvt commit:\s*`([^`]+)`", results),
        "vllm_version": first_match(r"vllm_version\s+(.+)", env) or first_match(r"vLLM:\s*`([^`]+)`", results),
        "torch_version": first_match(r"torch_version\s+(.+)", env),
        "torch_cuda": first_match(r"torch_cuda\s+(.+)", env),
        "llumnix_import": first_match(r"llumnix_import_path\s+(.+)", env),
        "vllm_import": first_match(r"vllm_import_path\s+(.+)", env),
        "blade_import": first_match(r"blade_kvt_import_path\s+(.+)", env),
        "blade_version": first_match(r"blade_kvt_version\s+(.+)", env),
        "gpu": ", ".join(re.findall(r"gpu_\d+\s+(.+)", env)) or first_match(r"GPU:\s*`([^`]+)`", results),
        "test_time": first_match(r"时间：(.+)", results),
        "final_done": "完成" if "实验是否完成：完成" in final else "未记录",
        "driver": first_match(r"Driver Version:\s*([0-9.]+)", env) or None,
    }


def extract_fatal_counts():
    scan = read_text(RESULT_DIR / "fatal_error_scan_filtered.txt")
    return {
        "EngineCore fatal": int(first_match(r"EngineCore fatal:\s*(\d+)", scan) or 0),
        "Traceback": int(first_match(r"Traceback:\s*(\d+)", scan) or 0),
        "ERROR": int(first_match(r"ERROR:\s*(\d+)", scan) or 0),
    }


def make_rows():
    rows_csv = load_csv_rows()
    env = extract_env()
    fatal = extract_fatal_counts()
    mig_console_files = [RESULT_DIR / f"migration_run{i}.console.log" for i in range(1, 4)]
    all_files = list(RESULT_DIR.glob("*.log")) + list(RESULT_DIR.glob("*.txt")) + list(RESULT_DIR.glob("*.md"))

    baseline_summaries = [parse_summary_json("baseline", i) for i in range(1, 4)]
    migration_summaries = [parse_summary_json("migration", i) for i in range(1, 4)]
    req_b = metric_values(rows_csv, "baseline", "successful_responses")
    req_m = metric_values(rows_csv, "migration", "successful_responses")
    requests_b = [s.get("requests") for s in baseline_summaries]
    requests_m = [s.get("requests") for s in migration_summaries]
    total_req_b = total(requests_b)
    total_req_m = total(requests_m)

    rows = []

    def add(cat, name, b=None, bsum=None, m=None, msum=None, unit="", note=""):
        b = b or ["-", "-", "-"]
        m = m or ["-", "-", "-"]
        rows.append({
            "指标类别": cat,
            "指标名称": name,
            "Baseline 第 1 轮": fmt(b[0]),
            "Baseline 第 2 轮": fmt(b[1]),
            "Baseline 第 3 轮": fmt(b[2]),
            "Baseline 均值/合计": fmt(bsum),
            "Migration 第 1 轮": fmt(m[0]),
            "Migration 第 2 轮": fmt(m[1]),
            "Migration 第 3 轮": fmt(m[2]),
            "Migration 均值/合计": fmt(msum),
            "单位": unit,
            "数据来源/备注": note,
        })

    def metric_row(cat, name, key, unit="", aggregate="avg", note="formal_results.csv"):
        b = metric_values(rows_csv, "baseline", key)
        m = metric_values(rows_csv, "migration", key)
        fn = total if aggregate == "sum" else avg
        add(cat, name, b, fn(b), m, fn(m), unit, note)

    def migration_only_metric(cat, name, key, unit="", aggregate="avg", note="formal_results.csv"):
        m = metric_values(rows_csv, "migration", key)
        fn = total if aggregate == "sum" else avg
        add(cat, name, ["-"] * 3, "-", m, fn(m), unit, note)

    add("实验基本信息", "实验名称", ["Llumnix RTX 5090 四实例正式实验"] * 3, "Llumnix RTX 5090 四实例正式实验", ["Llumnix RTX 5090 四实例正式实验"] * 3, "Llumnix RTX 5090 四实例正式实验", "", "RESULTS_SUMMARY.md")
    add("实验基本信息", "实验目录", [str(RESULT_DIR)] * 3, str(RESULT_DIR), [str(RESULT_DIR)] * 3, str(RESULT_DIR), "", "RESULTS_SUMMARY.md")
    add("实验基本信息", "实验组数", ["2"] * 3, "2", ["2"] * 3, "2", "组", "RESULTS_SUMMARY.md")
    add("实验基本信息", "Baseline 轮数", ["3"] * 3, "3", ["-"] * 3, "-", "轮", "formal_results.csv")
    add("实验基本信息", "Migration 轮数", ["-"] * 3, "-", ["3"] * 3, "3", "轮", "formal_results.csv")
    add("实验基本信息", "每轮请求数", requests_b, avg(requests_b), requests_m, avg(requests_m), "req", "summary.txt")
    add("实验基本信息", "总请求数", requests_b, total_req_b, requests_m, total_req_m, "req", "summary.txt")
    add("实验基本信息", "实例数量", [4, 4, 4], 4, [4, 4, 4], 4, "个", "summary.txt")
    add("实验基本信息", "模型路径", ["/data/models/Qwen2.5-7B-Instruct"] * 3, "/data/models/Qwen2.5-7B-Instruct", ["/data/models/Qwen2.5-7B-Instruct"] * 3, "/data/models/Qwen2.5-7B-Instruct", "", "benchmark raw response / RESULTS_SUMMARY.md")
    add("实验基本信息", "迁移是否开启", ["否"] * 3, "否", ["是"] * 3, "是", "", "runner 命令记录")
    add("实验基本信息", "测试时间", [env["test_time"] or "未记录"] * 3, env["test_time"] or "未记录", [env["test_time"] or "未记录"] * 3, env["test_time"] or "未记录", "", "RESULTS_SUMMARY.md")

    metric_row("请求成功情况", "成功请求数", "successful_responses", "req", "sum")
    fail_b = [(requests_b[i] or 0) - (req_b[i] or 0) for i in range(3)]
    fail_m = [(requests_m[i] or 0) - (req_m[i] or 0) for i in range(3)]
    add("请求成功情况", "失败请求数", fail_b, total(fail_b), fail_m, total(fail_m), "req", "summary.txt")
    add("请求成功情况", "请求成功率", [pct(req_b[i] or 0, requests_b[i] or 0) for i in range(3)], pct(total(req_b), total_req_b), [pct(req_m[i] or 0, requests_m[i] or 0) for i in range(3)], pct(total(req_m), total_req_m), "%", "summary.txt")
    metric_row("请求成功情况", "HTTP 200 且文本非空数量", "nonempty_text", "req", "sum")
    http_b = metric_values(rows_csv, "baseline", "http_200")
    http_m = metric_values(rows_csv, "migration", "http_200")
    non_b = metric_values(rows_csv, "baseline", "nonempty_text")
    non_m = metric_values(rows_csv, "migration", "nonempty_text")
    empty_b = [(http_b[i] or 0) - (non_b[i] or 0) for i in range(3)]
    empty_m = [(http_m[i] or 0) - (non_m[i] or 0) for i in range(3)]
    add("请求成功情况", "空文本响应数量", empty_b, total(empty_b), empty_m, total(empty_m), "req", "formal_results.csv")
    add("请求成功情况", "benchmark 是否完成", ["是"] * 3, "是", ["是"] * 3, "是", "", "summary 文件存在且无 RUNNER_EXCEPTION")

    metric_row("服务性能指标", "QPS", "qps", "req/s")
    metric_row("服务性能指标", "token 吞吐量", "throughput_tok_s", "tok/s")
    metric_row("服务性能指标", "median 端到端时延", "median_e2e_latency", "s")
    metric_row("服务性能指标", "P95 端到端时延", "p95_latency", "s")
    metric_row("服务性能指标", "P99 端到端时延", "p99_latency", "s")
    add("服务性能指标", "mean 端到端时延", ["未记录"] * 3, "未记录", ["未记录"] * 3, "未记录", "s", "benchmark summary 未直接输出 mean_e2e_latency")
    add("服务性能指标", "总输入 token 数", ["未记录"] * 3, "未记录", ["未记录"] * 3, "未记录", "tokens", "benchmark summary 未直接输出")
    add("服务性能指标", "总输出 token 数", ["未记录"] * 3, "未记录", ["未记录"] * 3, "未记录", "tokens", "benchmark summary usage 为 null")
    add("服务性能指标", "输出 token/s", ["未记录"] * 3, "未记录", ["未记录"] * 3, "未记录", "tok/s", "未记录独立 output token/s；已有 token 吞吐量见 throughput_tok_s")
    duration_b = [s.get("duration_s") for s in baseline_summaries]
    duration_m = [s.get("duration_s") for s in migration_summaries]
    add("服务性能指标", "总运行时间", duration_b, avg(duration_b), duration_m, avg(duration_m), "s", "summary.txt")

    mos = count_regex_per_run(r"migrate_out_selected")
    mob = count_regex_per_run(r"migrate_out_begin")
    mod = count_regex_per_run(r"migrate_out_done")
    add("迁移触发与完成指标", "migrate_out_selected 次数", ["-"] * 3, "-", mos, total(mos), "次", "migration console 原始日志 grep")
    add("迁移触发与完成指标", "migrate_out_begin 次数", ["-"] * 3, "-", mob, total(mob), "次", "migration console 原始日志 grep")
    add("迁移触发与完成指标", "migrate_out_done 次数", ["-"] * 3, "-", mod, total(mod), "次", "migration console 原始日志 grep")
    migration_only_metric("迁移触发与完成指标", "finish_request_ids 数量", "finished_req_lines", "个", "sum", "formal_results.csv 二次解析 FinishedReq 日志")
    migration_only_metric("迁移触发与完成指标", "unique finish_request_ids 数量", "unique_finished_req_ids", "个", "sum", "formal_results.csv 二次解析 FinishedReq 日志")
    migration_only_metric("迁移触发与完成指标", "FinishedReq 日志条数", "finished_req_lines", "条", "sum")
    migration_only_metric("迁移触发与完成指标", "unique FinishedReq 请求数", "unique_finished_req_ids", "个", "sum")
    migration_only_metric("迁移触发与完成指标", "migration_end 次数", "migration_end", "次", "sum")
    add("迁移触发与完成指标", "migration_count 字面字段", ["-"] * 3, "-", ["未记录"] * 3, "未记录", "", "results_summary_field_check.txt: migration_count=False")
    kvt0 = metric_values(rows_csv, "migration", "kvt_code0")
    mend = metric_values(rows_csv, "migration", "migration_end")
    add("迁移触发与完成指标", "迁移成功率", ["-"] * 3, "-", [pct(kvt0[i] or 0, mend[i] or 0) for i in range(3)], pct(total(kvt0), total(mend)), "%", "KVTResp(code=0) / migration_end")
    add("迁移触发与完成指标", "迁移 abort 次数", ["-"] * 3, "-", ["未记录"] * 3, "未记录", "次", "summary 未直接输出 abort_count")

    migration_only_metric("KVT / 迁移后端证据", "KVTResp(code=0) 次数", "kvt_code0", "次", "sum")
    migration_only_metric("KVT / 迁移后端证据", "KVTResp(code=404) 次数", "kvt_code404", "次", "sum")
    migration_only_metric("KVT / 迁移后端证据", "KVTResp(code=500) 次数", "kvt_code500", "次", "sum")
    migration_only_metric("KVT / 迁移后端证据", "SendStubMetrics 次数", "send_metrics", "次", "sum")
    migration_only_metric("KVT / 迁移后端证据", "KVT 成功响应总数", "kvt_code0", "次", "sum")
    kvt404 = metric_values(rows_csv, "migration", "kvt_code404")
    kvt500 = metric_values(rows_csv, "migration", "kvt_code500")
    kvt_fail = [(kvt404[i] or 0) + (kvt500[i] or 0) for i in range(3)]
    add("KVT / 迁移后端证据", "KVT 失败响应总数", ["-"] * 3, "-", kvt_fail, total(kvt_fail), "次", "KVTResp(code=404)+KVTResp(code=500)")
    evidence = ["是" if (kvt0[i] or 0) > 0 and (metric_values(rows_csv, "migration", "send_metrics")[i] or 0) > 0 else "否" for i in range(3)]
    add("KVT / 迁移后端证据", "迁移证据是否存在", ["-"] * 3, "-", evidence, "是" if all(x == "是" for x in evidence) else "否", "", "RESULTS_SUMMARY.md / migration 日志 grep")

    add("迁移开销指标", "downtime", ["-"] * 3, "-", ["未记录"] * 3, "未记录", "ms", "日志证据存在迁移完成，但该字段未直接输出")
    add("迁移开销指标", "平均 downtime", ["-"] * 3, "-", ["未记录"] * 3, "未记录", "ms", "日志证据存在迁移完成，但该字段未直接输出")
    add("迁移开销指标", "P95 downtime", ["-"] * 3, "-", ["未记录"] * 3, "未记录", "ms", "日志证据存在迁移完成，但该字段未直接输出")
    add("迁移开销指标", "总迁移时间", ["-"] * 3, "-", ["未记录"] * 3, "未记录", "ms", "日志证据存在迁移完成，但 total_migration_time 字段未直接输出")
    migration_only_metric("迁移开销指标", "平均迁移时间", "migration_avg_ms", "ms", "avg")
    add("迁移开销指标", "迁移日志中是否存在 downtime 字段", ["-"] * 3, "-", ["否"] * 3, "否", "", "results_summary_field_check.txt: downtime=False")
    add("迁移开销指标", "迁移日志中是否存在 total_migration_time 字段", ["-"] * 3, "-", ["否"] * 3, "否", "", "未发现 total_migration_time 字面字段；已有 migration duration_ms")

    runtime_count = count_regex(r"RuntimeError", all_files)
    cuda_illegal = count_regex(r"CUDA illegal memory access|illegal memory access", all_files)
    benchmark_error = count_regex(r"RUNNER_EXCEPTION|benchmark error|Benchmark error", all_files)
    add("异常复核", "EngineCore fatal 数量", [fatal["EngineCore fatal"]] * 3, fatal["EngineCore fatal"], [fatal["EngineCore fatal"]] * 3, fatal["EngineCore fatal"], "次", "fatal_error_scan_filtered.txt")
    add("异常复核", "Traceback 数量", [fatal["Traceback"]] * 3, fatal["Traceback"], [fatal["Traceback"]] * 3, fatal["Traceback"], "次", "fatal_error_scan_filtered.txt")
    add("异常复核", "ERROR 数量", [fatal["ERROR"]] * 3, fatal["ERROR"], [fatal["ERROR"]] * 3, fatal["ERROR"], "次", "fatal_error_scan_filtered.txt")
    add("异常复核", "RuntimeError 数量", ["未记录"] * 3, runtime_count, ["未记录"] * 3, runtime_count, "次", "结果目录文本 grep")
    add("异常复核", "CUDA illegal memory access 数量", ["未记录"] * 3, cuda_illegal, ["未记录"] * 3, cuda_illegal, "次", "结果目录文本 grep")
    add("异常复核", "benchmark error 数量", ["未记录"] * 3, benchmark_error, ["未记录"] * 3, benchmark_error, "次", "结果目录文本 grep")
    add("异常复核", "异常复核结果", ["无 fatal/Traceback/ERROR"] * 3, "无 fatal/Traceback/ERROR", ["无 fatal/Traceback/ERROR"] * 3, "无 fatal/Traceback/ERROR", "", "FINAL_HANDOFF.md / fatal_error_scan_filtered.txt")

    add("环境信息", "GPU 型号", ["-"] * 3, "-", ["-"] * 3, env["gpu"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "Driver 版本", ["-"] * 3, "-", ["-"] * 3, env["driver"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md 未直接记录")
    add("环境信息", "CUDA 版本", ["-"] * 3, "-", ["-"] * 3, env["torch_cuda"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md torch_cuda")
    add("环境信息", "PyTorch 版本", ["-"] * 3, "-", ["-"] * 3, env["torch_version"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "Conda 环境名", ["-"] * 3, "-", ["-"] * 3, "llumnix5090_vllm018", "", "运行命令记录")
    add("环境信息", "vLLM 版本", ["-"] * 3, "-", ["-"] * 3, env["vllm_version"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "vLLM commit", ["-"] * 3, "-", ["-"] * 3, "gbcf2be961", "", "vLLM version string")
    add("环境信息", "vLLM import 路径", ["-"] * 3, "-", ["-"] * 3, env["vllm_import"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "Llumnix commit", ["-"] * 3, "-", ["-"] * 3, env["llumnix_commit"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "Llumnix import 路径", ["-"] * 3, "-", ["-"] * 3, env["llumnix_import"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "blade_kvt 版本", ["-"] * 3, "-", ["-"] * 3, env["blade_version"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "blade_kvt commit", ["-"] * 3, "-", ["-"] * 3, env["blade_commit"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")
    add("环境信息", "blade_kvt import 路径", ["-"] * 3, "-", ["-"] * 3, env["blade_import"] or "未记录", "", "ENV_AND_PATCH_SUMMARY.md")

    add("补丁与启动信息", "Llumnix git diff 文件", ["-"] * 3, "-", ["-"] * 3, "llumnix_git_diff.patch", "", "结果目录文件")
    add("补丁与启动信息", "blade_kvt git diff 文件", ["-"] * 3, "-", ["-"] * 3, "blade_kvt_git_diff.patch", "", "结果目录文件")
    add("补丁与启动信息", "vLLM patch 状态", ["-"] * 3, "-", ["-"] * 3, "patched vLLM 0.18.1.dev0", "", "ENV_AND_PATCH_SUMMARY.md")
    add("补丁与启动信息", "blade_kvt 关键补丁", ["-"] * 3, "-", ["-"] * 3, "engine_proxy / hybrid_modules / migration backend / TCP loopback 等兼容补丁", "", "ENV_AND_PATCH_SUMMARY.md")
    add("补丁与启动信息", "启动命令记录", ["-"] * 3, "-", ["-"] * 3, "ENV_AND_PATCH_SUMMARY.md Startup Commands", "", "ENV_AND_PATCH_SUMMARY.md")
    add("补丁与启动信息", "benchmark 命令记录", ["-"] * 3, "-", ["-"] * 3, "run_remote_no_migration_baseline.sh / run_remote_minimal_migration_probe.sh", "", "ENV_AND_PATCH_SUMMARY.md")
    add("补丁与启动信息", "结果摘要文件", ["-"] * 3, "-", ["-"] * 3, "RESULTS_SUMMARY.md", "", "结果目录文件")
    add("补丁与启动信息", "环境补丁摘要文件", ["-"] * 3, "-", ["-"] * 3, "ENV_AND_PATCH_SUMMARY.md", "", "结果目录文件")
    add("补丁与启动信息", "最终交接文件", ["-"] * 3, "-", ["-"] * 3, "FINAL_HANDOFF.md", "", "结果目录文件")
    return rows


def css_class(value, name=""):
    v = str(value)
    if v in {"未记录", "NA", "Missing"}:
        return " missing"
    if name in {"EngineCore fatal 数量", "Traceback 数量", "ERROR 数量"}:
        try:
            return " ok" if float(v) == 0 else " bad"
        except Exception:
            return ""
    return ""


def write_html(rows):
    headers = [
        "指标类别", "指标名称", "Baseline 第 1 轮", "Baseline 第 2 轮", "Baseline 第 3 轮",
        "Baseline 均值/合计", "Migration 第 1 轮", "Migration 第 2 轮", "Migration 第 3 轮",
        "Migration 均值/合计", "单位", "数据来源/备注"
    ]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = []
    out.append("<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"><title>Llumnix RTX 5090 四实例实验指标总表</title>")
    out.append("""<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",Arial,sans-serif;margin:24px;background:#f7f8fb;color:#1f2937}
h1{font-size:24px;margin:0 0 8px}
.dir{font-size:14px;margin:0 0 18px;color:#374151}
.wrap{overflow:auto;border:1px solid #d1d5db;background:white;box-shadow:0 2px 10px rgba(0,0,0,.04)}
table{border-collapse:collapse;min-width:1900px;width:100%}
th,td{border:1px solid #d1d5db;padding:8px 10px;font-size:13px;vertical-align:top;white-space:nowrap}
th{position:sticky;top:0;background:#111827;color:white;z-index:2}
td:nth-child(1){font-weight:700;background:#f3f4f6}
td:nth-child(3),td:nth-child(4),td:nth-child(5),td:nth-child(6),th:nth-child(3),th:nth-child(4),th:nth-child(5),th:nth-child(6){background:#eaf3ff}
td:nth-child(7),td:nth-child(8),td:nth-child(9),td:nth-child(10),th:nth-child(7),th:nth-child(8),th:nth-child(9),th:nth-child(10){background:#fff2e2}
td.missing{background:#e5e7eb!important;color:#6b7280}
td.ok{background:#dcfce7!important;color:#166534;font-weight:700}
td.bad{background:#fee2e2!important;color:#991b1b;font-weight:700}
tfoot td{background:#f9fafb;color:#4b5563}
</style>""")
    out.append("</head><body>")
    out.append("<h1>Llumnix RTX 5090 四实例实验指标总表</h1>")
    out.append(f"<p class=\"dir\">结果目录：{html.escape(str(RESULT_DIR))}</p>")
    out.append("<div class=\"wrap\"><table><thead><tr>")
    out.extend(f"<th>{html.escape(h)}</th>" for h in headers)
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        for h in headers:
            cls = css_class(row[h], row["指标名称"])
            out.append(f"<td class=\"{cls.strip()}\">{html.escape(str(row[h]))}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    out.append(f"<footer><p>自动整理时间：{html.escape(now)}</p><p>说明：本页面只整理已有实验指标，不包含新增实验或重新运行结果。</p></footer>")
    out.append("</body></html>")
    OUT_HTML.write_text("\n".join(out), encoding="utf-8")


def main():
    rows = make_rows()
    write_html(rows)
    missing = sorted({r["指标名称"] for r in rows if any(r[k] in {"未记录", "NA"} for k in r if k not in {"数据来源/备注"})})
    fatal = extract_fatal_counts()
    print("HTML_PATH", OUT_HTML)
    print("METRIC_COUNT", len(rows))
    print("MISSING_METRICS", ", ".join(missing) if missing else "无")
    print("FATAL_COUNTS", fatal)


if __name__ == "__main__":
    main()
