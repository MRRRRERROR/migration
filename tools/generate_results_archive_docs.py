from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / "results"
ARCHIVE = ROOT / "results_归档"
INDEX = ARCHIVE / "00_归档索引"


SERVER_ROWS = [
    ("5090_4inst_formal_20260721_014617", "2026-07-21 16:35:44", 3001553, "—"),
    ("5090_baselines_20260721_172757", "2026-07-21 18:39:28", 362732053, "trace/jsonl, scheduler log, runtime log"),
    ("5090_load_sweep_20260721_191608", "2026-07-21 19:19:36", 17597454, "run_summary, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime/KVT log"),
    ("5090_migration_baselines_20260819_194734", "2026-08-19 19:47:34", 70742, "04_step_summaries, 05_final"),
    ("5090_migration_efficiency_20260721_151416", "2026-07-21 15:14:16", 5725975, "—"),
    ("5090_migration_efficiency_20260721_151901", "2026-07-21 15:19:01", 5726221, "—"),
    ("5090_migration_efficiency_20260721_153754", "2026-07-21 15:37:55", 5726221, "—"),
    ("5090_migration_policy_baselines_20260819_201922", "2026-08-20 13:09:07", 4469047832, "run_summary, aggregated, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_multi_baseline_500req_20260721_204013", "2026-07-21 21:20:01", 490975180, "run_summary, aggregated, 04_step_summaries, 05_final, trace/jsonl, runner/scheduler/runtime log"),
    ("5090_multi_baseline_500req_20260819_143158", "2026-08-19 15:19:43", 34190059, "run_summary, aggregated, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_multi_baseline_500req_20260819_153800", "2026-08-19 19:22:44", 2254957239, "run_summary, aggregated, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_multi_baseline_readiness_smoke_20260819", "2026-08-19 15:10:26", 3097019, "run_summary, aggregated, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_multi_baseline_readiness_smoke_gpu055_20260819", "2026-08-19 15:37:16", 3102937, "run_summary, aggregated, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_multi_baseline_smoke_20260819_141612", "2026-08-19 14:16:18", 5189505, "run_summary, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_multi_baseline_smoke_20260819_142706", "2026-08-19 14:27:10", 5698702, "run_summary, 04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("5090_paper_supplement_20260821_170651", "2026-08-23 17:33:04", 4449080525, "run_summary, aggregated, 04_step_summaries, 05_final, migration events, trace/jsonl, runner/scheduler/runtime/KVT log"),
    ("backups_phase2_", "2026-07-21 15:43:42", 20094, "—"),
    ("backups_phase2_20260721_154415", "2026-07-21 15:44:15", 20094, "—"),
    ("baseline_no_migration_20260721_004024", "2026-07-21 00:42:20", 7711804, "runtime log"),
    ("baseline_no_migration_20260721_014624", "2026-07-21 01:47:21", 4915358, "runtime log"),
    ("baseline_no_migration_20260721_014733", "2026-07-21 01:48:31", 4919008, "runtime log"),
    ("baseline_no_migration_20260721_014842", "2026-07-21 01:49:40", 4936782, "runtime log"),
    ("complex_mixed_high_load", "2026-09-17 18:57:09", 13845202092, "run_summary, aggregated, migration events, trace/jsonl, runner/scheduler/runtime log"),
    ("full_data_export_20260824_151951", "2026-08-24 15:21:00", 8266061373, "run_summary, aggregated, trace/jsonl, runner/scheduler/runtime/KVT log"),
    ("manual_analysis_20260824_145157", "2026-08-24 15:05:02", 1644525, "aggregated"),
    ("minimal_migration_probe_20260721_004501", "2026-07-21 00:46:16", 10553274, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_005007", "2026-07-21 00:51:25", 10818185, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_010357", "2026-07-21 01:05:15", 11032301, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_010637", "2026-07-21 01:17:53", 70173510, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_012823", "2026-07-21 01:29:42", 11348115, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_013838", "2026-07-21 01:40:04", 18081076, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_014952", "2026-07-21 01:51:25", 46823209, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_015137", "2026-07-21 01:53:07", 51151706, "scheduler/runtime log"),
    ("minimal_migration_probe_20260721_015319", "2026-07-21 01:54:51", 52140007, "scheduler/runtime log"),
    ("ordinary_multireq_stability_20260721_003355", "2026-07-21 00:34:58", 2188154, "runtime log"),
    ("trace_downtime_slo_smoke_20260904_162214", "2026-09-04 16:24:14", 8948814, "04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
    ("trace_downtime_slo_smoke_20260904_162645", "2026-09-04 16:34:58", 18929290, "04_step_summaries, 05_final, trace/jsonl, scheduler/runtime log"),
]


def size_text(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= 1024
    raise AssertionError


def scan(path: Path) -> tuple[int, int, float, list[str]]:
    files = [p for p in path.rglob("*") if p.is_file()]
    size = sum(p.stat().st_size for p in files)
    mtime = max((p.stat().st_mtime for p in files), default=path.stat().st_mtime)
    names = [p.name.lower() for p in files]
    dirs = {p.name for p in path.rglob("*") if p.is_dir()}
    flags: list[str] = []
    if "run_summary.json" in names:
        flags.append("run_summary")
    if any("aggregated" in n and n.endswith((".json", ".csv")) for n in names):
        flags.append("aggregated json/csv")
    if "04_step_summaries" in dirs:
        flags.append("04_step_summaries")
    if "05_final" in dirs:
        flags.append("05_final")
    if "migration_efficiency_events.jsonl" in names:
        flags.append("migration events")
    if any("trace" in n or n.endswith(".jsonl") for n in names):
        flags.append("trace/jsonl")
    for label, needle in (("runner log", "runner"), ("scheduler log", "scheduler"), ("runtime log", "runtime")):
        if any(needle in n and n.endswith(".log") for n in names):
            flags.append(label)
    if any(("kvt" in n or "migserver" in n) and n.endswith(".log") for n in names):
        flags.append("KVT/MigServer log")
    return len(files), size, mtime, flags


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def inventory_docs() -> None:
    server = [
        "# 服务器实验结果盘点",
        "",
        "- 扫描位置：`/data/home/zylu/llumnix-v018-main/results`",
        f"- 顶层实验目录：{len(SERVER_ROWS)} 个",
        "- 扫描方式：只读 SSH；大小为服务器文件字节总量。",
        "",
        "| 顶层目录 | 修改时间 | 总大小 | 检测到的关键内容 |",
        "|---|---|---:|---|",
    ]
    server.extend(f"| `{n}` | {m} | {size_text(s)} | {f} |" for n, m, s, f in SERVER_ROWS)
    write(INDEX / "SERVER_RESULTS_INVENTORY.md", "\n".join(server))

    local = [
        "# 本机实验结果盘点",
        "",
        f"- 扫描位置：`{LOCAL}`",
        f"- 顶层实验目录：{sum(1 for p in LOCAL.iterdir() if p.is_dir())} 个",
        "- 本表只统计原始 `results`，不把新建的 `results_归档` 重复计入。",
        "",
        "| 顶层目录 | 修改时间 | 总大小 | 文件数 | 检测到的关键内容 |",
        "|---|---|---:|---:|---|",
    ]
    for p in sorted((p for p in LOCAL.iterdir() if p.is_dir()), key=lambda x: x.name):
        count, size, mtime, flags = scan(p)
        local.append(f"| `{p.name}` | {datetime.fromtimestamp(mtime):%Y-%m-%d %H:%M:%S} | {size_text(size)} | {count} | {', '.join(flags) or '—'} |")
    local += [
        "",
        "## 顶层结果文件",
        "",
        f"- `fig12_v2_load_sr_retry_once_summary.json`（{size_text((LOCAL / 'fig12_v2_load_sr_retry_once_summary.json').stat().st_size)}）",
        f"- `full_data_export_20260824_151951.tar.gz`（{size_text((LOCAL / 'full_data_export_20260824_151951.tar.gz').stat().st_size)}）",
        "",
        "## 其他已发现的本机历史目录",
        "",
        "- `downloaded_results_0721/5090_4inst_formal_20260721_014617`：7 月正式迁移结果的历史下载副本；归档时已用服务器缺少的同目录文件补齐。",
    ]
    write(INDEX / "LOCAL_RESULTS_INVENTORY.md", "\n".join(local))


EXPERIMENTS = [
    ("2026-07-21", "2026-07-21_四实例正式迁移验证", "results/5090_4inst_formal_20260721_014617", "downloaded_results_0721/5090_4inst_formal_20260721_014617", "已完整归档", "机制验证", "是"),
    ("2026-07-21", "2026-07-21_迁移效率_Figure10一期", "results/5090_migration_efficiency_20260721_151901", "无", "已归档关键结果", "正式结果", "按服务器现存目录完整"),
    ("2026-07-21", "2026-07-21_公平Baseline_迁移开销对比", "results/5090_baselines_20260721_172757", "无", "已完整归档", "baseline", "是"),
    ("2026-07-21", "2026-07-21_500请求多Baseline", "results/5090_multi_baseline_500req_20260721_204013", "无", "已归档关键结果", "baseline", "否（传输中止，约 150.05 MB/490.98 MB）"),
    ("2026-08-19", "2026-08-19_迁移策略主实验_54轮", "results/5090_migration_policy_baselines_20260819_201922", "results/5090_migration_policy_baselines_20260819_201922", "本机已有", "正式结果", "是（1098 文件，字节数与服务器一致）"),
    ("2026-08-19", "2026-08-19_500请求多基线实验", "results/5090_multi_baseline_500req_20260819_153800", "results/5090_multi_baseline_500req_20260819_153800", "仅 summary", "baseline", "否"),
    ("2026-08-21", "2026-08-21_论文补充实验", "results/5090_paper_supplement_20260821_170651", "results/5090_paper_supplement_20260821_170651", "仅 summary", "正式结果", "否"),
    ("2026-08-24", "2026-08-24_Figure12-v2_强制碎片化实验", "results/5090_paper_supplement_20260821_170651/02_fig12_fragmentation_v2", "results/5090_paper_supplement_20260821_170651（本机摘要子集）", "已归档关键结果", "正式结果", "否（摘要和 smoke 子集）"),
    ("2026-08-24", "2026-08-24_Figure12-v2_LoadSR重试汇总", "同上；重试汇总为本机独立文件", "results/fig12_v2_load_sr_retry_once_summary.json", "仅 summary", "failure diagnosis", "否"),
    ("2026-08-24", "2026-08-24_实验全量数据导出", "results/full_data_export_20260824_151951", "results/full_data_export_20260824_151951；results/full_data_export_20260824_151951.tar.gz", "本机已有", "正式结果", "是（本机目录及 tar）"),
    ("2026-08-31 至 2026-09-03", "2026-08-31至09-03_WSL模拟器实验", "服务器 results 顶层未发现同名目录", "results/wsl_sim_*", "本机已有", "simulator", "按本机现有数据完整"),
    ("2026-09-04", "2026-09-04_停机时间与SLO验证", "results/trace_downtime_slo_smoke_20260904_162645", "results/trace_downtime_slo_smoke_20260904_162645", "本机已有", "机制验证", "是（本机文件更多）"),
    ("2026-09-05", "01_ConfigD_C80_高压稳定性压力测试", "results/complex_mixed_high_load/config_d/run_20260905_153725", "同服务器相对路径", "本机已有", "stress test", "是（484 文件，字节数一致）"),
    ("2026-09-13", "02_ConfigE_C40_Smoke", "results/complex_mixed_high_load/config_e/run_20260913_185344", "同服务器相对路径", "本机已有", "smoke", "是（67 文件，字节数一致）"),
    ("2026-09-14", "03_ConfigE_C32_Smoke", "results/complex_mixed_high_load/config_e_c32/run_20260914_150539", "同服务器相对路径", "本机已有", "smoke", "是（33 文件，字节数一致）"),
    ("2026-09-17", "04_ConfigE_C32_KVT500诊断复现", "results/complex_mixed_high_load/config_e_c32_diag/run_20260917_185607", "同服务器相对路径（本机仅诊断包）", "已归档关键结果", "failure diagnosis", "否（本机 20.72 MB；服务器 441.81 MB）"),
]


def index_docs() -> None:
    lines = [
        "# Llumnix 实验归档总表",
        "",
        "本次按用户最新要求停止补全，只整理本机已有数据以及停止前已经传完的目录；未完成传输不删除，并明确标记完整性。服务器原始结果全程只读。",
        "",
        "| 日期 | 中文实验名 | 服务器原始路径 | 本机原始路径 | 归档路径 | 状态 | 用途 | 是否完整 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for date, name, server, local, status, purpose, complete in EXPERIMENTS:
        if name.startswith(("01_", "02_", "03_", "04_")):
            archive_path = f"results_归档/2026-09-05至09-17_复杂混合高负载_ConfigD-E/{name}"
        else:
            archive_path = f"results_归档/{name}"
        lines.append(f"| {date} | {name} | `{server}` | `{local}` | `{archive_path}` | {status} | {purpose} | {complete} |")
    lines += [
        "",
        "## 完整性口径",
        "",
        "- “字节数一致”表示归档前本机目录的文件数和文件总字节数与服务器只读盘点一致。",
        "- “仅 summary”表示保留汇总/最终表，但没有本轮继续拉取服务器 raw runs 和完整日志。",
        "- 7 月 500 请求实验在用户要求停止补全时已留下 59 个文件、150.05 MB，原目录未删除。",
    ]
    write(INDEX / "EXPERIMENT_ARCHIVE_INDEX.md", "\n".join(lines))

    missing = """# 缺失或未补全实验清单

按最新要求，本次不再补全服务器数据；下列记录用于以后追溯。

| 重点实验 | 服务器检查结果 | 当前本机/归档状态 |
|---|---|---|
| `5090_4inst_formal_20260721_014617` | 已发现，3.00 MB | 已完整归档（25 文件） |
| `5090_migration_efficiency_20260721_151901` | 已发现，5.73 MB | 已按服务器现存目录完整归档 |
| `5090_baselines_20260721_172757` | 已发现，362.73 MB | 已完整归档 |
| `5090_multi_baseline_500req_20260721_204013` | 已发现，490.98 MB | 传输中止；现有 59 文件、150.05 MB，保留为部分归档 |
| Figure12 v2 完整原始目录 | 顶层没有独立目录；位于 `5090_paper_supplement_20260821_170651/02_fig12_fragmentation_v2` | 本机有汇总、smoke 子集和全量导出中的 run summaries/logs；未补全服务器原始目录 |

## 服务器存在但按要求未继续补全

- `5090_multi_baseline_500req_20260819_153800`：服务器 2.25 GB；本机和归档仅 summary（约 811.53 KB）。
- `5090_paper_supplement_20260821_170651`：服务器 4.45 GB；本机和归档仅摘要子集（约 359.94 KB）。
- `complex_mixed_high_load/config_e_c32_diag/run_20260917_185607`：服务器 441.81 MB；本机和归档为 20.72 MB 诊断包，完整 runtime/KVT/MigServer 日志未补齐。

这些条目均不是“服务器缺失”；只是本次遵照“先不要补全”的要求停止传输。
"""
    write(INDEX / "MISSING_EXPERIMENTS.md", missing)

    directory_guide = """# results_归档 目录说明

## 使用方式

- `00_归档索引`：服务器/本机盘点、总表、缺失清单和实验脚本副本。
- 日期开头的中文目录：按实验日期和实验阶段组织；目录内保留原始英文目录名，方便回溯。
- `README_实验说明.md`：记录原始路径、目的、可确认配置、主要结果、数据类型和当前定位。

## 状态说明

- `已完整归档`：本轮已确认目录完整，或文件数和字节数与服务器一致。
- `本机已有`：直接从本机现有 `results` 整理，没有重复从服务器下载。
- `已归档关键结果`：保留现有关键文件或停止传输前的数据，仍有 raw/log 缺口。
- `仅 summary`：只有汇总和最终表，不含完整原始运行数据。

## 原则

- 原始 `results` 和 `downloaded_results_0721` 未删除、未重命名。
- 服务器仅执行盘点和复制，没有修改、重命名或删除。
- 失败、invalid、诊断证据与正常结果一起保留。
- 不同日期的 500req 实验分别归档。
"""
    write(INDEX / "README_目录说明.md", directory_guide)

    scripts = sorted(p.name for p in (INDEX / "实验脚本").iterdir() if p.is_file())
    script_doc = [
        "# 实验脚本副本",
        "",
        "这些文件从本机项目现有脚本中复制，用于追溯 Config D/E、downtime/SLO、迁移策略、Figure10、Figure12 和模拟器实验。未复制整个项目源码。",
        "",
    ]
    script_doc.extend(f"- `{name}`" for name in scripts)
    write(INDEX / "实验脚本" / "README.md", "\n".join(script_doc))


README_DATA = {
    "2026-07-21_四实例正式迁移验证": ("2026-07-21", "results/5090_4inst_formal_20260721_014617", "验证四实例正式迁移流程和结果字段。", "实例数：4；其余未从现有结果确认。", "正式结果 CSV 和字段检查结果已保留。", "机制验证"),
    "2026-07-21_迁移效率_Figure10一期": ("2026-07-21", "results/5090_migration_efficiency_20260721_151901", "记录 Figure10 一期迁移效率验证与审计材料。", "GPU、模型、实例数、request_num、concurrency、workload、policy、seed：未从现有结果确认。", "服务器现存目录主要为审计、grep 和环境快照材料；未发现标准 run_summary。", "正式性能实验"),
    "2026-07-21_公平Baseline_迁移开销对比": ("2026-07-21", "results/5090_baselines_20260721_172757", "保留公平 baseline 与迁移开销对比的原始日志和 trace。", "GPU、模型、实例数、request_num、concurrency、workload、policy、seed：未从现有结果确认。", "完整目录已保留；结论应以目录内 summary/日志为准。", "baseline"),
    "2026-07-21_500请求多Baseline": ("2026-07-21", "results/5090_multi_baseline_500req_20260721_204013", "比较 500 请求场景下多种 baseline。", "request_num：500；其余未从现有结果确认。", "当前仅保留停止传输前的 59 文件、150.05 MB，不作为完整结论集。", "baseline（部分归档）"),
    "2026-08-19_迁移策略主实验_54轮": ("2026-08-19", "results/5090_migration_policy_baselines_20260819_201922", "完成迁移策略主对比实验。", "3 workloads × 6 policies × 3 repeats = 54 runs；其余字段以 run_summary/config 为准。", "1098 文件、4.47 GB；文件数和字节数与服务器一致。", "正式性能实验"),
    "2026-08-19_500请求多基线实验": ("2026-08-19", "results/5090_multi_baseline_500req_20260819_153800", "保留 500 请求多基线实验的现有汇总结果。", "request_num：500；其余未从现有结果确认。", "本机只有 aggregated/05_final 等 summary，完整 raw runs 未归档。", "baseline（仅 summary）"),
    "2026-08-21_论文补充实验": ("2026-08-21 至 2026-08-24", "results/5090_paper_supplement_20260821_170651", "汇总 Figure10 后续、Figure12、Config C、碎片化、Load-SR retry 与长输入 TTFT。", "配置散见各 summary；未统一推断。", "本机现有 summary 子集已归档；服务器完整 raw runs/logs 未继续补全。", "正式性能实验（仅 summary）"),
    "2026-08-24_Figure12-v2_强制碎片化实验": ("2026-08-24", "results/5090_paper_supplement_20260821_170651/02_fig12_fragmentation_v2", "验证强制碎片化 Config C 下四种策略。", "4 × RTX 5090；Qwen2.5-7B-Instruct；实例数 4；request_num 400/轮；concurrency 40；seed 20260821。", "12 轮完成并严格验收。QPS：不迁移 2.5547、Load-SR 4.0246、TPOT Tail 3.6445、Proposed 3.5789；对应 E2E P99 为 14.5942/10.3029/11.4262/11.3439 秒。", "正式性能实验（摘要与 smoke 子集）"),
    "2026-08-24_Figure12-v2_LoadSR重试汇总": ("2026-08-24", "results/fig12_v2_load_sr_retry_once_summary.json", "保留 Figure12 v2 Config C 的 Load-SR 单次重试结果。", "4 × RTX 5090；Qwen2.5-7B-Instruct；实例数 4；request_num 400；concurrency 40；policy load_sr；seed 20260821。", "该轮状态 completed、fairness fair；详细指标见 JSON。", "failure diagnosis（仅 summary）"),
    "2026-08-24_实验全量数据导出": ("2026-08-24", "results/full_data_export_20260824_151951", "保存论文实验的全量导出、报告、run summaries 和日志集合。", "跨多个实验配置，不能用单一配置概括。", "目录和 466.15 MB tar 包均已保留；本机目录包含 2604 个文件。", "正式结果导出"),
    "2026-08-31至09-03_WSL模拟器实验": ("2026-08-31 至 2026-09-03", "服务器 results 顶层未发现；本机 results/wsl_sim_*", "集中保存环境验证、四实例基础/实测、迁移触发窗口、极端触发、阈值扫描、碎片化触发和单次均衡触发。", "模拟器配置随子目录变化；未从现有结果统一确认。", "11 个本机原始目录已按 8 类整理，原始目录名保留为子目录。", "simulator"),
    "2026-09-04_停机时间与SLO验证": ("2026-09-04", "results/trace_downtime_slo_smoke_20260904_162645", "验证 server/visible/extra downtime 与 SLO 分析链路。", "GPU、模型、实例数、request_num、concurrency、workload、policy、seed：以 config/summary 为准。", "本机 82 文件、19.00 MB，包含 analyzer 输出、trace、summary 和 runtime/scheduler 日志。", "机制验证 / smoke"),
    "2026-09-05至09-17_复杂混合高负载_ConfigD-E": ("2026-09-05 至 2026-09-17", "results/complex_mixed_high_load", "集中保存 Config D/E 压力、smoke 与 KVT500 诊断实验。", "配置分别记录在四个子目录 README。", "前三组与服务器文件数和字节数一致；KVT500 诊断只保留本机已有诊断包。", "stress test / smoke / failure diagnosis"),
}

COMPLEX_READMES = {
    "01_ConfigD_C80_高压稳定性压力测试": ("2026-09-05", "results/complex_mixed_high_load/config_d/run_20260905_153725", "高压稳定性压力测试，不用于正式公平性能结论。", "4 × RTX 5090；实例数 4；request_num 1200；concurrency 80；五类请求、五阶段 workload；seed 未从现有结果确认。", "预期 12 slots，10 个 canonical slots 完成；失败、KVT500、abort 和 traceback 证据均保留。", "stress test"),
    "02_ConfigE_C40_Smoke": ("2026-09-13", "results/complex_mixed_high_load/config_e/run_20260913_185344", "Config E concurrency 40 冒烟验证。", "concurrency 40；其余字段以目录内 config/summary 为准。", "67 文件、839.77 MB，与服务器字节数一致；包含 run summary、migration events 和日志。", "smoke"),
    "03_ConfigE_C32_Smoke": ("2026-09-14", "results/complex_mixed_high_load/config_e_c32/run_20260914_150539", "Config E concurrency 32 冒烟与 KVT500 上下文保留。", "concurrency 32；其余字段以目录内 config/summary 为准。", "33 文件、415.47 MB，与服务器字节数一致；包含 KVT500_CONTEXT、follow-up、events 和 runtime 日志。", "smoke"),
    "04_ConfigE_C32_KVT500诊断复现": ("2026-09-17", "results/complex_mixed_high_load/config_e_c32_diag/run_20260917_185607", "复现并诊断 Config E C32 的 KVT500。", "concurrency 32；其余字段未从现有诊断包统一确认。", "当前保留 KVT500_CONTEXT_FULL、EVENT_TABLE、DIAGNOSIS、诊断 tar 和 analyzer；完整 runtime/KVT/MigServer 日志未补齐。", "failure diagnosis（关键结果）"),
}


def readme_text(title: str, data: tuple[str, str, str, str, str, str], path: Path) -> str:
    date, server, purpose, config, result, role = data
    _, _, _, flags = scan(path)
    return f"""# {title}

## 时间

{date}

## 服务器原始路径

`/data/home/zylu/llumnix-v018-main/{server}`

## 实验目的

{purpose}

## 配置

{config}

未明确列出的 GPU、模型、实例数、request_num、concurrency、workload、policy、seed 均为“未从现有结果确认”，不做推测。

## 主要结果

{result}

## 数据说明

- 当前目录检测到：{', '.join(flags) or '未检测到标准命名的 summary/raw/events/logs/traces'}。
- 原始目录名和路径见本 README 及总索引。
- 数据完整性状态见 `00_归档索引/EXPERIMENT_ARCHIVE_INDEX.md`。

## 当前定位

{role}
"""


def readmes() -> None:
    for name, data in README_DATA.items():
        path = ARCHIVE / name
        write(path / "README_实验说明.md", readme_text(name, data, path))
    complex_root = ARCHIVE / "2026-09-05至09-17_复杂混合高负载_ConfigD-E"
    for name, data in COMPLEX_READMES.items():
        path = complex_root / name
        write(path / "README_实验说明.md", readme_text(name, data, path))


def main() -> None:
    INDEX.mkdir(parents=True, exist_ok=True)
    inventory_docs()
    index_docs()
    readmes()


if __name__ == "__main__":
    main()
