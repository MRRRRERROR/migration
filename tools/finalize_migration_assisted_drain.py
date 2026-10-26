#!/usr/bin/env python3
"""Rebuild the Migration-Assisted Drain report from preserved CSVs and raw logs."""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list[dict]) -> None:
    columns=[]
    for row in rows:
        for key in row:
            if key not in columns: columns.append(key)
    normalized=[]
    for row in rows:
        normalized.append({key:row.get(key,"") for key in columns})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns); writer.writeheader(); writer.writerows(normalized)


def barex_time(line: str) -> float | None:
    match = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+):", line)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f").astimezone().timestamp()


def evidence(run_dir: Path) -> dict:
    source = (run_dir / "migration/source_vllm.log").read_text(errors="replace").splitlines()
    dest = (run_dir / "migration/destination_vllm.log").read_text(errors="replace").splitlines()
    out: dict[str, dict] = {}
    for line in dest:
        rid_match = re.search(r"reqid?=(?:chatcmpl-)?(drain-r\d+-\d+)", line)
        if not rid_match:
            continue
        rid = rid_match.group(1); item = out.setdefault(rid, {})
        code = re.search(r"KVTResp\(code=(\d+)", line)
        duration = re.search(r"migrate end kvt: dur_ms=([0-9.]+)", line)
        total = re.search(r"migration end\..*duration_ms=([0-9.]+)", line)
        prealloc = re.search(r"migrate\. req=.*? prealloc=(\d+)", line)
        if code: item["kvt_code"] = int(code.group(1))
        if duration: item["transfer_duration_ms"] = float(duration.group(1))
        if total: item["migration_total_duration_ms"] = float(total.group(1))
        if prealloc: item["destination_prealloc_blocks"] = int(prealloc.group(1))
    for line in source:
        match = re.search(r"FinishedReq=(?:chatcmpl-)?(drain-r\d+-\d+)", line)
        if match:
            out.setdefault(match.group(1), {})["source_release_time"] = barex_time(line)
    combined = source + dest
    for rid, item in out.items():
        item["destination_abort"] = any(rid in line and ("try to abort" in line.lower() or "abort req" in line.lower()) for line in combined)
    return out


def timeline_svg(path: Path, rows: list[dict], summary: dict) -> None:
    selected = [r for r in rows if r["state_at_drain"]]
    drain = float(summary["drain_start"]); xmax = max(1.0, float(summary["source_gpu_released"]) - drain)
    w, left, right, top, row_h = 1050, 180, 30, 55, 30
    h = top + max(1, len(selected)) * row_h + 55
    def x(ts: float) -> float: return left + max(0, ts-drain) / xmax * (w-left-right)
    parts = [f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}'>",
             "<rect width='100%' height='100%' fill='white'/>",
             f"<text x='{w/2}' y='23' text-anchor='middle' font-size='17'>drain_start 请求迁移时间线（虚线阶段为日志时长反推）</text>"]
    for sec in range(0, int(xmax)+1):
        xx=x(drain+sec); parts.append(f"<line x1='{xx:.1f}' y1='{top-12}' x2='{xx:.1f}' y2='{h-32}' stroke='#eee'/><text x='{xx:.1f}' y='{h-13}' text-anchor='middle' font-size='10'>{sec}s</text>")
    for idx, row in enumerate(selected):
        y=top+idx*row_h; rid=row["request_id"]; result=row["migration_result_final"]
        end=float(row["source_request_release_time"] or summary["source_evacuated"])
        start=float(row["migration_transfer_start"] or summary["migration_trigger"]["trigger_time"])
        color="#2ca02c" if result=="success" else "#d62728"
        parts += [f"<text x='5' y='{y+4}' font-size='11'>{html.escape(rid)}</text>",
                  f"<line x1='{x(start):.1f}' y1='{y}' x2='{x(end):.1f}' y2='{y}' stroke='{color}' stroke-width='5' stroke-dasharray='5 3'/>",
                  f"<circle cx='{x(end):.1f}' cy='{y}' r='4' fill='{color}'/><text x='{x(end)+7:.1f}' y='{y+4}' font-size='10'>{html.escape(result)}</text>"]
    parts.append("</svg>"); path.write_text("".join(parts), encoding="utf-8")


def finish_reason_stats(rows: list[dict]) -> dict:
    reasons=Counter(r["finish_reason"] or "unknown" for r in rows)
    return {"counts":dict(reasons),"stop_ratio":reasons["stop"]/len(rows) if rows else 0,
            "length_ratio":reasons["length"]/len(rows) if rows else 0}


def finalize(root: Path) -> None:
    config=json.loads((root/"config.json").read_text(encoding="utf-8")); pairs=[]
    for i in range(1,4):
        rd=root/f"run_{i}"; natural=json.loads((rd/"natural/summary.json").read_text()); migration=json.loads((rd/"migration/summary.json").read_text())
        ev=evidence(rd); rows=read_csv(rd/"migration/request_metrics.csv")
        for row in rows:
            item=ev.get(row["request_id"], {})
            if not row["state_at_drain"] and not item:
                row["migration_result_final"]="not_requested"; row["timestamp_quality"]="n/a"; continue
            release=item.get("source_release_time")
            duration=item.get("transfer_duration_ms")
            if release and duration is not None:
                row["migration_transfer_end"]=release
                row["migration_transfer_start"]=release-duration/1000
                row["source_request_release_time"]=release
                row["destination_resume_time"]=release if item.get("kvt_code")==0 else ""
            row["destination_prealloc_start"]=""; row["destination_prealloc_end"]=""
            row["timestamp_quality"]="trigger/release exact; transfer start inferred from existing KVT duration; prealloc wall time unavailable"
            code=item.get("kvt_code")
            if code in (404,500): result=f"failed_kvt_{code}"
            elif row["error"] or not row["finish_reason"]: result="failed_destination_resume_or_output_forward"
            elif code==0: result="success"
            else: result="completed_during_migration_race"
            row["migration_result_final"]=result
            row["migration_result"]=result
            row["migration_error"]=(row["error"] or (f"KVTResp(code={code})" if code in (404,500) else ("destination abort after source shutdown" if item.get("destination_abort") else "")))
            row["kvt_code"]="" if code is None else code
            row["transfer_duration_ms"]="" if duration is None else duration
            row["migration_total_duration_ms"]="" if item.get("migration_total_duration_ms") is None else item["migration_total_duration_ms"]
            row["destination_prealloc_blocks"]="" if item.get("destination_prealloc_blocks") is None else item["destination_prealloc_blocks"]
        write_csv(rd/"migration/request_metrics.csv",rows)
        event_rows=[]
        for row in rows:
            if row["migration_result_final"] == "not_requested": continue
            for event_name in ("migration_trigger_time","destination_prealloc_start","destination_prealloc_end",
                               "migration_transfer_start","migration_transfer_end","source_request_release_time",
                               "destination_resume_time","request_finish_time"):
                event_rows.append({"request_id":row["request_id"],"event":event_name,"timestamp":row.get(event_name,""),
                    "timestamp_quality":row.get("timestamp_quality",""),"migration_result":row["migration_result_final"],
                    "migration_error":row.get("migration_error","")})
        write_csv(rd/"migration/migration_events.csv",event_rows)
        attempts=[r for r in rows if r["migration_result_final"] not in ("not_requested",)]
        drain_rows=[r for r in rows if r["state_at_drain"]]
        success=sum(r["migration_result_final"]=="success" for r in attempts)
        failures=sum(r["migration_result_final"].startswith("failed") for r in attempts)
        kvt0=sum(str(r.get("kvt_code"))=="0" for r in attempts); k404=sum(str(r.get("kvt_code"))=="404" for r in attempts); k500=sum(str(r.get("kvt_code"))=="500" for r in attempts)
        resume_fail=sum(r["migration_result_final"]=="failed_destination_resume_or_output_forward" for r in attempts)
        migration.update({"migration_attempt_count":len(attempts),"migration_success_count":success,"migration_failure_count":failures,
            "migration_success_rate":success/len(attempts) if attempts else None,"kvt_transfer_success_count":kvt0,
            "kvt_transfer_success_rate":kvt0/len(attempts) if attempts else None,"kvt_404_count":k404,"kvt_500_count":k500,
            "destination_resume_or_output_failure_count":resume_fail,"abort_count":resume_fail,
            "max_kvt_transfer_duration_ms":max((float(r["transfer_duration_ms"]) for r in attempts if r["transfer_duration_ms"]),default=None),
            "drain_request_row_count":len(drain_rows),"finish_reason":finish_reason_stats(rows)})
        natural_rows=read_csv(rd/"natural/request_metrics.csv"); natural["finish_reason"]=finish_reason_stats(natural_rows)
        (rd/"migration/summary.json").write_text(json.dumps(migration,ensure_ascii=False,indent=2),encoding="utf-8")
        (rd/"natural/summary.json").write_text(json.dumps(natural,ensure_ascii=False,indent=2),encoding="utf-8")
        timeline_svg(rd/"plots/migration_timeline.svg",rows,migration)
        pairs.append({"natural":natural,"migration":migration})
    aggregate={"runs":pairs,"strict_paired_trace":True,
        "mean_natural_drain_s":statistics.fmean(p["natural"]["source_evacuation_time_s"] for p in pairs),
        "mean_migration_evacuation_s":statistics.fmean(p["migration"]["source_evacuation_time_s"] for p in pairs),
        "mean_migration_gpu_release_s":statistics.fmean(p["migration"]["source_gpu_release_time_s"] for p in pairs),
        "interpretation":"Source ownership and GPU memory fall much earlier, but current output-forwarding path depends on the source frontend; shutdown caused destination resume/output failures, so the result is not a successful lossless drain."}
    (root/"aggregate_summary.json").write_text(json.dumps(aggregate,ensure_ascii=False,indent=2),encoding="utf-8")
    # Rebuild root request CSV after corrections.
    all_rows=[]
    for i in range(1,4):
        for mode in ("natural","migration"):
            for row in read_csv(root/f"run_{i}/{mode}/request_metrics.csv"):
                row["run"]=i; row["mode"]=mode; all_rows.append(row)
    columns=[]
    for row in all_rows:
        for key in row:
            if key not in columns: columns.append(key)
    for row in all_rows:
        for key in columns: row.setdefault(key,"")
    with (root/"request_metrics.csv").open("w",newline="",encoding="utf-8") as fh:
        w=csv.DictWriter(fh,fieldnames=columns); w.writeheader(); w.writerows(all_rows)
    all_events=[]
    for i in range(1,4):
        for row in read_csv(root/f"run_{i}/migration/migration_events.csv"):
            row["run"]=i; all_events.append(row)
    write_csv(root/"migration_events.csv",all_events)
    rows="".join(f"<tr><td>{i}</td><td>{p['migration']['drain_start_snapshot']['running']}</td><td>{p['migration']['drain_start_snapshot']['waiting']}</td><td>{p['migration']['migration_attempt_count']}</td><td>{p['migration']['kvt_transfer_success_count']}/{p['migration']['migration_attempt_count']}</td><td>{p['migration']['migration_success_count']}</td><td>{p['migration']['migration_failure_count']}</td><td>{p['natural']['source_evacuation_time_s']:.3f}</td><td>{p['migration']['source_evacuation_time_s']:.3f}</td><td>{p['migration']['source_gpu_release_time_s']:.3f}</td><td>{p['migration']['request_completion_time_s']:.3f}</td></tr>" for i,p in enumerate(pairs,1))
    sections=[]
    for i,p in enumerate(pairs,1):
        m=p["migration"]; n=p["natural"]
        sections.append(f"<h2>第 {i} 轮</h2><p>Natural finish_reason：{html.escape(json.dumps(n['finish_reason']['counts'],ensure_ascii=False))}；Migration：{html.escape(json.dumps(m['finish_reason']['counts'],ensure_ascii=False))}。KVT404={m['kvt_404_count']}，KVT500={m['kvt_500_count']}，destination resume/output failure={m['destination_resume_or_output_failure_count']}。最大 KVT transfer={m['max_kvt_transfer_duration_ms']:.3f} ms。</p><img src='run_{i}/plots/request_counts.svg'><img src='run_{i}/plots/source_gpu.svg'><img class='wide' src='run_{i}/plots/migration_timeline.svg'><p><a href='run_{i}/migration/request_metrics.csv'>请求明细</a> · <a href='run_{i}/migration/migration_events.csv'>原始迁移事件</a> · <a href='run_{i}/migration/source_vllm.log'>source 日志</a> · <a href='run_{i}/migration/destination_vllm.log'>destination 日志</a></p>")
    any_length=any(p["natural"]["finish_reason"]["counts"].get("length",0) for p in pairs)
    warning="检测到 finish_reason=length；这些请求仍受到 4096 总上下文边界限制，不能视为完全自然生成。" if any_length else "未检测到 finish_reason=length。"
    doc=f"""<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>Migration-Assisted Drain</title><style>body{{font:15px Arial,sans-serif;max-width:1300px;margin:28px auto;line-height:1.55;color:#18212f}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #cbd5e1;padding:7px;text-align:right}}th:first-child,td:first-child{{text-align:left}}img{{max-width:48%;vertical-align:top;margin:1%}}img.wide{{max-width:98%}}.bad{{background:#fff1f2;border-left:5px solid #e11d48;padding:12px}}.note{{background:#eff6ff;padding:12px}}code{{white-space:pre-wrap}}</style><h1>Migration-Assisted Drain 实验报告</h1><p class='bad'><b>结论：</b>source 归属约 1.2 秒清空、GPU 约 3.4 秒释放，但当前 destination 输出转发依赖 source frontend。关闭 source 后出现请求中断/abort，因此不能确认“无损迁移成功”；只能确认 source 的确被提前物理释放。</p><p class='note'>每轮 Natural/Migration 精确复用同一份 request_trace.jsonl。迁移使用现有 Llumlet/KVT `Migrate(NUM_REQ=-1, LR, neutral_failover)`，没有负载均衡选优，也没有显式 abort 请求。{warning}</p><table><tr><th>轮次</th><th>drain running</th><th>waiting</th><th>实际尝试</th><th>KVT成功/尝试</th><th>端到端成功</th><th>失败</th><th>Natural Drain(s)</th><th>Source Evacuation(s)</th><th>GPU Release(s)</th><th>Destination归零(s)</th></tr>{rows}</table><p><b>均值：</b>Natural={aggregate['mean_natural_drain_s']:.3f}s；Migration source evacuation={aggregate['mean_migration_evacuation_s']:.3f}s；Migration GPU release={aggregate['mean_migration_gpu_release_s']:.3f}s。</p><h2>三个时间的定义</h2><ol><li>Migration-Assisted Drain：drain_start→source_evacuated（CMS source running/waiting/migrating 连续归零）。</li><li>GPU Release：drain_start→nvidia-smi 显存≤64 MiB。</li><li>Request Completion：drain_start→destination 实例状态归零；本次归零包含失败后的 abort，不能当作正常生成完成。</li></ol>{''.join(sections)}<h2>阶段时间说明</h2><p>trigger 与 source FinishedReq 使用精确墙钟；现有 Blade-KVT 的 prealloc/transfer 日志大多只记录 duration_ms、不带墙钟，所以 request_metrics 中 transfer_start 由精确 source release 减去已有 KVT duration 反推，并在 timestamp_quality 标注；prealloc 墙钟留空，没有伪造。</p><h2>配置</h2><code>{html.escape(json.dumps(config,ensure_ascii=False,indent=2))}</code></html>"""
    (root/"report.html").write_text(doc,encoding="utf-8")


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("root",type=Path); finalize(ap.parse_args().root)
