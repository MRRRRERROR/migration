import csv
import html
import math
import re
from pathlib import Path
from statistics import mean, median


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "llama2-7b_migration_efficiency_clean_20260701_231125"
OUT_SUMMARY = DATA_DIR / "total_migration_time_summary.csv"
OUT_RAW = DATA_DIR / "total_migration_time_raw.csv"
OUT_HTML = ROOT / "llama2_total_migration_time_report.html"
LENGTHS = [256, 512, 1024, 1536, 2048]


EVENT_RE = re.compile(r"event=(\S+) request_id=(\S+).*timestamp=([0-9.]+)")
BLOCK_RE = re.compile(r"blocks=(\d+)")


def pct(values, q):
    if not values:
        return ""
    values = sorted(values)
    return values[round((len(values) - 1) * q)]


def fmt(value, digits=2):
    if value in ("", None):
        return "未触发"
    return f"{float(value):.{digits}f}"


def parse_events():
    raw_rows = []
    summary_rows = []
    for seq_len in LENGTHS:
        path = DATA_DIR / f"migration_events_{seq_len}.log"
        events = {}
        blocks = {}
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = EVENT_RE.search(line)
            if not match:
                continue
            event, request_id, timestamp = match.group(1), match.group(2), float(match.group(3))
            events.setdefault(request_id, {})[event] = timestamp
            block_match = BLOCK_RE.search(line)
            if block_match:
                blocks[request_id] = int(block_match.group(1))

        total_times = []
        precopy_times = []
        downtime_times = []
        for request_id, row_events in events.items():
            required = {"migrate_begin", "migrate_out_stage_0", "migrate_out_stage_a", "migrate_in"}
            if required <= set(row_events):
                total_ms = (row_events["migrate_in"] - row_events["migrate_begin"]) * 1000
                precopy_ms = (row_events["migrate_out_stage_a"] - row_events["migrate_out_stage_0"]) * 1000
                downtime_ms = (row_events["migrate_in"] - row_events["migrate_out_stage_a"]) * 1000
                setup_ms = (row_events["migrate_out_stage_0"] - row_events["migrate_begin"]) * 1000
                total_times.append(total_ms)
                precopy_times.append(precopy_ms)
                downtime_times.append(downtime_ms)
                raw_rows.append({
                    "seq_len": seq_len,
                    "request_id": request_id,
                    "blocks": blocks.get(request_id, ""),
                    "setup_ms": setup_ms,
                    "precopy_ms": precopy_ms,
                    "downtime_ms": downtime_ms,
                    "total_migration_ms": total_ms,
                })

        summary_rows.append({
            "seq_len": seq_len,
            "complete_migrations": len(total_times),
            "total_mean_ms": mean(total_times) if total_times else "",
            "total_p50_ms": median(total_times) if total_times else "",
            "total_p95_ms": pct(total_times, 0.95),
            "precopy_mean_ms": mean(precopy_times) if precopy_times else "",
            "downtime_mean_ms": mean(downtime_times) if downtime_times else "",
        })
    return summary_rows, raw_rows


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def line_chart(summary):
    valid = [r for r in summary if r["total_mean_ms"] != ""]
    width, height = 980, 440
    left, right, top, bottom = 84, 34, 34, 72
    plot_w = width - left - right
    plot_h = height - top - bottom
    max_y = max(float(r["total_mean_ms"]) for r in valid) * 1.15

    def x(seq):
        return left + (math.log2(seq) - math.log2(256)) / (math.log2(2048) - math.log2(256)) * plot_w

    def y(value):
        return top + (max_y - float(value)) / max_y * plot_h

    y_ticks = [0, 400, 800, 1200, 1600, 2000]
    y_ticks = [v for v in y_ticks if v <= max_y]
    grid = []
    for tick in y_ticks:
        yy = y(tick)
        grid.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" class="grid"/>')
        grid.append(f'<text x="{left-12}" y="{yy+5:.1f}" class="axis-label" text-anchor="end">{tick}</text>')
    for seq in LENGTHS:
        xx = x(seq)
        grid.append(f'<line x1="{xx:.1f}" y1="{top}" x2="{xx:.1f}" y2="{height-bottom}" class="grid faint"/>')
        grid.append(f'<text x="{xx:.1f}" y="{height-34}" class="axis-label" text-anchor="middle">{seq}</text>')

    points = " ".join(f'{x(int(r["seq_len"])):.1f},{y(r["total_mean_ms"]):.1f}' for r in valid)
    markers = []
    for r in valid:
        seq = int(r["seq_len"])
        total = float(r["total_mean_ms"])
        markers.append(f'<circle cx="{x(seq):.1f}" cy="{y(total):.1f}" r="5" class="dot"/>')
        markers.append(f'<text x="{x(seq):.1f}" y="{y(total)-10:.1f}" text-anchor="middle" class="point-label">{total:.0f}</text>')

    missing_x = x(2048)
    missing = (
        f'<line x1="{missing_x:.1f}" y1="{top}" x2="{missing_x:.1f}" y2="{height-bottom}" class="missing-line"/>'
        f'<text x="{missing_x-8:.1f}" y="{top+24}" class="missing-label" text-anchor="end">2048 未触发迁移</text>'
    )
    return f"""
    <svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="总迁移时间随序列长度变化">
      <rect width="{width}" height="{height}" class="chart-bg"/>
      {''.join(grid)}
      <line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" class="axis"/>
      <line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" class="axis"/>
      <polyline points="{points}" class="series"/>
      {''.join(markers)}
      {missing}
      <text x="{width/2:.1f}" y="{height-6}" class="axis-title" text-anchor="middle">序列长度</text>
      <text x="22" y="{height/2:.1f}" class="axis-title rotated" text-anchor="middle">总迁移时间 / ms</text>
    </svg>
    """


def table_summary(rows):
    body = []
    for r in rows:
        body.append(
            "<tr>"
            f"<td>{r['seq_len']}</td>"
            f"<td>{r['complete_migrations']}</td>"
            f"<td>{fmt(r['total_mean_ms'])}</td>"
            f"<td>{fmt(r['total_p50_ms'])}</td>"
            f"<td>{fmt(r['total_p95_ms'])}</td>"
            f"<td>{fmt(r['precopy_mean_ms'])}</td>"
            f"<td>{fmt(r['downtime_mean_ms'])}</td>"
            "</tr>"
        )
    return "\n".join(body)


def table_raw(rows):
    body = []
    for r in rows:
        body.append(
            "<tr>"
            f"<td>{r['seq_len']}</td>"
            f"<td class='mono'>{html.escape(r['request_id'])}</td>"
            f"<td>{r['blocks']}</td>"
            f"<td>{fmt(r['setup_ms'])}</td>"
            f"<td>{fmt(r['precopy_ms'])}</td>"
            f"<td>{fmt(r['downtime_ms'])}</td>"
            f"<td>{fmt(r['total_migration_ms'])}</td>"
            "</tr>"
        )
    return "\n".join(body)


def main():
    summary, raw = parse_events()
    write_csv(OUT_SUMMARY, summary)
    write_csv(OUT_RAW, raw)
    valid = [r for r in summary if r["total_mean_ms"] != ""]
    min_total = min(float(r["total_mean_ms"]) for r in valid)
    max_total = max(float(r["total_mean_ms"]) for r in valid)

    doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Llama-2-7B 总迁移时间报告</title>
  <style>
    :root {{
      --bg: #f6f7fb; --panel: #fff; --text: #111827; --muted: #64748b;
      --border: #dfe4ec; --blue: #2563eb; --red: #dc2626; --grid: #e6ebf2;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", Arial, sans-serif; }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 28px 22px 48px; }}
    h1 {{ margin: 0 0 8px; font-size: 30px; letter-spacing: 0; }}
    h2 {{ margin: 0 0 14px; font-size: 20px; }}
    p {{ margin: 0; color: var(--muted); line-height: 1.65; }}
    .meta {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }}
    .pill {{ border: 1px solid var(--border); background: var(--panel); padding: 7px 10px; border-radius: 6px; color: #334155; font-size: 14px; }}
    .cards {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }}
    .card, section {{ background: var(--panel); border: 1px solid var(--border); border-radius: 8px; box-shadow: 0 1px 2px rgba(15,23,42,.04); }}
    .card {{ padding: 16px; }}
    .label {{ color: var(--muted); font-size: 13px; }}
    .value {{ font-size: 25px; font-weight: 700; margin-top: 4px; }}
    section {{ padding: 18px; margin-top: 16px; }}
    .note {{ border-left: 4px solid #d97706; background: #fff7ed; padding: 12px 14px; border-radius: 6px; color: #7c2d12; margin-top: 12px; }}
    .chart-wrap, .table-wrap {{ overflow-x: auto; }}
    .chart {{ width: 100%; min-width: 760px; height: auto; }}
    .chart-bg {{ fill: #fff; }}
    .grid {{ stroke: var(--grid); stroke-width: 1; }}
    .faint {{ opacity: .65; }}
    .axis {{ stroke: #334155; stroke-width: 1.2; }}
    .axis-label, .axis-title, .point-label {{ fill: #334155; font-size: 14px; }}
    .axis-title {{ font-weight: 700; }}
    .rotated {{ transform: rotate(-90deg); transform-origin: 22px 220px; }}
    .series {{ fill: none; stroke: var(--blue); stroke-width: 3; stroke-linejoin: round; stroke-linecap: round; }}
    .dot {{ fill: var(--blue); }}
    .point-label {{ font-size: 12px; fill: #475569; }}
    .missing-line {{ stroke: var(--red); stroke-dasharray: 5 5; }}
    .missing-label {{ fill: var(--red); font-size: 13px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid var(--border); text-align: right; white-space: nowrap; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f8fafc; color: #334155; font-weight: 700; }}
    tbody tr:hover {{ background: #f8fafc; }}
    .mono {{ font-family: Consolas, "Cascadia Mono", monospace; font-size: 12px; text-align: left; }}
    details {{ margin-top: 12px; }}
    summary {{ cursor: pointer; color: var(--blue); font-weight: 700; margin-bottom: 10px; }}
    @media (max-width: 860px) {{ main {{ padding: 20px 12px 36px; }} .cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} h1 {{ font-size: 24px; }} }}
  </style>
</head>
<body>
<main>
  <h1>Llama-2-7B 总迁移时间报告</h1>
  <p>总迁移时间定义为 <strong>migrate_in - migrate_begin</strong>，表示从开始迁移到目标实例接管完成的完整耗时。</p>
  <div class="meta">
    <span class="pill">模型：Llama-2-7B</span>
    <span class="pill">GPU：2 x RTX 3090</span>
    <span class="pill">完整迁移记录：{len(raw)} 条</span>
    <span class="pill">数据目录：{html.escape(DATA_DIR.name)}</span>
  </div>

  <div class="cards">
    <div class="card"><div class="label">最短总迁移均值</div><div class="value">{min_total:.2f} ms</div></div>
    <div class="card"><div class="label">最长总迁移均值</div><div class="value">{max_total:.2f} ms</div></div>
    <div class="card"><div class="label">有效序列长度</div><div class="value">4 / 5</div></div>
    <div class="card"><div class="label">未触发组</div><div class="value">2048</div></div>
  </div>

  <section>
    <h2>总迁移时间趋势</h2>
    <p>该指标包含迁移启动、预拷贝阶段、最后停机阶段以及目标实例接管完成所经历的总时间。</p>
    <div class="chart-wrap">{line_chart(summary)}</div>
    <div class="note">2048 组 benchmark 完成，但没有触发有效迁移事件，因此不纳入总迁移时间统计。</div>
  </section>

  <section>
    <h2>总迁移时间汇总表</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>序列长度</th>
            <th>完整迁移数</th>
            <th>总迁移均值 ms</th>
            <th>总迁移 P50 ms</th>
            <th>总迁移 P95 ms</th>
            <th>预拷贝均值 ms</th>
            <th>停机均值 ms</th>
          </tr>
        </thead>
        <tbody>{table_summary(summary)}</tbody>
      </table>
    </div>
  </section>

  <section>
    <h2>原始总迁移明细</h2>
    <details>
      <summary>展开 {len(raw)} 条原始迁移记录</summary>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>序列长度</th>
              <th>请求 ID</th>
              <th>块数</th>
              <th>启动开销 ms</th>
              <th>预拷贝 ms</th>
              <th>停机时间 ms</th>
              <th>总迁移时间 ms</th>
            </tr>
          </thead>
          <tbody>{table_raw(raw)}</tbody>
        </table>
      </div>
    </details>
  </section>
</main>
</body>
</html>
"""
    OUT_HTML.write_text(doc, encoding="utf-8")
    print(OUT_HTML)
    print(OUT_SUMMARY)
    print(OUT_RAW)


if __name__ == "__main__":
    main()
