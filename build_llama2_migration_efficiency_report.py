import csv
import html
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "llama2-7b_migration_efficiency_clean_20260701_231125"
SUMMARY_CSV = DATA_DIR / "final_migration_efficiency_summary.csv"
RAW_CSV = DATA_DIR / "final_migration_efficiency_raw.csv"
OUT_HTML = ROOT / "llama2_migration_efficiency_report.html"


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def as_float(value):
    if value in ("", None):
        return None
    return float(value)


def fmt(value, digits=2):
    value = as_float(value)
    if value is None:
        return "未触发"
    return f"{value:.{digits}f}"


def svg_line_chart(rows):
    points = []
    for row in rows:
        seq = int(row["seq_len"])
        dt = as_float(row["downtime_mean_ms"])
        bc = as_float(row["blocking_mean_ms"])
        if dt is not None and bc is not None:
            points.append((seq, dt, bc))

    width, height = 980, 440
    left, right, top, bottom = 82, 36, 36, 72
    plot_w = width - left - right
    plot_h = height - top - bottom
    max_y = max(max(p[1], p[2]) for p in points) * 1.12
    min_x, max_x = 256, 2048

    def x(seq):
        return left + (math.log2(seq) - math.log2(min_x)) / (math.log2(max_x) - math.log2(min_x)) * plot_w

    def y(value):
        return top + (max_y - value) / max_y * plot_h

    def poly(idx):
        return " ".join(f"{x(seq):.1f},{y(row[idx]):.1f}" for seq, *row in points)

    y_ticks = [0, 400, 800, 1200, 1600, 2000]
    y_ticks = [v for v in y_ticks if v <= max_y]
    x_ticks = [256, 512, 1024, 1536, 2048]

    grid = []
    for tick in y_ticks:
        yy = y(tick)
        grid.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" class="grid"/>')
        grid.append(f'<text x="{left-12}" y="{yy+5:.1f}" class="axis-label" text-anchor="end">{tick}</text>')
    for tick in x_ticks:
        xx = x(tick)
        grid.append(f'<line x1="{xx:.1f}" y1="{top}" x2="{xx:.1f}" y2="{height-bottom}" class="grid faint"/>')
        grid.append(f'<text x="{xx:.1f}" y="{height-34}" class="axis-label" text-anchor="middle">{tick}</text>')

    markers = []
    for seq, dt, bc in points:
        markers.append(f'<circle cx="{x(seq):.1f}" cy="{y(dt):.1f}" r="5" class="dot migration"/>')
        markers.append(f'<circle cx="{x(seq):.1f}" cy="{y(bc):.1f}" r="5" class="dot blocking"/>')
        markers.append(
            f'<text x="{x(seq):.1f}" y="{y(dt)-10:.1f}" class="point-label" text-anchor="middle">{dt:.0f}</text>'
        )
        markers.append(
            f'<text x="{x(seq):.1f}" y="{y(bc)-10:.1f}" class="point-label" text-anchor="middle">{bc:.0f}</text>'
        )

    missing_x = x(2048)
    missing = (
        f'<line x1="{missing_x:.1f}" y1="{top}" x2="{missing_x:.1f}" y2="{height-bottom}" class="missing-line"/>'
        f'<text x="{missing_x-8:.1f}" y="{top+24}" class="missing-label" text-anchor="end">2048 未触发迁移</text>'
    )

    return f"""
    <svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="迁移停机时间和阻塞拷贝耗时对比">
      <rect x="0" y="0" width="{width}" height="{height}" class="chart-bg"/>
      {''.join(grid)}
      <line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" class="axis"/>
      <line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" class="axis"/>
      <polyline points="{poly(0)}" class="series migration-line"/>
      <polyline points="{poly(1)}" class="series blocking-line"/>
      {''.join(markers)}
      {missing}
      <text x="{width/2:.1f}" y="{height-6}" class="axis-title" text-anchor="middle">序列长度</text>
      <text x="22" y="{height/2:.1f}" class="axis-title rotated" text-anchor="middle">时间 / ms</text>
      <g transform="translate({width-330}, 42)">
        <rect x="0" y="0" width="292" height="72" class="legend-bg"/>
        <line x1="18" y1="25" x2="62" y2="25" class="series migration-line"/>
        <circle cx="40" cy="25" r="5" class="dot migration"/>
        <text x="78" y="30" class="legend-text">Llumnix 迁移停机时间</text>
        <line x1="18" y1="52" x2="62" y2="52" class="series blocking-line"/>
        <circle cx="40" cy="52" r="5" class="dot blocking"/>
        <text x="78" y="57" class="legend-text">阻塞式 KV 拷贝</text>
      </g>
    </svg>
    """


def build_summary_table(rows):
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(row['seq_len'])}</td>"
            f"<td>{html.escape(row['complete_migrations'])}</td>"
            f"<td>{fmt(row['downtime_mean_ms'])}</td>"
            f"<td>{fmt(row['downtime_p50_ms'])}</td>"
            f"<td>{fmt(row['downtime_p95_ms'])}</td>"
            f"<td>{fmt(row['blocking_mean_ms'])}</td>"
            f"<td>{fmt(row['blocking_p50_ms'])}</td>"
            f"<td>{fmt(row['blocking_p95_ms'])}</td>"
            "</tr>"
        )
    return "\n".join(body)


def build_raw_table(rows):
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(row['seq_len'])}</td>"
            f"<td class='mono'>{html.escape(row['request_id'])}</td>"
            f"<td>{html.escape(row['blocks'])}</td>"
            f"<td>{fmt(row['blocking_copy_ms'])}</td>"
            f"<td>{fmt(row['downtime_ms'])}</td>"
            "</tr>"
        )
    return "\n".join(body)


def main():
    summary = read_csv(SUMMARY_CSV)
    raw = read_csv(RAW_CSV)
    valid = [r for r in summary if as_float(r["downtime_mean_ms"]) is not None]
    best_ratio = max(as_float(r["blocking_mean_ms"]) / as_float(r["downtime_mean_ms"]) for r in valid)

    html_doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Llama-2-7B 迁移效率实验报告</title>
  <style>
    :root {{
      --bg: #f6f7fb;
      --panel: #ffffff;
      --text: #111827;
      --muted: #64748b;
      --border: #dfe4ec;
      --blue: #2563eb;
      --orange: #d97706;
      --green: #059669;
      --red: #dc2626;
      --grid: #e6ebf2;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", Arial, sans-serif;
      line-height: 1.55;
    }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 28px 22px 48px; }}
    h1 {{ margin: 0 0 8px; font-size: 30px; letter-spacing: 0; }}
    h2 {{ margin: 0 0 14px; font-size: 20px; }}
    p {{ margin: 0; color: var(--muted); }}
    .hero {{ margin-bottom: 18px; }}
    .meta {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }}
    .pill {{
      border: 1px solid var(--border);
      background: var(--panel);
      padding: 7px 10px;
      border-radius: 6px;
      color: #334155;
      font-size: 14px;
    }}
    .grid-cards {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }}
    .card, section {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
    }}
    .card {{ padding: 16px; }}
    .card .label {{ color: var(--muted); font-size: 13px; }}
    .card .value {{ font-size: 25px; font-weight: 700; margin-top: 4px; }}
    section {{ padding: 18px; margin-top: 16px; }}
    .note {{
      border-left: 4px solid var(--orange);
      background: #fff7ed;
      padding: 12px 14px;
      border-radius: 6px;
      color: #7c2d12;
      margin-top: 12px;
    }}
    .chart-wrap {{ overflow-x: auto; }}
    .chart {{ width: 100%; min-width: 760px; height: auto; }}
    .chart-bg, .legend-bg {{ fill: #fff; }}
    .grid {{ stroke: var(--grid); stroke-width: 1; }}
    .faint {{ opacity: .65; }}
    .axis {{ stroke: #334155; stroke-width: 1.2; }}
    .axis-label, .axis-title, .legend-text, .point-label {{ fill: #334155; font-size: 14px; }}
    .axis-title {{ font-weight: 700; }}
    .rotated {{ transform: rotate(-90deg); transform-origin: 22px 220px; }}
    .series {{ fill: none; stroke-width: 3; stroke-linejoin: round; stroke-linecap: round; }}
    .migration-line {{ stroke: var(--blue); }}
    .blocking-line {{ stroke: var(--orange); }}
    .dot.migration {{ fill: var(--blue); }}
    .dot.blocking {{ fill: var(--orange); }}
    .point-label {{ font-size: 12px; fill: #475569; }}
    .legend-bg {{ stroke: var(--border); rx: 6; }}
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
    .table-wrap {{ overflow-x: auto; }}
    .foot {{ margin-top: 14px; font-size: 13px; color: var(--muted); }}
    @media (max-width: 860px) {{
      main {{ padding: 20px 12px 36px; }}
      .grid-cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      h1 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
<main>
  <div class="hero">
    <h1>Llama-2-7B 迁移效率实验报告</h1>
    <p>基于当前 2 x RTX 3090 环境，对论文 Figure 10 的迁移停机时间实验做缩小复现。</p>
    <div class="meta">
      <span class="pill">模型：Llama-2-7B</span>
      <span class="pill">路径：/home/wuqiushuang/models/llama-2-7b-full</span>
      <span class="pill">实例：2 个 Llumnix/vLLM 实例</span>
      <span class="pill">GPU：2 x RTX 3090</span>
      <span class="pill">有效迁移：{len(raw)} 条</span>
    </div>
  </div>

  <div class="grid-cards">
    <div class="card"><div class="label">最短平均停机时间</div><div class="value">50.12 ms</div></div>
    <div class="card"><div class="label">最长平均停机时间</div><div class="value">87.43 ms</div></div>
    <div class="card"><div class="label">最大阻塞拷贝均值</div><div class="value">1725.70 ms</div></div>
    <div class="card"><div class="label">最大均值倍率</div><div class="value">{best_ratio:.1f}x</div></div>
  </div>

  <section>
    <h2>迁移停机时间对比</h2>
    <p>停机时间按作者 Figure 10 的思路计算：<strong>migrate_in - migrate_out_stage_a</strong>。阻塞式 KV 拷贝按阶段时间差计算：<strong>migrate_out_stage_a - migrate_out_stage_0</strong>。</p>
    <div class="chart-wrap">
      {svg_line_chart(summary)}
    </div>
    <div class="note">2048 组 benchmark 完成，但当前配置下没有触发有效迁移事件，因此不参与图线统计。</div>
  </section>

  <section>
    <h2>汇总数据</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>序列长度</th>
            <th>完整迁移数</th>
            <th>停机均值 ms</th>
            <th>停机 P50 ms</th>
            <th>停机 P95 ms</th>
            <th>阻塞拷贝均值 ms</th>
            <th>阻塞拷贝 P50 ms</th>
            <th>阻塞拷贝 P95 ms</th>
          </tr>
        </thead>
        <tbody>
          {build_summary_table(summary)}
        </tbody>
      </table>
    </div>
  </section>

  <section>
    <h2>实验结论</h2>
    <p>在 256 到 1536 的有效范围内，阻塞式 KV 拷贝耗时从 379.61 ms 增长到 1725.70 ms，而 Llumnix 迁移停机时间从 50.12 ms 增长到 87.43 ms。趋势与论文 Figure 10 的主结论一致：迁移停机时间增长较慢，阻塞拷贝随序列长度显著增加。</p>
  </section>

  <section>
    <h2>原始迁移明细</h2>
    <details>
      <summary>展开 {len(raw)} 条原始迁移记录</summary>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>序列长度</th>
              <th>请求 ID</th>
              <th>块数</th>
              <th>阻塞拷贝 ms</th>
              <th>停机时间 ms</th>
            </tr>
          </thead>
          <tbody>
            {build_raw_table(raw)}
          </tbody>
        </table>
      </div>
    </details>
    <div class="foot">数据来源：{html.escape(str(DATA_DIR.name))} / final_migration_efficiency_summary.csv 与 final_migration_efficiency_raw.csv。</div>
  </section>
</main>
</body>
</html>
"""
    OUT_HTML.write_text(html_doc, encoding="utf-8")
    print(OUT_HTML)


if __name__ == "__main__":
    main()
