#!/usr/bin/env python3
from html.parser import HTMLParser
from pathlib import Path
from datetime import datetime
import html


DESKTOP = Path.home() / "Desktop"
SRC = DESKTOP / "llumnix_5090_4inst_metrics_table.html"
DST = DESKTOP / "llumnix_5090_4inst_metrics_table_compact.html"

HEADERS = [
    "指标类别",
    "指标名称",
    "Baseline 第 1 轮",
    "Baseline 第 2 轮",
    "Baseline 第 3 轮",
    "Baseline 均值/合计",
    "Migration 第 1 轮",
    "Migration 第 2 轮",
    "Migration 第 3 轮",
    "Migration 均值/合计",
    "单位",
    "数据来源/备注",
]

DROP_METRICS = {
    "Driver 版本",
    "mean 端到端时延",
    "总输入 token 数",
    "总输出 token 数",
    "输出 token/s",
    "migration_count 字面字段",
    "迁移 abort 次数",
    "downtime",
    "平均 downtime",
    "P95 downtime",
    "总迁移时间",
    "RuntimeError 数量",
    "CUDA illegal memory access 数量",
    "benchmark error 数量",
}


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_td = False
        self.in_th = False
        self.in_tr = False
        self.cell = []
        self.row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.in_tr = True
            self.row = []
        elif tag in {"td", "th"} and self.in_tr:
            self.in_td = tag == "td"
            self.in_th = tag == "th"
            self.cell = []

    def handle_endtag(self, tag):
        if tag in {"td", "th"} and (self.in_td or self.in_th):
            text = " ".join("".join(self.cell).split())
            self.row.append(text)
            self.in_td = False
            self.in_th = False
        elif tag == "tr" and self.in_tr:
            if self.row:
                self.rows.append(self.row)
            self.in_tr = False

    def handle_data(self, data):
        if self.in_td or self.in_th:
            self.cell.append(data)


def clean(v):
    return v.strip() if isinstance(v, str) else ""


def is_missing(v):
    return clean(v) in {"未记录", "NA", "Missing", ""}


def compact_series(values, summary):
    vals = [clean(v) for v in values]
    summary = clean(summary)
    if all(v == "-" for v in vals) and summary == "-":
        return "-"
    if all(is_missing(v) for v in vals) and is_missing(summary):
        return ""
    parts = []
    if not all(v == "-" for v in vals):
        parts.append(" / ".join(vals))
    if summary and summary not in {"-", ""}:
        label = "均值/合计"
        parts.append(f"{label}: {summary}")
    return "；".join(parts) if parts else "-"


def parse_rows():
    parser = TableParser()
    parser.feed(SRC.read_text(encoding="utf-8"))
    data_rows = []
    for row in parser.rows:
        if row == HEADERS or len(row) != len(HEADERS):
            continue
        item = dict(zip(HEADERS, row))
        if item["指标名称"] in DROP_METRICS:
            continue
        baseline = compact_series(
            [item["Baseline 第 1 轮"], item["Baseline 第 2 轮"], item["Baseline 第 3 轮"]],
            item["Baseline 均值/合计"],
        )
        migration = compact_series(
            [item["Migration 第 1 轮"], item["Migration 第 2 轮"], item["Migration 第 3 轮"]],
            item["Migration 均值/合计"],
        )
        if baseline == "" and migration == "":
            continue
        data_rows.append({
            "category": item["指标类别"],
            "metric": item["指标名称"],
            "baseline": baseline,
            "migration": migration,
            "unit": item["单位"],
            "note": item["数据来源/备注"],
        })
    return data_rows


def status_class(metric, value):
    if metric in {"EngineCore fatal 数量", "Traceback 数量", "ERROR 数量"}:
        return "ok" if value in {"0", "0；均值/合计: 0"} or "0" in value else "bad"
    return ""


def write_html(rows):
    by_cat = {}
    for row in rows:
        by_cat.setdefault(row["category"], []).append(row)

    out = []
    out.append("<!doctype html>")
    out.append("<html lang=\"zh-CN\">")
    out.append("<head>")
    out.append("<meta charset=\"utf-8\">")
    out.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
    out.append("<title>Llumnix RTX 5090 四实例实验指标总表</title>")
    out.append("""
<style>
:root{
  color-scheme: light;
  --bg:#f5f7fb;
  --panel:#ffffff;
  --text:#18212f;
  --muted:#5d6777;
  --line:#d7deea;
  --head:#1d2a3d;
  --blue:#eaf4ff;
  --orange:#fff2df;
  --green:#e3f7e9;
  --red:#fde8e8;
}
*{box-sizing:border-box}
body{
  margin:0;
  padding:24px;
  background:var(--bg);
  color:var(--text);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",Arial,sans-serif;
}
main{max-width:1180px;margin:0 auto}
h1{margin:0 0 6px;font-size:24px;font-weight:700}
.dir{margin:0 0 18px;color:var(--muted);font-size:13px;word-break:break-all}
section{margin:0 0 18px}
h2{margin:0;padding:10px 12px;background:var(--head);color:#fff;font-size:15px;font-weight:600;border-radius:8px 8px 0 0}
table{width:100%;border-collapse:collapse;background:var(--panel);table-layout:fixed;border:1px solid var(--line);border-top:0}
th,td{border:1px solid var(--line);padding:8px 9px;font-size:12.5px;vertical-align:top;line-height:1.45;overflow-wrap:anywhere}
th{background:#f1f4f8;color:#273244;font-weight:700}
th.metric{width:18%}
th.base{width:24%;background:var(--blue)}
th.mig{width:24%;background:var(--orange)}
th.unit{width:7%}
th.note{width:27%}
td.metric{font-weight:650}
td.base{background:var(--blue)}
td.mig{background:var(--orange)}
td.ok{background:var(--green);color:#146c2e;font-weight:700}
td.bad{background:var(--red);color:#9b1c1c;font-weight:700}
footer{margin-top:18px;color:var(--muted);font-size:12px}
@media print{
  body{padding:12px;background:white}
  main{max-width:none}
  section{break-inside:avoid}
  th,td{font-size:10.5px;padding:5px}
}
</style>
""")
    out.append("</head><body><main>")
    out.append("<h1>Llumnix RTX 5090 四实例实验指标总表</h1>")
    out.append("<p class=\"dir\">结果目录：/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617</p>")
    for category, cat_rows in by_cat.items():
        out.append("<section>")
        out.append(f"<h2>{html.escape(category)}</h2>")
        out.append("<table>")
        out.append("<thead><tr><th class=\"metric\">指标名称</th><th class=\"base\">Baseline</th><th class=\"mig\">Migration</th><th class=\"unit\">单位</th><th class=\"note\">数据来源/备注</th></tr></thead>")
        out.append("<tbody>")
        for row in cat_rows:
            bcls = status_class(row["metric"], row["baseline"])
            mcls = status_class(row["metric"], row["migration"])
            out.append("<tr>")
            out.append(f"<td class=\"metric\">{html.escape(row['metric'])}</td>")
            out.append(f"<td class=\"base {bcls}\">{html.escape(row['baseline'])}</td>")
            out.append(f"<td class=\"mig {mcls}\">{html.escape(row['migration'])}</td>")
            out.append(f"<td>{html.escape(row['unit'])}</td>")
            out.append(f"<td>{html.escape(row['note'])}</td>")
            out.append("</tr>")
        out.append("</tbody></table></section>")
    out.append(f"<footer><p>自动整理时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p><p>说明：本页面只整理已有实验指标，不包含新增实验或重新运行结果。</p></footer>")
    out.append("</main></body></html>")
    DST.write_text("\n".join(out), encoding="utf-8")


def main():
    rows = parse_rows()
    write_html(rows)
    print(f"source={SRC}")
    print(f"output={DST}")
    print(f"kept_metrics={len(rows)}")
    print(f"removed_metrics={len(DROP_METRICS)}")


if __name__ == "__main__":
    main()
