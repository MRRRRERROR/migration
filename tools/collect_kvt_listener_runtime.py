#!/usr/bin/env python3
"""Collect KVT listener runtime evidence without issuing inference requests."""

from __future__ import annotations

import os
import re
import socket
import subprocess
from datetime import datetime
from pathlib import Path


root = Path(os.environ["LOAD_SWEEP_ROOT"]).resolve()
logs = Path("/tmp/llumnix/logs")
destination = root / "04_step_summaries" / "kvt_listener_runtime_check.md"


def socket_check(host: str, port: int) -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        sock.connect((host, port))
        return "success"
    except Exception as exc:
        return f"failed: {type(exc).__name__}: {exc}"
    finally:
        sock.close()


ss_output = subprocess.run(["ss", "-ltnp"], text=True, capture_output=True).stdout
related = [
    line for line in ss_output.splitlines()
    if any(token in line.lower() for token in ("36218", "36226", "36234", "36242", "python", "ray", "vllm", "kvt", "barex"))
]

pattern = re.compile(
    r"listen|listener|server|bind|port|36242|kvt|tx_stub|rx_stub|connector|XSimpleTcpConnector",
    re.IGNORECASE,
)
log_hits = []
advertised = set()
for path in sorted(logs.glob("*.log")):
    for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        if pattern.search(line):
            log_hits.append(f"{path.name}:{number}: {line}")
        advertised.update(re.findall(r"(?:127\.0\.0\.1|10\.112\.184\.62):(?:36218|36226|36234|36242)", line))

lines = [
    "# KVT Listener Runtime Check",
    "",
    f"- 检查时间：{datetime.now().astimezone().isoformat(timespec='seconds')}",
    f"- VLLM_HOST_IP：{os.environ.get('VLLM_HOST_IP', 'UNSET')}",
    f"- advertised endpoints：{', '.join(sorted(advertised)) or '未找到'}",
    f"- socket 10.112.184.62:36242：{socket_check('10.112.184.62', 36242)}",
    f"- socket 127.0.0.1:36242：{socket_check('127.0.0.1', 36242)}",
    "",
    "## ss -ltnp 相关监听",
    "",
    "```text",
    *(related or ["未发现相关监听"]),
    "```",
    "",
    "## Listener / KVT 日志（最后 200 条）",
    "",
    "```text",
    *log_hits[-200:],
    "```",
]
destination.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(destination)
print("\n".join(lines[:20]))
