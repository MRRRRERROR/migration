# RTX 5090 Profiling Data Audit

- Generated: `2026-08-20T05:59:15.006999+00:00`
- Source logs: `9`
- Parsed request samples: `4500`
- Source condition: `Hybrid_no_migration`, 4x RTX 5090, Qwen2.5-7B-Instruct
- Aggregation: up to 100 nearest measured samples per grid point; p50 feeds the scheduler
- Low-load ITL p50 (batch <= 8): `14.000 ms`
- High-load ITL p50 (batch >= 32): `15.000 ms`
- TTFT grid: `1, 8, 16, 32, 64, 128, 256, 512, 1024, 2048`
- TPOT batch grid: `1, 2, 4, 8, 16, 32, 50, 64`
- TPOT context grid: `8, 16, 32, 64, 128, 256, 512, 1024`

This file is derived from real completed requests. It is not an imported H20 profile and does not fabricate migration evidence.
