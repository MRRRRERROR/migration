# Experiment Analysis Summary

## Files analyzed

- `opt-1.3b_20260628_153807/`: previous valid opt-1.3B run with Llumnix migration enabled.
- `baseline_first2_20260701/baseline_sim_opt-1.3b_20260701_192646/`: latest baseline first two groups.

## Validity check

| Group | Valid benchmark? | Evidence |
|---|---:|---|
| previous opt-1.3B Llumnix run | yes | `successful_responses 300`; complete latency and migration logs |
| baseline rr_no_migration | no | benchmark failed: `Cannot connect to host 127.0.0.1:1234 ssl:default [Connect call failed ('127.0.0.1', 1234)]` |
| baseline load_no_migration | no | benchmark failed: `Cannot connect to host 127.0.0.1:1234 ssl:default [Connect call failed ('127.0.0.1', 1234)]` |

The two baseline folders contain startup logs, but `server_instance.csv` has only 2 rows and 2 rows respectively, while the valid run has 3585 rows. They should not be used for performance comparison.

## Previous opt-1.3B Llumnix run

- Requests completed: 300
- Duration: 103.8344 s
- Effective QPS: 2.8892
- Throughput: 2961.89 tokens/s
- Prompt tokens: 153781
- Response tokens: 153765
- Median E2E latency: 50.9946 s
- Mean E2E latency: 49.0952 s
- P95 E2E latency: 71.8895 s
- Max E2E latency: 74.9674 s
- Median per-token latency: 0.1020 s/token

## Migration summary for previous run

- Migration attempts: 34
- Finished migrations: 30
- Aborted/non-finished migrations: 4
- Finished migration cost mean: 271.36 ms
- Finished migration cost p50: 267.49 ms
- Finished migration cost p95: 398.16 ms
- Finished migration cost max: 539.54 ms
- Migrated blocks mean/p50/max: 40.50 / 39.00 / 67
- KV receive events: 64
- Avg received KV size: 65.44 MB
- Avg KV receive speed: 0.5376 GB/s
- Median KV receive speed: 0.5000 GB/s

## Interpretation

The only currently usable performance dataset is the previous opt-1.3B Llumnix run. It demonstrates that the system can complete a 300-request workload with real migration records and measurable KV transfer cost. The latest baseline first two groups are not comparable yet because their benchmark clients never connected to the serving endpoint.

For a clean baseline comparison, rerun `rr_no_migration` and `load_no_migration` with a startup guard that waits until both Ray instances exist and TCP port `127.0.0.1:1234` is connectable before launching `benchmark_serving.py`.
