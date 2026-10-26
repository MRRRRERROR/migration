# Empty destination endpoint validation report

## Scope

Only the migration frontend endpoint boundary and experiment-side counters/validation entry points were changed. The migration algorithm, KVT backend, blade_kvt state machine, vLLM EngineCore, CUDA, PyTorch, and vLLM build were not changed.

## Backup and diffs

- Backup: `01_audit/backup_empty_endpoint_guard_20260822/`
- Frontend diff: `01_audit/empty_endpoint_guard_frontend.diff`
- Counter diff: `01_audit/empty_endpoint_guard_counter.diff`
- Runner diff: `01_audit/empty_endpoint_guard_runner.diff`

## Constructed invalid-endpoint test

- Input: `dst_host=''`, `dst_port=0`
- Returned code: `1002` (non-success)
- Connect calls: `0`
- Structured event: `invalid_migration_endpoint`, `migration_status=failed_migration`, `action=skip_socket_connect`
- Fake success: none

## Figure 12 endpoint-guard smoke

- Directory: `01_smoke/figure12_endpoint_guard_smoke/fragmentation_medium/tpot_tail/run_1/`
- Requests/concurrency: `50 / 20`
- Successful and nonempty: `50 / 50`
- KVTResp code 0/404/500: `19 / 0 / 0`
- FinishedReq: `19`
- EngineCore fatal / Traceback / ERROR / abort: `0 / 0 / 0 / 0`
- invalid_migration_endpoint / stale_migration_update: `0 / 0`
- Result: passed

## Figure 12 tpot_tail formal single-run retry

- Directory: `03_runs/figure12_endpoint_guard_retry/fragmentation_medium/tpot_tail/run_1/`
- Requests/concurrency: `500 / 50`
- Successful and nonempty: `500 / 500`
- QPS: `10.521613449992193`
- Output throughput: `2289.5030867183013 tok/s`
- KVTResp code 0/404/500: `92 / 0 / 0`
- FinishedReq lines / unique IDs: `92 / 88`
- EngineCore fatal / Scheduler fatal / Traceback / ERROR: `0 / 0 / 0 / 0`
- RuntimeError / CUDA illegal memory access / abort: `0 / 0 / 0`
- invalid_migration_endpoint / stale_migration_update: `0 / 0`
- Result: passed

## Conclusion

The invalid endpoint branch is fail-closed and cannot enter the socket path or produce a successful migration response. Normal tpot_tail migration remained functional under both validation loads. The failed Figure 12 run can be replaced by the separate endpoint-guard retry for validation purposes, and the remaining Figure 12 experiments may resume after explicit approval.
