# KVT500 Instrumentation Points

All changes are structured, read-only diagnostic logging. They do not alter migration selection, allocation, transfer, retry, timeout, or error handling.

| Event | File | Function | Line | Key fields | Hypothesis tested |
|---|---|---|---:|---|---|
| `MIGRATION_SELECTED` | `project/python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py` | `migrate` | 550 | attempt/request/source/destination, token state | Selection eligibility and initial snapshot |
| `PREALLOC_BEGIN` | same | `migrate` | 615 | requested blocks, source/destination | Frontend-to-destination handoff |
| `PREALLOC_ACK` | same | `migrate` | 628 | response code, destination readiness | Whether preallocation succeeded before source failure |
| `PREALLOC_BEGIN` | `runtime_overlay/blade_kvt/hybrid_connector/migration/backend.py` | `_on_migrate_to` | 433 | requested blocks, source/destination | Destination receipt of preallocation request |
| `PREALLOC_ACK` | same | `_on_migrate_to` | 464 | response code; destination blocks unavailable at this protocol point | Exact destination acknowledgement semantics |
| `SOURCE_TRANSFER_SUBMIT` | `runtime_overlay/blade_kvt/hybrid_connector/kvtbackend.py` | `submit_transfer_kv` | 1192 | scheduler and inflight membership, token state | State when asynchronous source transfer is submitted |
| `BEFORE_SOURCE_BLOCK_LOOKUP` | same | `_migration_bypass` | 1363 | request state, memberships | Source state immediately before lookup |
| `SCHED_GET_RAW_BLOCKS` | same | `get_kvblk_ids` | 231 | complete raw group shape and IDs | Scheduler/KV-manager return value |
| `SCHED_GET_ADAPTED_BLOCKS` | same | `get_kvblk_ids` | 259 | complete adapted group shape and IDs | Hybrid block-shape adapter behavior |
| `AFTER_SOURCE_BLOCK_LOOKUP` | same | `_migration_bypass` | 1376 | complete source group shape and IDs | First empty-vector boundary |
| `MIGRATION_BYPASS_BLOCKS` | same | `_migration_bypass` | 1390 | groups and access indices | Migration bypass construction |
| `PREQMETA_BLOCKS` | same | `_migration_bypass`, `_build_req_send_batch` | 1415, 1858 | complete PReqMeta block shape | PReqMeta preservation |
| `SEND_BATCH_BLOCKS` | same | `_build_req_send_batch` | 1895 | batch block shape and access indices | Python batching transformation |
| `PYTHON_SEND_BOUNDARY` | same | `_start_req_send` | 1944 | final Python-side group shape | Python/C++ boundary |
| `KVTResp500` | same | `_wait_kvt_state` | 1175 | response code, request/inflight membership | Correlation with the failed attempt |
| `BLOCK_MUTATION:PREEMPT` | `runtime_overlay/vllm/v1/core/sched/scheduler.py` | `_preempt_request` | 970, 986 | before/after groups and request state | Preemption race |
| `FINISH_REQUEST` | same | `finish_requests` | 1838 | state before terminal handling | Finish ordering |
| `REQUEST_FINISHED` | same | `_free_request` | 1866 | state before free | Terminal lifecycle ordering |
| `BLOCK_MUTATION:FREE` | same | `_free_blocks` | 1892 | before/after groups | Free race |
| `SCHEDULER_REMOVE` | same | `_free_blocks` | 1906 | post-remove membership | Scheduler removal ordering |
| `BLOCK_MUTATION` | `runtime_overlay/vllm/v1/core/kv_cache_manager.py` | `allocate_slots`, `free`, `remove_skipped_blocks` | 363-496 | before/after groups and mutation type | Allocation/update/free transitions |

The destination preallocation protocol acknowledges before scheduler allocation in this code version. Therefore the destination events explicitly record that destination block IDs are not yet available and never label them as source blocks.
