# KVT500 真实代码路径

## 本次运行使用的路径

1. `llumnix/manager.py::_migrate` 异步调用 source instance 的 `migrate_out`。
2. `llumnix/llumlet/llumlet.py:migrate_out` 用 `asyncio.create_task` 启动迁移；启用 engine migration interface 时，`MigrationCoordinator.migrate_out` 转到 `BackendVLLMV1.migrate_out`。
3. `remote_policy_src/kvt_migration_frontend.py:migrate_out` 从 scheduler state queue 读取最新 running/waiting snapshot；`get_migrated_requests` 只选择 request/token 状态。
4. `_dispatch_and_collect_tasks` 为 request 创建异步 `migrate` coroutine；`migrate` 序列化 `EngineCoreRequest` 发给 destination。request_id 来自 `req.request_id`；此消息不携带 source block-ID 快照。
5. destination `blade_kvt.hybrid_connector.migration.backend.migrate` 读取 `OUTPUT_TOKENS_N`，在 destination scheduler 预分配 KV blocks；`prealloc_ack code=0` 只确认 destination allocation。`stage_copy_begin.block_count` 也是 destination `blockids` 的计数。
6. destination `_kvt_suspend` 构造 `KVTDInfo(blkids=destination blockids, cached_tokens=..., max_tokens=...)`，经 `_kvt_rpc` 调用 source。
7. source `kvtbackend.submit_transfer_kv` 将 `RKVTDInfo` 放入 `_dinfoq` 并唤醒 scheduler/core。
8. source `kvtbackend.build_backend_meta` 调用 `_step_dinfoq`。对 migration request，`_step_dinfoq` 设置 `KVTState`，调用 `try_setup_save(req)`，随后进入 `_migration_bypass`。
9. `_migration_bypass` 此时才调用 `get_kvblk_ids(self, reqid)`；未传 `blks` 时它调用 `engine_proxy.sched_get_kvblk_ids(reqid)`，最终读取 source scheduler 的 `kv_cache_manager` block table。这个查询与步骤 3 的选择快照不是同一快照。
10. `_migration_bypass` 生成 `PReqMeta(p_block_ids=source live lookup, d_block_ids=KVTDInfo.blkids, seen_tokens=destination cached_tokens, ...)`。现有断言只检查 source 外层 list 非空，没有检查每个 inner vector 是否覆盖要传的 token 区间。
11. `_build_req_send_batch` 将二者写入 `bladekv.ReqMeta.src_block_ids/dst_block_ids`；`_start_req_send` 调用 `KVTransferClient.start_req_send`。
12. C++ `client.cpp:create_step_tasks` 创建 freeze task；`tx_stub.cpp` 在 `send_cache` 数据准备中按 `seen_tokens / block_size` 访问 source block vector。失败日志中 index 为 0（seen=0）或 1（seen=16），而 vector size=0，随后返回 `FinishedReq State=2` 和上层 `KVTResp(code=500)`。
13. destination migration backend 将失败作为 `load fail` abort target copy；source request 未被成功 suspend/remove，继续在 source scheduler 生成并最终完成。

## 参数与所有权

| value | producer / owner | consumer | lifecycle risk |
|---|---|---|---|
| request_id | vLLM Request / EngineCoreRequest | frontend、destination backend、source KVT | snapshots and live request are correlated only by string ID |
| destination block IDs | destination prealloc / destination `kv_cache_manager` | `KVTDInfo.blkids` → source KVT ReqMeta | target abort frees them after KVT failure |
| source block IDs | source `kv_cache_manager` | `_migration_bypass/get_kvblk_ids` → ReqMeta | read live after prealloc; scheduler allocation/preemption/finish may replace or free them |
| running selection state | frontend scheduler snapshot queue | LCR selection and token counts | asynchronous and may be stale by the time source blocks are queried |
| `_infly_kvt` | source `kvtbackend` | delta/final metadata sequencing | first freeze migration path can send directly without retaining a stable block snapshot; later updates may become stale after task completion |

## 哪些函数可改变或失效 block 状态

- vLLM scheduler scheduling/allocation path: `kv_cache_manager.allocate_slots`, block-table update and preemption handling.
- vLLM finish path: `Scheduler.finish_requests` / KV cache manager free; exact installed vLLM source is not present in the diagnostic pack, so the report does not invent a line number.
- blade_kvt `try_setup_save` and scheduler integration: changes connector/save state, but current local evidence does not show it frees blocks.
- blade_kvt `_step_finished_reqs`: removes `_infly_kvt` state and emits final/abort metadata after `SchedulerOutput.finished_req_ids`; it does not establish that user finish preceded this failure.
- frontend `update_snapshots` / `clear_finished_reqs`: replaces selection snapshots and deletes cached EngineCoreRequest objects after requests disappear from scheduler; it does not own KV blocks.

## Legacy path distinction

`llumnix/llumlet/migration_coordinator.py::_migrate_out_onestage` has an older explicit sequence `get_request_incremental_blocks → remove_running_request → pre_alloc_cache → _send_cache`. The Config E logs instead name `kvt_migration_frontend.py` and `blade_kvt.hybrid_connector.migration.backend/kvtbackend`; therefore the older coordinator path is useful lifecycle context but is not treated as the direct failing call chain.
