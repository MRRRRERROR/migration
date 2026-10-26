# 下一轮最小埋点方案

目标是一轮实验区分：source block table 临时为空、request/preemption 生命周期变化、block group 形状错误、或 stale migration task。所有日志使用同一个 `migration_event_id/attempt_id`，并包含单调时钟与 wall clock。

| point | canonical file / function | insert position | fields | hypothesis |
|---|---|---|---|---|
| 1 | `llumnix/.../kvt_migration_frontend.py::get_migrated_requests` / selection helper（本地证据：`remote_policy_src/kvt_migration_frontend.py`） | request 被标记 selected 的同一临界区 | request_id, attempt_id, src,dst, scheduler_step, status, finished, computed/output tokens, in_running/in_waiting, `kv_cache_manager.get_block_ids` group lengths + IDs | selection 时 source 是否已有 blocks；H1/H2 |
| 2 | `blade_kvt/hybrid_connector/migration/backend.py::migrate` | 发送 destination prealloc 前 | request_id, attempt_id, snapshot tokens, src,dst, prealloc count | 对齐 snapshot 与 prealloc |
| 3 | 同上，prealloc response code=0 后 | ack time, destination group lengths + IDs, cached/max tokens | 明确 destination only，排除目标分配 |
| 4 | source `blade_kvt/hybrid_connector/kvtbackend.py::submit_transfer_kv` | `_dinfoq.append` 前 | request_id, attempt_id, destination group lengths, cached/max, request exists, scheduler membership, `_infly_kvt` membership | 判断 RPC 到达时 source request 是否仍有效 |
| 5 | `kvtbackend.py::_step_dinfoq` | `try_setup_save` 前后各一次 | status, finished, scheduler step, live source group lengths + IDs, object identity, computed tokens, in running/waiting, `_infly_kvt` | `try_setup_save` 是否改变可见 block state；H1/F |
| 6 | `kvtbackend.py::_migration_bypass` | `get_kvblk_ids` 返回后、构造 `PReqMeta` 前 | full group shape, per-group IDs, required first/last block index derived from seen/new, destination shape, request status, membership | 捕获 Python 中首次 empty；区分 outer/inner shape |
| 7 | `kvtbackend.py::_build_req_send_batch` / `_start_req_send` | 写入 `bladekv.ReqMeta` 前 | same shape plus step/substep/freeze and object/attempt id | 验证 Python→C++ 边界是否改变数据 |
| 8 | C++ `client.cpp::create_step_tasks` | task creation | req_id, step, seen/new, src group count and each group length/IDs, dst equivalent | 定位进入 C++ 后的数据形状 |
| 9 | C++ `tx_stub.cpp` send path | `.at()` 访问前 | requested group/block index, vector identity, size, req_id, step | 精确确认哪个 vector 为空 |
| 10 | vLLM scheduler finish/preempt path (`Scheduler.finish_requests`, preemption handler) | remove/free 前 | request_id, reason, scheduler step, block IDs before free, status, migration flag/attempt | 检验 finish/free/preemption race |
| 11 | KV cache manager free/remove/update functions | 对 request block table mutate 前后 | request_id, operation, old/new group shape, owner/refcount if available, scheduler step | H1/B/A |
| 12 | `kvtbackend.py::_step_finished_reqs` | pop `_infly_kvt` 前后 | request_id, finished_req_ids source, `_infly_kvt` before/after, meta shape | 分清 KVT task finish 与 user request finish |
| 13 | destination migration failure/rollback | abort target copy 和 reset source migration flag 时 | request_id, attempt_id, response code, source/destination state | 证明 fallback，不混淆 target `request_finished` |

最小判定规则：

1. point 1 非空、point 5 前为空且出现 update/free/preempt → 生命周期竞争。
2. point 5 live table 非空、point 6 为空 → `get_kvblk_ids`/hybrid group transformation bug。
3. point 6 非空、point 7/8 为空 → Python→C++ metadata conversion bug。
4. point 8 非空、point 9 空 → C++ task ownership/lifetime bug。
5. point 1 已空但 request reported running → scheduler snapshot 与 KV manager 状态不同步或选取条件错误。

日志需限制为迁移 request，并输出 group lengths 与短 ID 列表；不要仅打印总 block_count，因为本故障是 inner vector 为空而外层可能非空。
