# KVT500 Root Cause Confirmation

## 1. 实验配置

- Config E C32, `tpot_tail`, 1200 requests, concurrency 32, seed 20260905.
- 独立代码副本与运行时 overlay；主代码树和共享 site-packages 未修改。
- 诊断轮实际进入请求阶段，但因长期无进展在 50/1200 时终止。

## 2. 实际复现情况

- KVT500：**未复现**。
- 完整诊断轮：0；部分执行轮：1。
- 请求完成数最后停在 50/1200，`migration_efficiency_events.jsonl` 自 14:10 后不再更新。
- 两次迁移均有 `PREALLOC_ACK(code=0)`，但没有 `migration_done`。

## 3. 失败 attempt 数

- 唯一 KVT500 attempt：0。
- 观测到迁移 attempt：2。

## 4. Source block vector 首次变空的位置

本轮未观察到 source vector 变空：

- `cmpl-config_e_0013-0#mig1`: raw/adapted/lookup/bypass/PReqMeta 均为 `[65]`。
- `cmpl-config_e_0034-0#mig1`: raw/adapted/lookup/bypass/PReqMeta 均为 `[61]`。

两次 attempt 都未进入 `SEND_BATCH_BLOCKS` 或 `PYTHON_SEND_BOUNDARY`，因此本轮不能判断原 KVT500 的首次空向量 stage。

## 5. 三个核心观测时点

| attempt | prealloc ack | source lookup | PReqMeta/send before |
|---|---|---|---|
| `cmpl-config_e_0013-0#mig1` | code=0，destination blocks 尚未在 ACK 时分配 | `[65]`，完整 ID 已保存 | PReqMeta=`[65]`；未到 send batch |
| `cmpl-config_e_0034-0#mig1` | code=0，destination blocks 尚未在 ACK 时分配 | `[61]`，完整 ID 已保存 | PReqMeta=`[61]`；未到 send batch |

## 6. 是否存在 block mutation

空向量出现前没有 mutation，因为本轮没有出现空向量。两次 attempt 在 source lookup 后只记录到不改变 shape 的 `ALLOCATE_OR_APPEND`。第一个请求的 `FREE/REMOVE` 是终止实验约两小时后才发生。

## 7. 是否存在 preempt/recompute/free

- `PREEMPT`: 未观察到。
- `RECOMPUTE`: 未观察到。
- `FREE/REMOVE`: 仅在人工终止后的清理路径中观察到，晚于迁移停滞，不能解释原 KVT500。

## 8. 是否是 adapter shape 问题

本轮两个 attempt 的 raw 与 adapted shape 完全一致，分别为 `[65]` 和 `[61]`。当前证据不支持 adapter 在这两个 attempt 中制造空向量，但因未复现 KVT500，不能全局排除。

## 9. 是否是 Python -> C++ 问题

两次 attempt 均未到 `PYTHON_SEND_BOUNDARY`，无法验证绑定层。不能确认，也不能排除原 KVT500 的 Python/C++ 边界问题。

## 10. 最终根因等级

**POSSIBLE / UNRESOLVED**

本轮没有复现目标故障，不能将 lifecycle race、adapter bug 或 binding bug 提升为 `HIGHLY_LIKELY` 或 `CONFIRMED`。

## 11. 精确涉及函数

- `blade_kvt.hybrid_connector.kvtbackend.PBackend._migration_bypass`
- `blade_kvt.hybrid_connector.kvtbackend.get_kvblk_ids`
- `blade_kvt.hybrid_connector.kvtbackend.PBackend._build_req_send_batch`
- `blade_kvt.hybrid_connector.kvtbackend.PBackend._start_req_send`
- `vllm.v1.core.kv_cache_manager.KVCacheManager.allocate_slots/free`
- `vllm.v1.core.sched.scheduler.Scheduler._free_blocks`

本轮实际停滞边界位于 `PReqMeta` 创建之后、`_build_req_send_batch` 埋点之前。

## 12. 修复建议

本任务未实施修复。当前证据不足以进入 KVT500 修复阶段。下一步应先解释或消除这次 `PReqMeta -> SEND_BATCH` 前的迁移停滞，再以相同 logging-only 埋点完成一轮自然结束的 C32 复现；只有捕获 KVT500 attempt 后，才能按首次空向量 stage 设计修复。

