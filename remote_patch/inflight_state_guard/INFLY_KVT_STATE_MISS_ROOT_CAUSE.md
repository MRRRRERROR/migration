# `_infly_kvt` 状态缺失根因审计

## 故障概述

Figure 12 `fragmentation_medium / tpot_tail / run_1` 在 500 请求、并发 50 的迁移压力下触发：

```text
blade_kvt/hybrid_connector/kvtbackend.py:1409
assert meta.reqid in self._infly_kvt
AssertionError
```

该异常发生在 vLLM EngineCore 调度线程内，因此一个迁移状态缺失会升级为 EngineCore fatal，并进一步产生 KVT 500 和请求 abort。

## 出错函数

出错函数是 `PBackend._update_infly_kvt(meta)`。它维护 scheduler 侧正在分阶段发送 KV 的请求状态：

- 首段 meta 带有 `d_block_ids`，未完成时插入 `_infly_kvt[reqid]`；
- 后续增量 meta 不带 `d_block_ids`，必须更新已有状态；
- `has_last_token=True` 时弹出状态，表示该分阶段发送序列结束。

旧实现假定后续 meta 到达时字典项必然存在，并使用断言维护该假设。

## `meta.reqid` 的来源

`meta.reqid` 来自 vLLM `SchedulerOutput` 中的请求标识：

- 新调度请求：`scheduled_new_reqs[*].req_id`；
- 已缓存请求：`scheduled_cached_reqs.req_ids[*]`。

`_step_sched_req()` 根据请求当前的 `KVTState`、源端 block、目标端 block、已计算 token 和本步新增 token 构造 `PReqMeta`，随后调用 `_update_infly_kvt()`。

## `_infly_kvt` 的插入与删除

插入发生在 `_update_infly_kvt()`：首段 meta 含 `d_block_ids` 且不是最后一段时写入字典。

删除有两条主要路径：

1. `_update_infly_kvt()` 收到 `has_last_token=True` 的后续 meta 时 `pop(reqid)`；
2. `_step_finished_reqs()` 遍历 `SchedulerOutput.finished_req_ids` 时提前 `pop(reqid, None)`，并生成 `abort_metas` 通知 worker 结束该传输。

abort/cancel 路径还会清理或终止请求及发送状态；这些控制事件与 scheduler 输出、异步 KVT 回调并非严格串行。

## 可导致 stale update 的时序

最直接的竞态发生在同一次 `build_backend_meta()`：

1. `build_backend_meta()` 先调用 `_step_finished_reqs()`；
2. 请求出现在 `finished_req_ids` 中，`_infly_kvt` 项被弹出；
3. 随后 `build_backend_meta()` 调用 `_step_sched_req()`；
4. 同一份或滞后的 scheduler 状态仍为该请求生成不含 `d_block_ids` 的增量 meta；
5. `_update_infly_kvt()` 收到后续更新，但对应状态已经因 finish/abort/cancel 被删除。

类似窗口也可能出现在迁移 abort、迁移 cancel、目标端释放或异步 send-done 与下一次 scheduler step 交错时。共同特征是：控制面已经终止迁移，而数据面或调度输出仍携带一个晚到的后续更新。

## 请求完成、迁移终止与目标释放关系

- 请求完成会优先进入 `finished_req_ids`，源端删除 in-flight 状态并产生终止 meta；
- migration abort/cancel 会终止当前迁移，但晚到的 scheduler/KVT 更新仍可能已经排队；
- 目标端释放发生后，继续发送增量没有有效接收状态；
- 因此状态缺失代表该更新已过期，不能重新创建 `_infly_kvt`，也不能计为迁移成功。

## 最小修复点

最小修复限定在 `PBackend._update_infly_kvt()` 及其三个调用点：

1. 将状态存在断言改为显式判断；
2. 缺失时记录 `event=stale_migration_update` 和 `reason=migration_state_miss`；
3. 返回 `False`，调用点不把该 meta 加入 `nonfreeze_metas`；
4. 不访问缺失字典项，不重建迁移状态，不产生 KVT 成功响应；
5. 不主动 abort 源请求，让仍健康的源端执行路径保留完成机会。

该修复只隔离晚到事件，不改变迁移选择、KV 生命周期、EngineCore 或迁移算法。

## 调度层保护建议

以下措施可进一步降低竞态概率，但不属于本次最小修复，暂不实施：

- 每个源实例同时只允许一个 in-flight migration；
- 每个目标实例同时只接收一个 migration；
- 跳过剩余 token 太少的请求；
- 跳过已经 finish、abort、preempt 或迁移状态不稳定的请求。

这些限制会改变迁移并发和策略行为，应在最小状态保护验证完成后单独评估。
