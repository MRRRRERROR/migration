# Stale Migration State Guard 验证报告

## 修改范围

- 修改源码：是，仅修改 `blade_kvt/hybrid_connector/kvtbackend.py`。
- 未修改 vLLM EngineCore、Llumnix 调度算法、KVT 传输协议或迁移状态机。
- 备份：`/data/home/zylu/tmp/blade-kvt/backups/inflight_state_guard_20260822_151900/kvtbackend.py`。
- 根因审计：`01_audit/INFLY_KVT_STATE_MISS_ROOT_CAUSE.md`。
- 完整 diff：`01_audit/blade_kvt_stale_guard.diff`。

## 保护逻辑

`PBackend._update_infly_kvt()` 现在返回布尔值。后续 meta 的 `request_id` 不在 `_infly_kvt` 时：

1. 记录 `event=stale_migration_update`、`reason=migration_state_miss`；
2. 记录请求 ID、源实例、目标实例和 token 进度；
3. 返回 `False`；
4. 三个 scheduler 调用点不把 stale meta 加入 `nonfreeze_metas`；
5. 不重建状态、不访问缺失项、不产生 KVT 成功响应、不主动 abort 健康源请求。

## 最小构造验证

- stale meta 返回：`False`
- `_infly_kvt` 保持为空
- 正常首段/末段状态机：通过
- 假成功迁移：未产生
- 结果：通过

日志：`01_audit/stale_guard_minimal_test.log`

## tpot_tail Smoke

配置：50 请求，并发 20。

- 成功请求：50/50
- 空文本：0
- KVTResp(code=0)：18
- KVTResp(code=500)：0
- FinishedReq：18
- migration_end：18
- abort：0
- EngineCore fatal：0
- Traceback：0
- ERROR：0
- stale_migration_update：0

结果：通过。

## tpot_tail 500 请求单轮重跑

配置：500 请求，并发 50，仅重跑原失败的 `tpot_tail` 第 1 轮，结果保存在独立 retry 目录。

- 成功请求：500/500
- 空文本：0
- KVTResp(code=0)：91
- KVTResp(code=500)：0
- FinishedReq：91
- migration_end：91
- abort：0
- EngineCore fatal：0
- CUDA illegal memory access：0
- stale_migration_update：0
- Traceback：2
- ERROR：72

本轮未复现 `_infly_kvt` AssertionError，也没有 EngineCore fatal。出现了一次独立的迁移目标寻址失败：

```text
dst_host=''
dst_port=0
socket.gaierror: [Errno -5] No address associated with hostname
Migration dispatch summary: 0 accepted, 0 rejected, 1 failed.
```

该请求留在源端并最终完成，因此 500/500 请求成功，且没有 KVT 500 或 abort；但由于验收标准要求 `Traceback=0`，本轮不能判定为完全通过。

## 结论

- stale state guard 的构造测试和 50 请求 smoke 通过。
- 500 请求重跑消除了原有 AssertionError/EngineCore fatal，但出现独立的空目标地址迁移失败。
- 当前不继续 Figure 12 剩余实验，也不启动 Figure 13。
- 下一步应单独审计目标实例 endpoint 生成/失效时序；这不属于本次 stale-state 最小修复范围。
