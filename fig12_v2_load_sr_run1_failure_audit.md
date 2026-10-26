# Figure 12 v2 Load-SR run_1 失败审计

- 审计对象：`02_fig12_fragmentation_v2/formal_selected/forced_fragmentation/config_c/load_sr/run_1`
- 结论：原轮次为 `invalid_incomplete`，不得计入正式性能结果；唯一一次相同配置重试通过，原失败归档为 `transient_kvt_rpc_failure`，Load-SR 不标记为 stability-limited。

## 1. 第一个 KVTResp(code=500)

- request_id：`cmpl-fig12v2_formal_selected_config_c_occupancy_127-0`
- 源日志：`vllm_1.log:3945`
- 目标 endpoint：`policy_load_sr_1`，`127.0.0.1:36226`
- 后端直接错误：`vllm_3.log:3741` 中 `KVT tx_stub fail to send data ... vector::_M_range_check: __n 0 >= size 0`。
- 随后出现 `migration backend async_update failed ... migration backend kvt rpc failed`，并以 `reason='load fail'` 中止该请求。

## 2. 与 399/400 的对应关系

该 request 正是 399/400 中唯一未成功请求。逐请求记录显示：HTTP 200、文本非空，但 `request_status=abort`、`finish_reason=abort`，因此不能计为成功。其余 399 个请求均为 `finish_reason=length`。

## 3. abort=8 的语义

- 请求级 `load fail` abort：8 次，均为 `output=True`。
- 正常迁移暂停 `migration.suspend`：324 次，均为 `output=False`，不属于请求失败。
- 8 个 load-fail 请求中，仅 `occupancy_127` 最终为 abort；其余 7 个请求仍在健康路径完成。

## 4. failed_migration=17 的原因分布

`failed_migration_count=17` 是事件计数，不等于 17 个唯一失败请求：

| 原因/证据 | 次数 | 说明 |
|---|---:|---|
| load fail | 8 | 8 个请求级迁移失败结果 |
| KVT rpc failed | 8 | 与上述 8 个 load fail 对应，属于同一批失败链路，不应再次按唯一请求相加 |
| KVTResp(code=404) | 7 | 对应 8 个未完成迁移中的 7 个 |
| KVTResp(code=500) | 1 | 对应 `occupancy_127` |
| stale_migration_update | 1 | 状态已清理后的迟到更新，guard 已安全忽略 |
| invalid_migration_endpoint | 0 | 本轮未出现 |
| destination capacity insufficient | 0 | 日志未发现明确记录 |

parser 的 17 次可由 `8 load fail + 7 KVT404 + 1 KVT500 + 1 stale` 解释；这些类别存在因果重叠，不能宣称有 17 个独立失败请求。

## 5. KVT500 前目标实例负载

调度日志显示迁移目标为 `policy_load_sr_1`，失败前目标实例处于繁忙服务状态。该轮 `target_overload_after_migration_count=0`，没有直接的“目标过载”判定证据，因此不能断言 KVT500 由目标容量不足造成。可以确认的是，高并发下目标端同时承接正常请求与频繁迁移，接收压力明显高于 no-migration。

## 6. 并发迁移压力

- triggered migration：349
- prealloc/copy begin：331
- migration_done：323
- migration.suspend：324
- 331 次开始与 323 次完成之间相差 8，与 8 个 load-fail 请求一致。

这说明 Config C 下 Load-SR 产生了密集迁移活动，存在多请求迁移给目标端造成接收压力和状态竞争的可能；当前证据支持“高迁移并发相关”，但不足以认定为唯一根因。

## 7. no_migration 的碎片化与 HOL

no_migration 三轮均有效，并明确产生非零碎片化和 HOL blocking：

| 轮次 | fragmentation 非零采样数 | blocked HOL 累计数 | long-input TTFT P99 (s) |
|---|---:|---:|---:|
| run_1 | 399 | 1520 | 10.35977 |
| run_2 | 395 | 1541 | 10.67720 |
| run_3 | 397 | 1533 | 10.68011 |

因此 Config C 已实现 Figure 12 v2 所需的碎片化与长输入阻塞场景。

## 8. Config C 是否仍适用

适用。no_migration 3/3 严格有效，且 pressure sweep 阶段已验证 Config C 能同时制造非零碎片化、HOL blocking 和可测 long-input TTFT。当前失败应归于 Load-SR 在该压力窗口下的迁移稳定性，而不是 workload 无效。

## 9. Load-SR 定性

唯一一次受控重试达到 400/400、文本非空 400/400、最终 request abort=0、KVT500=0、runtime fatal/Traceback=0。原失败归档为 `transient_kvt_rpc_failure`，Load-SR 不标记为稳定性受限基线，可以继续 run_2/run_3。

## 唯一重试结果

- 结果目录：`02_fig12_fragmentation_v2/retry_load_sr_run1_once/forced_fragmentation/config_c/load_sr/run_1`
- successful responses：400/400
- HTTP 200 且文本非空：400/400
- 最终 request abort（`finish_reason=abort`）：0
- 内部迁移 load-fail abort：6；均回退后完成请求，不属于最终请求中止
- KVTResp：code=0 为 320，code=404 为 6，code=500 为 0
- EngineCore/Scheduler fatal、runtime Traceback、CUDA illegal memory access：均为 0
- fragmentation 非零采样：439；blocked HOL 累计数：953
- long-input TTFT P99：6.45809 s
- fairness：fair，gpu_contention=false
