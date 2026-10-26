# KVT500 Root Cause Report

## 1. 已确认现象

Config E C32 的 5 个失败请求均完成 destination prealloc（code=0），随后 source KVT send task 在 `tx_stub.cpp:199` 访问 size=0 的 vector 并返回 KVTResp 500。异常下标分别为 0 或 1，严格对应 `seen_tokens / block_size`（0/16 或 16/16）。

`stage_copy_begin.block_count` 的 11/5/18/14/7 是 destination preallocated blocks，不是 source block table。因此“prealloc 成功且 block_count 非零”与“source vector 为空”并不矛盾。

## 2. 真实代码路径

`kvt_migration_frontend` 从异步 scheduler snapshot 选择 request，只传 request/token 状态 → destination `migration.backend.migrate` 预分配 destination blocks → destination `_kvt_suspend` 把 destination block IDs 发给 source → source `kvtbackend.submit_transfer_kv` 入 `_dinfoq` → `build_backend_meta` → `_step_dinfoq` → `try_setup_save` → `_migration_bypass` → `get_kvblk_ids` → `sched_get_kvblk_ids` 实时读取 source `kv_cache_manager` → `PReqMeta.p_block_ids` → `_build_req_send_batch` → `KVTransferClient.start_req_send` → `client.cpp:create_step_tasks` → `tx_stub.cpp` send → KVTResp。

Python 中首次可能产生/接纳空 source vector 的具体函数是 `blade_kvt.hybrid_connector.kvtbackend._migration_bypass`。当前代码只检查 source block groups 的外层 list 非空，没有验证 inner vector 或所需 index。

## 3. 五个失败 request 的共同点

- 全部来自 source instance 31103；destination 分布为 instance 0 两次、instance 2 三次。
- 每个 request 只被选择迁移一次，排除重复 selection 为共同原因。
- destination prealloc 都成功且 destination block_count 非零。
- source task 均为 freeze=1；C++ 在 task 创建后立即因 source-side size=0 vector 失败。
- empty 前没有 request 级 FinishedReq、finish_request、free、scheduler remove 或 user final completion 证据。
- KVT500 后 source request 继续生成，最终 5/5 success。这说明原 source 没有被成功 suspend/remove，失败回退保留了 source 推理。

## 4. 已排除原因

- destination prealloc rejection：5/5 code=0。
- IPC timeout、IncompleteReadError、Python RuntimeError/Traceback、EngineCore fatal：均无对应证据。
- 重复迁移 selection：每 request 的 migration_initiated=1。
- `FinishedReq` 先触发 block free：`FinishedReq State=2` 总在 empty error 之后，是失败 KVT task 的完成通知。
- 用户请求已完成：五个 request 的最终 generated_len 事件都在 KVT500 之后，且中间仍有 source token 增长。

## 5. 当前最有证据的根因候选

最可能是 source scheduler/KV manager 与异步 migration 生命周期不同步：选择阶段依据 request/token snapshot，destination prealloc 完成后，source `_migration_bypass` 才按 request_id 实时查询 block table；两者不是同一快照，也没有 block pin/lock。该查询返回了含 empty inner vector 的 source block IDs，代码未校验便传给 C++，最终 `tx_stub` 失败。

目前无法进一步确认空 vector 是由临时 block-table update/replacement、preemption/recompute 可见性窗口，还是 `sched_get_kvblk_ids` / block-group 适配形状错误造成。

## 6. 还缺哪一个关键证据

缺少同一 attempt 在三个点的 source block-ID group shape：迁移 selected 时、prealloc ack 后立即、`_migration_bypass` / send 前立即。只要补齐这三个观测点，并同时记录 scheduler membership/status 与所有 block mutation，就能确定空值在哪里首次出现。

现有证据不足以安全选择最终修复策略；足以加入诊断性 invariant，但不应据此直接决定 snapshot+pin、retry、cancel 或 adapter shape fix。

## 7. 下一轮最小埋点方案

在 frontend selection、destination prealloc ack、source `submit_transfer_kv`、`_step_dinfoq` 的 `try_setup_save` 前后、`_migration_bypass` 的 `get_kvblk_ids` 后、`_build_req_send_batch`、C++ `create_step_tasks/tx_stub`、vLLM finish/preempt/free/update 路径打印同一 attempt_id。字段必须含 request status、finished、computed tokens、scheduler membership、`_infly_kvt` membership、每个 block group 的长度与 IDs、以及 C++ 实际访问的 group/block index。不要只打印总 block_count。
