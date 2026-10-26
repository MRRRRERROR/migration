# KVT500 根因候选

## Confirmed

### C1. prealloc success 与 source block vector 是两套独立状态

destination `migration.backend.migrate` 先分配自己的 `blockids` 并返回 code=0。`stage_copy_begin.block_count` 记录的也是这组 destination IDs。之后 source 才在 `kvtbackend._migration_bypass` 调用 `get_kvblk_ids(reqid)` 读取 source scheduler/KV manager 的实时 block table。因此 prealloc code=0 不能推出 source vector 非空。

### C2. empty 首次可见于 source KVT C++ send task

五次均先出现 `client.cpp:288 create_step_tasks(freeze=1)`，紧接着 `tx_stub.cpp:199 KVT tx_stub fail to send data`。异常的访问下标严格等于 `seen_tokens / block_size`：0082 为 0/16=0，其余四次为 16/16=1；被访问 vector 的 size 均为 0。这直接证明 source send task 收到的、用于按 token block index 访问的 KV block vector 为空。

Python 路径中最早可能产生/传递该空 source vector 的具体函数是 `blade_kvt.hybrid_connector.kvtbackend._migration_bypass`：它通过 `get_kvblk_ids → sched_get_kvblk_ids` 获取 source blocks，随后写入 `PReqMeta.p_block_ids`。当前代码只断言外层 list 非空，没有断言 inner vectors 非空或长度足以覆盖 `seen_tokens + new_tokens`。

### C3. selection/prealloc 与 source lookup 不是同一快照

`kvt_migration_frontend` 用 scheduler state queue 的 request snapshot 选择请求并传递 token counts；它没有携带 block IDs。迁移由 coroutine/task 异步执行，destination prealloc 完成后，source `_step_dinfoq` 才查询 live KV manager。代码中没有看到跨这段窗口持有 block table 快照、pin/reference 或 scheduler lock。

### C4. FinishedReq/free 不是已发现的触发原因

所有 `FinishedReq State=2` 都在 empty-vector 错误之后几十微秒出现。日志中没有对应 request 在 empty 前的 `finish_request`、free、scheduler remove 或 final user completion。五个 request 均在 KVT500 后继续生成并以完整 max_tokens 成功结束。因此“请求先完成并释放 blocks，导致本次 empty”没有直接证据，且时序不支持把它列为当前主因。

## Highly likely

### H1. source block-table 可见性/生命周期窗口导致 live lookup 返回空 inner vector

最符合全部证据的解释是：迁移选择依据的 request/token snapshot 仍有效，但 `_migration_bypass` 在另一个 scheduler step 中通过 `sched_get_kvblk_ids` 读取 source `kv_cache_manager` 时，读到了尚未建立、正被替换或暂时不可见的 group vector。该空值未经校验进入 `ReqMeta.src_block_ids`，最终在 C++ `tx_stub` 中失败。

这比“destination prealloc 错”更强，因为 destination blocks 均非零；也比“request 已结束”更强，因为请求随后继续生成。尚不能确认空值属于“临时更新窗口”“适配层取错表/形状”还是“preemption/recompute 后旧 request identity 与新 block table 脱节”。

### H2. stale request snapshot 放大上述窗口

frontend 的 `update_snapshots` 从容量 1 的 queue 取最新副本，选择后异步 dispatch。source block lookup 不使用同一 snapshot，而按字符串 request_id 重新查 live table。即使 request 仍在 running，token snapshot 与 block table 也可能来自不同 scheduler step。五个失败均符合这一结构。

## Possible

- **A request finished/free 与异步 migration race**：代码允许 finish path 释放 block table，但本次五个事件在 empty 前均无日志证据，且 source 后续继续运行；当前仅为理论可能。
- **B block table 被更新/替换**：符合 live-query 设计和 empty 现象，但缺少 prealloc 后与 send 前两次 source block-ID 日志，无法确认具体更新动作。
- **C stale migration task**：异步 task 与 snapshot 分离，机制上可能；但每个失败 request 只有一次 migration selection，不能证明 task 已过期。
- **D 重复迁移同一 request**：本轮每个 request 的 `migration_initiated` 均为 1，已基本排除为这五次的共同触发。
- **E scheduler 与 KVT 生命周期不同步**：这是 H1/H2 的上层描述，强证据是两个状态源和 `_infly_kvt`/late update；缺少同一时刻的 scheduler membership + source block table 联合观测。
- **F block-ID 形状/适配错误**：`list[list[int]]` 在 Python 到 C++ 的边界可能含非空 outer list 但 empty inner vector；异常形态与此一致。需要打印每个 group 的长度和 ID，才能与生命周期空表区分。

## 是否足够修复

还不足以选择最终修复。证据足以增加 fail-fast 校验或拒绝空 source blocks，但不足以判断正确语义应当是：锁定并复制选择时 block table、等待下一 scheduler step 重查、pin blocks、取消迁移，还是修正 block-group 适配。直接选择其中一种可能引入 KV 一致性问题。
