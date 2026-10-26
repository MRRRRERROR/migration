# Token 迁移仿真接入与跨域检查

调度接入更新：见 [SIMULATOR_SCHEDULING_PROGRESS.md](SIMULATOR_SCHEDULING_PROGRESS.md)。当前已复用快照 batch 分类和运行请求选择方法，完整 Go 全局调度与 vLLM 批计划仍未接入；下文早期审计中的限制应结合该更新阅读。

日期：2026-10-20。检查范围为本机 legacy 仿真源码，以及 2026-10-11 同步的服务器 V1 源码快照。没有重新连接服务器核对 10 月 11 日以后的变更，没有运行 GPU 实验。

## 已实现与入口

新增 `llumnix_sim/` 是 CPU 离散事件执行路径，移植了服务器 token 协议中可用于仿真的行为语义。它不导入真实 EngineCore，不执行真实模型，也不通过 `asyncio.sleep` 计时。

原 `llumnix/backends/vllm/sim_executor.py`、`sim_llm_engine.py` 仍是 legacy vLLM/Ray/HTTP 仿真路径，只支持原 KV 模拟。向旧 API 启动命令添加 token 参数不会启用这次新增的功能；应使用下述新入口。

原路径的检查结果：

| 位置 | 当前行为 | 对跨域研究的限制 |
| --- | --- | --- |
| `sim_executor.py::execute_model_async` | 实际 asyncio.sleep，保留宿主机/Ray 开销 | 实例数增加时可能测到宿主机瓶颈，没有独立虚拟时钟 |
| `sim_executor.py::send_cache` | block 字节数 / 单一固定带宽，随后返回 True | 没有 domain、传播时延、链路共享和传输失败 |
| `sim_llm_engine.py::send_cache` | 忽略目标传输细节，仅等待源执行器 | 不能表示跨域接收确认、链路重试和提交不确定性 |
| `sim_executor.py::determine_num_available_blocks` | cache profile 不命中时回退到 880 blocks | 配置不匹配可能被隐藏，改变容量/迁移触发条件 |
| `profiling.py` 与 JSON→pkl 转换器 | decode 使用总上下文，但转换器写每请求上下文 | 会改变不同 batch 下的成本语义；数据口径需单独修正 |
| `entrypoints/vllm_v1/arg_utils.py` | 明确禁止 V1 simulator mode | 原模拟器并非已同步 V1 token runtime 的直接执行环境 |

因此选择增加独立、显式的 token 离散事件路径，而没有把 token 迁移简化为旧 send_cache 上的一次额外 sleep。

```powershell
python -B tools/run_migration_simulation.py --config configs/simulator/cross_domain.json --mode token_live --output results/token_live_demo
python -B tools/run_migration_simulation.py --config configs/simulator/cross_domain.json --mode token_stop --output results/token_stop_demo
python -B tools/run_migration_simulation.py --config configs/simulator/cross_domain.json --mode kv --output results/kv_demo
python -B tools/run_migration_simulation.py --config configs/simulator/cross_domain.json --mode none --output results/no_migration_demo
python -B -m unittest discover -s tests/simulator -v
```

均从项目根目录运行，无第三方依赖。输出 `summary.json`、`events.jsonl`、有效 `config.json` 与含源码 SHA256 的 `run_manifest.json`。

| 源码 | 职责 |
| --- | --- |
| `llumnix_sim/engine.py` | 请求历史、批处理、KV 容量、源/目标副本和迁移状态机 |
| `llumnix_sim/protocol.py` | 迁移配置、attempt、num_tokens-1 就绪条件和释放条件 |
| `llumnix_sim/clock.py` | 可取消事件与确定性虚拟时间 |
| `llumnix_sim/network.py` | 有方向域间链路、带宽公平共享、序列化和传播延迟 |
| `llumnix_sim/latency.py` | 明确的计算抽象、原 LatencyMemData 的程序接口适配器 |
| `tools/run_migration_simulation.py` | JSON 场景驱动与日志输出 |
| `tests/simulator/test_migration.py` | 无 GPU 的状态、资源与时序验证 |

## 移植的协议语义

依据快照中的 `token_migration/config.py`、`runtime.py`、`gateway.py`：

- `token_stop`：一致 decode 边界暂停，传输 prompt/output 历史，目标重建，确认路由后释放源。
- `token_live`：初始快照后源继续输出；目标保留已建 KV，逐次接收非空增量并重建。最多 3 轮、默认 gap16，达到轮数或 gap 阈值后最终暂停补齐。初始重建和最终补齐不计入后台轮数。
- 目标达到 `computed == prompt_length + output_length - 1` 才能接管。重建不采样、不输出；最后一个已输出 token 留待接管后的正常 decode 处理。
- 原 prompt、已输出历史和总输出预算分开保存，历史不会重复输出，也不会将 max_tokens 重置为新的完整预算。
- 整次迁移使用一个绝对虚拟 deadline，包含 hello、排队、重建、增量和提交前等待。
- 提交前先排空源输出；源释放要求目标 commit proof 和原 gateway 路由 ACK 同时存在。
- 提交前失败通过目标取消和取消确认往返后恢复源。提交之后即使 deadline 到达，也保持源栅栏，不能双端输出。
- 目标重建进入目标的批处理预算并占用 KV blocks；存在目标背景请求时，会影响其计算和完成时间。
- token 恢复后的请求再次迁移显式拒绝；不同 model/tokenizer identity 拒绝。KV 还检查布局与 block_size 兼容。
- 每个实例的迁入/迁出合计受 `max_migrations` 限制，默认 1，超过时显式拒绝。

KV 对照使用一次后台复制加一次最终暂停补齐；按有效 KV 的 block 数计费，扩展了已复制的部分 block 会重传。KV 也计入恢复所需 token 历史元数据。它不是原 migration_coordinator 可配置多阶段算法的逐行复刻。

原始协议中真实进程 epoch、终态 ledger、重复/迟到 RPC 的故障处理没有完整移植；可靠传输下的仿真 attempt 唯一 ID 不等于重启后的 fencing 协议。

## 当前跨域模型已经覆盖

- 每个实例配置 domain；网关可通过顶层 domains 配置成独立域。
- 跨域链路必须显式给出，A→B 和 B→A 独立配置；缺失时直接报错，不按本地链路处理。
- `bandwidth_bytes_per_ms` 为 bytes/ms，`latency_ms` 为单向传播延迟。例子中的 125000 bytes/ms 对应约 1 Gbit/s；10ms 单向约对应 20ms 传播 RTT，另加序列化和竞争等待。
- 同一有向域间链路上的并发流共享总带宽，新流加入会重新计算在途流的完成时间。
- hello、prepare、ready、commit、cancel、路由 marker/ACK，以及 token/KV payload 都消耗链路资源。
- 接管后输出继续传往原 gateway，因此跨域输出转发会进入客户端 token 时间戳。
- 源迁移前的输出排空和目标取消确认都等待网络，不使用跨域零成本的全局状态切换。

同域链路默认 1000000 bytes/ms、0ms 传播，可在 links 中显式覆盖。该默认值是场景假设，不是硬件实测。

当前把客户端与原 gateway 视为同一位置；没有独立模拟客户端最后一跳、HTTP/SSE 序列化和网关处理队列。端到端时间戳是在这个假设下定义的。

## 仍缺少的逻辑：按研究目标排序

| 优先级 | 缺口 | 当前假设/影响 | 下一步 |
| --- | --- | --- | --- |
| P0 | 丢包、断链、分区、提交回复丢失 | 当前可靠必达，最终能收到取消或路由 ACK；无法评价故障下可用性 | 注入控制/数据消息丢失，加入 status 查询、取消 CAS、重试、幂等与不确定提交恢复 |
| P0 | 进程重启与 epoch/ledger | 当前节点不重启；不能证明跨域故障下不发生旧主复活 | 模拟 incarnation 变化、持久终态与恢复限制，覆盖迟到 prepare/commit/ACK |
| P0 | 调度与策略接入 | 当前由配置指定请求、目标和迁移时间；批处理是简化 FCFS/轮转，未运行真实 V1 调度器 | 增加可插拔迁移策略、域内/跨域 admission、指标上报延迟和观测陈旧度；建立与 V1 调度行为的对照 |
| P0 | 拥塞下显存回收/抢占 | 当前不模拟 swap/preemption；容量不足可能造成 admission deadlock，并显式报错 | 实现与被研究系统一致的抢占、重算、排队与拒绝策略，不能用无限 KV 容量掩盖 |
| P1 | 多跳与多级共享瓶颈 | 当前每个 domain pair 是一条独立瓶颈；A→B 与 A→C 不共享出口 | 建模源/目标 NIC、域出口、WAN、网关，按路径共享资源 |
| P1 | WAN 变化与传输协议 | 当前恒定带宽/延迟，无 TCP 慢启动、重传、抖动、背景流和费用 | 带宽/延迟 trace、连接复用、连接建立与加密成本，按需要加入传输行为 |
| P1 | 拷贝与推理争用 | token 重建已竞争 GPU；KV 复制目前只竞争网络 | 加入 copy engine、PCIe/HBM 带宽和计算干扰的资源模型 |
| P1 | 请求生命周期与客户端重连 | 当前固定输出长度，无 EOS/stop string、用户取消、HTTP disconnect 或入口迁移 | 增加终止/取消事件，原 gateway 失效及连接重建；统计端到端中断而非仅源暂停 |
| P1 | 冷启动和模型驻留 | 当前目标模型始终在线，无加载、扩缩容与域配额 | 模型/实例状态机、目标准备时间、跨域资源授权与容量约束 |
| P2 | 异构并行配置 | 每实例抽象一 GPU；identity/layout 只是兼容性标签 | 按目标设备/TP/PP 建模计算、KV 重分片与通信，token 兼容性也需真实验证 |

若论文只研究无故障、固定网络、同模型场景，可以明确限定这些假设；若声称跨域可靠性、WAN 故障恢复或自动策略优势，对应 P0 能力必须先补齐。

## 真实服务器代码跨域部署的额外阻碍

以下是对已同步快照的检查，不代表 10 月 20 日服务器当前版本：

- `token_migration/runtime.py:140` 只监听 `127.0.0.1`，`:183` 的远程调用也连接本机；`:447` 起明确拒绝非 localhost 的目标。需使用真正可路由的 endpoint，并改造发现/地址发布。
- 协议传递由 `time.monotonic()` 产生的绝对 deadline，目标直接比较本机 monotonic 时间。不同主机的 monotonic 原点不能直接比较；需要定义跨主机的期限协议，例如源端全程计时并传递剩余预算、目标本地期限与提交安全约束。
- 控制数据使用 pickle，当前是本地实验信任假设；跨管理域需明确认证、授权、消息格式与版本协商，以及传输保护。
- 原 API/SSE 连接被保留。接管后源 GPU/Core 可释放与原 API 可关闭是不同能力；不能把源 KV 释放等同于整个源域退出。
- 目标重启恢复、已恢复请求再次迁移，目前原实现也有明确限制；仿真不能自行假设真实系统已支持。

这次没有修改服务器监听地址或部署代码。

## 计算模型与结果边界

示例使用显式的 illustrative affine 参数；这些数值只用于跑通逻辑。默认计算为：

`prefill_base + prefill_per_new_token * 新计算token数 + prefill_per_cached_token * 已有上下文数`

`decode_base + decode_per_request * batch大小 + decode_per_context_token * 总上下文长度`

同一 batch 的 prefill/replay 与 decode 延迟相加。它没有精确刻画请求长度分布、attention 形状或真实 V1 的 replay 调度规则。`LatencyModel(legacy=latency_mem)` 可复用原 LatencyMemData 的查询/拟合逻辑，其中 decode 第二维为总上下文；cached-context 项必须另外校准。当前 CLI 不直接导入旧 pkl，避免间接加载 Ray/vLLM 依赖。

原 JSON→pkl 转换器使用 tokens_per_request 而执行器读取总上下文的问题仍需单独处理，不能据此认为旧 profile 已准确接入。

token ID 由输出位置确定，用来验证无重放、预算和顺序；不能验证真实模型 token/logit 等价。控制帧按固定 control_bytes 计费，token 按 4 字节；不是实际 HTTP/TCP/TLS 全线速字节测量。

`source_pause_ms` 是源暂停至目标提交/源恢复的逻辑区间；`source_fence_ms` 延续至源释放/取消确认。客户端实际中断应查看逐 token 时间戳和 max_itl_ms，不能用源暂停替代。

## 本次验证

25 项测试通过，含 60 组固定随机种子的不同负载/链路/期限组合。覆盖三个模式、输出预算与顺序、num_tokens-1 边界、后台轮数、重建对目标背景任务的干扰、共享链路守恒、不同 RTT、目标容量失败、目标取消确认、提交后超时、源提前完成、再次迁移拒绝、布局/身份不兼容、独立网关域、迁移并发限制、与已同步 V1 配置默认值一致性、确定性重跑。

`results/token_simulation_20261020/` 保存四种模式的相同场景试运行与测试结果。它们是功能回归证据，不能作为实测校准后的论文性能结果。
