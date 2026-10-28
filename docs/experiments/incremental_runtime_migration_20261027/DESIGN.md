# 运行中 KV 迁移相对运行前调度的增量收益

本地同机两张获准的 RTX 5090（物理 0/1），各一实例。保留当前服务器兼容补丁，独立实验 overlay/端口/目录；不覆盖历史、不推送、不发布。研究期不退出源端、不卸载模型、不扩缩容，不使用 token 迁移。

## 代码审查（实施前）

- 工程 `/data/home/zylu/llumnix-v018-main`；基准 aa9097c899fd2f290e031f8a88f854afe4f0b415，有已列出的未提交兼容修改。
- 实际环境 llumnix5090_vllm018：vLLM 0.18.1.dev0+gbcf2be961.d20260719.cu133，blade_kvt 0.1.dev189+g6ebc08a19.d20260822.nobarex，Llumnix 0.1.1.dev1+gaa9097c，torch 2.14.0.dev20260717+cu130，transformers 4.57.6。包名中的 nobarex 不能证明运行时不加载 Barex，部署后还核对 /proc/PID/maps 与二进制哈希。
- 请求可复用 OpenAI SSE completions 和 Llumnix 输出转发；Request/EngineCoreRequest 保留 queue_server_address、arrival_time 等字段，支持目标端向原 API 的逻辑请求继续输出。
- 当前 FCW/迁移选择器使用异步 scheduler snapshot，waiting 本身不能证明从未执行；现有 Request 有 num_preemptions，PREEMPTED 与 WAITING 不同。新增 ever_scheduled 轨迹，不能只看重置后的 num_computed_tokens=0。
- KV 复用既有 NUM_REQ 接口、限定 request_id 的选择器和结构化事件。RPC accepted 不等于 handoff 成功；以 dst_resume/migration_done、migration_attempt_cancelled/migration_abort、错误及客户端结果关联分类。
- 复用 NVML、SSE token IDs、既有 live_load 观察方式；状态读取和队列操作都在 EngineCore 边界进行，后台线程不直接访问 scheduler。
- 已知 Barex 正常卸载缺陷是单独限制。研究窗口内持续保留服务；收尾异常记录并阻断后续执行，不能用服务退出码或 GPU 空闲代替正常关闭证明。

## A/B/C 和 B 的原子协议

路由统一入口；比较规则相同：最小 running+waiting+reservations+尚未被快照覆盖的入口请求数，KV 使用率破同分，仍同分用 request_id 哈希确定。不能读取未来输出长度。

A 仅路由。B 在 A 上允许已经入源端等待队列、确实从未执行的请求调整。C 完整复用 B，再加周期性运行中 KV 迁移。

B 在 EngineCore 串行调度边界同时校验 WAITING、computed/output=0、ever_scheduled=false、preemptions=0、KV blocks=0、无传输状态。目的端先预留事务，源端用既有 connector 的不向客户端输出 abort 的收尾路径原子撤出；传送同一个 EngineCoreRequest（原 ID、arrival_time、输出地址和采样参数不变），由目标原子接纳。事务响应可重复查询，目标承诺后禁止回滚源端；不确定接纳时保留源端 hold，不重新执行。只有确认目标未接纳且终止预留后才能回滚。所有状态转移单独记录，不把内部 detach 记为用户失败。

先验证真正队列转移、源端开始执行竞态拒绝、抢占历史拒绝、重复消息幂等、失败回滚、输出身份和计时。CPU 合成检查只算协议检查，不能当真机结果。

## 控制器候选参数（正式前按预实验冻结）

检查周期 1s；快照最大年龄 2s。等待调整：总负载差至少 2，最早到达的合格 waiting 优先，每次一个。运行迁移：running 差至少 3，连续 3 次；最短当前 computed context 优先、最大 context 4096。目标 KV 使用率低于 85%，且空闲 block 能覆盖当前上下文加固定 512-token 余量。全局同时最多一个会话，会话后冷却 10s，同逻辑请求冷却 60s。不会每次强制迁移，不要求每轮必迁。指标不可用时拒绝。保留全部拒绝原因和输入状态。

这些是收益探测基线，不宣称最优。只能在预实验中依据有效性和机会冻结，不能以正式结果调优或强制偏置路由。

## 负载、校准、顺序

Qwen2.5-7B-Instruct/bf16/9216/max_tokens4096/temperature0/n1/正常 EOS；KV 4GiB、batch8192、batch-invariant 和同步调度沿用此前验证的配置。保存实际展开的所有启动参数、代码/模型/tokenizer/generation config 哈希。

真实 ShareGPT 提示词，冻结目标 tokenizer 输入 IDs。统一入口，无预设源端；计划为独立种子的泊松开放到达。校准和预实验、正式种子互不混用。正式每轮发送600s，尾部观察上限600s；失败、取消、超时全部保留。

先从总到达率2/3/4 rps的独立600s校准轨迹检查稳定性：两端 waiting 斜率<=0.01请求/s且后段均值增长<=2，完成速率与观察到达匹配，TTFT/E2E/TPOT后段无持续上升，错误0，抢占率受控且不持续增大。趋势不清楚不冻结，补必要观察或下调。中负载选有明显余量点，高负载选接近已观测稳定能力点，不能假称测到了全局容量。

实际执行器的保守筛选还要求：后200s完成数不少于该窗口到达数的90%，后段E2E中位数不超过前段的1.25倍或增加1s（二者较宽者），观察到的抢占数为0。若高档不稳定且尚不足两个稳定点，独立追加0.5/1/1.5 rps；不是针对某个方案改配置。校准结束再核对TTFT、TPOT、尾部分布及Prometheus累计抢占，不能把布尔筛选值视为容量证明。所有发送器滞后原样保留。

正确性通过后预实验2负载×A/B/C各1轮。若无有效运行迁移机会且 B 已消除不均衡，交付6轮证据，不机械扩到30轮。否则冻结代码和参数，用5个独立种子按平衡交错次序跑30轮。每轮请求清单、初始空请求/前缀缓存条件和预热约定相同。污染轮保留并按预定规则补一次；重复污染则停止，不抢其他任务。

控制器在600s发送期与随后收尾期均保持相同动作权限，至所有请求完成或固定尾部截止；收尾不再发送新请求。不把停止总到达后自然减少的在途数量冒充迁移效果。

真机正确性记录：等待接管及目标拒绝后的源端回滚均保持原ID、arrival_time和输出地址，正常EOS；同一运行请求经真实KV迁移后输出1139 token，与参考token IDs完全一致。原生source_release、dst_resume、migration_done和source block lease acquire/release已逐项核对。最初验收日志字段误读的失败记录保留，修正后按原始证据重新判定，没有重启实例或重复请求。

## 统计

主结果是全部请求的完成时间分布。失败或截止未完成没有完成时间；完整请求数不足p分位覆盖比例时，该分位标记不可达到，并报告截止下界，不能只取成功子集。另列成功条件分布和全部终止耗时，避免快失败改善主延迟。

按种子给C−B、B−A、均值及基于5个配对样本的t区间（明确小样本限制），不以请求数代替独立重复。C所选请求事后分组，用相同ID提取B。其他源端和原目标请求按决策时的ID集合记录。SSE只能称输出块间隔；传输量不可测时保留缺失。对齐 token IDs、输出长度和finish_reason；不一致时不把耗时差全归因迁移。

原始数据、失败/排除记录、CSV和简洁报告保存在本独立目录。公开参考仓库只作为参考，目前网络读取未成功；不拿它覆盖真实服务。
