# 本轮验证实验的数据与实现审计

范围：本机现有仿真、profiles、配置及结果；未连接服务器、未扩展 Go 或 vLLM 排队/抢占。此次仅增加实验驱动、汇总与必要测试，仿真内核未修改。

| 项目 | 已有来源 | 本轮使用与限制 |
|---|---|---|
| decode | `profiles/5090_qwen25_7b_measured/tpot_5090_qwen25_7b.json`，4500 个已完成请求、9 份 gateway 日志的邻近样本统计 | 优先复用 p50；这是实测派生客户端 TPOT，不是 GPU step profile。把它作为 batch 耗时属于估计。该历史表基于完成样本，不能用于证明过载/失败性能。本轮自身所有结果完整保留。 |
| prefill | 同目录 ttft JSON | TTFT 包含排队和网关开销，不能直接当 prefill 耗时，否则会在 chunk 调度中重复计费。本轮不套用；保留 latency.py 既有示例模型。 |
| token 重建 | `results/two_gpu_service_alive_20261009_03/coverage_E/analysis.json` 中存在 replay/recompute_active_s | 例如一次约 2111 初始重建 tokens + 5 增量 tokens、累计约 192.69 ms。它是特定运行下的累计时间，缺少本轮 batch/cached-prefix/负载联合模型；不按 token 比例外推。本轮重建仍用既有示例 prefill 模型，属于主要不确定性。 |
| KV 传输 | engine.py 按 blocks 向上取整，network.py 共享链路串行化与传播 | 单流串行化为 bytes/(bytes/ms)，加单向时延；并发流均分链路容量。不是测得的 PCIe/NIC 拷贝曲线。未建模 KV copy 与推理争用。 |
| KV 大小 | cross_domain.json 的 57344 bytes/token | 继承假设值，未核对本次模型真实 tensor 布局；block_size=16 继承现有默认值。 |
| WAN | cross_domain.json 为 125000 bytes/ms、单向10 ms | 0.1/1/10 Gbit/s 是围绕已有配置的 0.1/1/10 倍敏感性扫描，没有 WAN 测量依据。换算：Gbit/s × 10^9 / 8 / 1000 = bytes/ms。 |
| 目标负载 | TPOT batch grid 包含 1/8/32 | 选作三个并发请求水平，是实验设计；不是测得的低/中/高 GPU 利用率。 |
| 功率 | `results/migration_net_benefit_20261014_01/runtime_verified_01/.../power.csv` 等 | 有原始日志，但没有接入可覆盖 compute/replay/copy/空闲的功率模型，故本轮不调节功率、不下节能结论。 |

真实测得的请求速率校准另见 `results/migration_net_benefit_20261014_01/runtime_verified_01/calibration.json`，含 0.3/0.6/0.9 请求/秒。这些是不同工作负载下的到达率实验，不能换算成本轮并发数或利用率。

## 计算、参数与随机性

decode 查询先用 `total_context / batch_size` 转成表里的 tokens_per_request，再选择最近 batch、最近 context 网格；距离相同取较小网格坐标。不拟合新斜率，越界查询计数保存在各 summary，不能隐藏外推。本次全部运行越界查询数为 0，但在网格内不代表服务时间模型已经校准。

prefill/replay（ms）沿用 `1 + 0.02 * new_tokens + 0.0001 * cached_tokens`，各系数均为原有示例值。decode 的旧仿射系数保留在配置中以完整记录 LatencyModel 初始参数，但实验实际 decode 由 TPOT proxy 覆盖。不存在把这些系数称为 RTX 5090 硬件特性的依据。

所有可调输入集中在 `configs/simulator/validation_matrix.json`。未特别标明实测派生的参数均为原有示例或显式实验假设：请求预算512，实际长度240–272，候选prompt128，源其他prompt512，目标prompt256，源其他4请求，后续请求400/800 ms到达，触发200 ms，观察上限100000 ms。长度由每个种子独立 Random 生成，模式间复制同一轨迹；种子101/102/103不表示真实生产分布。同 seed 下改变带宽不改变轨迹。

四模式新请求始终路由 source。所有组在200 ms用同一真实 SR 方法观察源请求，全部选到同一 candidate；只有迁移组执行动作。每次均新建 Simulation，初始空KV与全部实例参数一致；目标负载是条件间唯一请求数量变化。none 模式也保留候选决策记录。

## 复现和证据

从项目根目录执行，输出目录必须不存在，以防覆盖旧结果：

```powershell
python -B -m unittest discover -s tests/simulator -v
python -B tools/run_validation_matrix.py --config configs/simulator/validation_matrix.json --output results/validation_matrix_reproduce
python -B tools/analyze_validation_matrix.py --input results/validation_matrix_reproduce
```

驱动先运行中负载/中带宽的4模式验收，验证后执行9条件×首种子，再执行另外两个种子。验收不以收益或迁移成功率为条件，不修改模型来获得正收益。每次运行保存配置、summary、逐事件日志和错误审计，配对比较保存全部请求。累计耗时/字节是模型输出，非实测数据。

本次结果：`results/validation_matrix_20261020_02/REPORT.md`，41项测试通过，12个Python文件语法检查通过；全部112次运行（正式108+验收4）输出完整、KV归零，未完成、迁移失败及超时条目为0。28组配置逐项检查除mode外完全相同。原始实验入口第一次因 relative_to 路径错误在写manifest前退出，尚无仿真结果，保留目录 `validation_matrix_20261020_01`；修复为绝对配置路径后重跑，没有改变参数。

本次有效性仅为软件/记账/配对逻辑验证。不能证明真实服务性能、迁移协议的实际网络故障恢复、模型token等价或完整vLLM调度等价。无真实排队/抢占，尤其限制过载容量、等待尾延迟与内存紧张情况下的推论。
