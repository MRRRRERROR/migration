# 实际环境与审查要点

实际工程`/data/home/zylu/llumnix-v018-main`，提交`aa9097c899fd2f290e031f8a88f854afe4f0b415`，原有13个跟踪文件的兼容修复保持dirty，另存完整diff与清单。没有以公开参考仓库旧代码覆盖服务器。独立控制器和EngineCore边界适配保存在实验目录；正式开始前冻结20个Python文件SHA，最终再次核对。

| 项目 | 实际配置/版本 |
|---|---|
| 设备 | 同机物理GPU0/1，RTX5090，各一个实例 |
| GPU0 UUID | GPU-0fc3a8e6-93f3-9c89-f694-bf490b219907 |
| GPU1 UUID | GPU-9ce9c226-678f-8cb4-4cf6-47c18d5b638a |
| 模型 | /data/models/Qwen2.5-7B-Instruct，bf16 |
| 上下文/输出 | 9216 / max_tokens4096，temperature0、n1、正常EOS |
| KV/batch | 每实例4294967296字节，max_num_batched_tokens8192，同步调度、eager、TRITON_ATTN，batch-invariant开启 |
| vLLM | 0.18.1.dev0+gbcf2be961.d20260719.cu133 |
| blade_kvt | 0.1.dev189+g6ebc08a19.d20260822.nobarex |
| Llumnix | 0.1.1.dev1+gaa9097c |
| Torch | 2.14.0.dev20260717+cu130 |
| transformers / aiohttp / grpcio | 4.57.6 / 3.14.1 / 1.82.1 |
| Python | /data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python |
| overlay | /data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay |

包名的nobarex不代表运行时没有Barex：实际进程映射包含libaccl_barex.so.1.5.3，native_loaded.json及冻结清单保存实际库路径和哈希。模型配置、tokenizer和generation_config保存哈希；权重另有GPU释放后的内容SHA，保留初始文件stat清单，不伪称启动前已计算权重内容SHA。

统一入口18370，实例API18270/18271，KVT41270/41271，worker41370/41371，gRPC59170/59171，Redis16579，native46570/46578。最终展开命令和选定环境保存在services_01/effective_environments.jsonl与launch.jsonl；工厂函数改写前的调试打印不能代替实际命令。

请求入口复用真实vLLM completions和现有SSE回放。新增确定性路由取最小`(running+waiting+reserved+未观测入口请求, KV使用率)`，并列按逻辑ID的SHA256。路由先决定实例再读取请求body，不访问未来输出长度。A/B/C共用同一实现。

原工程没有可直接安全使用的无状态等待搬迁接口，因此新增EngineCore串行边界命令：目标预留→源端原子资格复核/序列化/摘除→目标同ID接纳→源hold释放。复用HybridConnector的on_abort_req/_step_aborting及req2corereq，禁止把已执行、被抢占后重新等待的请求当作无状态请求。目标接纳状态不明时不回滚源，防止重复执行。真实接管/回滚及执行竞态验收保存同ID、arrival_time、输出地址和完整输出证据。

运行迁移复用既有KV接口及原生copy/suspend/resume路径，只限定单一当前上下文候选。成功要求source_release、dst_resume、migration_done及源block lease配平，不以RPC ACK判定。迁移取消与用户失败分开，逐次关联404返回事件及客户端结果。未实测网络传输字节，不把KV规模或读取日志字节数当成传输量。

R/W、KV block池和近期活跃引擎步耗时来自Core边界快照；唤醒线程只投递WAKEUP，不在线程外操作scheduler。步耗时包含执行与输出处理，不等于纯GPU kernel时间。独立NVML请求200ms采样，Prometheus请求1s采集；读取频率不等于设备/引擎统计的真实更新频率。

模型从17:21加载后贯穿校准、预实验和正式阶段；轮间确认空请求/事务，统一16-token预热后清前缀缓存。实例常驻，不声称每轮重置所有CUDA/通信分配器。研究为本地两卡实验，不是跨数据中心或生产规模实验。停止阶段的已知native故障与资源释放证据见OPEN_ISSUES.md。
