# 真实调度接入进度（2026-10-20）

## 本次范围

已接入 **2026-10-11 服务器快照**中的两部分决策代码：

1. `TokenRuntime.begin_schedule/skip`：重建与普通工作分批、交替；只有重建工作时连续重建，暂停和重建完成请求跳过。
2. `KVTMigrationFrontend` 与基类的运行请求排序、过滤、成本及预算选择：SR/LR/FCR/LCR/RANDOM/REMAINING_LONGEST/COMPOSITE。

`llumnix_sim/scheduling.py` 按显式方法名单抽取 AST，原方法体不改写，仅延迟类型注解求值，并提供无 GPU 状态视图。不会运行真实构造函数、启动线程、RPC 或加载 vLLM。源码缺失/方法缺失直接报错；不回退自写算法。回放 gate 的 monotonic 改为虚拟毫秒除以 1000，初始 replay_turn=False 来自原构造函数。

这是快照源码复用，不是完整引擎部署，也不证明快照仍等于服务器当前版本。运行必须保留快照目录。CLI manifest 记录四份决策源文件的 SHA256；Python 进程内方法缓存，改快照后须启动新进程。

## 调用关系与边界

`配置 selections → 虚拟时刻 → 读取源实例当前请求 → 原请求选择方法 → migrate → 原有资源/协议检查 → 迁移事件`

原有 `migrations` 显式 request_id 回放仍保留。`--mode none` 同时关闭两种迁移入口，其他模式覆盖两种入口。选择日志记录时间、源/目标、规则、预算、随机种子和选中 ID；后续拒绝、取消等结果仍完整保留。

源/目标、触发时刻与迁移模式仍由实验配置提供，**不是 Go 调度输出**。目前只映射运行请求；FCW/FCWSR 因缺少真实等待队列而报错。源状态在决策执行时同步可见，尚未加入监控采样/传输延迟。

局部 batch 分类已用真实方法；分类内仍采用仿真原有轮转、chunk 和 block 分配，尚未复用 vLLM 完整 schedule/preemption。两类都存在而本轮无可执行任务时，仿真允许一次零时间切换重试，防止遗漏能推进的另一类；两类均阻塞时不忙等，交由已有资源事件唤醒或报 admission deadlock。这个事件驱动补偿没有声称模拟真实调度 CPU 时间。

真实预算行为：每请求成本与总预算比较，而不是与剩余预算比较。例：TOKEN 预算 12，两个请求上下文 6 和 10，会选中两个，总成本 16。此行为保留并显式测试，没有为收益调整。

## 参数依据

- COMPOSITE 的 `remaining=max(0,max_tokens-generated)` 与 `score=remaining/max(context,1)` 直接来自快照。remaining 是预算上限，不是实际剩余生成长度预测。工作负载现可显式提供 output_length；省略时仍假设生成到 max_tokens，尚无经过校准的 EOS 模型。
- `composite_min_remaining=8 tokens`、`composite_max_context=4096 tokens` 来自快照默认环境参数，不是硬件参数；接口要求显式传入。NUM_REQ/TOKEN 为计数，RATIO 为 (0,1] 容量占比，容量基于 blocks×block_size。
- 示例 seed=7、触发80 ms、预算1、源目标、请求长度均为人为功能检查输入。RANDOM 使用快照 SHA256(seed:request_id) 排序。
- 链路、计算、KV 字节及其他参数沿用 `TOKEN_SIMULATOR_AND_CROSS_DOMAIN_AUDIT.md` 已注明的示例假设；未重新标定，未添加能量/功率公式。

## 运行

```powershell
python -B -m unittest discover -s tests/simulator -v
python -B tools/run_migration_simulation.py --config configs/simulator/source_policy.json --output results/scheduling_20261020/token_live
```

## 继续工作的优先级

本轮已实际执行：32 项 unittest 全通过（包含原 25 项），10 个 Python 文件语法检查通过；source_policy 场景 none/kv/token_stop/token_live 四种模式各完成 3 个请求、释放全部 KV，后三种各完成一次迁移。结果保存在 `results/scheduling_20261020/`，包含完整测试日志、各模式事件、配置和源码指纹。本轮检查无失败；未执行 Go/GPU/服务器或完整 vLLM 等价性检查。旧实验结果保留。

1. 取得与实际部署版本对应的完整 Go scheduler 源码及 go.mod/go.sum，核对版本，建立 JSON 状态/动作适配器，运行原 policy。当前本地 `remote_scheduler_src` 只有 consts/policy，未发现 Go 可执行文件；不能宣称全局调度已复用。
2. 实现 dispatch、监控快照采样与延迟、全局 reschedule 触发，明确状态可见性和并列排序，验证同状态下真实与仿真的决策一致性。
3. 补完整本地 waiting/running/preemption 调度状态；当前 batch gate 对齐不代表完整 batch plan 对齐。
4. 引入 EOS/长度不确定性和校准数据；跨域模式选择单独作为研究策略，而非冒充已有服务器能力。

没有修改服务器、真实配置或快照。功能测试通过只支持指定抽象下的逻辑正确性，不能替代性能模型校准、完整调度等价性或真实跨域实验。

## 定时跟进：分离预算与工作负载真值

新增 `add_request(..., output_length=...)`，单位为输出 token 个数，要求正整数且不超过 max_tokens。它是实验输入，保存在执行器私有字典，不传入真实请求选择方法的视图。原先的 max_tokens 仍作为对策略可见的预算；不提供实际长度时保留旧行为，并在结果中标记 `budget_limit_assumption`，显式输入标记 `explicit_workload_input`。显式输入并不等于实测数据，来源由工作负载说明负责。

执行器在指定长度停止生成，客户端收齐才记完成，记录 stop/length 原因；迁移后的预算不重置。源在 live 过程中提前结束会走已有取消协议，取消结果不改成成功。这里的 stop 是工作负载指定的逻辑终止，没有执行模型采样、EOS token 或 stop-string 检测；不能据此声称模拟了真实 EOS 分布。

新增四项检查：交换未来输出长度但保持当前状态相同，REMAINING_LONGEST 仍给出同一选择；提前结束期间取消迁移；三种迁移后严格遵守实际长度并释放内存；非法长度报错与默认假设记录。36 项测试全通过，10 个 Python 文件语法检查通过。四种 CLI 模式均输出指定的 40/60/80 tokens 并释放 KV；这些长度为人为功能测试输入，不是测量或优化参数。记录见 `results/scheduling_followup_20261020/`，旧结果未覆盖。本轮检查无失败。

全局 Go 调度、完整 vLLM 队列/抢占和真实 EOS/性能模型校准仍未完成；本次没有运行服务器或 GPU 检查。
