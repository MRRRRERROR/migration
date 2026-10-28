# 最终复核入口（2026-10-28）

30轮已完成、GPU已释放。以下历史启动命令只供未来独立复现，不得对本次目录重启。正常卸载验收失败；见OPEN_ISSUES.md。本轮GPU原始数据无需也不应重跑。

在本目录运行离线复核（只使用CPU）：

```powershell
python analysis_sources_01/verify_final.py paired_abc_01
python analysis_sources_01/review_prometheus.py paired_abc_01
python analysis_sources_01/review_calibration.py calibration_reference_01
python analysis_sources_01/analyze.py paired_abc_01 --include-core-locations
python analysis_sources_01/finalize_delivery.py paired_abc_01
```

分析需要matplotlib；analysis_runtime.json和analysis_sources_01/SOURCE_SHA256.json记录解释器、隔离依赖和分析源版本。正式运行源码位于paired_abc_01/execution_sources，SHA在formal_frozen_code.json，独立审计结果在final_audit。原始blocked和run_failure不改写，查看正常退出限制及独立资源释放证据。

完整原始压缩包在上层raw_bundle_20261028_01.tar.gz，1718242987字节，SHA256为04e3fabb91f9e9e14c03481e0e74e5555508b7ad609faef907d201137a56608d。解压快照保留失败和污染轮；accepted镜像只有paired_abc_01的6预实验+30正式及共享services_01。校准另存calibration_reference_01。Windows镜像将服务目录相对符号链接物化，原链接记录在server_snapshot_01/SNAPSHOT_SYMLINKS.json；不修改原始文件。

下面保留执行过程中的复现说明，早期“当前正在运行”的描述均已过时。

# 复核与复现

## 当前执行版本与防重复启动

2026-10-27正式活动根是父实验下`paired_abc_01`，CPU控制器470406，GPU core337350/337218。六个有效预实验后，**同一个控制器已自动进入正式30轮**，不是六轮后清理再启动。当前不能另起formal.py或runner；原服务目录有排他控制器锁。旧文档中的独立formal.py仅是备用实现，不是本次实际执行路径。

正式冻结清单`paired_abc_01/formal_frozen_code.json`，顺序`formal_order.json`，门槛`pretrial_gate.json`，参数`frozen_pretrial.json`。实际命令是：

```bash
"$PY" -u "$R/execution_sources/runner.py" --root "$R" \
  --resume-services --calibration-rates 0.5,2 --fixed-profiles \
  --reuse-calibration "$P/results/incremental_runtime_migration_20261027_01/transport_fixed_01"
```

此处R是活动子根，不是父根。该命令只记录本次续接路径，**不要对仍在运行的服务重复执行**。它核查PID、创建时间、标签和端口，沿用原暖模型，并引用已闭合的独立校准；没有伪造一轮新校准。原03校准种子6300/6301，两档0.5/2rps；发送器单独验收在replay_driver_fixed_01/sender_acceptance_01。污染04和05补跑原因见failures/04_foreign_gpu_process。

如需在以后空闲的同样两卡上完整新复现：建立不同结果根，复制冻结源码及固定输入，核查相同依赖/overlay/模型哈希，确认本实验实例已退出及两卡可用，再执行新部署模式（**不加resume或reuse**）：

```bash
"$PY" -u "$R/execution_sources/runner.py" --root "$R" \
  --calibration-rates 0.5,2 --calibration-seed-base 6300 \
  --fixed-profiles --sender-acceptance
```

新部署先做真实正确性验收和两档新校准，然后六个预实验，门通过才冻结进入30轮。资源不足或校准/正确性失败即保留现场停止，不自动改成仿真或搜索有利参数。本段具体参数优先于下文早期候选档位说明。

## 独立离线分析

最终分析源与冻结的运行代码分开保留，不覆盖活动execution_sources。分析只读原始记录并输出派生CSV、图和报告；不启动任何服务。先把闭合轮次和实际共享services_01复制到完整本地镜像，不保留断开的远程符号链接。

```bash
python analysis_sources_01/review_calibration.py calibration_snapshot_01
python analysis_sources_01/verify_final.py paired_abc_01
python analysis_sources_01/analyze.py paired_abc_01 --include-core-locations
```

analyze需要matplotlib；服务器环境未安装或修改此依赖。实际本地使用Codex bundled Python以及项目tmp/incremental_analysis_deps中的隔离matplotlib3.11.2/numpy2.5.4，MPLCONFIGDIR指向项目tmp/incremental_mpl_config。设置sys.path后用runpy执行即可。analysis_version.json记录分析源SHA；final_audit/frozen_code_check.csv独立验证冻结运行源码没有改变。

主结果FINDINGS.md及report.html；summary.csv为逐轮指标，paired_round_differences.csv为逐种子C−B/B−A，paired_seed_uncertainty.csv仅在各负载全部5对有效有限时产生均值与df4 t区间。request_metrics.csv、migration_sessions.csv、waiting_transfer_transactions.csv及原始JSON保存请求、取消/404/用户结果、迁移与事务的对应关系。verify_final另按原生事件和NVML采样检查动作与GPU干扰，不把NVML相邻采样之间的未观测时段称为连续无干扰证明。

实际目录：`/data/home/zylu/llumnix-v018-main/results/incremental_runtime_migration_20261027_01`。本地副本：`D:\llumnix-ray-main\results\incremental_runtime_migration_20261027_01`。实验实现见项目 `tools/incremental_serving`，服务器运行的是结果目录内 `execution_sources` 的隔离副本，未改写服务器既有兼容修复。

当前实例与控制器启动身份都保存在结果目录，禁止直接重复运行启动命令。已有服务的续接只核验本实验记录的 PID、创建时间、启动标签及端口，不认领其他服务。

## 已执行路径

1. 保存实际工程提交、dirty diff、版本、模型与 tokenizer 配置，核对物理 0/1 两卡 UUID 和占用。
2. 用目标模型聊天模板重新编码 1815 条去重用户文本，逐条核对冻结的 token IDs 和 128～4096 长度。无放大独立样本数；回放按种子均匀、有放回抽样。
3. 验证 DTO 往返、抢占排除、幂等提交、等待转移、回滚、运行抢占竞态拒绝、真实 KV 迁移接续及输出一致性。
4. 开放式泊松回放，独立校准种子 6100～6102；若需降档，6200～6202。预实验使用7101/7102；正式种子8101～8105。
5. 每轮发送600s、固定尾部上限600s。两实例持续在位，轮次间空请求/空事务检查、统一16-token预热、再次清除前缀缓存；不卸载、不做排空实验。

首个控制器因原生事件字段 `event_name` 误读为 `event` 而暂停。`failures/01_event_name_parser` 保留原源码和失败现场。`correctness_01/qualified_gates_02.json` 根据同一批原始日志重新判定，没有重发请求或重启实例。原始误判保存在 `gates.json`，更正结论在 `result.json`。

## 命令

在经过资源核验的新独立结果目录中复制 `execution_sources`；实际使用环境：

```bash
PY=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python
P=/data/home/zylu/llumnix-v018-main
export PATH="$(dirname "$PY"):$PATH"
export PYTHONPATH="/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay:$P/tools:$P/python/discovery:$P/python:$P"
```

以下分析命令不启动服务：

```bash
"$PY" "$R/execution_sources/test_protocol.py"
"$PY" "$R/execution_sources/test_analysis.py"
"$PY" "$R/execution_sources/analyze.py" "$R" --include-core-locations
"$PY" "$R/execution_sources/pretrial_gate.py" "$R"
```

`R` 是待复核的独立结果目录。新实验目录必须包含本版 `execution_sources`；先用 `audit.py --root "$R"` 生成独立的输入副本和环境审计，再确认固定物理0/1可用、无已有控制器后启动：

```bash
"$PY" -u "$R/execution_sources/runner.py" --root "$R"
```

需提供与实际 `effective_environments.jsonl` 一致的服务环境，尤其 `VLLM_HOST_IP=127.0.0.1`、`VLLM_BATCH_INVARIANT=1`、`KVT_LEASE_TRACE=1`、`KVT_LEASE_RECHECK=1`、`KVT_LEASE_FAULT=`、`KVT500_TRACE_NATIVE=1`、`LLUMNIX_ENABLE_STRUCTURED_MIGRATION_LOG=1`、`LLUMNIX_TOKEN_TESTING=`。启动参数的最终展开结果以每个服务目录的记录为准；工厂函数的调试打印包含改写前默认值，不能代替实际启动命令。

正式阶段仅在六轮有效、机会分析支持继续时执行，`formal.py` 检查 `pretrial_gate.json`，不因“应该有收益”扩大实验。无机会时先交付预实验，不自动搜索有利参数。

## 复核原始数据

- `audit/`：环境、dirty diff、原始模板误判记录、正确模板一致性、实际库/源码清单。
- `services_01/`：展开命令、环境、进程身份、原生事件、两端输出验证、NVML CSV、Prometheus原文、EngineCore连续状态。
- 每轮 `replay.json`、`sends.jsonl`、`requests.jsonl`：同ID计划/实际到达、延迟、输出token IDs、finish_reason、错误；`measurement_rows.json` 保留尾部截止未完成项。
- 每轮 `decisions.jsonl`、`waiting_transfers.jsonl`、`running_attempts.jsonl`、`migration_terminals.jsonl`：观测、拒绝原因、事务/尝试、原生终态与 block lease 释放证据。
- 每轮 `states.jsonl`、`validity.json`、`capacity_assessment.json`：状态时间线、有效性和校准判据。

`migrating_reqs` 是此版本前端的延迟清理选择器缓存，原样保留。会话结束使用迁移终态、源端 acquire/release 配平及前端任务状态，不能把缓存成员非空误认成仍在传输。

SSE间隔为输出块间隔。TPOT=(客户端完成时间−首非空块时间)/(输出token数−1)，含客户端尾部开销。EngineCore活跃步耗时含执行与输出处理。未实测的网络字节数保留缺失。GPU功率不是服务器总用电。

首次控制器的 `execution_engines_observed` 含排队位置，不能直接当作执行位置。最终 `execution_locations.csv` 逐请求用分实例 EngineCore 非空输出事件核实执行位置；缺失证据明确标记。后续控制器还将状态观测限制为 ever_scheduled=true，不改变路由或迁移规则。

服务停止只向本实验身份核实的进程发送SIGTERM。正常卸载的历史限制不因这轮性能测量自动解除；异常退出单独保留，不能记作正常退出验收。
