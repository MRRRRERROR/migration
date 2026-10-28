# 运行中迁移的增量收益实验

本实验为同机两张 GPU 的本地实验，比较运行中 KV 迁移相对于相同动态路由和无状态等待转移规则的增量。

服务期请求转移正确性验收：通过。已形成完整计量数据的轮次：36。正常卸载另见资源证据，不包含在该准入验收中。

生成报告时状态：blocked；当前目录 None。校准/预实验/正式完整记录数分别为 0/6/30；未完成阶段不能当作已有结果。

另有故障记录：RuntimeError("Authorized GPU busy/query unavailable: {'role': 'S', 'gpu_index': 0, 'gpu_uuid': 'GPU-0fc3a8e6-93f3-9c89-f694-bf490b219907', 'monotonic_s': 1524374.789691985, 'wall_ns': 1793127095005706946, 'power_w': 37.414, 'memory_mib': 1.0625, 'reserved_memory_mib': 498.0, 'nvml_memory_api': 'v2', 'utilization_pct': 100, 'temperature_c': 45.0, 'sm_clock_mhz': 2017.0, 'memory_clock_mhz': 14001.0, 'power_limit_w': 575.0, 'compute_pids': '[]'}")。失败现场与原始日志保留，需按调用栈和时间区分测量与收尾；不自动重写各轮原始validity。

formal_high_s8101：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/4 次。B−A的E2E p95差值 -0.465 秒，C−B差值 -0.247 秒；负值代表延迟下降。有效性：True。

formal_high_s8102：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/3 次。B−A的E2E p95差值 0.723 秒，C−B差值 -0.660 秒；负值代表延迟下降。有效性：True。

formal_high_s8103：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/5 次。B−A的E2E p95差值 0.851 秒，C−B差值 -0.258 秒；负值代表延迟下降。有效性：True。

formal_high_s8104：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/8 次。B−A的E2E p95差值 -0.546 秒，C−B差值 0.735 秒；负值代表延迟下降。有效性：True。

formal_high_s8105：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/5 次。B−A的E2E p95差值 -0.587 秒，C−B差值 0.122 秒；负值代表延迟下降。有效性：True。

formal_medium_s8101：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/3 次。B−A的E2E p95差值 -2.424 秒，C−B差值 2.193 秒；负值代表延迟下降。有效性：True。

formal_medium_s8102：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/3 次。B−A的E2E p95差值 -0.413 秒，C−B差值 -0.808 秒；负值代表延迟下降。有效性：True。

formal_medium_s8103：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/3 次。B−A的E2E p95差值 -0.318 秒，C−B差值 -0.300 秒；负值代表延迟下降。有效性：True。

formal_medium_s8104：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/0 次。B−A的E2E p95差值 1.377 秒，C−B差值 0.035 秒；负值代表延迟下降。有效性：True。

formal_medium_s8104_C 未发起运行中迁移。最大运行数差 4，差至少3的最长同方向连续观测片段 1.257 秒；逐次拒绝计数 {'difference_insufficient': 664, 'difference_not_persistent': 6}。没有运行中动作时，该次C−B差异不能归因于运行中迁移收益。

formal_medium_s8105：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/3 次。B−A的E2E p95差值 -0.386 秒，C−B差值 -0.376 秒；负值代表延迟下降。有效性：True。

pre_high：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/7 次。B−A的E2E p95差值 -1.877 秒，C−B差值 0.487 秒；负值代表延迟下降。有效性：True。

pre_medium：A/B/C实际等待转移完成 0/0/0 次；运行中KV迁移完成 0/0/4 次。B−A的E2E p95差值 0.487 秒，C−B差值 0.390 秒；负值代表延迟下降。有效性：True。

pre_high_C 同ID输出核对：1251 对完整输出中，0 对不一致；B/C实际收到输出token合计 932824/932824。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

pre_medium_C 同ID输出核对：300 对完整输出中，0 对不一致；B/C实际收到输出token合计 234719/234719。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_high_s8101_C 同ID输出核对：1184 对完整输出中，0 对不一致；B/C实际收到输出token合计 870077/870077。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_high_s8102_C 同ID输出核对：1262 对完整输出中，0 对不一致；B/C实际收到输出token合计 924038/924038。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_high_s8103_C 同ID输出核对：1285 对完整输出中，0 对不一致；B/C实际收到输出token合计 926288/926288。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_high_s8104_C 同ID输出核对：1202 对完整输出中，0 对不一致；B/C实际收到输出token合计 855184/855184。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_high_s8105_C 同ID输出核对：1214 对完整输出中，0 对不一致；B/C实际收到输出token合计 861829/861829。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_medium_s8101_C 同ID输出核对：302 对完整输出中，0 对不一致；B/C实际收到输出token合计 214898/214898。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_medium_s8102_C 同ID输出核对：301 对完整输出中，0 对不一致；B/C实际收到输出token合计 220414/220414。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_medium_s8103_C 同ID输出核对：317 对完整输出中，0 对不一致；B/C实际收到输出token合计 211327/211327。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

formal_medium_s8104_C 同ID输出核对：278 对完整输出中，0 对不一致；B/C实际收到输出token合计 206633/206633。C没有运行中迁移对象，无法估计迁移对象、源端其他请求与目标端已有请求三组的迁移影响。

formal_medium_s8105_C 同ID输出核对：306 对完整输出中，0 对不一致；B/C实际收到输出token合计 191463/191463。三组同ID延迟与工作量详见 paired_B_groups.csv；分组可能重叠。

正式配对结果：[{"load": "medium", "metric": "C_minus_B_p95_s", "planned_pairs": 5, "observed_pairs": 5, "finite_pairs": 5, "mean_difference_s": 0.14888528417795896, "ci95_low_s": -1.3183124810209665, "ci95_high_s": 1.6160830493768843, "method": "5 independent workload-seed pairs, Student t df=4; small-sample descriptive uncertainty; no request-level independence assumption", "missing_pair_policy": "No aggregate estimate if any planned seed pair missing/nonfinite"}, {"load": "medium", "metric": "B_minus_A_p95_s", "planned_pairs": 5, "observed_pairs": 5, "finite_pairs": 5, "mean_difference_s": -0.4328061564359814, "ci95_low_s": -2.105063804407349, "ci95_high_s": 1.2394514915353865, "method": "5 independent workload-seed pairs, Student t df=4; small-sample descriptive uncertainty; no request-level independence assumption", "missing_pair_policy": "No aggregate estimate if any planned seed pair missing/nonfinite"}, {"load": "high", "metric": "C_minus_B_p95_s", "planned_pairs": 5, "observed_pairs": 5, "finite_pairs": 5, "mean_difference_s": -0.06161692114546895, "ci95_low_s": -0.7125930717751945, "ci95_high_s": 0.5893592294842567, "method": "5 independent workload-seed pairs, Student t df=4; small-sample descriptive uncertainty; no request-level independence assumption", "missing_pair_policy": "No aggregate estimate if any planned seed pair missing/nonfinite"}, {"load": "high", "metric": "B_minus_A_p95_s", "planned_pairs": 5, "observed_pairs": 5, "finite_pairs": 5, "mean_difference_s": -0.004877884453162551, "ci95_low_s": -0.9056095643421405, "ci95_high_s": 0.8958537954358154, "method": "5 independent workload-seed pairs, Student t df=4; small-sample descriptive uncertainty; no request-level independence assumption", "missing_pair_policy": "No aggregate estimate if any planned seed pair missing/nonfinite"}]。仅5个种子，区间依赖种子层面差值的近似分布；不能据此宣称广泛普适性。

A 只动态路由；B 额外允许在 EngineCore 边界确认从未执行的等待请求转移；C 在完整 B 上追加周期性运行中 KV 迁移。实际动作次数及完成/取消/失败/未发起分类见 summary.csv、migration_terminals.jsonl 和 decisions.jsonl。没有实际等待转移时，不能把 B−A 的运行波动视为等待转移的贡献。

B−A 与 C−B 的逐轮差值见 paired_round_differences.csv。负差表示延迟下降。预实验每个负载只有一次配对，不能据此声称稳定收益或统计显著。正式五种子结果才允许按种子计算均值和不确定区间。

C 选择的迁移对象、当时源端其他请求、目标端已有请求均按同 request_id 提取 B 对应结果；这是依赖 C 决策的事后分组，不是独立随机对照。见 paired_B_groups.csv。

是否存在机会：见 opportunity_summary.json 中运行数差的持续时间、持续差异片段和逐次拒绝原因。零次迁移本身不能证明没有机会，也不能证明迁移无价值。

状态曲线同时展示 running、waiting、KV 占用和近期 EngineCore 活跃步耗时。步耗时包含引擎执行与输出处理，不等同于纯 GPU kernel 时间。传输量若原日志未实测则保持缺失，不用 KV 大小冒充网络传输量。

主 E2E 分位以所有已发送请求为分母；失败和超时不删除，有限完成时间覆盖不足相应分位时填空。TTFT 表是完成请求的条件分布，完成率需同时阅读。TPOT=(客户端完成时间−首个非空输出块时间)/(输出token数−1)，是客户端均值口径。SSE 间隔称输出块间隔。

有输出 token IDs 或 finish_reason 不一致时，同时报告输出工作量差异，不能将全部延迟差归于迁移。GPU 功率仅作辅助观测，不代表服务器或机房总用电。

结论范围：只适用于这里的模型、两张 GPU、负载和冻结的简单控制规则，不能外推所有运行中迁移或跨数据中心系统。
