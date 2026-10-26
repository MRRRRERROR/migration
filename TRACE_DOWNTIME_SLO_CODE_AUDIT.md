# Llumnix 停机时间与 SLO Trace 代码审计

## 当前执行路径

当前 RTX 5090 / vLLM 0.18 实验使用以下路径：

- 服务入口：`vllm serve`，通过 Llumnix gateway 暴露 OpenAI `/v1/completions` 流式接口。
- 论文补充实验 runner：`run_paper_supplement_experiments.py` 与 `run_figure12_v2.py`。
- 通用在线 benchmark：`benchmarks/benchmark_serving.py`。
- Figure 12 自定义请求函数：`run_figure12_v2.py::stream_completion`。
- 汇总入口：`run_figure12_v2.py::enrich_summary`、`aggregate_figure12_fragmentation.py` 和各 runner 的 `--phase aggregate`。

旧版仓库中的 `llumnix/entrypoints/vllm_v1/api_server.py::generate_benchmark` 会在每次 generator 更新时生成 `per_token_latency`，并返回 `request_id`。v0.18 当前服务树不再使用该 API，因此不能把旧文件直接补进当前运行路径。

## Token 时间戳能力

`benchmarks/benchmark_serving.py::async_request_openai_completions` 已记录客户端收到流式数据的 TTFT 与 ITL，但只保留聚合数组，没有按请求写出时间戳、墙钟起点和 request ID。Figure 12 的 `stream_completion` 也记录首个非空 SSE 数据块时间，但不保存后续逐 token 时间。

新客户端 `tools/trace_benchmark_client.py` 使用 `X-Request-Id` 固定请求 ID，并保存客户端 `time.time()` 墙钟起点及相对 `perf_counter()` token 到达时间。客户端用每个 SSE 事件携带的累计 `completion_tokens` 计算 token 增量；同一 SSE 事件新增的 token 在客户端同一时刻可见，因此记录相同接收时间。只有最终 token 数与展开后的时间戳数严格相等且所有非空块都有 usage 计数时，才标记 `strict_per_token_trace=true`。

`server` 模式保留为兼容旧 `/generate_benchmark` 的接口。它要求响应同时提供可对齐的 `request_start_wall_time_ms`，否则只能计算 TTFT/TPOT，不能计算用户可见停机时间。

## 迁移事件能力

当前 `python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py` 已将事件写入 `LLUMNIX_MIGRATION_EFFICIENCY_LOG` 指定的 JSONL。已确认真实结果包含：

- `migration_initiated`：迁移开始；
- `final_suspend_begin`：源请求进入最终暂停；
- `dst_resume`：目标端恢复；
- `migration_done`：迁移结束；
- `invalid_migration_endpoint`、`stale_migration_update` 等失败事件。

这些记录包含 `request_id`、`timestamp_ms`、`source_instance_id`、`destination_instance_id`，可直接解析并与 `X-Request-Id` 对齐。新增补丁只在 `LLUMNIX_ENABLE_STRUCTURED_MIGRATION_LOG=1` 时，将可见事件额外写成 `LLUMNIX_MIGRATION_EVENT {...}` logger 行；原 JSONL 和迁移返回逻辑保持不变。

## 时钟与严格性

- migration event 使用 `time.time()`；client trace 保存请求开始时的 `time.time()` 与之后相对 `perf_counter()` 时间。
- OpenAI response ID 与 EngineCore ID 的关系为 `<response_id>-<DP rank>`；分析器只接受唯一的纯数字 DP 后缀匹配。
- 绝对 token 时间按 `request_start_wall_time_ms + token_timestamps_ms` 对齐。
- 找不到迁移前最后一个 token或迁移后第一个 token时，`visible_downtime_ms=null`。
- SSE 块不能证明为单 token、request ID 不一致或缺少墙钟起点时，不估算、不均摊、不伪造 downtime。
- `visible_downtime_ms` 是用户可见 token gap，不等同于 `migration_internal_time_ms`。

## 最小修改文件

1. `tools/trace_benchmark_client.py`：新增、默认不采集，CLI 开关启用。
2. `tools/analyze_downtime_slo.py`：新增纯离线分析器。
3. `python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py`：仅增加默认关闭的结构化 logger 镜像。

没有修改 KVT/blade_kvt 搬运逻辑、vLLM EngineCore、迁移状态机或策略算法。
