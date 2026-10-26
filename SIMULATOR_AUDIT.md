# Llumnix Simulator Route Audit

> 2026-10-20 更新：本文保留 2026-08-30 的历史审计结论。新增 token 迁移离散事件仿真入口为 `tools/run_migration_simulation.py`，当前能力与跨域缺口见 [TOKEN_SIMULATOR_AND_CROSS_DOMAIN_AUDIT.md](docs/TOKEN_SIMULATOR_AND_CROSS_DOMAIN_AUDIT.md)。旧 legacy HTTP 仿真路径仍不支持 token 迁移。

生成时间：2026-08-30

## 目标

只做策略级仿真准备，不跑真实模型实验。目标路径是：用已有 vLLM simulator 后端，加上 RTX 5090 + Qwen2.5-7B 的真实实例日志生成 `profiling .pkl`，再用 `--simulator-mode` 复现 4 实例策略实验趋势。

## 1. sim_llm_engine.py 审计

文件：`llumnix/backends/vllm/sim_llm_engine.py`

结论：

- simulator 后端类是 `BackendSimVLLM`。
- simulator 入口依赖 legacy vLLM 后端，不是 `vllm_v1`。
- `BackendSimVLLM` 会从 `instance_args.profiling_result_file_path` 读取 profiling 数据。
- profiling 数据通过 `ProfilingDatabase` 加载，并按：
  - `model_name = os.path.basename(model_config.model)`
  - `SimParallelConfig(tp_size, pp_size)`
  取出对应 `LatencyMemData`。
- cache 迁移在 simulator 中通过 `send_cache(len(src_blocks))` 模拟，不跑真实 KV 传输。

重要限制：

- `llumnix/entrypoints/vllm_v1/arg_utils.py` 明确断言：`Llumnix does not support simulator mode for vLLM v1.`
- 因此 simulator smoke 应使用 `llumnix.entrypoints.vllm.api_server`，不要使用 `llumnix.entrypoints.vllm_v1.api_server`。
- `llumnix/entrypoints/vllm/arg_utils.py` 要求 simulator mode 下关闭 async output processing，即需要传 `--disable-async-output-proc`。

## 2. profiling.py 输入输出格式

文件：`llumnix/backends/profiling.py`

输入：

- `--log-csv-path` 指向真实 Llumnix benchmark 产生的实例 CSV。
- CSV 需要至少包含：
  - `bs`
  - `profiling_data`
- `profiling_data` 格式由 `_extract_data()` 解析，典型值：
  - `('prefill', batch_size, total_seq_len, stage_latency)`
  - `('decode', batch_size, total_seq_len, stage_latency)`

输出：

- `--database` 指定 `.pkl` 文件。
- pkl 内是 `ProfilingDatabase`，保存：
  - model 名称
  - parallel config
  - prefill latency
  - decode latency
  - cache config / num gpu blocks
  - migration bandwidth / swap bandwidth

生成命令需要提供：

- `--model`
- `--tp`
- `--pp`
- `--gpu-memory-utilization`
- `--block-size`
- `--max-num-batched-tokens`
- `--num-gpu-blocks`
- `--new-data`

## 3. tools/generate_5090_profiling_from_logs.py 审计

文件：`tools/generate_5090_profiling_from_logs.py`

结论：

- 该脚本读取 gateway 日志，不读取 `server_instance.csv`。
- 它搜索的日志模式是：`03_runs/*/Hybrid_no_migration/run_*/gateway.log`。
- 它解析 `status:200`、`RT`、`TTFT`、`ITL`、`input_tokens`、`output_tokens`、`work`。
- 输出是：
  - `ttft_5090_qwen25_7b.json`
  - `tpot_5090_qwen25_7b.json`
  - `PROFILING_DATA_AUDIT.md`
- 这些 JSON 可以作为 5090 请求级延迟画像或策略分析输入，但不是 `llumnix.backends.profiling.ProfilingDatabase` 的直接输入。

判断：

- 不能直接用该脚本生成 simulator 所需的 `qwen2p5_7b_5090_profile.pkl`。
- 要生成 simulator pkl，需要 `server_instance.csv`，或者新增转换脚本把 gateway JSON/log 转成 `ProfilingDatabase` 结构。

## 4. 已找到的 --log-instance-info CSV

这些 CSV 的表头包含 `bs` 和 `profiling_data`，格式上可以被 `llumnix.backends.profiling.py` 读取：

| 路径 | 大小 | 时间 | 备注 |
|---|---:|---|---|
| `D:\llumnix-ray-main\baseline_first2_retry_20260701_204759\load_no_migration\server_instance.csv` | 9426 | 2026-07-01T20:51:25 | 可读，但不是 5090 Qwen2.5-7B 正式日志 |
| `D:\llumnix-ray-main\baseline_first2_retry_20260701_204759\rr_no_migration\server_instance.csv` | 16482 | 2026-07-01T20:49:12 | 可读，但不是 5090 Qwen2.5-7B 正式日志 |
| `D:\llumnix-ray-main\baseline_sim_opt-1.3b_20260701_192646\load_migration_defrag\server_instance.csv` | 25756 | 2026-07-01T20:24:18 | 可读，但是 OPT/simulator 旧记录 |
| `D:\llumnix-ray-main\enhanced_sim_opt-1.3b_20260701_212246\llumnix_sim_migration\server_instance.csv` | 18494 | 2026-07-01T21:37:14 | 可读，但是 OPT/simulator 旧记录 |
| `D:\llumnix-ray-main\enhanced_sim_opt-1.3b_20260701_212246\load_migration_defrag\server_instance.csv` | 25435 | 2026-07-01T21:33:08 | 可读，但是 OPT/simulator 旧记录 |
| `D:\llumnix-ray-main\enhanced_sim_opt-1.3b_20260701_212246\load_no_migration\server_instance.csv` | 36096 | 2026-07-01T21:29:10 | 可读，但是 OPT/simulator 旧记录 |
| `D:\llumnix-ray-main\enhanced_sim_opt-1.3b_20260701_212246\rr_no_migration\server_instance.csv` | 39434 | 2026-07-01T21:24:55 | 可读，但是 OPT/simulator 旧记录 |
| `D:\llumnix-ray-main\llumnix_sim_opt-1.3b_20260701_210403\server_instance.csv` | 20069 | 2026-07-01T21:05:20 | 可读，但是 OPT/simulator 旧记录 |
| `D:\llumnix-ray-main\opt-1.3b_20260628_153807\server_instance.csv` | 2336874 | 2026-06-28T15:58:33 | 可读，但是 OPT 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\clean_server_256_instance.csv` | 136425 | 2026-07-01T23:13:07 | 可读，但是 LLaMA2-7B 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\clean_server_512_instance.csv` | 234845 | 2026-07-01T23:15:09 | 可读，但是 LLaMA2-7B 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\clean_server_1024_instance.csv` | 430309 | 2026-07-01T23:17:21 | 可读，但是 LLaMA2-7B 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\clean_server_1536_instance.csv` | 588928 | 2026-07-01T23:19:49 | 可读，但是 LLaMA2-7B 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\clean_server_2048_instance.csv` | 13489 | 2026-07-01T23:21:41 | 可读，但是 LLaMA2-7B 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\rerun_server_256_instance.csv` | 534755 | 2026-07-01T23:27:32 | 可读，但是 LLaMA2-7B 旧记录 |
| `D:\llumnix-ray-main\llama2-7b_migration_efficiency_clean_20260701_231125\rerun_server_2048_instance.csv` | 13062 | 2026-07-01T23:29:41 | 可读，但是 LLaMA2-7B 旧记录 |

5090 正式结果目录：

- `D:\llumnix-ray-main\downloaded_results_0721\5090_4inst_formal_20260721_014617\formal_results.csv`
- 该文件是正式实验汇总，不包含 `bs` / `profiling_data`，不能直接作为 `profiling.py` 输入。

## 5. 已找到的 profiling .pkl

本地审计范围内未发现已有 `.pkl` profiling 文件。

已找到的 5090 profiling 相关文件是 JSON：

- `D:\llumnix-ray-main\results\5090_migration_policy_baselines_20260819_201922\00_config\profiling_5090\ttft_5090_qwen25_7b.json`
- `D:\llumnix-ray-main\results\5090_migration_policy_baselines_20260819_201922\00_config\profiling_5090\tpot_5090_qwen25_7b.json`
- `D:\llumnix-ray-main\results\5090_migration_policy_baselines_20260819_201922\00_config\profiling_5090\PROFILING_DATA_AUDIT.md`

## 6. 是否能直接生成 qwen2p5_7b_5090_profile.pkl

当前本地结果中不能直接生成严格的 `qwen2p5_7b_5090_profile.pkl`。

原因：

- `profiling.py` 需要带 `bs` 和 `profiling_data` 的 `server_instance.csv`。
- 本地已拉取的 RTX 5090 Qwen2.5-7B 正式目录没有发现这种 CSV。
- 已有 `tools/generate_5090_profiling_from_logs.py` 生成的是 TTFT/TPOT JSON，不是 simulator pkl。

可以直接生成的情况：

- 在远端 `/data/home/zylu/llumnix-v018-main` 或历史日志中找到 RTX 5090 + Qwen2.5-7B + `--log-instance-info` 产生的 `server_instance.csv`。

不能找到该 CSV 时：

- 需要写一个小转换脚本，把已有 5090 gateway log / TTFT TPOT JSON 转成 `ProfilingDatabase` pkl；这属于下一阶段，不在本次只读审计内。

## 7. 推荐 profiling pkl 路径

远端推荐路径：

```bash
/data/home/zylu/llumnix-v018-main/results/simulator_5090_qwen25_7b/qwen2p5_7b_5090_profile.pkl
```

本地镜像路径：

```text
D:\llumnix-ray-main\results\simulator_5090_qwen25_7b\qwen2p5_7b_5090_profile.pkl
```

## 8. 下一步生成 profiling 的命令

下面命令需要把 `<CSV_WITH_bs_AND_profiling_data>` 替换成远端真实 5090 Qwen2.5-7B 的 `server_instance.csv`。

```bash
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

mkdir -p results/simulator_5090_qwen25_7b

python -m llumnix.backends.profiling \
  --database results/simulator_5090_qwen25_7b/qwen2p5_7b_5090_profile.pkl \
  --log-csv-path <CSV_WITH_bs_AND_profiling_data> \
  --model Qwen2.5-7B-Instruct \
  --tp 1 \
  --pp 1 \
  --gpu-memory-utilization 0.50 \
  --block-size 16 \
  --max-num-batched-tokens 2048 \
  --num-gpu-blocks <NUM_GPU_BLOCKS_FROM_5090_INSTANCE_LOG> \
  --new-data
```

## 9. 下一步 simulator smoke 命令

先启动 4 实例 simulator 服务：

```bash
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

pkill -f "llumnix.entrypoints.vllm.api_server" || true
pkill -f "benchmark/benchmark_serving.py" || true
ray stop --force || true
sleep 5

export CUDA_VISIBLE_DEVICES=0,1,2,3
export CUDA_HOME=$CONDA_PREFIX
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:$LD_LIBRARY_PATH

python -u -m llumnix.entrypoints.vllm.api_server \
  --config-file configs/vllm.yml \
  --port 1234 \
  --ray-cluster-port 6389 \
  --initial-instances 4 \
  --model /data/models/Qwen2.5-7B-Instruct \
  --trust-remote-code \
  --max-model-len 1024 \
  --max-num-batched-tokens 2048 \
  --gpu-memory-utilization 0.50 \
  --dispatch-policy flood \
  --simulator-mode \
  --profiling-result-file-path results/simulator_5090_qwen25_7b/qwen2p5_7b_5090_profile.pkl \
  --disable-async-output-proc \
  --log-instance-info \
  --log-filename qwen_5090_sim_smoke.log \
  2>&1 | tee qwen_5090_sim_smoke.console.log
```

再开另一个终端跑最小 benchmark：

```bash
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

python benchmark/benchmark_serving.py \
  --ip_ports 127.0.0.1:1234 \
  --tokenizer /data/models/Qwen2.5-7B-Instruct \
  --backend vLLM \
  --gen_random_prompts \
  --random_prompt_count 5 \
  --random_prompt_lens_mean 64 \
  --random_prompt_lens_range 1 \
  --variable_prompt_lens_distribution uniform \
  --allow_variable_generation_length \
  --variable_response_lens_mean 32 \
  --variable_response_lens_range 1 \
  --variable_response_lens_distribution uniform \
  --qps 1 \
  --distribution poisson \
  --log_latencies \
  --fail_on_response_failure
```

## 10. 本阶段结论

simulator 入口和 pkl 生成工具都存在，路线可行；当前缺口是缺少 RTX 5090 + Qwen2.5-7B 的 `server_instance.csv`，或者缺少从现有 5090 gateway JSON/log 转 `ProfilingDatabase` pkl 的转换脚本。
