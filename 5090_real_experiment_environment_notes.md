# RTX 5090 上运行 Llumnix 真实实验的环境排查记录

记录时间：2026-07-07

## 目标

希望在 4 张 RTX 5090 服务器上运行 Llumnix 真实实验，主要用于测试多实例调度和迁移机制，而不是只做仿真实验。

服务器信息：

- 登录用户：`zylu`
- 服务器：`10.112.184.62`
- GPU：4 x NVIDIA GeForce RTX 5090
- 目标模型：`/data/models/Qwen2.5-7B-Instruct`
- 项目路径：`/data/home/zylu/llumnix-ray-main`
- 虚拟环境：`/data/home/zylu/venvs/llumnix5090`

## 已完成的工作

### 1. 确认 GPU 可见

服务器能通过 `nvidia-smi` 看到 4 张 RTX 5090，说明 NVIDIA 驱动和 GPU 设备本身可用。

### 2. 修复 PyTorch 对 RTX 5090 的支持

初始环境中的 PyTorch 版本较旧，不支持 RTX 5090 的 `sm120` 架构，出现类似提示：

```text
NVIDIA GeForce RTX 5090 with CUDA capability sm_120 is not compatible with the current PyTorch installation.
```

随后安装了支持 `sm120` 的 PyTorch nightly：

```text
torch 2.12.0.dev20260408+cu128
CUDA 12.8
arch list: sm_75, sm_80, sm_86, sm_90, sm_100, sm_120
```

并通过 CUDA 矩阵乘法测试，确认 PyTorch 本身可以调用 5090。

### 3. 让 vLLM 能 import

安装 vLLM 0.24.0 后，最初缺少 CUDA 13 runtime：

```text
ImportError: libcudart.so.13: cannot open shared object file
```

之后补充了 CUDA 13 runtime 相关库，并设置：

```bash
export LD_LIBRARY_PATH=/data/home/zylu/venvs/llumnix5090/lib/python3.12/site-packages/nvidia/cu13/lib:$LD_LIBRARY_PATH
```

最终 `import vllm` 可以成功：

```text
vllm 0.24.0
```

### 4. 补齐部分 vLLM 运行依赖

由于 vLLM 使用了 `--no-deps` 方式安装，后续陆续补充了若干缺失依赖，例如：

- `cbor2`
- `openai-harmony`
- `pybase64`
- `cachetools`
- `flashinfer-python`
- `flashinfer-cubin`
- `torchvision`

其中 `flashinfer-python` 和 `flashinfer-cubin` 曾出现版本不一致：

```text
flashinfer-cubin version (0.6.13) does not match flashinfer version (0.6.14)
```

后将二者对齐到可 import 的组合。

### 5. 模型权重可以加载

在 vLLM 测试过程中，Qwen2.5-7B 权重可以加载成功：

```text
Model loading took 14.29 GiB memory
GPU KV cache size: 60,784 tokens
Maximum concurrency for 1,024 tokens per request: 59.36x
```

这说明：

- 模型路径没有问题
- 权重文件没有问题
- 单张 5090 显存足够加载该模型
- vLLM 已经能进入模型加载和 KV cache 初始化阶段

## 当前阻塞点

真实推理最终卡在 vLLM 的 attention kernel 上，而不是 Llumnix 调度逻辑上。

### 1. 默认 FlashAttention 后端不兼容

默认路径会尝试使用 vLLM 自带或依赖的 FlashAttention，报错包括：

```text
undefined symbol
FlashAttention version not detected
```

这通常说明 FlashAttention 二进制扩展与当前 PyTorch nightly ABI 不匹配。

当前环境中 PyTorch 是 `2.12.0.dev20260408+cu128`，而 vLLM 0.24.0 的依赖要求中包含：

```text
torch==2.11.0
```

因此默认 FlashAttention 路径存在 ABI 兼容风险。

### 2. `torch_sdpa` 后端不可用

尝试通过 Python API 指定：

```python
attention_backend="torch_sdpa"
```

该参数确实传入了 vLLM，但 vLLM 0.24.0 的 v1 attention registry 中没有注册该后端：

```text
ValueError: Backend TORCH_SDPA must be registered before use.
```

因此在当前版本中无法直接用 PyTorch SDPA 替代 FlashAttention/FlashInfer。

### 3. FlashInfer attention 识别不了 RTX 5090

随后改用：

```python
attention_backend="flashinfer"
```

并关闭 FlashInfer sampler：

```bash
export VLLM_USE_FLASHINFER_SAMPLER=0
```

关闭 sampler 后，vLLM 继续向前推进，说明该设置有效：

```text
FlashInfer top-p/top-k sampling disabled via VLLM_USE_FLASHINFER_SAMPLER=0.
Using AttentionBackendEnum.FLASHINFER backend.
```

但 FlashInfer attention JIT 阶段仍失败：

```text
Failed to get device capability: SM 12.x requires CUDA >= 12.9.
RuntimeError: FlashInfer requires GPUs with sm75 or higher
```

该报错中的 “requires GPUs with sm75 or higher” 对 5090 来说是误导性的。RTX 5090 是 `sm120`，显然高于 `sm75`。真正的问题是 FlashInfer 当前组合没有正确识别或支持 `sm120` / CUDA 版本。

## 结论

目前 5090 上真实推理跑不通的根因不是：

- GPU 不可用
- 显存不足
- 模型文件错误
- Llumnix 代码错误

而是：

```text
RTX 5090 sm120 太新，当前非容器 venv 中的 vLLM / FlashAttention / FlashInfer / PyTorch nightly 组合没有完全兼容。
```


## 为什么 Docker 可能解决

Docker 本身不会自动解决 `sm120` 兼容问题。它的意义在于可以换一整套干净、匹配的底层软件栈，例如：

```text
CUDA 13 devel 镜像
支持 sm120 的 PyTorch
与该 PyTorch 匹配的 vLLM
与 CUDA / PyTorch 匹配编译的 FlashInfer 或 FlashAttention
容器内自带 nvcc 和编译工具
```

当前宿主机只有 NVIDIA 驱动，没有 CUDA Toolkit 开发环境：

```text
nvcc: command not found
```

这意味着如果需要从源码编译 FlashInfer / FlashAttention / vLLM CUDA 扩展，当前宿主机环境不够完整。

## Docker 权限情况

服务器已安装 Docker：

```text
Docker version 29.3.0
```

但当前用户没有 Docker socket 权限：

```text
permission denied while trying to connect to the docker API at unix:///var/run/docker.sock
```

原因是 Docker socket 权限基本等价于宿主机 root 权限，因此共享服务器通常不会直接给普通用户开放。

如果后续继续走容器路线，需要管理员或师兄协助：

```bash
sudo usermod -aG docker zylu
```

或由管理员直接提供一个能访问 GPU 的 CUDA 13 容器环境。

## 后续可选路线

### 路线 A：使用容器重新配置环境

这是在 5090 上跑真实实验最推荐的路线。

需要管理员提供 Docker 权限，或帮忙启动一个支持 GPU 的容器。目标环境建议为：

```text
CUDA 13 devel
Python 3.12
支持 sm120 的 PyTorch
匹配版本的 vLLM / FlashInfer / FlashAttention
```

优点：

- 环境干净
- 不污染宿主机
- 可以使用 CUDA Toolkit / nvcc
- 更适合为 5090 编译或安装匹配的 kernel

缺点：

- 需要管理员权限或容器支持

### 路线 B：在宿主机重建干净 venv

可以新建一个干净虚拟环境继续尝试，但不建议删除旧环境。

原因是重建 venv 只能清理 Python 包混乱，不能解决宿主机没有 `nvcc` 的问题。如果找不到完全匹配的现成 wheel，仍可能卡在 FlashAttention / FlashInfer。

建议只作为低成本尝试，不要继续在当前已混合大量依赖的 `llumnix5090` 环境里补包。

### 路线 C：回到成熟架构 GPU 跑真实实验

如果目标是尽快获得真实实验数据，建议使用 3090、A100、H100 等当前 vLLM 支持更成熟的 GPU。

之前在 3090 服务器上已经跑通过 Llumnix + vLLM，并完成了 Llama-2-7B 迁移效率实验。

### 路线 D：在 5090 上跑仿真实验

如果主要目标是测试 Llumnix 调度逻辑，可以优先在 5090 上运行 Llumnix simulator。

仿真模式不依赖真实 vLLM attention kernel，因此不会被 FlashAttention / FlashInfer 的 `sm120` 兼容问题卡住。

适合测试：

- 多实例调度策略
- 请求分配
- 迁移策略
- 不同负载下的调度效果

但仿真实验不能替代真实 GPU kernel 的迁移开销测量。

## 当前建议

暂时不要继续在当前 venv 中补包。

如果坚持在 5090 上跑真实实验，下一步应优先争取容器环境，而不是继续手动修当前虚拟环境。

可以向管理员或师兄说明：

```text
当前 RTX 5090 上本地 venv 已能让 PyTorch 识别 sm120，也能让 vLLM 加载模型，
但真实推理卡在 FlashAttention / FlashInfer 对 sm120 的兼容问题。
宿主机没有 nvcc，无法方便地从源码编译匹配的 CUDA kernel。
希望提供一个 CUDA 13 devel + GPU 访问权限的 Docker/容器环境，
用于重新配置 vLLM 和 Llumnix 真实实验环境。
```

