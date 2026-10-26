# RTX 5090 上 Llumnix 真实实验调试总结与后续方案

记录日期：2026-07-08

## 1. 实验目标

本轮调试的目标是在 4 张 RTX 5090 服务器上运行 Llumnix 真实实验，用于后续测试多实例调度、迁移机制和调度策略效果。

服务器基本信息：

- 登录用户：`zylu`
- 服务器地址：`10.112.184.62`
- GPU：4 x NVIDIA GeForce RTX 5090
- 项目路径：`/data/home/zylu/llumnix-ray-main`
- 模型路径：`/data/models/Qwen2.5-7B-Instruct`
- 当前主要环境：`llumnix5090_cuda13`
- 环境路径：`/data/home/zylu/.conda/envs/llumnix5090_cuda13`

## 2. 已完成工作

### 2.1 确认 GPU 可用

服务器能正常看到 4 张 RTX 5090。驱动支持 CUDA 13.0，说明硬件和驱动层面具备运行新架构 GPU 的基础条件。

### 2.2 解决普通用户没有系统 CUDA Toolkit 的问题

服务器宿主机没有 `nvcc`，普通用户也没有 sudo 权限，无法安装系统级 CUDA Toolkit。

采用的解决办法是在用户目录下使用 conda 创建新环境，并安装 CUDA Toolkit 和 nvcc：

```bash
conda create -n llumnix5090_cuda13 python=3.12
conda activate llumnix5090_cuda13
conda install -c nvidia -c conda-forge cuda-toolkit cuda-nvcc
```

最终确认：

```text
nvcc: NVIDIA Cuda compiler driver
Cuda compilation tools, release 13.0, V13.0.88
```

这说明用户态 CUDA 13 开发环境已经可用。

### 2.3 配置支持 RTX 5090 的 PyTorch

早期环境中的 PyTorch 不支持 RTX 5090 的 `sm_120` 架构，会出现类似提示：

```text
NVIDIA GeForce RTX 5090 with CUDA capability sm_120 is not compatible with the current PyTorch installation.
```

后续安装支持 CUDA 13.0 的 PyTorch，并验证 GPU 矩阵计算通过：

```text
torch 2.11.0+cu130
torch cuda 13.0
arch list: sm_75, sm_80, sm_86, sm_90, sm_100, sm_120
gpu: NVIDIA GeForce RTX 5090
```

这说明 PyTorch 已经能识别并使用 RTX 5090。

### 2.4 配置 vLLM 和 FlashInfer

安装 vLLM 0.24.0 后，逐步补齐依赖，并解决了 CUDA runtime、FlashInfer 版本不匹配等问题。

最终 FlashInfer 能识别 CUDA 13.0 和 RTX 5090 的 `sm_120`：

```text
FLASHINFER_CUDA_VERSION: 13.0
CUDA_VERSION: 13.0
FLASHINFER_CUDA_ARCH_LIST: {(12, '0f')}
NVCC found: Yes
```

### 2.5 单独 vLLM 推理已经跑通

使用 `/data/models/Qwen2.5-7B-Instruct` 进行单卡 vLLM smoke test，模型能够加载并生成文本。

这一步很关键，说明：

- RTX 5090 可用；
- CUDA 13 用户态环境可用；
- PyTorch 可用；
- FlashInfer 基本可用；
- vLLM 单独推理可用；
- 模型文件路径和权重没有问题。

因此，当前问题不是 GPU 不能跑，也不是模型坏了。

## 3. 当前阻塞点

### 3.1 Llumnix 服务没有成功启动

尝试使用 `llumnix.entrypoints.vllm_v1.api_server` 启动 Llumnix 后，Ray 能连接成功：

```text
Connecting to existing Ray cluster at address: 10.112.184.62:6389...
Connected to Ray cluster.
```

但是服务没有继续完成初始化：

- 没有打开 `127.0.0.1:1234` 服务端口；
- 没有创建 `scaler`、`manager`、`instance_...` 等 Llumnix actor；
- benchmark 无法连接服务；
- 进程长时间存在但 CPU 使用率很低，表现为卡住。

检查 Ray 后发现：

```text
resources: {'GPU': 1.0, 'CPU': 192.0, ...}
actors:
```

这说明 Ray 资源存在，但 Llumnix actor 没有创建出来。

### 3.2 已发现的兼容性问题

调试过程中已经暴露出多个 Llumnix 与新版 vLLM 的接口不兼容问题：

- 旧入口 `llumnix.entrypoints.vllm.api_server` 不兼容 vLLM 0.24；
- vLLM 0.24 中 `EngineConfig` 不再能从 `vllm.config` 导入；
- `AsyncStream` 导入路径发生变化；
- `--host` 参数与 vLLM 0.24 CLI 发生冲突；
- `--worker-use-ray`、`--disable-async-output-proc` 等旧参数在 vLLM v1 路径下不再适用；
- Llumnix vLLM-v1 入口虽然能进入 Ray 连接阶段，但后续 actor 初始化没有成功完成。

因此当前判断是：

> 5090 真实实验没有跑通的主要原因不是硬件或模型，而是 Llumnix 代码与 vLLM 0.24 / CUDA 13 / RTX 5090 组合存在兼容性问题。

## 4. 当前结论

截至目前，本轮 5090 调试得到的结论如下：

1. RTX 5090 硬件可用。
2. 用户态 conda CUDA 13.0 环境可用。
3. PyTorch 已经支持 `sm_120`。
4. FlashInfer 已经基本识别 CUDA 13 和 `sm_120`。
5. 单独 vLLM 真实推理可以跑通。
6. Llumnix 真实服务暂未跑通，卡在 Ray/Llumnix actor 初始化阶段。
7. 当前阻塞点更可能是 Llumnix 与新版 vLLM 的适配问题，而不是 GPU、模型或 CUDA 本身的问题。

一句话总结：

> 5090 上的底层推理链路已经打通，但 Llumnix 对 vLLM 0.24 的适配还不完整，因此真实 Llumnix 服务没有成功启动。

## 5. 后续可行方案

### 方案 A：继续在 5090 上适配 Llumnix + vLLM 0.24

这是最直接但工程量最大的路线。

下一步应该做的是给 Llumnix 初始化链路加日志，定位卡点：

- `llumnix/entrypoints/setup.py`
- `init_scaler`
- `ray.get(scaler.is_ready.remote())`
- `scaler.init_instances.remote(...)`
- `instance.is_ready.remote()`
- `llumnix/scaler.py`
- `llumnix/llumlet/llumlet.py`
- `llumnix/entrypoints/vllm_v1/api_server_actor.py`

目标是确认到底是：

- actor 没创建；
- actor 创建了但 import 失败；
- vLLM engine 初始化失败；
- Ray 调度卡住；
- 还是 Llumnix 与 vLLM 0.24 的对象接口不兼容。

优点：

- 如果成功，可以直接利用 4 张 5090 做真实多实例调度实验；
- 对后续扩展有价值；
- 能真正验证新 GPU 上的服务系统。

缺点：

- 需要改 Llumnix 源码；
- 可能要连续处理多个 vLLM 0.24 API 变化；
- 时间不可控。



### 方案 B：换成 Llumnix 原始支持的 vLLM 版本

Llumnix 论文和当前项目更可能是基于较旧 vLLM 版本开发的。可以尝试找到项目原始依赖版本，使用更接近作者环境的 vLLM。

问题是：较旧 vLLM / PyTorch 很可能不支持 RTX 5090 的 `sm_120`。

因此这条路线在 5090 上存在矛盾：

- Llumnix 需要旧 vLLM；
- 5090 需要新 PyTorch / 新 CUDA / 新 kernel。

优点：

- Llumnix 代码改动少；
- 更接近作者实现。

缺点：

- 旧 vLLM 很可能跑不了 5090；
- 可能重新遇到 `sm_120 not compatible`；
- 可能需要从源码编译 vLLM 和 CUDA kernel。



### 方案 E：争取 Docker 权限或由管理员提供容器

5090 的环境兼容问题本质上是 CUDA、PyTorch、vLLM、FlashInfer、FlashAttention 版本组合复杂。Docker 能提供一个干净、可复现、可编译的环境。

当前普通用户没有 Docker socket 权限：

```text
permission denied while trying to connect to the docker API at unix:///var/run/docker.sock
```

如果师兄或管理员能提供 Docker 权限，可以尝试：

- CUDA 13 devel 镜像；
- PyTorch cu130；
- vLLM 0.24；
- FlashInfer cu130；
- 必要时从源码编译 kernel。

优点：

- 环境最干净；
- 不污染宿主机；
- 适合编译；
- 可复现性更好。

缺点：

- 需要管理员配合；
- Docker 权限相当于较高系统权限，实验室服务器不一定愿意开放。



