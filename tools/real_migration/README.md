# Llumnix real migration scripts

These scripts are intended for the lab server setup:

- repo: `~/llumnix-ray-main`
- virtualenv: `~/venvs/llumnix`
- model: `~/models/opt-125m`
- GPUs: `0,1`
- service port: `1234`
- Ray port: `6389`

## 1. Check GPUs

```bash
cd ~/llumnix-ray-main
bash tools/real_migration/check_gpu_ready.sh
```

Run the 2-GPU experiment only when both GPUs are idle except for small Xorg processes.

## 2. Start 2-GPU Llumnix

```bash
cd ~/llumnix-ray-main
bash tools/real_migration/start_2gpu_service.sh
```

On the RTX 4090 server configured with `tools/setup_4090_server.sh`, use the
guarded smoke-test wrapper. It refuses to start if either selected GPU already
has more than 256 MiB of compute allocations:

```bash
cd ~/llumnix-ray-main
CUDA_VISIBLE_DEVICES=1,2 bash tools/real_migration/start_4090_smoke.sh
```

The script starts the service in the background and prints the experiment directory.

## 3. Run benchmark

Use the printed `EXP_DIR`:

```bash
EXP_DIR="$HOME/llumnix_experiments/<printed-name>" \
bash tools/real_migration/run_migration_benchmark.sh
```

For a lighter run:

```bash
PROMPT_COUNT=100 QPS=4 PROMPT_MEAN=256 RESPONSE_MEAN=256 \
EXP_DIR="$HOME/llumnix_experiments/<printed-name>" \
bash tools/real_migration/run_migration_benchmark.sh
```

## 4. Stop

```bash
EXP_DIR="$HOME/llumnix_experiments/<printed-name>" \
bash tools/real_migration/stop_llumnix.sh
```

## Useful overrides

```bash
CUDA_VISIBLE_DEVICES=0,1
MODEL_PATH="$HOME/models/opt-125m"
PORT=1234
RAY_PORT=6389
INITIAL_INSTANCES=2
```
