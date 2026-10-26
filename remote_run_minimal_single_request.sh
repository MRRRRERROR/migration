set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

cd /data/home/zylu/llumnix-v018-main

pkill -f 'redis-server.*6379' || true
pkill -f './bin/gateway' || true
pkill -f './bin/scheduler' || true
pkill -f 'vllm serve' || true
pkill -f 'discovery.discovery' || true
sleep 3

export CUDA_VISIBLE_DEVICES=0,1,2,3
export CUDA_HOME=$CONDA_PREFIX
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:/data/home/zylu/opt/barex/usr/lib:${LD_LIBRARY_PATH:-}
export MODEL_NAME=Qwen/Qwen2.5-7B-Instruct
export MODEL_PATH=/data/models/Qwen2.5-7B-Instruct

python remote_minimal_single_request.py 2>&1 | tee minimal_single_request_0720.log
