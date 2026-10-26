"""Minimal existing TCP connector lifetime probe; only approved GPU 1."""
import argparse
import json
import os
from pathlib import Path
import socket
import time

from two_gpu_nvml_monitor import NVML


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    parser.add_argument('--socket',action='store_true')
    parser.add_argument('--explicit',action='store_true')
    args=parser.parse_args()
    root=Path(args.root);root.mkdir(exist_ok=False,parents=True)
    nv=NVML()
    if nv.processes(nv.handle(1)):
        raise RuntimeError('Approved physical GPU 1 busy; no process touched')
    import torch
    from blade_kvt.kv_transfer import KVTransferServer, KVTransferProtocolType
    assert os.environ['CUDA_VISIBLE_DEVICES']=='1'
    tensor=torch.zeros(65536,device='cuda',dtype=torch.uint8)
    server=KVTransferServer(inst_id='barex_exit_probe',tp_size=1,worker_id=0,worker_tp_rank=0,
        block_bytes=1024,token_bytes=64,naming_url='',layers=[tensor],protocols=[KVTransferProtocolType.TCP],num_kv_heads=1,num_gdn_layers=0)
    time.sleep(1)
    if args.socket:
        with socket.create_connection(('127.0.0.1',int(os.environ['BLLM_KVTRANS_PORT_BASE'])),timeout=5):
            time.sleep(.5)
        time.sleep(1)
    if args.explicit:
        from blade_kvt import kvtransfer_ops
        kvtransfer_ops.shutdown_kv_transfer()
    (root/'probe.json').write_text(json.dumps({'pid':os.getpid(),'socket_used':args.socket,'explicit_shutdown':args.explicit,
        'gpu_uuid':nv.row(1,'D')['gpu_uuid'],'scope':'raw TCP connection lifetime only; not LLM request or migration validity'},indent=2))
    print('PROBE_END pid='+str(os.getpid()),flush=True)


if __name__=='__main__':
    main()
