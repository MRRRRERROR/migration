"""Task-scoped SCP using the already configured OpenSSH connection."""
import argparse
import json
from pathlib import Path
import subprocess

WORKSPACE=Path(__file__).resolve().parents[2]


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=('get','put'));p.add_argument('manifest');a=p.parse_args()
    records=json.loads(Path(a.manifest).read_text(encoding='utf-8'))
    for item in records:
                remote=item['remote'];local=Path(item['local']).resolve()
                if not local.is_relative_to(WORKSPACE):raise ValueError('Local path outside task workspace')
                if not remote.startswith(('/data/home/zylu/llumnix-v018-main/','/data/home/zylu/llumnix-kvt500-2gpu/','/data/home/zylu/.conda/envs/llumnix5090_vllm018/')):
                    raise ValueError('Remote path outside named serving environment')
                if a.action=='get':
                    local.parent.mkdir(parents=True,exist_ok=True)
                    subprocess.run(['scp','-o','ConnectTimeout=15','zylu@10.112.184.62:'+remote,str(local)],check=True)
                else:
                    if not remote.startswith('/data/home/zylu/llumnix-v018-main/results/incremental_runtime_migration_20261027_01/'):
                        raise ValueError('Uploads restricted to independent experiment result directory')
                    subprocess.run(['scp','-o','ConnectTimeout=15',str(local),'zylu@10.112.184.62:'+remote],check=True)
                print(a.action,remote,flush=True)


if __name__=='__main__':main()
