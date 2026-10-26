"""One read-only progress snapshot; does not schedule or acquire resources."""
import argparse
import json
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    args=parser.parse_args()
    root=Path(args.root)
    out={}
    for name in ('capacity_calibration.json','final_config.json','acceptance_passed.json','experiment_blocked.json'):
        path=root/name
        if path.exists():
            out[name]=json.loads(path.read_text())
    out['rounds']=[]
    for folder in sorted(root.glob('*_*')):
        if not folder.is_dir() or not (folder/'round_config.json').exists():
            continue
        row={'name':folder.name}
        if (folder/'summary.json').exists():
            s=json.loads((folder/'summary.json').read_text())
            row.update({k:s[k] for k in ('sent','completed','raw_counts','finish_reasons','send_delay_s')})
            row['status']='complete'
        elif (folder/'instance_samples.jsonl').exists():
            lines=(folder/'instance_samples.jsonl').read_text().splitlines()
            if lines:
                state=json.loads(lines[-1])
                row['elapsed_s']=state['elapsed_s']
                row['queues']={role:{k:(state.get(role) or {}).get(k) for k in ('running','waiting','migration_tasks')} for role in ('S','D')}
                row['status']='running'
        else:
            row['status']='starting'
        out['rounds'].append(row)
    print(json.dumps(out,ensure_ascii=False))


if __name__=='__main__':
    main()
