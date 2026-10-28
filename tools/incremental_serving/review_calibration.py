"""Descriptive review of the independently closed calibration; never retune rates."""
import argparse
import json
from pathlib import Path
from analyze import read,write,quantile,mean


def main():
    parser=argparse.ArgumentParser();parser.add_argument('root');args=parser.parse_args();root=Path(args.root);out=[]
    for folder in sorted(root.glob('calibration_*')):
        if not (folder/'measurement_rows.json').exists():continue
        rows=read(folder/'measurement_rows.json');windows=[]
        for lo,hi in [(200,400),(400,600)]:
            subset=[r for r in rows if lo<=r['planned_offset_s']<hi]
            values=dict(offset_window=[lo,hi],requests=len(subset),complete=sum(bool(r.get('completed')) for r in subset))
            for key in ['e2e_s','ttft_s','tpot_s','send_lag_s']:
                a=[r[key] for r in subset if r.get('completed') and r.get(key) is not None]
                values[key+'_p50']=quantile(a,.5);values[key+'_p95']=quantile(a,.95)
            values['mean_output_tokens']=mean([len(r['token_ids']) for r in subset if r.get('completed')])
            windows.append(values)
        out.append(dict(round=folder.name,summary=read(folder/'summary.json'),
            capacity_assessment=read(folder/'capacity_assessment.json'),steady_windows=windows,
            method='Descriptive nearest-rank quantiles. Original capacity gate used statistical median; original assessment remains authoritative. No posthoc rate selection.'))
    write(root/'capacity_review.json',out);print(json.dumps(out,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
