"""Export recorded Prometheus counters without inventing event timestamps."""
import argparse
import csv
import gzip
import json
from pathlib import Path
import re
from bisect import bisect_left,bisect_right

from analyze import read,write,csvfile


PREEMPT=re.compile(r'^vllm:num_preemptions_total(?:\{[^}]*\})?\s+(\S+)',re.M)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('root');args=parser.parse_args();root=Path(args.root)
    path=root/'services_01/prometheus.jsonl.gz';samples={0:[],1:[]};errors=[]
    if not path.exists():
        write(root/'prometheus_review_status.json',dict(available=False,reason='shared recorded metrics missing'));return
    try:
        with gzip.open(path,'rt',encoding='utf-8') as inp:
            for number,line in enumerate(inp,1):
                if not line.strip():continue
                record=json.loads(line);matches=PREEMPT.findall(record.get('text',''))
                t=record.get('end_monotonic_s',record['monotonic_s'])
                if record.get('status')==200 and matches:
                    value=sum(float(x) for x in matches);samples[record['engine']].append((t,value))
                else:errors.append(dict(record=number,engine=record['engine'],monotonic_s=t,status=record.get('status'),error=record.get('error'),counter_present=bool(matches)))
    except (EOFError,OSError,json.JSONDecodeError) as error:
        write(root/'prometheus_review_status.json',dict(available=False,reason=repr(error),partial_samples={str(i):len(a) for i,a in samples.items()}));return
    for a in samples.values():a.sort()
    summary=[]
    for f in sorted(root.iterdir()):
        if not f.is_dir() or not (f/'measurement_rows.json').exists():continue
        timeline=read(f/'timeline.json');start=timeline['start_monotonic_s'];states=[]
        with (f/'states.jsonl').open(encoding='utf-8') as inp:
            for line in inp:
                if line.strip():states.append(json.loads(line)['monotonic_s'])
        end=max(states+[r.get('finish_monotonic_s',start) for r in read(f/'measurement_rows.json')])
        for engine,a in samples.items():
            times=[x[0] for x in a];lo=bisect_right(times,start)-1;hi=bisect_left(times,end)
            before=a[lo] if lo>=0 else None;after=a[hi] if hi<len(a) else None
            change=after[1]-before[1] if before and after else None
            summary.append(dict(round=f.name,engine=engine,before_sample_monotonic_s=before[0] if before else None,
                after_sample_monotonic_s=after[0] if after else None,counter_before=before[1] if before else None,
                counter_after=after[1] if after else None,observed_counter_change=change,
                counter_reset_observed=change is not None and change<0,
                successful_samples_inside=sum(start<=t<=end for t in times),
                errors_inside=sum(e['engine']==engine and start<=e['monotonic_s']<=end for e in errors),
                interpretation='Observed cumulative-counter change between bracketing scrapes; vLLM statistics publication can lag actual preemption, so not exact event timing.'))
    csvfile(root/'prometheus_preemption_review.csv',summary);csvfile(root/'prometheus_scrape_errors.csv',errors)
    write(root/'prometheus_review_status.json',dict(available=True,samples={str(i):len(a) for i,a in samples.items()},
        whole_recorded_period={str(i):dict(first=a[0] if a else None,last=a[-1] if a else None) for i,a in samples.items()},
        scrape_errors=len(errors),requested_scrape_interval_s=1,statistics_publication_cadence='not directly measured'))
    print(json.dumps(dict(rounds=len(summary)//2,samples={str(i):len(a) for i,a in samples.items()},errors=len(errors))))


if __name__=='__main__':main()
