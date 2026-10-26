"""Power metrics on a shared host monotonic clock; missing readings stay missing."""
import math
import statistics


def mean_window(rows, start, end, role):
    values = [float(r['power_w']) for r in rows if r['role'] == role
              and start <= float(r['monotonic_s']) < end and r.get('power_w') not in (None, '')]
    # At 200 ms there should be five samples. Reject large collection gaps.
    return statistics.mean(values) if len(values) >= 3 else None


def energy(rows, start, end, role):
    pts = sorted((float(r['monotonic_s']), float(r['power_w'])) for r in rows
                 if r['role'] == role and start <= float(r['monotonic_s']) <= end
                 and r.get('power_w') not in (None, ''))
    covered = 0.; joules = 0.
    for (a, p), (b, q) in zip(pts, pts[1:]):
        if b-a <= .6:
            joules += (p+q)/2*(b-a); covered += b-a
    return {'joules': joules if covered >= .98*(end-start) else None, 'covered_s': covered}


def power_metrics(rows, t0):
    p0 = mean_window(rows, t0-60, t0, 'S')
    d0 = mean_window(rows, t0-60, t0, 'D')
    out = {'source_p0_w': p0, 'destination_p0_w': d0, 'points': {}, 'thresholds': {}}
    for t in (1, 5, 10, 30, 60, 120):
        s = mean_window(rows, t0+t-1, t0+t, 'S')
        d = mean_window(rows, t0+t-1, t0+t, 'D')
        drop = p0-s if p0 is not None and s is not None else None
        gain = d-d0 if d0 is not None and d is not None else None
        out['points'][str(t)] = {'source_power_w': s, 'source_drop_w': drop,
            'source_drop_ratio': drop/p0 if drop is not None and p0 else None,
            'destination_increase_w': gain,
            'total_power_change_w': gain-drop if gain is not None and drop is not None else None}
    for ratio in (.10, .25, .50):
        consecutive = 0; found = None
        for t in range(1, 601):
            s = mean_window(rows, t0+t-1, t0+t, 'S')
            consecutive = consecutive+1 if p0 and s is not None and p0-s >= p0*ratio else 0
            if consecutive >= 5:
                found = {'start_s': t-5, 'confirmed_s': t}; break
        out['thresholds'][str(ratio)] = found
    for role in ('S', 'D'):
        avg = mean_window(rows, t0+60, t0+300, role)
        base = p0 if role == 'S' else d0
        out[role+'_mean_change_60_300_w'] = avg-base if avg is not None and base is not None else None
        for t in (60,120): out[role+'_energy_'+str(t)] = energy(rows, t0, t0+t, role)
    return out


def poisson_trace(prompts, seed, duration, source_rate, destination_rate):
    import random
    rows=[]
    for role,rate in (('S',source_rate),('D',destination_rate)):
        rng=random.Random(seed*100+(0 if role=='S' else 1)); t=rng.expovariate(rate); i=0
        while t < duration:
            index=rng.randrange(len(prompts))
            rows.append({'request_id':f'p{seed}-{role}-{i:05d}', 'planned_offset_s':t,
                         'initial_role':role,'prompt_index':index,'seed':seed})
            i+=1; t+=rng.expovariate(rate)
    return sorted(rows,key=lambda r:r['planned_offset_s'])
