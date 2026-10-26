"""Reconstruct timing from saved events. Does not rerun or modify simulation."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.run_validation_matrix import ClientTPOTProxy


def overlap(a, b, c, d):
    return max(0.0, min(b, d)-max(a, c))


def explain(folder, profile):
    config = json.loads((folder/'config.json').read_text())
    cfg = config['experiment']
    trigger = cfg['trigger_ms']
    events = [json.loads(line) for line in (folder/'events.jsonl').read_text().splitlines()]
    prompt = {r['request_id']:r['prompt_length'] for r in config['requests']}
    counts = dict.fromkeys(prompt, 0)
    model = ClientTPOTProxy(cfg['compute'], profile)
    batches = []
    for e in events:
        if e['event'] == 'batch_started':
            decode = [t for t in e['tasks'] if t['kind']=='decode']
            contexts = {t['request_id']:prompt[t['request_id']]+counts[t['request_id']] for t in decode}
            n = len(decode)
            total = sum(contexts.values())
            batches.append(dict(start=e['time_ms'], end=e['time_ms']+e['duration_ms'],
                                instance=e['instance'], tasks=e['tasks'], contexts=contexts,
                                decode_count=n, context_total=total, context_avg=total/n if n else None,
                                decode_ms=model.decode(n, total), duration_ms=e['duration_ms']))
        elif e['event']=='token_generated':
            counts[e['request_id']] += 1
    active = {b['instance']:b for b in batches if b['start'] <= trigger < b['end']}
    own = [b for b in batches if b['end'] > trigger and any(
        t['request_id']=='candidate' and t['kind']=='decode' for t in b['tasks'])]
    decode_ms = prefill_ms = 0.0
    for b in own:
        if b['start'] < trigger:
            # All recorded trigger batches are pure decode; do not invent
            # an ordering inside a mixed batch if a future experiment differs.
            assert math.isclose(b['duration_ms'], b['decode_ms']), 'Mixed trigger batch unsupported'
            decode_ms += b['end']-trigger
        else:
            decode_ms += b['decode_ms']
            prefill_ms += b['duration_ms']-b['decode_ms']
    finish = next(e['time_ms'] for e in events if e['event']=='request_completed' and e['request_id']=='candidate')
    generated = max(e['time_ms'] for e in events if e['event']=='token_generated' and e['request_id']=='candidate')
    paused = next((e['time_ms'] for e in events if e['event']=='source_paused'), None)
    commit = next((e['time_ms'] for e in events if e['event']=='taken_over'), None)
    replay = [b for b in batches if any(t['request_id']=='candidate' and t['kind']=='replay' for t in b['tasks'])]
    result = dict(trigger_active=active, remaining_ms=finish-trigger, finish_ms=finish,
                  decode_ms=decode_ms, mixed_prefill_ms=prefill_ms, output_tail_ms=finish-generated,
                  replay_total_device_ms=sum(b['duration_ms'] for b in replay),
                  replay_batches=replay, paused_ms=paused, takeover_ms=commit,
                  candidate_decode_step_values=sorted(set(b['decode_ms'] for b in own)))
    if paused is None:
        result['other_wait_ms'] = finish-trigger-decode_ms-prefill_ms-(finish-generated)
        return result
    # Before takeover, there are no target->source output sends: all gateways
    # are local and there is only one migration. Check wire intervals do not
    # overlap on the same directed link before using bytes/bandwidth+latency.
    wires = []
    for e in events:
        if e['event'] != 'wire_send' or e['time_ms'] >= commit:
            continue
        assert e['source'] in ('source','target') and e['target'] in ('source','target')
        end = e['time_ms']+e['bytes']/next(
            link['bandwidth_bytes_per_ms'] for link in config['links']
            if link['source']==('a' if e['source']=='source' else 'b') and
            link['target']==('a' if e['target']=='source' else 'b'))+cfg['latency_ms']
        for old in wires:
            if old['source']==e['source'] and old['target']==e['target']:
                assert old['end'] <= e['time_ms']+1e-8, 'Shared-link delivery cannot be reconstructed with single-flow formula'
        wires.append(dict(e, end=end))
    wire_pause = sum(overlap(w['time_ms'],w['end'],paused,commit) for w in wires)
    rebuild_pause = sum(overlap(b['start'],b['end'],paused,commit) for b in replay)
    # Network phases and replay are sequential on this attempt, not additive
    # to source decode running concurrently before pause.
    for w in wires:
        for b in replay:
            assert overlap(max(paused,w['time_ms']), min(commit,w['end']), b['start'],b['end']) < 1e-7
    pause_wait = commit-paused-wire_pause-rebuild_pause
    target_batches = [b for b in own if b['instance']=='target']
    after_wait = generated-commit-sum(b['duration_ms'] for b in target_batches)
    result.update(before_pause_elapsed_ms=paused-trigger, pause_total_ms=commit-paused,
                  pause_transport_ms=wire_pause, pause_rebuild_ms=rebuild_pause,
                  pause_queue_or_boundary_wait_ms=pause_wait,
                  post_takeover_decode_ms=sum(b['decode_ms'] for b in target_batches),
                  post_takeover_batch_wait_ms=after_wait,
                  background_replay_hidden_ms=sum(b['duration_ms'] for b in replay)-rebuild_pause,
                  pre_takeover_wire_intervals=wires,
                  source_first_batch_after_pause=next(b for b in batches if b['instance']=='source' and b['start']>=paused),
                  target_first_decode_batch=target_batches[0])
    partition = (paused-trigger)+(commit-paused)+sum(b['duration_ms'] for b in target_batches)+after_wait+finish-generated
    assert math.isclose(partition,finish-trigger,abs_tol=1e-7)
    assert pause_wait >= -1e-7 and after_wait >= -1e-7
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    args = parser.parse_args()
    cfg = json.loads((args.input/'experiment.json').read_text())
    profile_path = ROOT/cfg['tpot_profile']
    profile = json.loads(profile_path.read_text())
    manifest = json.loads((args.input/'manifest.json').read_text())
    assert hashlib.sha256(profile_path.read_bytes()).hexdigest() == manifest['files'][cfg['tpot_profile'].replace('/','\\')]
    rows = []
    for phase in ('screen','repeat'):
        for condition in sorted((args.input/phase).iterdir()):
            arms = {mode:explain(condition/mode,profile) for mode in ('none','kv','token_stop','token_live')}
            base = arms['none']
            for mode, data in arms.items():
                data['decode_saved_ms'] = base['decode_ms']-data['decode_ms']
                data['mixed_prefill_saved_ms'] = base['mixed_prefill_ms']-data['mixed_prefill_ms']
                data['additional_nondecode_ms'] = (data['remaining_ms']-data['decode_ms']-data['mixed_prefill_ms'])-(
                    base['remaining_ms']-base['decode_ms']-base['mixed_prefill_ms'])
                data['net_delta_ms'] = data['remaining_ms']-base['remaining_ms']
                assert math.isclose(data['net_delta_ms'],data['additional_nondecode_ms']-
                                    data['decode_saved_ms']-data['mixed_prefill_saved_ms'],abs_tol=1e-7)
            rows.append(dict(condition=condition.name,phase=phase,arms=arms))
    (args.input/'timing_explanation.json').write_text(json.dumps(rows,indent=2),encoding='utf-8')
    print('Verified partitions:', len(rows)*3)
    for mode in ('kv','token_stop','token_live'):
        print(mode,{key:(min(r['arms'][mode][key] for r in rows),max(r['arms'][mode][key] for r in rows))
                    for key in ('decode_saved_ms','mixed_prefill_saved_ms','additional_nondecode_ms')})


if __name__ == '__main__':
    main()
