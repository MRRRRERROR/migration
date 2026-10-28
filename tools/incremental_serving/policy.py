"""Deterministic, no-oracle policies and pure queue eligibility checks."""
from dataclasses import dataclass,asdict
import hashlib
import math


@dataclass(frozen=True)
class PolicyConfig:
    period_s:float=1.0
    state_max_age_s:float=2.0
    waiting_load_gap:int=2
    running_gap:int=3
    persistent_checks:int=3
    cooldown_s:float=10.0
    request_cooldown_s:float=60.0
    max_destination_kv_fraction:float=.85
    decode_margin_tokens:int=512
    max_migration_context:int=4096
    session_timeout_s:float=60.0


def never_executed(request):
    return (request.get('state')=='waiting' and request.get('status')=='WAITING'
            and request.get('computed_tokens')==0 and request.get('generated_tokens')==0
            and request.get('ever_scheduled') is False and request.get('preemptions')==0
            and request.get('kv_blocks')==0 and not request.get('transfer_active',False))


def load(state,extra=0):
    return state['running']+state['waiting']+state.get('reserved',0)+extra


def choose_route(request_id,states,extras):
    # Same rule in A/B/C. Extra counts only ingress arrivals absent from the
    # current engine snapshot; forwarded SSE connections are never engine load.
    scores=[(load(state,extra),state['kv_usage']) for state,extra in zip(states,extras)]
    best=min(scores);tied=[i for i,score in enumerate(scores) if score==best]
    return tied[int(hashlib.sha256(request_id.encode()).hexdigest(),16)%len(tied)]


def choose_waiting(states,now,cfg,ban):
    source=max(range(2),key=lambda i:(load(states[i]),i));destination=1-source
    s,d=states[source],states[destination]
    if load(s)-load(d)<cfg.waiting_load_gap:return None,'difference_insufficient'
    eligible=[r for r in s['requests'] if never_executed(r) and now>=ban.get(r['logical_id'],0)]
    eligible.sort(key=lambda r:(r['arrival_time'],r['request_id']))
    if not eligible:return None,'no_candidate'
    req=eligible[0]
    blocks=math.ceil(req['input_tokens']/s['block_size'])+1
    if d['kv_usage']>=cfg.max_destination_kv_fraction or d['free_kv_blocks']<blocks:
        return None,'destination_capacity_insufficient'
    return dict(source=source,destination=destination,request=req),'waiting_transfer'


def choose_running(states,now,cfg,ban):
    source=max(range(2),key=lambda i:(states[i]['running'],load(states[i]),i));destination=1-source
    s,d=states[source],states[destination]
    if s['running']-d['running']<cfg.running_gap:return None,'difference_insufficient'
    eligible=[r for r in s['requests'] if r['state']=='running' and r['generated_tokens']>0
              and not r.get('transfer_active',False) and now>=ban.get(r['logical_id'],0)
              and r['computed_tokens']<=cfg.max_migration_context]
    eligible.sort(key=lambda r:(r['computed_tokens'],r['arrival_time'],r['request_id']))
    if not eligible:return None,'no_candidate'
    req=eligible[0]
    blocks=math.ceil((req['computed_tokens']+cfg.decode_margin_tokens)/s['block_size'])
    if d['kv_usage']>=cfg.max_destination_kv_fraction or d['free_kv_blocks']<blocks:
        return None,'destination_capacity_insufficient'
    return dict(source=source,destination=destination,request=req),'running_kv_migration'


class TransferLedger:
    """Pure idempotent ownership ledger, used by actual EngineCore commands."""
    def __init__(self):self.records={}
    def reserve(self,transaction,request_id):
        existing=self.records.get(transaction)
        if existing:
            if existing['request_id']!=request_id:raise ValueError('Transaction identity collision')
            return existing.copy()
        record={'request_id':request_id,'state':'reserved'}
        self.records[transaction]=record
        return record.copy()
    def commit(self,transaction):
        record=self.records[transaction]
        if record['state']=='aborted':raise ValueError('Cannot admit an aborted reservation')
        record['state']='committed';return record.copy()
    def abort(self,transaction):
        record=self.records[transaction]
        if record['state'] in ('committed','admitting'):raise ValueError('Cannot roll back an admitted/ambiguous destination')
        record['state']='aborted';return record.copy()
    def status(self,transaction):return self.records.get(transaction,{'state':'unknown'}).copy()
