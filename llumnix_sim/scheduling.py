"""Execute explicitly selected decision methods from the audited server snapshot.

Only method definitions are compiled: no runtime constructor, RPC, GPU imports,
or service configuration is executed. Method bodies are not rewritten. Missing
definitions fail closed. Source hashes accompany results. This is not the full
vLLM scheduler or Go global scheduler.
"""
import ast
import hashlib
import logging
from enum import Enum
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'server_snapshots/token_migration_20261011_01/project/python/llumnix/llumnix'


@lru_cache(maxsize=1)
def source_decisions():
    namespace = {'Enum': Enum, 'hashlib': hashlib, 'Tuple': Tuple,
                 'logger': logging.getLogger(__name__)}
    hashes = {}

    def load(relative, name, methods=None, bases=()):
        path = SOURCE / relative
        content = path.read_bytes()
        hashes[str(path.relative_to(ROOT)).replace('\\', '/')] = hashlib.sha256(content).hexdigest()
        tree = ast.parse(content, filename=str(path))
        original = next((n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name), None)
        if original is None:
            raise RuntimeError(f'{path}: missing decision class {name}')
        if methods is None:
            selected = original
        else:
            body = [n for n in original.body if isinstance(n, ast.FunctionDef) and n.name in methods]
            if {n.name for n in body} != set(methods):
                raise RuntimeError(f'{path}: missing decision methods {methods}')
            selected = ast.ClassDef(name=name, bases=[ast.Name(id=b, ctx=ast.Load()) for b in bases],
                                    keywords=[], body=body, decorator_list=[])
        module = ast.Module(body=[ast.ImportFrom(module='__future__',
                            names=[ast.alias(name='annotations')], level=0), selected], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
        return namespace[name]

    migration_type = load('utils.py', 'MigrationType')
    request_policy = load('utils.py', 'RequestMigrationPolicy')
    gate = load('token_migration/runtime.py', 'TokenRuntime', ('begin_schedule', 'skip'))
    load('migration_frontend/vllm_v1/base_migration_frontend.py', 'BaseMigrationFrontend',
         ('_get_sorted_indices', '_select_requests_from_list', '_get_budget'))
    selector = load('migration_frontend/vllm_v1/kvt_migration_frontend.py', 'KVTMigrationFrontend',
                    ('_get_sorted_indices', '_get_sorting_key', '_get_request_cost',
                     '_policy_features', '_should_skip_migration', '_set_migrating_status',
                     '_log_policy_decision'), ('BaseMigrationFrontend',))
    return gate, selector, migration_type, request_policy, hashes, namespace


class ReplayGate:
    def __init__(self):
        gate, *_, namespace = source_decisions()
        # Per-instance virtual monotonic clock. Clone method globals to avoid
        # shared mutable clocks between instances or simulations.
        import types
        context = dict(namespace, time=SimpleNamespace(monotonic=lambda: self.now_ms / 1000))
        self.begin = types.FunctionType(gate.begin_schedule.__code__, context)
        self.skip = types.FunctionType(gate.skip.__code__, context)
        self.state = SimpleNamespace(replay_turn=False, replay_batch=False)
        self.now_ms = 0

    def choose(self, replicas, now_ms):
        self.now_ms = now_ms
        views = {r.request_id: SimpleNamespace(
            _token_replay=r.replay, _token_paused=r.paused,
            num_computed_tokens=r.computed, num_tokens=r.length) for r in replicas}
        self.state.scheduler = SimpleNamespace(requests=views)
        self.begin(self.state)
        return {rid for rid, req in views.items() if not self.skip(self.state, req)}


def select_running(sim, source, *, policy, budget_type, budget, seed,
                   composite_min_remaining, composite_max_context):
    """Use real frontend ordering, skip and budget logic on current source state.

    Queue classification/preemption is not modeled, so waiting-first policies
    explicitly fail instead of inventing a waiting queue.
    """
    _, cls, types, policies, _, _ = source_decisions()
    selected_policy = policies(policy)
    if policy in ('FCW', 'FCWSR'):
        raise ValueError('Waiting-first policies require a real waiting-queue adapter')
    migration_type = types(budget_type)
    if isinstance(budget, bool) or not isinstance(budget, (int, float)) or not 0 < budget < float('inf'):
        raise ValueError('Migration budget must be finite and positive')
    if migration_type != types.RATIO and (not isinstance(budget, int)):
        raise ValueError('NUM_REQ/TOKEN budgets must be integer counts')
    if migration_type == types.RATIO and budget > 1:
        raise ValueError('RATIO budget must be in (0, 1]')
    for name, value in [('composite_min_remaining', composite_min_remaining),
                        ('composite_max_context', composite_max_context)]:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f'{name} must be a nonnegative token count')
    node = sim.instances[source]
    frontend = cls()
    frontend._policy_seed = seed
    frontend._active_migration_policy = selected_policy.value
    frontend._composite_min_remaining = composite_min_remaining
    frontend._composite_max_context = composite_max_context
    frontend.migrating_reqs = {r.id for r in sim.requests.values()
                              if r.attempt is not None and sim.attempts[r.attempt].active}
    frontend.scheduler = SimpleNamespace(cache_config=SimpleNamespace(
        num_gpu_blocks=node.capacity, block_size=node.block_size))
    rows = []
    for replica in node.replicas.values():
        req = sim.requests[replica.request_id]
        if req.owner != source or req.generation_done or replica.replay:
            continue
        rows.append((SimpleNamespace(request_id=req.id, prompt_token_ids=list(req.prompt_ids),
                     sampling_params=SimpleNamespace(max_tokens=req.max_tokens)), len(req.output_ids)))
    params = SimpleNamespace(migration_type=migration_type, num_reqs=budget,
                             num_tokens=budget, kv_cache_usage_ratio=budget)
    actual_budget = frontend._get_budget(params)
    indices = frontend._get_sorted_indices(selected_policy, rows)
    selected, remaining = frontend._select_requests_from_list(
        rows, indices, actual_budget, migration_type)
    return [r.request_id for r, _ in selected], actual_budget, remaining
