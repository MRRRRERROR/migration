"""Virtual-time batched inference with KV and stop/live token migration.

This intentionally models one GPU per instance, one request sequence, FCFS
admission with round-robin service, ideal reliable ordered control, and explicit
directed domain links. Batch classes use the snapshot token runtime gate; the
complete vLLM batch planner and Go global scheduler are not executed.
"""
import math
from dataclasses import dataclass, field

from .clock import Clock
from .latency import LatencyModel
from .network import Network
from .protocol import Attempt, MigrationConfig, ready, release_allowed
from .scheduling import ReplayGate, select_running


@dataclass
class Request:
    id: str
    prompt_ids: list
    max_tokens: int
    arrival: float
    owner: str
    gateway_domain: str
    gateway_owner: object = None
    output_ids: list = field(default_factory=list)
    client_ids: list = field(default_factory=list)
    client_times: list = field(default_factory=list)
    pending_outputs: dict = field(default_factory=dict)
    delivered_positions: set = field(default_factory=set)
    attempt: object = None
    restored: bool = False
    generation_done: bool = False
    completed_at: object = None
    flush_waiters: list = field(default_factory=list)


@dataclass
class Replica:
    request_id: str
    prompt_ids: list
    output_ids: list = field(default_factory=list)
    computed: int = 0
    blocks: int = 0
    paused: bool = False
    replay: bool = False
    attempt_id: object = None

    @property
    def length(self):
        return len(self.prompt_ids) + len(self.output_ids)


class Instance:
    def __init__(self, simulation, row):
        supported = {'id', 'domain', 'model_identity', 'kv_layout', 'kv_blocks', 'block_size',
                     'max_batch_tokens', 'max_sequences', 'chunk_tokens', 'compute', 'max_migrations'}
        if set(row) - supported:
            raise ValueError('Unknown instance fields: ' + str(sorted(set(row) - supported)))
        self.sim = simulation
        self.id = row['id']
        self.domain = row['domain']
        if not isinstance(self.id, str) or not self.id or not isinstance(self.domain, str) or not self.domain:
            raise ValueError('Instance/domain IDs must be nonempty strings')
        self.identity = row.get('model_identity', 'same-model-tokenizer-dtype')
        self.kv_layout = row.get('kv_layout', 'same-layout')
        self.capacity = row.get('kv_blocks', 4096)
        self.block_size = row.get('block_size', 16)
        self.batch_tokens = row.get('max_batch_tokens', 256)
        self.max_sequences = row.get('max_sequences', 32)
        self.chunk_tokens = row.get('chunk_tokens', 128)
        self.max_migrations = row.get('max_migrations', 1)
        for value in (self.capacity, self.block_size, self.batch_tokens, self.max_sequences,
                      self.chunk_tokens, self.max_migrations):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError('Instance capacities/budgets must be positive integers')
        self.compute = LatencyModel(row.get('compute'))
        self.replicas = {}
        self.busy = False
        self.wake_pending = False
        self.boundary_callbacks = []
        self.used = 0
        self.peak_blocks = 0
        self.cursor = 0
        self.compute_ms = 0.0
        self.replay_tokens = 0
        self.replay_gate = ReplayGate()

    def reserve(self, replica, length):
        required = math.ceil(length / self.block_size)
        extra = required - replica.blocks
        if self.used + extra > self.capacity:
            return False
        self.used += extra
        replica.blocks = required
        self.peak_blocks = max(self.peak_blocks, self.used)
        return True

    def remove(self, request_id):
        replica = self.replicas.pop(request_id, None)
        if replica is not None:
            self.used -= replica.blocks
        self.sim.wake_all()

    def boundary(self, callback):
        if self.busy:
            self.boundary_callbacks.append(callback)
        else:
            callback()

    def wake(self):
        if not self.busy and not self.wake_pending:
            self.wake_pending = True
            self.sim.clock.after(0, self._schedule)

    def _schedule(self):
        self.wake_pending = False
        if self.busy:
            return
        replicas = list(self.replicas.values())
        if not replicas:
            return
        offset = self.cursor % len(replicas)
        replicas = replicas[offset:] + replicas[:offset]
        self.cursor += 1
        allowed = self.replay_gate.choose(replicas, self.sim.clock.now)
        tasks = []
        budget = self.batch_tokens
        prefill_tokens = cached_context = decode_batch = decode_context = 0
        for replica in replicas:
            if not budget or len(tasks) >= self.max_sequences:
                break
            request = self.sim.requests[replica.request_id]
            if replica.request_id not in allowed:
                continue
            if replica.paused:
                continue
            if replica.replay:
                remaining = replica.length - 1 - replica.computed
                if remaining <= 0:
                    continue
                kind = 'replay'
            elif request.owner != self.id or request.generation_done:
                continue
            elif not replica.output_ids:
                remaining = len(replica.prompt_ids) - replica.computed
                kind = 'prefill'
            else:
                remaining = 1
                kind = 'decode'
            amount = min(remaining, budget, self.chunk_tokens) if kind != 'decode' else 1
            if not self.reserve(replica, replica.length + (0 if replica.replay else 1)):
                continue
            tasks.append((replica, kind, amount))
            budget -= amount
            if kind == 'decode':
                decode_batch += 1
                decode_context += replica.length
            else:
                prefill_tokens += amount
                cached_context += replica.computed
        if not tasks:
            # A replay-only turn can be blocked on capacity while normal work
            # can progress. Retry once with the other class; never spin when
            # both classes cannot run.
            if getattr(self, '_empty_class_turn', False):
                self._empty_class_turn = False
                return
            if any(r.replay and r.computed < r.length - 1 and not r.paused for r in replicas) and any(
                    not r.replay and not r.paused for r in replicas):
                self._empty_class_turn = True
                self.wake()
            return
        self._empty_class_turn = False
        duration = (self.compute.prefill(prefill_tokens, cached_context)
                    + self.compute.decode(decode_batch, decode_context))
        self.busy = True
        self.compute_ms += duration
        self.sim.event('batch_started', instance=self.id, duration_ms=duration,
                       tasks=[{'request_id': r.request_id, 'kind': kind, 'tokens': n} for r, kind, n in tasks])
        self.sim.clock.after(duration, lambda: self._finish(tasks))

    def _finish(self, tasks):
        # Complete a whole batch before taking snapshots or pausing sources.
        for replica, kind, amount in tasks:
            if kind == 'replay':
                self.replay_tokens += amount
            if self.replicas.get(replica.request_id) is not replica:
                continue  # Cancelled work still occupied the GPU until this boundary.
            replica.computed += amount
            request = self.sim.requests[replica.request_id]
            if kind == 'replay':
                attempt = self.sim.attempts[replica.attempt_id]
                attempt.replay_tokens += amount
                if ready(replica.computed, len(replica.prompt_ids), len(replica.output_ids)):
                    self.sim._target_ready(attempt)
            elif kind == 'decode' or replica.computed == len(replica.prompt_ids):
                self.sim._emit(self, replica, request)
        self.busy = False
        callbacks, self.boundary_callbacks = self.boundary_callbacks, []
        for callback in callbacks:
            callback()
        self.sim.check_invariants()
        self.wake()


class Simulation:
    def __init__(self, config):
        self.clock = Clock()
        rows = config['instances']
        if not rows or len({row['id'] for row in rows}) != len(rows):
            raise ValueError('Instance IDs must be nonempty and unique')
        self.instances = {row['id']: Instance(self, row) for row in rows}
        domains = {node.domain for node in self.instances.values()} | set(config.get('domains', []))
        self.network = Network(self.clock, domains, config.get('links', []))
        self.kv_bytes_per_token = config.get('kv_bytes_per_token', 57344)
        self.output_bytes = config.get('output_wire_bytes_per_token', 64)
        for value in (self.kv_bytes_per_token, self.output_bytes):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError('Wire byte counts must be positive integers')
        self.requests = {}
        self._output_lengths = {}
        self._length_origins = {}
        self.attempts = {}
        self.events = []
        self.config = config

    def event(self, name, attempt=None, **fields):
        row = {'time_ms': self.clock.now, 'event': name}
        if attempt is not None:
            row.update(attempt_id=attempt.id, request_id=attempt.request_id, mode=attempt.config.mode)
        row.update(fields)
        self.events.append(row)

    def wake_all(self):
        for node in self.instances.values():
            node.wake()

    def add_request(self, request_id, instance, prompt_length=32, max_tokens=128,
                    arrival_ms=0, prompt_ids=None, gateway_domain=None, output_length=None):
        if request_id in self.requests:
            raise ValueError('Duplicate request ID: ' + request_id)
        node = self.instances[instance]
        prompt = list(prompt_ids) if prompt_ids is not None else list(range(prompt_length))
        if not prompt or any(isinstance(token, bool) or not isinstance(token, int)
                             or not 0 <= token <= 4294967295 for token in prompt):
            raise ValueError('Prompt must contain uint32 token IDs')
        if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or max_tokens <= 0:
            raise ValueError('Output budget must be positive')
        if output_length is not None and (isinstance(output_length, bool)
                or not isinstance(output_length, int) or not 0 < output_length <= max_tokens):
            raise ValueError(f'Request {request_id}: output_length must be an integer in [1, max_tokens]')
        if len(prompt) + max_tokens > node.capacity * node.block_size:
            raise ValueError('Request maximum context exceeds source KV capacity')
        gateway = gateway_domain or node.domain
        self.network.require(node.domain, gateway)
        request = Request(request_id, prompt, max_tokens, float(arrival_ms), instance, gateway,
                          gateway_owner=instance)
        self.requests[request_id] = request
        # Workload truth is private to the executor, not a request-policy field.
        # Omission preserves the explicitly labeled fixed-budget workload.
        self._output_lengths[request_id] = max_tokens if output_length is None else output_length
        self._length_origins[request_id] = ('budget_limit_assumption' if output_length is None
                                            else 'explicit_workload_input')

        def arrive():
            node.replicas[request_id] = Replica(request_id, list(prompt))
            self.event('request_arrived', request_id=request_id, instance=instance)
            node.wake()
        self.clock.at(arrival_ms, arrive)
        return request

    def schedule_migration(self, request_id, target, at_ms, mode='token_stop', **kwargs):
        config = MigrationConfig(mode=mode, **kwargs)
        self.clock.at(at_ms, lambda: self.migrate(request_id, target, config))

    def schedule_selection(self, source, target, at_ms, *, policy, budget_type, budget,
                           seed, composite_min_remaining, composite_max_context,
                           mode, migration_options=None):
        """External pair intent, real frontend selection at virtual execution time.

        Pair/trigger/mode are inputs, NOT presented as Go scheduler decisions.
        Each selected request still passes normal migration admission checks.
        """
        if source not in self.instances or target not in self.instances or source == target:
            raise ValueError(f'Invalid selection endpoints: {source} -> {target}')
        config = MigrationConfig(mode=mode, **(migration_options or {}))

        def decide():
            try:
                ids, actual_budget, remaining = select_running(
                    self, source, policy=policy, budget_type=budget_type, budget=budget,
                    seed=seed, composite_min_remaining=composite_min_remaining,
                    composite_max_context=composite_max_context)
                self.event('request_selection', source=source, target=target, policy=policy,
                           budget_type=budget_type, budget=actual_budget, remaining_budget=remaining,
                           selected_request_ids=ids, seed=seed)
                for rid in ids:
                    self.migrate(rid, target, config)
            except Exception as exc:
                raise RuntimeError(f'Selection at {self.clock.now} ms: {source}->{target}, '
                                   f'policy={policy}, budget={budget_type}:{budget}') from exc
        self.clock.at(at_ms, decide)

    def migrate(self, request_id, target, config=None):
        config = config or MigrationConfig()
        request = self.requests[request_id]
        source = self.instances[request.owner]
        destination = self.instances[target]
        attempt_id = 'migration-' + str(len(self.attempts) + 1)
        attempt = Attempt(attempt_id, request_id, source.id, target, config,
                          self.clock.now, self.clock.now + config.timeout_ms)
        self.attempts[attempt_id] = attempt
        reason = None
        if target == source.id:
            reason = 'source_equals_target'
        elif request.attempt is not None:
            reason = 'already_migrating'
        elif request.restored:
            reason = 're_migration_of_restored_request_unsupported'
        elif request.generation_done or request_id not in source.replicas:
            reason = 'source_not_running'
        elif any(sum(other.active and node.id in (other.source, other.target)
                     for other in self.attempts.values() if other.id != attempt_id) >= node.max_migrations
                 for node in (source, destination)):
            reason = 'migration_concurrency_limit'
        elif source.identity != destination.identity:
            reason = 'model_tokenizer_identity_mismatch'
        elif config.mode == 'kv' and (source.kv_layout != destination.kv_layout
                                      or source.block_size != destination.block_size):
            reason = 'kv_layout_mismatch'
        elif not source.replicas[request_id].output_ids:
            reason = 'source_not_at_decode_boundary'
        if reason:
            attempt.state = 'rejected'
            attempt.reason = reason
            attempt.ended = self.clock.now
            self.event('migration_rejected', attempt, reason=reason)
            return attempt
        # Every control direction and target->original gateway path is explicit.
        self.network.require(source.domain, destination.domain)
        self.network.require(destination.domain, source.domain)
        self.network.require(destination.domain, request.gateway_domain)
        self.network.require(request.gateway_domain, source.domain)
        request.attempt = attempt.id
        attempt.deadline_event = self.clock.at(attempt.deadline, lambda: self._timeout(attempt))
        self.event('migration_started', attempt, source=source.id, target=target, deadline_ms=attempt.deadline)

        def hello_received():
            self._send(attempt, destination, source, config.control_bytes,
                       lambda: source.boundary(lambda: self._snapshot(attempt, initial=True)), 'hello_reply')
        self._send(attempt, source, destination, config.control_bytes, hello_received, 'hello')
        return attempt

    def _send(self, attempt, source, target, size, callback, kind, token_count=0, kv_bytes=0,
              allow_cancelling=False):
        attempt.token_payload_bytes += token_count * attempt.config.token_bytes
        attempt.kv_payload_bytes += kv_bytes
        attempt.control_bytes += size - token_count * attempt.config.token_bytes - kv_bytes
        self.event('wire_send', attempt, source=source.id, target=target.id, kind=kind,
                   bytes=size, token_count=token_count, kv_bytes=kv_bytes)

        def delivered():
            if attempt.active and (attempt.state != 'cancelling' or allow_cancelling):
                callback()
        self.network.send(source.domain, target.domain, size, delivered)

    def _snapshot(self, attempt, initial=False):
        if not attempt.active or attempt.state == 'cancelling':
            return
        request = self.requests[attempt.request_id]
        source = self.instances[attempt.source]
        replica = source.replicas.get(request.id)
        if request.generation_done or replica is None:
            self._abort(attempt, 'source_completed')
            return
        if not ready(replica.computed, len(replica.prompt_ids), len(replica.output_ids)):
            raise AssertionError('Snapshot must be taken at a consistent decode boundary')
        target = self.instances[attempt.target]
        stop = (attempt.config.mode == 'token_stop' or not initial)
        if attempt.config.mode == 'token_live' and not initial:
            previous = target.replicas[request.id]
            gap = len(replica.output_ids) - len(previous.output_ids)
            self.event('background_gap', attempt, gap_tokens=gap, rounds=attempt.rounds)
            stop = gap <= attempt.config.final_gap_tokens or attempt.rounds >= attempt.config.max_rounds
        if stop:
            replica.paused = True
            if attempt.paused_at is None:
                attempt.paused_at = self.clock.now
                self.event('source_paused', attempt, output_length=len(replica.output_ids))
        final = stop
        prompt = list(replica.prompt_ids)
        outputs = list(replica.output_ids)
        computed = replica.computed
        if initial:
            count = len(prompt) + len(outputs)
            kind = 'prepare'
        else:
            count = len(outputs) - len(target.replicas[request.id].output_ids)
            kind = 'final_delta' if final else 'background_delta'
            if not final:
                if count <= 0:
                    raise AssertionError('Background rounds must contain a nonempty token increment')
                attempt.rounds += 1
        self.event('snapshot', attempt, initial=initial, final=final, prompt_length=len(prompt),
                   output_length=len(outputs), source_computed=computed)
        size = attempt.config.control_bytes
        # KV handoff also needs token history metadata. Count it rather than
        # passing history through an uncharged out-of-band channel.
        token_count = count
        size += count * attempt.config.token_bytes

        def prepared():
            old = target.replicas.get(request.id)
            if initial:
                if old is not None:
                    self._abort(attempt, 'target_request_exists')
                    return
                old = Replica(request.id, prompt, outputs, replay=attempt.config.mode != 'kv',
                              paused=attempt.config.mode == 'kv', attempt_id=attempt.id)
            if not target.reserve(old, len(prompt) + len(outputs)):
                self._abort(attempt, 'target_kv_capacity')
                return
            old.output_ids = outputs
            target.replicas[request.id] = old
            attempt.state = 'restoring'
            attempt.ready_callback = (lambda: self._final_ready(attempt)) if final else (
                lambda: source.boundary(lambda: self._snapshot(attempt)))
            if attempt.config.mode == 'kv':
                # Block-rounded transfer; computed tokens, not emitted history,
                # determine valid KV. The final emitted token has no KV yet.
                before_blocks = math.ceil(old.computed / source.block_size)
                after_blocks = math.ceil(computed / source.block_size)
                blocks = max(0, after_blocks - before_blocks)
                # A partially copied last block is retransferred if extended.
                if old.computed and old.computed % source.block_size and computed > old.computed:
                    blocks += 1
                payload = blocks * source.block_size * self.kv_bytes_per_token

                def copied():
                    old.computed = computed
                    self._target_ready(attempt)
                self._send(attempt, source, target, attempt.config.control_bytes + payload,
                           copied, 'kv_copy', kv_bytes=payload)
            elif ready(old.computed, len(prompt), len(outputs)):
                self._target_ready(attempt)
            else:
                target.wake()
        self._send(attempt, source, target, size, prepared, kind, token_count=token_count)

    def _target_ready(self, attempt):
        if not attempt.active or attempt.state in ('target_ready', 'cancelling'):
            return
        target = self.instances[attempt.target]
        replica = target.replicas[attempt.request_id]
        if not ready(replica.computed, len(replica.prompt_ids), len(replica.output_ids)):
            raise AssertionError('Target not ready at the num_tokens-1 boundary')
        attempt.state = 'target_ready'
        self.event('target_ready', attempt, computed=replica.computed, output_length=len(replica.output_ids),
                   prompt_length=len(replica.prompt_ids), rounds=attempt.rounds)
        callback = attempt.ready_callback
        self._send(attempt, target, self.instances[attempt.source], attempt.config.control_bytes,
                   callback, 'ready_reply')

    def _final_ready(self, attempt):
        if not attempt.active or attempt.state == 'cancelling':
            return
        request = self.requests[attempt.request_id]
        emitted = len(request.output_ids)
        # Drain source output sends before placing the gateway route marker.
        def flushed():
            if not attempt.active or attempt.state == 'cancelling':
                return
            self.event('source_output_flush_confirmed', attempt, output_length=emitted)
            self._send(attempt, self.instances[attempt.source], self.instances[attempt.target],
                       attempt.config.control_bytes, lambda: self._commit(attempt), 'commit')
        if len(request.client_ids) >= emitted:
            flushed()
        else:
            request.flush_waiters.append((emitted, flushed))

    def _commit(self, attempt):
        if not attempt.active or self.clock.now >= attempt.deadline:
            self._abort(attempt, 'deadline_before_commit')
            return
        request = self.requests[attempt.request_id]
        target = self.instances[attempt.target]
        replica = target.replicas[request.id]
        if (not ready(replica.computed, len(replica.prompt_ids), len(replica.output_ids))
                or replica.output_ids != request.output_ids):
            raise AssertionError('Commit requires complete, matching reconstruction')
        attempt.state = 'taken_over'
        attempt.commit_proof = attempt.id
        attempt.committed_at = self.clock.now
        request.owner = target.id
        request.restored = attempt.config.mode != 'kv'
        replica.replay = False
        replica.paused = False
        self.event('taken_over', attempt, computed=replica.computed, output_length=len(replica.output_ids))
        target.wake()
        self._send(attempt, target, self.instances[attempt.source], attempt.config.control_bytes,
                   lambda: self._route(attempt), 'commit_reply')

    def _route(self, attempt):
        request = self.requests[attempt.request_id]
        source = self.instances[attempt.source]
        size = attempt.config.control_bytes
        attempt.control_bytes += size
        self.event('wire_send', attempt, kind='route_marker', bytes=size,
                   source=source.id, target='original_gateway')

        def processed():
            if not attempt.active:
                return
            if attempt.commit_proof != attempt.id:
                raise AssertionError('Route update lacks target ownership proof')
            if request.completed_at is None:
                request.gateway_owner = attempt.target
            self.event('gateway_route_processed', attempt, owner=attempt.target)
            attempt.control_bytes += size
            self.event('wire_send', attempt, kind='route_ack', bytes=size,
                       source='original_gateway', target=source.id)
            self.network.send(request.gateway_domain, source.domain, size, acknowledged)

        def acknowledged():
            if not attempt.active:
                return
            attempt.gateway_ack = attempt.id
            self.event('gateway_route_confirmed', attempt)
            self._release(attempt)
        self.network.send(source.domain, request.gateway_domain, size, processed)

    def _release(self, attempt):
        if not release_allowed(attempt):
            raise AssertionError('Source release requires commit proof AND gateway acknowledgement')
        self.instances[attempt.source].remove(attempt.request_id)
        attempt.state = 'completed'
        attempt.ended = self.clock.now
        attempt.deadline_event.cancel()
        self.requests[attempt.request_id].attempt = None
        self.event('source_released', attempt)
        self.event('migration_completed', attempt, duration_ms=attempt.ended - attempt.started)
        self.check_invariants()

    def _timeout(self, attempt):
        if not attempt.active:
            return
        if attempt.commit_proof is not None:
            # Timeout cannot give ownership back to the source after commit.
            # Reliable transport eventually delivers route acknowledgement.
            self.event('deadline_after_commit_source_fenced', attempt)
        else:
            self._abort(attempt, 'deadline')

    def _abort(self, attempt, reason):
        if not attempt.active or attempt.state == 'cancelling':
            return
        if attempt.commit_proof is not None:
            raise AssertionError('Cannot roll back committed ownership')
        attempt.state = 'cancelling'
        attempt.reason = reason
        attempt.deadline_event.cancel()
        source = self.instances[attempt.source]
        target = self.instances[attempt.target]
        self.event('migration_cancel_requested', attempt, reason=reason)

        def cancelled_at_target():
            target.remove(attempt.request_id)
            self.event('target_cancelled', attempt)
            self._send(attempt, target, source, attempt.config.control_bytes, resume,
                       'cancel_reply', allow_cancelling=True)

        def resume():
            attempt.state = 'cancelled'
            attempt.ended = self.clock.now
            request = self.requests[attempt.request_id]
            request.attempt = None
            replica = source.replicas.get(request.id)
            if replica:
                replica.paused = False
            self.event('migration_cancelled', attempt, reason=reason, target_cancel_confirmed=True)
            self.wake_all()
        self._send(attempt, source, target, attempt.config.control_bytes, cancelled_at_target,
                   'cancel', allow_cancelling=True)

    def _emit(self, node, replica, request):
        if request.owner != node.id or replica.replay or replica.paused:
            raise AssertionError('Only the active owner can sample/output')
        position = len(replica.output_ids)
        if position != len(request.output_ids) or position >= request.max_tokens:
            raise AssertionError('Output replay/duplication or budget overflow')
        token = 1000 + position
        replica.output_ids.append(token)
        request.output_ids.append(token)
        self.event('token_generated', request_id=request.id, instance=node.id, position=position, token_id=token)
        self.network.send(node.domain, request.gateway_domain, self.output_bytes,
                          lambda: self._deliver(request, position, token))
        if len(request.output_ids) == self._output_lengths[request.id]:
            request.generation_done = True
            if request.attempt is not None:
                attempt = self.attempts[request.attempt]
                if attempt.commit_proof is None:
                    self._abort(attempt, 'source_completed')
            node.remove(request.id)

    def _deliver(self, request, position, token):
        if position in request.delivered_positions:
            raise AssertionError('Duplicate token delivered')
        request.delivered_positions.add(position)
        request.pending_outputs[position] = token
        while len(request.client_ids) in request.pending_outputs:
            next_position = len(request.client_ids)
            request.client_ids.append(request.pending_outputs.pop(next_position))
            request.client_times.append(self.clock.now)
            self.event('token_delivered', request_id=request.id, position=next_position)
        waiters, request.flush_waiters = request.flush_waiters, []
        for emitted, callback in waiters:
            if len(request.client_ids) >= emitted:
                callback()
            else:
                request.flush_waiters.append((emitted, callback))
        if len(request.client_ids) == self._output_lengths[request.id]:
            request.completed_at = self.clock.now
            request.gateway_owner = None
            self.event('request_completed', request_id=request.id,
                       finish_reason='length' if len(request.client_ids) == request.max_tokens else 'stop')

    def check_invariants(self):
        for node in self.instances.values():
            if node.used != sum(r.blocks for r in node.replicas.values()) or not 0 <= node.used <= node.capacity:
                raise AssertionError('KV resource accounting violated')
            for replica in node.replicas.values():
                if replica.computed > replica.length or replica.computed < 0:
                    raise AssertionError('Invalid KV progress')
        for request in self.requests.values():
            active = [node.id for node in self.instances.values()
                      if request.id in node.replicas and not node.replicas[request.id].paused
                      and not node.replicas[request.id].replay]
            if len(active) > 1 or (active and active[0] != request.owner):
                raise AssertionError('Multiple/wrong output owners')
            if request.client_ids != request.output_ids[:len(request.client_ids)]:
                raise AssertionError('Client output is not an exact prefix of generated output')

    def run(self, until_ms=None):
        self.clock.run(until=until_ms)
        self.check_invariants()
        if until_ms is None and any(r.completed_at is None for r in self.requests.values()):
            raise RuntimeError('Unfinished requests without runnable events; likely KV admission deadlock')
        return self.summary()

    def summary(self):
        requests = {}
        for request in self.requests.values():
            times = request.client_times
            requests[request.id] = {
                'owner': request.owner, 'prompt_length': len(request.prompt_ids), 'max_tokens': request.max_tokens,
                'output_tokens': len(request.client_ids), 'completed': request.completed_at is not None,
                'workload_output_length': self._output_lengths[request.id],
                'workload_length_origin': self._length_origins[request.id],
                'finish_reason': (None if request.completed_at is None else
                                  'length' if len(request.client_ids) == request.max_tokens else 'stop'),
                'ttft_ms': times[0] - request.arrival if times else None,
                'e2e_ms': request.completed_at - request.arrival if request.completed_at is not None else None,
                'max_itl_ms': max((b - a for a, b in zip(times, times[1:])), default=0.0),
                'token_timestamps_ms': list(times), 'token_ids': list(request.client_ids)}
        attempts = []
        for attempt in self.attempts.values():
            attempts.append({
                'attempt_id': attempt.id, 'request_id': attempt.request_id, 'mode': attempt.config.mode,
                'state': attempt.state, 'reason': attempt.reason, 'source': attempt.source, 'target': attempt.target,
                'rounds': attempt.rounds, 'token_payload_bytes': attempt.token_payload_bytes,
                'kv_payload_bytes': attempt.kv_payload_bytes, 'control_bytes': attempt.control_bytes,
                'replay_tokens': attempt.replay_tokens,
                'duration_ms': attempt.ended - attempt.started if attempt.ended is not None else None,
                'source_pause_ms': (attempt.committed_at if attempt.committed_at is not None else attempt.ended)
                    - attempt.paused_at
                    if attempt.ended is not None and attempt.paused_at is not None else None,
                'source_fence_ms': attempt.ended - attempt.paused_at
                    if attempt.ended is not None and attempt.paused_at is not None else None,
                'commit_proof': attempt.commit_proof, 'gateway_ack': attempt.gateway_ack})
        return {'time_unit': 'ms', 'clock': 'discrete_event',
                'scheduling_model': 'snapshot_replay_gate_with_simulated_round_robin_batches',
                'compute_model': 'illustrative_affine' if all(n.compute.legacy is None for n in self.instances.values())
                    else 'legacy_profile_with_explicit_cached_context_term',
                'requests': requests, 'migrations': attempts,
                'instances': {node.id: {'compute_ms': node.compute_ms, 'replay_tokens': node.replay_tokens,
                                       'peak_kv_blocks': node.peak_blocks, 'remaining_kv_blocks': node.used}
                              for node in self.instances.values()},
                'network': self.network.summary()}
