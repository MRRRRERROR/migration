"""Simulation port of the 2026-10-09 V1 token migration contract.

Reference: server_snapshots/token_migration_20261011_01/project/python/llumnix/
llumnix/token_migration/{config,runtime,gateway}.py. This is a behavioral port,
not an import of the GPU-dependent runtime. No semantic token/logit equivalence
is claimed: the simulator emits deterministic token IDs by output position.
"""
import math
from dataclasses import dataclass, field

MODES = ('kv', 'token_stop', 'token_live')


@dataclass(frozen=True)
class MigrationConfig:
    mode: str = 'kv'
    max_rounds: int = 3
    final_gap_tokens: int = 16
    timeout_ms: float = 30000.0
    token_bytes: int = 4
    control_bytes: int = 256

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError('Unknown migration mode: ' + self.mode)
        for name in ('max_rounds', 'final_gap_tokens', 'token_bytes', 'control_bytes'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(name + ' must be a nonnegative integer')
        if self.token_bytes == 0 or self.control_bytes == 0:
            raise ValueError('Wire sizes must be positive')
        if not math.isfinite(self.timeout_ms) or self.timeout_ms <= 0:
            raise ValueError('Migration timeout must be finite and positive')


@dataclass
class Attempt:
    id: str
    request_id: str
    source: str
    target: str
    config: MigrationConfig
    started: float
    deadline: float
    state: str = 'preparing'
    rounds: int = 0
    token_payload_bytes: int = 0
    kv_payload_bytes: int = 0
    control_bytes: int = 0
    replay_tokens: int = 0
    paused_at: object = None
    ended: object = None
    reason: object = None
    commit_proof: object = None
    committed_at: object = None
    gateway_ack: object = None
    deadline_event: object = field(default=None, repr=False)
    ready_callback: object = field(default=None, repr=False)

    @property
    def active(self):
        return self.state not in ('completed', 'cancelled', 'failed', 'rejected')


def ready(computed_tokens, prompt_length, output_length):
    """Last emitted token has not yet been processed into KV at takeover."""
    return computed_tokens == prompt_length + output_length - 1


def release_allowed(attempt):
    return attempt.commit_proof == attempt.id and attempt.gateway_ack == attempt.id
