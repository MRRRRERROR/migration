"""CPU-only discrete-event migration simulator; no Ray/vLLM import side effects."""

from .engine import Simulation

__all__ = ['Simulation']
