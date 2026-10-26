"""Explicit illustrative compute model, or adapter for a legacy LatencyMemData.

The legacy profile uses TOTAL context for decode. Token reconstruction includes
cached context as a separate feature; it must not silently be treated as free.
"""
import math


class LatencyModel:
    def __init__(self, config=None, legacy=None):
        values = dict(prefill_base_ms=1.0, prefill_ms_per_token=0.02,
                      prefill_ms_per_cached_token=0.0001, decode_base_ms=10.0,
                      decode_ms_per_request=0.1, decode_ms_per_context_token=0.0001)
        config = config or {}
        unknown = set(config) - set(values)
        if unknown:
            raise ValueError('Unknown compute parameters: ' + str(sorted(unknown)))
        values.update(config)
        if any(not math.isfinite(v) or v < 0 for v in values.values()):
            raise ValueError('Compute coefficients must be finite and nonnegative')
        if values['prefill_base_ms'] <= 0 or values['decode_base_ms'] <= 0:
            raise ValueError('Positive step overheads are required')
        self.values = values
        self.legacy = legacy

    def prefill(self, tokens, cached_context=0):
        if not tokens:
            return 0.0
        v = self.values
        if self.legacy is not None:
            aligned = tokens + (-tokens % 8)
            if aligned in self.legacy.prefill_latency:
                latency = self.legacy.prefill_latency[aligned][0]
            else:
                a, b = self.legacy.prefill_model_params
                latency = a * aligned + b
        else:
            latency = v['prefill_base_ms'] + v['prefill_ms_per_token'] * tokens
        return self._check(latency + v['prefill_ms_per_cached_token'] * cached_context)

    def decode(self, batch_size, total_context):
        if not batch_size:
            return 0.0
        v = self.values
        if self.legacy is not None:
            aligned = batch_size + (-batch_size % 8)
            key = (aligned, total_context)
            if key in self.legacy.decode_latency:
                latency = self.legacy.decode_latency[key][0]
            else:
                a, b, c = self.legacy.decode_model_params
                latency = a * aligned + b * total_context + c
        else:
            latency = (v['decode_base_ms'] + v['decode_ms_per_request'] * batch_size
                       + v['decode_ms_per_context_token'] * total_context)
        return self._check(latency)

    @staticmethod
    def _check(value):
        if not math.isfinite(value) or value <= 0:
            raise ValueError('Profile predicts nonpositive/nonfinite execution time')
        return float(value)
