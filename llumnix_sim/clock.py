"""Deterministic virtual clock. All times are milliseconds."""
import heapq
import itertools
import math
from dataclasses import dataclass


@dataclass
class Event:
    callback: object
    cancelled: bool = False

    def cancel(self):
        self.cancelled = True


class Clock:
    def __init__(self):
        self.now = 0.0
        self.queue = []
        self.sequence = itertools.count()

    def at(self, timestamp, callback):
        if not math.isfinite(timestamp) or timestamp < self.now:
            raise ValueError('Events must have finite, nondecreasing times')
        event = Event(callback)
        heapq.heappush(self.queue, (float(timestamp), next(self.sequence), event))
        return event

    def after(self, delay, callback):
        return self.at(self.now + delay, callback)

    def run(self, until=None, max_events=1000000):
        if until is not None and (not math.isfinite(until) or until < self.now):
            raise ValueError('Invalid simulation horizon')
        count = 0
        while self.queue:
            timestamp, sequence, event = heapq.heappop(self.queue)
            if event.cancelled:
                continue
            if until is not None and timestamp > until:
                heapq.heappush(self.queue, (timestamp, sequence, event))
                self.now = until
                break
            self.now = timestamp
            event.callback()
            count += 1
            if count >= max_events:
                raise RuntimeError('Event limit exceeded; inspect simulation for livelock')
        return count
