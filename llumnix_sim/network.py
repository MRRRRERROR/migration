"""Directed domain links with equal-share bandwidth and one-way propagation.

Bandwidth is bytes/ms. A link is a shared bottleneck, not an independent pipe
per request. Serialization and propagation are separate events.
"""
import math


class Link:
    def __init__(self, clock, bandwidth_bytes_per_ms, latency_ms):
        if not math.isfinite(bandwidth_bytes_per_ms) or bandwidth_bytes_per_ms <= 0:
            raise ValueError('Link bandwidth must be finite and positive')
        if not math.isfinite(latency_ms) or latency_ms < 0:
            raise ValueError('Link latency must be finite and nonnegative')
        self.clock = clock
        self.bandwidth = bandwidth_bytes_per_ms
        self.latency = latency_ms
        self.flows = []
        self.updated = clock.now
        self.timer = None
        self.bytes_sent = 0
        self.peak_flows = 0

    def _advance(self):
        if self.flows:
            service = (self.clock.now - self.updated) * self.bandwidth / len(self.flows)
            for flow in self.flows:
                flow[0] -= service
        self.updated = self.clock.now

    def send(self, size, callback):
        if not isinstance(size, int) or size < 0:
            raise ValueError('Payload size must be a nonnegative integer')
        self._advance()
        self.flows.append([float(size), callback])
        self.bytes_sent += size
        self.peak_flows = max(self.peak_flows, len(self.flows))
        self._schedule()

    def _schedule(self):
        if self.timer:
            self.timer.cancel()
        if self.flows:
            delay = max(0.0, min(flow[0] for flow in self.flows)) * len(self.flows) / self.bandwidth
            self.timer = self.clock.after(delay, self._complete)

    def _complete(self):
        self._advance()
        done = [flow for flow in self.flows if flow[0] <= 1e-6]
        self.flows = [flow for flow in self.flows if flow[0] > 1e-6]
        for _, callback in done:
            self.clock.after(self.latency, callback)
        self._schedule()


class Network:
    def __init__(self, clock, domains, links):
        self.clock = clock
        self.links = {}
        # Same-domain messages still have configured serialization/propagation.
        for domain in domains:
            self.links[(domain, domain)] = Link(clock, 1000000.0, 0.0)
        configured = set()
        for row in links:
            pair = (row['source'], row['target'])
            if pair in configured:
                raise ValueError('Duplicate directed link: ' + str(pair))
            configured.add(pair)
            if any(domain not in domains for domain in pair):
                raise ValueError('Link refers to unknown domain: ' + str(pair))
            self.links[pair] = Link(clock, row['bandwidth_bytes_per_ms'], row['latency_ms'])

    def require(self, source, target):
        if (source, target) not in self.links:
            raise ValueError('Missing explicit directed domain link: ' + str((source, target)))

    def send(self, source, target, size, callback):
        self.require(source, target)
        self.links[(source, target)].send(size, callback)

    def summary(self):
        return {source + '->' + target: {'bytes_sent': link.bytes_sent, 'peak_flows': link.peak_flows}
                for (source, target), link in sorted(self.links.items())}
