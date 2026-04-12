"""
Full-Text Acquisition System — Rate Limiter

Per-host rate limiter combining an `asyncio.Semaphore` (concurrency cap)
with a sliding-window counter (requests per second). Singleton — shared
across every worker, HTTP helper, and enrichment task so all calls to
a given API provider respect the same budget.

Design:
    - Each host has its own lock, deque of recent timestamps, and semaphore.
    - `acquire(host)` is an async context manager. It:
        1. Holds the per-host lock.
        2. Purges timestamps older than 1.0 seconds from the deque.
        3. If len(deque) >= limit, computes how long until the oldest
           entry ages out of the window, sleeps that long, purges again.
        4. Appends now to the deque, releases the lock.
        5. Acquires the per-host semaphore (concurrency cap).
        6. Yields the delay (seconds) so the caller can log
           RATE_LIMIT_APPLIED if delay > 0.
        7. Releases the semaphore on __aexit__.
    - Holding the lock during the sleep gives fair FIFO waiting — the
      caller that arrives first waits first.
    - Unknown hosts pass through without rate limiting (yield 0.0).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-host limits (requests per second)
# ---------------------------------------------------------------------------

# Source: published polite-pool / terms-of-use limits. Keep these
# conservative — a ban is more expensive than a second of waiting.
DEFAULT_LIMITS: Dict[str, int] = {
    "unpaywall": 5,
    "openalex": 10,
    "crossref": 45,          # polite pool
    "europepmc": 5,
    "semantic_scholar": 3,
}

# Sliding window duration
WINDOW_SECONDS: float = 1.0


def _normalize_host(host_or_url: str) -> Optional[str]:
    """Map an API name or URL to a canonical host key.

    Returns None if the host is not rate-limited (caller passes through).
    """
    if not host_or_url:
        return None

    key = host_or_url.strip().lower()

    # Exact API-name match (what retrieval_engine passes)
    if key in DEFAULT_LIMITS:
        return key

    # URL substring match (defensive — callers may pass full URLs)
    if "api.unpaywall.org" in key or "unpaywall" in key:
        return "unpaywall"
    if "api.openalex.org" in key or "openalex" in key:
        return "openalex"
    if "api.crossref.org" in key or "crossref" in key:
        return "crossref"
    if "europepmc.org" in key or "europepmc" in key:
        return "europepmc"
    if "semanticscholar.org" in key or "semantic_scholar" in key or "semantic-scholar" in key:
        return "semantic_scholar"

    return None


class RateLimiter:
    """Singleton per-host rate limiter."""

    _instance: Optional[RateLimiter] = None

    def __init__(self, limits: Optional[Dict[str, int]] = None) -> None:
        self._limits: Dict[str, int] = dict(limits or DEFAULT_LIMITS)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._windows: Dict[str, "deque[float]"] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        # Stats (for diagnostics / health dashboard)
        self._delays_applied: Dict[str, int] = {h: 0 for h in self._limits}
        self._requests_served: Dict[str, int] = {h: 0 for h in self._limits}
        self._total_delay_s: Dict[str, float] = {h: 0.0 for h in self._limits}

        for host, limit in self._limits.items():
            self._locks[host] = asyncio.Lock()
            self._windows[host] = deque()
            self._semaphores[host] = asyncio.Semaphore(limit)

    @classmethod
    def get_instance(cls) -> RateLimiter:
        """Get or create the singleton RateLimiter."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton (testing only)."""
        cls._instance = None

    @property
    def limits(self) -> Dict[str, int]:
        return dict(self._limits)

    def get_stats(self) -> Dict[str, Dict[str, float]]:
        """Return per-host stats for the health dashboard / audit log."""
        return {
            host: {
                "limit_per_second": self._limits[host],
                "requests_served": self._requests_served[host],
                "delays_applied": self._delays_applied[host],
                "total_delay_s": round(self._total_delay_s[host], 3),
                "in_window": len(self._windows[host]),
            }
            for host in self._limits
        }

    @asynccontextmanager
    async def acquire(self, host_or_url: str) -> AsyncIterator[float]:
        """Acquire a rate-limit slot for the given host.

        Usage:
            async with limiter.acquire("openalex") as delay_s:
                if delay_s > 0:
                    # log RATE_LIMIT_APPLIED
                    ...
                response = await http_client.get(url)

        The `delay_s` yielded is the number of seconds the caller was
        made to wait by the sliding window (0.0 if no wait).
        Unknown hosts pass through immediately with delay_s=0.0 and
        no concurrency cap.
        """
        host = _normalize_host(host_or_url)
        if host is None:
            # No limit configured — pass through
            yield 0.0
            return

        delay_applied = 0.0

        # Sliding-window admission control (holding the lock queues waiters fairly)
        async with self._locks[host]:
            now = time.monotonic()
            window = self._windows[host]

            # Purge timestamps outside the 1-second window
            while window and (now - window[0]) >= WINDOW_SECONDS:
                window.popleft()

            if len(window) >= self._limits[host]:
                # Window is full — wait until the oldest entry ages out.
                # Add a tiny epsilon so we're definitely past the window edge.
                oldest = window[0]
                delay_applied = max(
                    0.0,
                    WINDOW_SECONDS - (now - oldest) + 0.001,
                )
                if delay_applied > 0:
                    self._delays_applied[host] += 1
                    self._total_delay_s[host] += delay_applied
                    logger.debug(
                        "Rate-limit wait: host=%s limit=%d/s window_full=%d delay=%.3fs",
                        host, self._limits[host], len(window), delay_applied,
                    )
                    await asyncio.sleep(delay_applied)
                    # Purge again after sleep
                    now = time.monotonic()
                    while window and (now - window[0]) >= WINDOW_SECONDS:
                        window.popleft()

            # Record our timestamp
            window.append(now)
            self._requests_served[host] += 1

        # Concurrency cap — separate from rate. Yields control of the lock
        # so the next caller can queue up while this request is in flight.
        async with self._semaphores[host]:
            try:
                yield delay_applied
            finally:
                # Semaphore released automatically by the `async with`.
                # Nothing else to clean up — the timestamp stays in the
                # window until it ages out naturally.
                pass
