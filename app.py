"""
Token Bucket Rate Limiter + FastAPI backend.

Data model: hash map  key -> Bucket(tokens, last_refill)
  - key         : client identifier (API key if present, else IP address)
  - tokens      : float, tokens currently available (<= capacity)
  - last_refill : monotonic timestamp of the last time tokens were topped up

Run:   pip install fastapi uvicorn
       uvicorn app:app --reload
Test:  for i in $(seq 1 15); do curl -i localhost:8000/api/hello; done
"""
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


@dataclass
class Bucket:
    tokens: float
    last_refill: float


class TokenBucketRateLimiter:
    def __init__(
        self,
        capacity: int,
        refill_rate: float,                       # tokens added per second
        clock: Callable[[], float] = time.monotonic,
        cleanup_interval: float = 60.0,
    ):
        if capacity <= 0 or refill_rate <= 0:
            raise ValueError("capacity and refill_rate must be positive")
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._clock = clock
        self._buckets: Dict[str, Bucket] = {}
        self._lock = threading.Lock()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = clock()
        # an idle bucket is indistinguishable from a fresh one once fully refilled
        self._idle_ttl = capacity / refill_rate

    def allow(self, key: str, cost: int = 1) -> Tuple[bool, int, float]:
        """
        Returns (allowed, remaining_tokens, retry_after_seconds).
        """
        now = self._clock()
        with self._lock:
            self._maybe_cleanup(now)

            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = Bucket(tokens=float(self.capacity), last_refill=now)
                self._buckets[key] = bucket

            # lazy refill: compute tokens earned since last request
            elapsed = now - bucket.last_refill
            bucket.tokens = min(self.capacity, bucket.tokens + elapsed * self.refill_rate)
            bucket.last_refill = now

            if bucket.tokens >= cost:
                bucket.tokens -= cost
                return True, int(bucket.tokens), 0.0

            retry_after = (cost - bucket.tokens) / self.refill_rate
            return False, int(bucket.tokens), retry_after

    def _maybe_cleanup(self, now: float) -> None:
        """Evict idle buckets so memory doesn't grow without bound."""
        if now - self._last_cleanup < self._cleanup_interval:
            return
        self._last_cleanup = now
        stale = [k for k, b in self._buckets.items() if now - b.last_refill > self._idle_ttl]
        for k in stale:
            del self._buckets[k]


# ---------------------------- Backend ----------------------------

app = FastAPI(title="Rate Limited API")
limiter = TokenBucketRateLimiter(capacity=10, refill_rate=1.0)  # burst 10, 1 req/s sustained


def client_key(request: Request) -> str:
    api_key = request.headers.get("x-api-key")
    if api_key:
        return f"key:{api_key}"
    return f"ip:{request.client.host if request.client else 'unknown'}"


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    allowed, remaining, retry_after = limiter.allow(client_key(request))
    headers = {
        "X-RateLimit-Limit": str(limiter.capacity),
        "X-RateLimit-Remaining": str(remaining),
    }
    if not allowed:
        headers["Retry-After"] = str(int(retry_after) + 1)
        return JSONResponse(
            status_code=429,
            content={"error": "Too Many Requests", "retry_after_seconds": round(retry_after, 2)},
            headers=headers,
        )
    response = await call_next(request)
    response.headers.update(headers)
    return response


@app.get("/api/hello")
def hello():
    return {"message": "Hello! Your request was allowed."}
