from fastapi.testclient import TestClient

from app import TokenBucketRateLimiter, app

client = TestClient(app)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_burst_then_block():
    c = FakeClock()
    rl = TokenBucketRateLimiter(capacity=3, refill_rate=1, clock=c)
    assert [rl.allow("a")[0] for _ in range(4)] == [True, True, True, False]


def test_refill_over_time():
    c = FakeClock()
    rl = TokenBucketRateLimiter(capacity=2, refill_rate=1, clock=c)
    rl.allow("a"); rl.allow("a")
    assert rl.allow("a")[0] is False
    c.t += 1.0
    assert rl.allow("a")[0] is True


def test_clients_are_independent():
    c = FakeClock()
    rl = TokenBucketRateLimiter(capacity=1, refill_rate=1, clock=c)
    assert rl.allow("a")[0] is True
    assert rl.allow("a")[0] is False
    assert rl.allow("b")[0] is True


def test_capacity_cap():
    c = FakeClock()
    rl = TokenBucketRateLimiter(capacity=2, refill_rate=1, clock=c)
    c.t += 1000  # long idle must not exceed capacity
    assert [rl.allow("a")[0] for _ in range(3)] == [True, True, False]


def test_retry_after():
    c = FakeClock()
    rl = TokenBucketRateLimiter(capacity=1, refill_rate=2, clock=c)
    rl.allow("a")
    allowed, _, retry = rl.allow("a")
    assert not allowed and abs(retry - 0.5) < 1e-9


def test_http_success_headers():
    r = client.get("/api/hello", headers={"x-api-key": "http-headers"})
    assert r.status_code == 200
    assert r.json() == {"message": "Hello! Your request was allowed."}
    assert r.headers["X-RateLimit-Limit"] == "10"
    assert r.headers["X-RateLimit-Remaining"] == "9"


def test_http_burst_then_429():
    headers = {"x-api-key": "http-burst"}
    statuses = [client.get("/api/hello", headers=headers).status_code for _ in range(11)]
    assert statuses[:10] == [200] * 10
    assert statuses[10] == 429

    r = client.get("/api/hello", headers=headers)
    assert r.status_code == 429
    assert r.headers["X-RateLimit-Limit"] == "10"
    assert r.headers["X-RateLimit-Remaining"] == "0"
    assert "Retry-After" in r.headers
    assert r.json()["error"] == "Too Many Requests"


def test_http_clients_are_isolated():
    a = {"x-api-key": "http-iso-a"}
    b = {"x-api-key": "http-iso-b"}
    assert [client.get("/api/hello", headers=a).status_code for _ in range(10)] == [200] * 10
    assert client.get("/api/hello", headers=a).status_code == 429
    assert client.get("/api/hello", headers=b).status_code == 200
