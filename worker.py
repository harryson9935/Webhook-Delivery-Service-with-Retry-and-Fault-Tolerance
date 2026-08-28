"""
Worker pool for asynchronous webhook delivery.

Design:
- A fixed pool of worker threads polls the DB queue (see db.claim_due_deliveries)
  for deliveries whose next_retry_at has elapsed.
- Each delivery is POSTed to the destination endpoint with an HMAC-SHA256
  signature header (X-Webhook-Signature) so receivers can verify authenticity
  -- the same pattern used by Stripe/GitHub webhooks.
- On failure, the delivery is rescheduled with exponential backoff + jitter:
      delay = min(cap, base * 2^(attempt-1)) + random(0, jitter)
  After `max_attempts` failures the delivery is moved to a dead_letter
  status instead of being retried forever.
"""
import hashlib
import hmac
import json
import random
import threading
import time

import requests

import db
from logger import get_logger, log

logger = get_logger("worker")

# NOTE: these are scaled down from realistic production values (e.g. base=2s,
# cap=300s) purely so this demo finishes in seconds instead of minutes. The
# backoff *shape* (exponential + jitter + cap) is what matters and is
# production-representative; only the constants are compressed for the demo.
BASE_BACKOFF = 0.3     # seconds
MAX_BACKOFF = 5.0       # seconds cap
JITTER = 0.2            # seconds of random jitter added to every backoff
REQUEST_TIMEOUT = 0.6   # seconds


def sign_payload(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def compute_backoff(attempt_number: int) -> float:
    delay = min(MAX_BACKOFF, BASE_BACKOFF * (2 ** (attempt_number - 1)))
    return delay + random.uniform(0, JITTER)


def deliver_one(delivery_row, event_row, endpoint_row):
    payload = json.loads(event_row["payload"])
    body_obj = {
        "event_id": event_row["id"],
        "event_type": event_row["event_type"],
        "data": payload,
        "attempt": delivery_row["attempt_count"] + 1,
    }
    body_bytes = json.dumps(body_obj).encode()
    signature = sign_payload(endpoint_row["secret"], body_bytes)

    start = time.time()
    try:
        resp = requests.post(
            endpoint_row["url"],
            data=body_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": signature,
                "X-Webhook-Event": event_row["event_type"],
            },
            params={"failure_rate": endpoint_row["failure_rate"]},
            timeout=REQUEST_TIMEOUT,
        )
        latency_ms = (time.time() - start) * 1000
        success = 200 <= resp.status_code < 300
        return success, resp.status_code, latency_ms, None if success else f"HTTP {resp.status_code}"
    except requests.exceptions.Timeout:
        latency_ms = (time.time() - start) * 1000
        return False, None, latency_ms, "timeout"
    except requests.exceptions.RequestException as e:
        latency_ms = (time.time() - start) * 1000
        return False, None, latency_ms, str(e)


def worker_loop(worker_id: int, stop_event: threading.Event, poll_interval=0.15):
    conn = db.get_conn()
    while not stop_event.is_set():
        deliveries = db.claim_due_deliveries(limit=5)
        if not deliveries:
            time.sleep(poll_interval)
            continue
        for delivery in deliveries:
            event = conn.execute("SELECT * FROM events WHERE id=?", (delivery["event_id"],)).fetchone()
            endpoint = db.get_endpoint(delivery["endpoint_id"])
            if not endpoint or not event:
                continue
            success, code, latency_ms, error = deliver_one(delivery, event, endpoint)
            backoff = compute_backoff(delivery["attempt_count"] + 1)
            db.record_attempt_result(delivery, success, code, latency_ms, error, backoff)

            log(
                logger, "info" if success else "warning",
                "delivery attempt",
                worker_id=worker_id,
                delivery_id=delivery["id"],
                event_id=event["id"],
                event_type=event["event_type"],
                endpoint_id=endpoint["id"],
                attempt=delivery["attempt_count"] + 1,
                success=success,
                response_code=code,
                latency_ms=round(latency_ms, 2),
                error=error,
                next_backoff_s=round(backoff, 2) if not success else None,
            )


def start_worker_pool(num_workers=4):
    stop_event = threading.Event()
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker_loop, args=(i, stop_event), daemon=True)
        t.start()
        threads.append(t)
    return stop_event, threads
