"""
Runs the whole system in a single process for demonstration purposes:

  1. Starts the mock receiver (flaky downstream) on :5001
  2. Starts the webhook REST API on :5000
  3. Starts the worker pool (4 workers) that drains the delivery queue
  4. Registers several subscriber endpoints with different simulated
     failure rates (0%, 20%, 50%, 80%) to show how the system behaves
     across a spectrum of downstream reliability
  5. Publishes N events of varying types
  6. Waits for the queue to drain (or dead-letter)
  7. Exports:
       data/deliveries_dataset.csv   <- the "dataset" for this project
       data/delivery_attempts.csv
       charts/*.png                  <- result visualizations
       RESULTS.md                    <- written summary of findings
"""
import csv
import json
import threading
import time

import requests
from werkzeug.serving import make_server

import db
import worker
from logger import get_logger, log

logger = get_logger("simulate")

API_PORT = 5000
RECEIVER_PORT = 5001


class ServerThread(threading.Thread):
    def __init__(self, flask_app, port):
        super().__init__(daemon=True)
        self.srv = make_server("127.0.0.1", port, flask_app, threaded=True)
        self.ctx = flask_app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()


def wait_for_port(port, timeout=5):
    import socket
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not come up in time")


def run_simulation(num_events=120, num_workers=6, run_seconds_max=90):
    db.init_db("data/webhook_service.db")

    import app as api_app
    import receiver as receiver_app

    api_thread = ServerThread(api_app.app, API_PORT)
    recv_thread = ServerThread(receiver_app.app, RECEIVER_PORT)
    api_thread.start()
    recv_thread.start()
    wait_for_port(API_PORT)
    wait_for_port(RECEIVER_PORT)
    log(logger, "info", "servers up", api_port=API_PORT, receiver_port=RECEIVER_PORT)

    stop_event, worker_threads = worker.start_worker_pool(num_workers=num_workers)
    log(logger, "info", "worker pool started", num_workers=num_workers)

    base = f"http://127.0.0.1:{API_PORT}"
    recv_base = f"http://127.0.0.1:{RECEIVER_PORT}/webhook"

    # Register subscriber endpoints spanning a range of downstream reliability
    reliability_profiles = [
        ("reliable-partner", 0.0),
        ("mostly-stable-partner", 0.2),
        ("flaky-partner", 0.5),
        ("very-flaky-partner", 0.8),
    ]
    endpoints = {}
    for name, failure_rate in reliability_profiles:
        resp = requests.post(f"{base}/endpoints", json={
            "url": f"{recv_base}/{name}",
            "secret": f"secret-{name}",
            "event_types": ["*"],
            "failure_rate": failure_rate,
        })
        endpoints[name] = resp.json()["endpoint_id"]

    log(logger, "info", "endpoints registered", endpoints=endpoints)

    # Publish a mix of event types across all endpoints
    event_types = ["order.created", "payment.succeeded", "payment.failed",
                    "user.updated", "invoice.paid"]
    for i in range(num_events):
        etype = event_types[i % len(event_types)]
        requests.post(f"{base}/events", json={
            "event_type": etype,
            "data": {"seq": i, "amount": round(10 + i * 1.37, 2)},
            "max_attempts": 6,
        })

    log(logger, "info", "events published", count=num_events)

    # Wait for the queue to fully drain (all deliveries success or dead_letter)
    start = time.time()
    conn = db.get_conn()
    while time.time() - start < run_seconds_max:
        pending = conn.execute(
            "SELECT COUNT(*) c FROM deliveries WHERE status IN ('pending','processing','retrying')"
        ).fetchone()["c"]
        if pending == 0:
            break
        time.sleep(0.5)

    drain_time = time.time() - start
    log(logger, "info", "queue drained", drain_time_s=round(drain_time, 2))

    stop_event.set()
    time.sleep(0.5)

    snapshot = db.stats_snapshot()
    export_dataset()
    api_thread.shutdown()
    recv_thread.shutdown()
    return snapshot, drain_time


def export_dataset():
    conn = db.get_conn()

    deliveries = conn.execute("""
        SELECT d.id as delivery_id, e.event_type, ep.url as endpoint_url, ep.failure_rate,
               d.status, d.attempt_count, d.max_attempts, d.last_response_code,
               d.last_error, d.created_at, d.updated_at
        FROM deliveries d
        JOIN events e ON e.id = d.event_id
        JOIN endpoints ep ON ep.id = d.endpoint_id
    """).fetchall()
    with open("data/deliveries_dataset.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["delivery_id", "event_type", "endpoint_url", "endpoint_failure_rate",
                          "status", "attempt_count", "max_attempts", "last_response_code",
                          "last_error", "created_at", "updated_at", "total_latency_s"])
        for r in deliveries:
            writer.writerow([r["delivery_id"], r["event_type"], r["endpoint_url"],
                              r["failure_rate"], r["status"], r["attempt_count"],
                              r["max_attempts"], r["last_response_code"], r["last_error"],
                              r["created_at"], r["updated_at"],
                              round(r["updated_at"] - r["created_at"], 3)])

    attempts = conn.execute("""
        SELECT da.delivery_id, da.attempt_number, da.status, da.response_code,
               da.latency_ms, da.error, da.attempted_at
        FROM delivery_attempts da
        ORDER BY da.attempted_at ASC
    """).fetchall()
    with open("data/delivery_attempts.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["delivery_id", "attempt_number", "status", "response_code",
                          "latency_ms", "error", "attempted_at"])
        for r in attempts:
            writer.writerow([r["delivery_id"], r["attempt_number"], r["status"],
                              r["response_code"], round(r["latency_ms"], 2) if r["latency_ms"] else None,
                              r["error"], r["attempted_at"]])

    log(logger, "info", "dataset exported",
        deliveries=len(deliveries), attempts=len(attempts))


if __name__ == "__main__":
    snapshot, drain_time = run_simulation()
    print(json.dumps(snapshot, indent=2))
    print(f"drain_time_s={drain_time:.2f}")
