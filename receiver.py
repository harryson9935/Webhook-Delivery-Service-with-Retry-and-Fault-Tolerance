"""
A mock downstream receiver. Stands in for "the customer's server" that our
webhook service delivers events to. It deliberately fails a configurable
percentage of requests (random 500s, timeouts, connection resets) so the
retry / backoff / dead-letter logic has something real to react to.
"""
import random
import time

from flask import Flask, request, jsonify

app = Flask(__name__)
received_log = []  # in-memory record of what actually arrived, for verification


@app.route("/webhook/<endpoint_key>", methods=["POST"])
def receive(endpoint_key):
    failure_rate = float(request.args.get("failure_rate", 0.0))
    roll = random.random()

    if roll < failure_rate * 0.6:
        # simulate a slow/hanging downstream -> the caller's request will time out
        time.sleep(1.2)
        return jsonify({"error": "simulated timeout"}), 500

    if roll < failure_rate:
        # simulate a transient 5xx
        return jsonify({"error": "simulated transient failure"}), 503

    received_log.append({
        "endpoint_key": endpoint_key,
        "event_id": request.json.get("event_id") if request.is_json else None,
        "received_at": time.time(),
    })
    return jsonify({"status": "accepted"}), 200


@app.route("/_received", methods=["GET"])
def get_received():
    return jsonify(received_log)


if __name__ == "__main__":
    app.run(port=5001)
