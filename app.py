"""
Public REST API for the webhook delivery service.

    POST /endpoints          register a destination endpoint (subscriber)
    POST /events              publish an event -> fans out to matching endpoints
    GET  /events/<id>/status  delivery status of an event across all endpoints
    GET  /deliveries          list deliveries (filter by status)
    GET  /stats                aggregate reliability metrics
"""
from flask import Flask, request, jsonify

import db
from logger import get_logger, log

logger = get_logger("api")
app = Flask(__name__)


@app.route("/endpoints", methods=["POST"])
def register_endpoint():
    body = request.get_json(force=True)
    endpoint_id = db.create_endpoint(
        url=body["url"],
        secret=body.get("secret", "changeme"),
        event_types=body.get("event_types", ["*"]),
        failure_rate=body.get("failure_rate", 0.0),
    )
    log(logger, "info", "endpoint registered", endpoint_id=endpoint_id, url=body["url"])
    return jsonify({"endpoint_id": endpoint_id}), 201


@app.route("/events", methods=["POST"])
def publish_event():
    body = request.get_json(force=True)
    event_type = body["event_type"]
    payload = body.get("data", {})
    max_attempts = body.get("max_attempts", 6)

    event_id = db.create_event(event_type, payload)
    matching = db.list_active_endpoints(event_type)

    delivery_ids = []
    for ep in matching:
        did = db.create_delivery(event_id, ep["id"], max_attempts=max_attempts)
        delivery_ids.append(did)

    log(logger, "info", "event published", event_id=event_id, event_type=event_type,
        fanout=len(delivery_ids))
    return jsonify({"event_id": event_id, "deliveries_created": len(delivery_ids),
                     "delivery_ids": delivery_ids}), 201


@app.route("/events/<event_id>/status", methods=["GET"])
def event_status(event_id):
    conn = db.get_conn()
    rows = conn.execute("SELECT * FROM deliveries WHERE event_id=?", (event_id,)).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/deliveries", methods=["GET"])
def list_deliveries():
    status = request.args.get("status")
    conn = db.get_conn()
    if status:
        rows = conn.execute("SELECT * FROM deliveries WHERE status=?", (status,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM deliveries").fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/stats", methods=["GET"])
def stats():
    return jsonify(db.stats_snapshot())


if __name__ == "__main__":
    db.init_db("data/webhook_service.db")
    app.run(port=5000)
