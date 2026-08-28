"""
Structured (JSON-lines) logging for the webhook delivery service.
Every log line is a single JSON object so it can be grepped, shipped to
ELK/Datadog, or parsed for the analytics/report step of this project.
"""
import json
import logging
import os
import sys
import time

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "webhook_service.log")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": round(time.time(), 3),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # allow extra structured fields via logger.info(msg, extra={"extra_fields": {...}})
        extra_fields = getattr(record, "extra_fields", None)
        if extra_fields:
            payload.update(extra_fields)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(JsonFormatter())

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(JsonFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def log(logger: logging.Logger, level: str, message: str, **fields):
    getattr(logger, level)(message, extra={"extra_fields": fields})
