#!/usr/bin/env python3
import subprocess
import time
import json
import pika
from datetime import datetime, timezone
import re
from surrealdb import Surreal

# -------------------------------
# SurrealDB settings
# -------------------------------
SURREALDB_URL = "ws://127.0.0.1:8000"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"
DB_NS = "p2000"
DB_NAME = "p2000"
TABLE = "messages"

# -------------------------------
# RabbitMQ settings
# -------------------------------
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL = 300000  # 5 minutes in ms

# -------------------------------
# FLEX / P2000 parsing
# -------------------------------
PRIO_RE = re.compile(r"\b([AB]?1|[AB]?2|[AB]?3|PRIO ?[1235]|P ?[1235])\b", re.IGNORECASE)
GRIP_RE = re.compile(r"\bGRIP ?([1-4])\b", re.IGNORECASE)

# -------------------------------
# SurrealDB helpers
# -------------------------------
def is_surrealdb_running():
    """Check if SurrealDB WebSocket server is up."""
    try:
        db = Surreal(SURREALDB_URL)
        db.signin({"user": SURREALDB_USER, "pass": SURREALDB_PASS})
        db.use(namespace=DB_NS, database=DB_NAME)
        return True
    except Exception:
        return False

def start_surrealdb():
    print("Starting SurrealDB...")
    subprocess.Popen([
        "surreal", "start", "rocksdb:./p2000-surreal.db",
        "--bind", "0.0.0.0:8000",
        "--user", SURREALDB_USER,
        "--pass", SURREALDB_PASS,
        "--log", "stdout"
    ])
    for _ in range(10):
        if is_surrealdb_running():
            print("SurrealDB is running.")
            return
        time.sleep(1)
    raise RuntimeError("Failed to start SurrealDB")

# -------------------------------
# Insert message
# -------------------------------
def insert_message(msg):
    """Insert a P2000 message into SurrealDB, with deduplication."""
    db = Surreal(SURREALDB_URL)
    db.signin({"user": SURREALDB_USER, "pass": SURREALDB_PASS})
    db.use(namespace=DB_NS, database=DB_NAME)

    msg["received_at"] = datetime.now(timezone.utc).isoformat()

    # Parse prio and grip if missing
    if not msg.get("prio") and msg.get("message"):
        m = PRIO_RE.search(msg["message"])
        msg["prio"] = m.group(1).upper().replace(" ", "") if m else None
    if not msg.get("grip") and msg.get("message"):
        m = GRIP_RE.search(msg["message"])
        msg["grip"] = int(m.group(1)) if m else None

    # Deduplicate by raw
    try:
        result = db.query(f'SELECT * FROM {TABLE} WHERE raw = $raw', {"raw": msg["raw"]})
        if result and result[0]["result"]:
            print("Duplicate skipped:", msg["raw"])
            return
    except Exception as e:
        print("Deduplication check error:", e)

    # Build document
    doc = {
        "raw": msg["raw"],
        "time": msg["time"],
        "prio": msg.get("prio"),
        "grip": msg.get("grip"),
        "capcode": msg.get("capcode", []),
        "message": msg.get("message"),
        "received_at": msg["received_at"]
    }

    # Insert
    try:
        db.insert(TABLE, doc)
    except Exception as e:
        print("Insert error:", e)

# -------------------------------
# RabbitMQ consumer
# -------------------------------
def consume():
    params = pika.URLParameters(RABBITMQ_URL)
    con = pika.BlockingConnection(params)
    ch = con.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL})

    def cb(ch, method, props, body):
        try:
            msg = json.loads(body)  # Already JSON from your producer
            insert_message(msg)
        except Exception as e:
            print("Parse/insert error:", e, body)
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=cb)
    print("Waiting for messages…")
    ch.start_consuming()

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    if not is_surrealdb_running():
        start_surrealdb()
    consume()
