#!/usr/bin/env python3
import subprocess
import time
import json
import pika
import requests
from datetime import datetime, timezone
import re

# -------------------------------
# SurrealDB settings
# -------------------------------
SURREALDB_URL = "http://0.0.0.0:8000/sql"
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
    try:
        r = requests.get(SURREALDB_URL, auth=(SURREALDB_USER, SURREALDB_PASS))
        return r.status_code in (200, 400)
    except:
        return False

def start_surrealdb():
    print("Starting SurrealDB...")
    subprocess.Popen([
        "surreal", "start", "rocksdb:pi2000-surreal.db",
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

def surreal(query):
    """Send SurrealQL as JSON (required for v1+)."""
    return requests.post(
        SURREALDB_URL,
        headers={"Content-Type": "application/json"},
        json={"query": query},
        auth=(SURREALDB_USER, SURREALDB_PASS)
    )

def setup_db():
    """Create namespace, database, table, and indexes if needed."""
    cmds = [
        f"DEFINE NAMESPACE {DB_NS}",
        f"DEFINE DATABASE {DB_NAME}",
        f"USE NS {DB_NS}",
        f"USE DB {DB_NAME}",
        f"DEFINE TABLE {TABLE} SCHEMALESS",
        f"DEFINE INDEX idx_raw ON TABLE {TABLE} COLUMNS raw UNIQUE",
        f"DEFINE INDEX idx_time ON TABLE {TABLE} COLUMNS time",
        f"DEFINE INDEX idx_prio ON TABLE {TABLE} COLUMNS prio",
        f"DEFINE INDEX idx_capcode ON TABLE {TABLE} COLUMNS capcode",
    ]
    for c in cmds:
        r = surreal(c)
        if r.status_code >= 400:
            # SurrealDB may return 400/415 if it already exists — safe to ignore
            print("DB setup warning:", c, r.text)

# -------------------------------
# Insert message
# -------------------------------
def insert_message(msg):
    msg["received_at"] = datetime.now(timezone.utc).isoformat()

    # Parse prio and grip if missing
    if not msg.get("prio") and msg.get("message"):
        m = PRIO_RE.search(msg["message"])
        msg["prio"] = m.group(1).upper().replace(" ", "") if m else None
    if not msg.get("grip") and msg.get("message"):
        m = GRIP_RE.search(msg["message"])
        msg["grip"] = int(m.group(1)) if m else None

    # Deduplicate by raw
    check = surreal(f'USE NS {DB_NS}; USE DB {DB_NAME}; SELECT * FROM {TABLE} WHERE raw = "{msg["raw"]}"')
    try:
        if check.json()[0]["result"]:
            print("Duplicate skipped:", msg["raw"])
            return
    except Exception:
        pass

    # Build SurrealQL object literal
    capcodes = ",".join(msg.get("capcode", []))
    grip_val = msg["grip"] if msg.get("grip") is not None else "NONE"
    prio_val = f'"{msg["prio"]}"' if msg.get("prio") else "NONE"
    doc = (
        f'{{raw: "{msg["raw"]}", time: "{msg["time"]}", '
        f'prio: {prio_val}, grip: {grip_val}, '
        f'capcode: ["{capcodes}"], received_at: "{msg["received_at"]}"}}'
    )

    q = f'USE NS {DB_NS}; USE DB {DB_NAME}; INSERT INTO {TABLE} {doc}'
    r = surreal(q)
    if r.status_code >= 400:
        print("Insert error:", r.text)

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
            msg = json.loads(body)  # Already JSON from producer
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
    setup_db()
    consume()
