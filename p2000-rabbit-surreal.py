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
SURREALDB_URL = "http://127.0.0.1:8000/sql"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"

DB_NS = "p2000"
DB_NAME = "p2000"
TABLE = "messages"

HEADERS = {"Content-Type": "text/plain"}

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

def parse_message(raw):
    fields = raw.split("|")
    if len(fields) < 6:
        return None

    return {
        "raw": raw,
        "time": fields[1],
        "message": fields[5],
        "prio": (m.group(1).upper().replace(" ", "") if (m := PRIO_RE.search(fields[5])) else None),
        "grip": (int(m.group(1)) if (m := GRIP_RE.search(fields[5])) else None),
        "capcode": fields[4].split()
    }

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
        "surreal", "start", "rocksdb:p2000.db",
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
    """Always send exactly one query, no semicolon."""
    return requests.post(
        SURREALDB_URL,
        headers=HEADERS,
        data=query,
        auth=(SURREALDB_USER, SURREALDB_PASS)
    )

def setup_db():
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
            print("DB error:", c, r.text)

# -------------------------------
# Insert messages
# -------------------------------
def insert_message(msg):
    msg["received_at"] = datetime.now(timezone.utc).isoformat()

    # Dedup—raw is UNIQUE so this is cheap
    check = surreal(f'USE NS {DB_NS}; USE DB {DB_NAME}; SELECT * FROM {TABLE} WHERE raw = "{msg["raw"]}"')
    try:
        if check.json()[0]["result"]:
            print("Duplicate skipped:", msg["raw"])
            return
    except:
        pass

    doc = json.dumps(msg)

    q = f"USE NS {DB_NS}; USE DB {DB_NAME}; INSERT INTO {TABLE} {doc}"
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

    ch.queue_declare(queue=QUEUE_NAME, durable=True,
                     arguments={'x-message-ttl': QUEUE_TTL})

    def cb(ch, method, props, body):
        raw = body.decode()
        msg = parse_message(raw)
        if msg:
            insert_message(msg)
        else:
            print("Parse error:", raw)
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
