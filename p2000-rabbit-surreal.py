#!/usr/bin/env python3
import subprocess
import time
import json
import pika
import requests
from datetime import datetime, timezone

# -------------------------------
# SurrealDB settings
# -------------------------------
SURREALDB_URL = "http://0.0.0.0:8000/sql"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"

DB_NS = "p2000"
DB_NAME = "p2000"
TABLE = "messages"

HEADERS = {"Content-Type": "application/json"}

# -------------------------------
# RabbitMQ settings
# -------------------------------
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL = 300000  # 5 minutes in ms

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
    """Send a query string to SurrealDB via JSON POST."""
    payload = json.dumps({"query": query})
    return requests.post(
        SURREALDB_URL,
        headers=HEADERS,
        data=payload,
        auth=(SURREALDB_USER, SURREALDB_PASS)
    )

def dict_to_surreal(d):
    """Convert Python dict to SurrealDB object literal for INSERT."""
    parts = []
    for k, v in d.items():
        if isinstance(v, str):
            v = v.replace('"', '\\"')  # escape quotes
            parts.append(f'{k}: "{v}"')
        elif isinstance(v, list):
            list_val = "[" + ", ".join(f'"{x}"' if isinstance(x, str) else str(x) for x in v) + "]"
            parts.append(f'{k}: {list_val}')
        elif v is None:
            parts.append(f'{k}: NONE')
        else:
            parts.append(f'{k}: {v}')
    return "{" + ", ".join(parts) + "}"

def setup_db():
    # Define namespace, database, table, indexes
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

    # Deduplication via raw
    check = surreal(f'USE NS {DB_NS}; USE DB {DB_NAME}; SELECT * FROM {TABLE} WHERE raw = "{msg["raw"]}"')
    try:
        if check.json()[0]["result"]:
            print("Duplicate skipped:", msg["raw"])
            return
    except:
        pass

    doc_str = dict_to_surreal(msg)
    q = f'USE NS {DB_NS}; USE DB {DB_NAME}; INSERT INTO {TABLE} {doc_str}'
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
            msg = json.loads(body)
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
