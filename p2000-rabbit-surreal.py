#!/usr/bin/env python3
import json
import time
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

HEADERS = {"Content-Type": "text/plain"}

# -------------------------------
# RabbitMQ settings
# -------------------------------
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL = 300000  # 5 minutes in ms

# -------------------------------
# FLEX / P2000 parsing regex
# -------------------------------
PRIO_RE = re.compile(r"\b([AB]?1|[AB]?2|[AB]?3|PRIO ?[1235]|P ?[1235])\b", re.IGNORECASE)
GRIP_RE = re.compile(r"\bGRIP ?([1-4])\b", re.IGNORECASE)

# -------------------------------
# SurrealDB helper
# -------------------------------
def surreal(query):
    """Send a single SurrealQL statement as text/plain"""
    r = requests.post(
        SURREALDB_URL,
        headers=HEADERS,
        data=query,
        auth=(SURREALDB_USER, SURREALDB_PASS)
    )
    if r.status_code >= 400:
        print("SurrealDB error:", query, r.text)
    return r

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
    surreal(f'USE NS {DB_NS}')
    surreal(f'USE DB {DB_NAME}')
    check = surreal(f'SELECT * FROM {TABLE} WHERE raw = "{msg["raw"]}"')
    try:
        if check.json()[0]["result"]:
            print("Duplicate skipped:", msg["raw"])
            return
    except Exception:
        pass

    # Prepare SurrealQL object literal
    capcodes = msg.get("capcode", [])
    capcodes_literal = "[" + ",".join(f'"{c}"' for c in capcodes) + "]"
    prio_val = f'"{msg["prio"]}"' if msg.get("prio") else "NONE"
    grip_val = msg["grip"] if msg.get("grip") is not None else "NONE"

    doc = (
        f'{{raw: "{msg["raw"]}", time: "{msg["time"]}", '
        f'prio: {prio_val}, grip: {grip_val}, '
        f'capcode: {capcodes_literal}, received_at: "{msg["received_at"]}"}}'
    )

    surreal(f'INSERT INTO {TABLE} {doc}')

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
            msg = json.loads(body)  # messages are JSON from producer
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
    consume()
