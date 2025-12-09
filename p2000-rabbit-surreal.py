#!/usr/bin/env python3
import asyncio
import json
import re
from datetime import datetime, timezone

import pika
from surrealdb import Surreal

# -------------------------------
# SurrealDB settings
# -------------------------------
SURREALDB_URL = "http://127.0.0.1:8000"  # SurrealDB HTTP endpoint
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
async def connect_surreal():
    db = Surreal(SURREALDB_URL)
    await db.connect()
    await db.signin({"user": SURREALDB_USER, "pass": SURREALDB_PASS})
    await db.use(namespace=DB_NS, database=DB_NAME)
    return db

async def insert_message(db, msg):
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
        existing = await db.query(f'SELECT * FROM {TABLE} WHERE raw = $raw', {"raw": msg["raw"]})
        if existing and len(existing[0]["result"]) > 0:
            print("Duplicate skipped:", msg["raw"])
            return
    except Exception as e:
        print("Deduplication check failed:", e)

    # Build object for insertion
    doc = {
        "raw": msg["raw"],
        "time": msg["time"],
        "prio": msg.get("prio"),
        "grip": msg.get("grip"),
        "capcode": msg.get("capcode", []),
        "received_at": msg["received_at"],
        "message": msg.get("message")
    }

    try:
        await db.insert(TABLE, doc)
    except Exception as e:
        print("Insert failed:", e, doc)

# -------------------------------
# RabbitMQ consumer
# -------------------------------
def start_consumer(loop):
    params = pika.URLParameters(RABBITMQ_URL)
    con = pika.BlockingConnection(params)
    ch = con.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL})

    async def handle_message(body):
        msg = json.loads(body)
        await insert_message(loop.db, msg)

    def cb(ch, method, props, body):
        try:
            loop.run_until_complete(handle_message(body))
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
    loop = asyncio.get_event_loop()
    loop.db = loop.run_until_complete(connect_surreal())
    start_consumer(loop)
