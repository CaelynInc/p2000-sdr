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
# SurrealDB connection
# -------------------------------
async def connect_surreal():
    """Connect to SurrealDB v2 HTTP server with async auth."""
    db = Surreal(SURREALDB_URL)       # No auth in constructor
    await db.connect()                 # Connect to server
    await db.signin({"user": SURREALDB_USER, "pass": SURREALDB_PASS})  # Authenticate
    await db.use(namespace=DB_NS, database=DB_NAME)
    return db

# -------------------------------
# Insert message
# -------------------------------
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
        res = await db.select(TABLE, where={"raw": msg["raw"]})
        if res:  # Duplicate exists
            print("Duplicate skipped:", msg["raw"])
            return
    except Exception:
        pass

    # Build document
    doc = {
        "raw": msg["raw"],
        "time": msg["time"],
        "prio": msg.get("prio"),
        "grip": msg.get("grip"),
        "capcode": msg.get("capcode", []),
        "received_at": msg["received_at"]
    }

    try:
        await db.insert(TABLE, doc)
    except Exception as e:
        print("Insert error:", e, doc)

# -------------------------------
# RabbitMQ consumer
# -------------------------------
def consume(db):
    params = pika.URLParameters(RABBITMQ_URL)
    con = pika.BlockingConnection(params)
    ch = con.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL})

    def cb(ch, method, props, body):
        try:
            msg = json.loads(body)  # Already JSON from producer
            asyncio.run(insert_message(db, msg))
        except Exception as e:
            print("Parse/insert error:", e, body)
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=cb)
    print("Waiting for messages…")
    ch.start_consuming()

# -------------------------------
# Main
# -------------------------------
async def main():
    db = await connect_surreal()
    consume(db)

if __name__ == "__main__":
    asyncio.run(main())
