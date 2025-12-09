#!/usr/bin/env python3
import asyncio
import json
import re
from datetime import datetime, timezone

import aio_pika  # Async RabbitMQ client
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
    """Connect to SurrealDB v2 async HTTP server."""
    db = Surreal(SURREALDB_URL)      # <- just the URL
    await db.connect()                # Connect to server
    await db.signin({"user": SURREALDB_USER, "pass": SURREALDB_PASS})
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
# Async RabbitMQ consumer
# -------------------------------
async def consume(db):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL})
        queue = await channel.get_queue(QUEUE_NAME)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        msg = json.loads(message.body)
                        await insert_message(db, msg)
                    except Exception as e:
                        print("Parse/insert error:", e, message.body)

# -------------------------------
# Main
# -------------------------------
async def main():
    db = await connect_surreal()
    await consume(db)

if __name__ == "__main__":
    asyncio.run(main())
