#!/usr/bin/env python3
import json
import re
import asyncio
from datetime import datetime, timezone
import pika
from surrealdb import Surreal

# -------------------------------
# SurrealDB settings
# -------------------------------
SURREALDB_URL = "http://127.0.0.1:8000"
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
async def setup_db(db: Surreal):
    """Setup namespace, database, table, indexes"""
    try:
        await db.query(f"DEFINE NAMESPACE {DB_NS};")
        await db.query(f"DEFINE DATABASE {DB_NAME};")
        await db.use(DB_NS, DB_NAME)
        await db.query(f"DEFINE TABLE {TABLE} SCHEMALESS;")
        await db.query(f"DEFINE INDEX idx_raw ON {TABLE} COLUMNS raw UNIQUE;")
        await db.query(f"DEFINE INDEX idx_time ON {TABLE} COLUMNS time;")
        await db.query(f"DEFINE INDEX idx_prio ON {TABLE} COLUMNS prio;")
        await db.query(f"DEFINE INDEX idx_capcode ON {TABLE} COLUMNS capcode;")
        print("Database setup completed.")
    except Exception as e:
        print("DB setup warning:", e)

async def insert_message(db: Surreal, msg: dict):
    """Insert a message into SurrealDB, with deduplication"""
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
        res = await db.query(f'SELECT * FROM {TABLE} WHERE raw = "{msg["raw"]}";')
        if res and res[0]["result"]:
            print("Duplicate skipped:", msg["raw"])
            return
    except Exception as e:
        print("Deduplication check failed:", e)

    # Ensure capcodes is a list
    capcodes = msg.get("capcode") or []
    doc = {
        "raw": msg["raw"],
        "time": msg["time"],
        "prio": msg.get("prio"),
        "grip": msg.get("grip"),
        "capcode": capcodes,
        "received_at": msg["received_at"],
    }

    try:
        await db.insert(TABLE, doc)
    except Exception as e:
        print("Insert error:", e, msg)

# -------------------------------
# RabbitMQ consumer
# -------------------------------
def consume(db: Surreal):
    params = pika.URLParameters(RABBITMQ_URL)
    con = pika.BlockingConnection(params)
    ch = con.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL})

    def cb(ch, method, props, body):
        try:
            msg = json.loads(body)
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
    db = Surreal(SURREALDB_URL, username=SURREALDB_USER, password=SURREALDB_PASS)
    await db.connect()
    await setup_db(db)
    consume(db)

if __name__ == "__main__":
    asyncio.run(main())
