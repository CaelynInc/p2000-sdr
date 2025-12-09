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
SURREALDB_URL = "http://127.0.0.1:8000"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"
DB_NS = "p2000"
DB_NAME = "p2000"
TABLE_NAME = "messages"

HEADERS = {"Content-Type": "application/surrealql"}

# -------------------------------
# RabbitMQ settings
# -------------------------------
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL = 300000  # 5 minutes in ms

# -------------------------------
# FLEX / P2000 Parsing helpers
# -------------------------------
PRIO_RE = re.compile(r"\b([AB]?1|[AB]?2|[AB]?3|PRIO ?[1235]|P ?[1235])\b", re.IGNORECASE)
GRIP_RE = re.compile(r"\bGRIP ?([1-4])\b", re.IGNORECASE)

def parse_message(raw):
    fields = raw.split("|")
    if len(fields) < 6:
        return None

    msg = {
        "raw": raw,
        "time": fields[1],
        "prio": None,
        "grip": None,
        "capcode": fields[4].split()
    }

    # prio
    m = PRIO_RE.search(fields[5])
    if m:
        msg["prio"] = m.group(1).upper().replace(" ", "")

    # GRIP
    m = GRIP_RE.search(fields[5])
    if m:
        msg["grip"] = int(m.group(1))

    return msg

# -------------------------------
# SurrealDB control
# -------------------------------
def is_surrealdb_running():
    try:
        r = requests.get(f"{SURREALDB_URL}/sql", auth=(SURREALDB_USER, SURREALDB_PASS))
        return r.status_code in [200, 400]
    except requests.exceptions.ConnectionError:
        return False

def start_surrealdb():
    print("Starting SurrealDB...")
    subprocess.Popen([
        "surreal", "start", "rocksdb:p2000-surreal.db",
        "--bind", "0.0.0.0:8000",  # allow external access
        "--user", SURREALDB_USER,
        "--pass", SURREALDB_PASS,
        "--log", "stdout"
    ])
    for _ in range(10):
        if is_surrealdb_running():
            print("SurrealDB is running.")
            return
        time.sleep(1)
    raise RuntimeError("Failed to start SurrealDB.")

def exec_query(query):
    try:
        resp = requests.post(
            f"{SURREALDB_URL}/sql",
            headers=HEADERS,
            data=query,
            auth=(SURREALDB_USER, SURREALDB_PASS)
        )
        if resp.status_code >= 400:
            print(f"[!] Warning: Could not execute query: {query}\nResponse: {resp.text}")
        return resp
    except Exception as e:
        print(f"[!] Error executing query: {query}\n{e}")
        return None

def setup_database():
    queries = [
        f"CREATE NS `{DB_NS}`;",
        f"USE NS `{DB_NS}` DB `{DB_NAME}`;",
        f"CREATE TABLE `{TABLE_NAME}`;",
        f"CREATE INDEX idx_time ON `{TABLE_NAME}` COLUMNS time;",
        f"CREATE INDEX idx_prio ON `{TABLE_NAME}` COLUMNS prio;",
        f"CREATE INDEX idx_grip ON `{TABLE_NAME}` COLUMNS grip;",
        f"CREATE INDEX idx_capcode ON `{TABLE_NAME}` COLUMNS capcode;",
        f"CREATE INDEX idx_raw ON `{TABLE_NAME}` COLUMNS raw;"
    ]
    for q in queries:
        exec_query(q)
    print("Database setup done.")

def insert_message(msg):
    # Add received timestamp (timezone-aware)
    msg['received_at'] = datetime.now(timezone.utc).isoformat()

    # Deduplication check
    check_query = f"USE NS `{DB_NS}` DB `{DB_NAME}`; SELECT * FROM `{TABLE_NAME}` WHERE raw = {json.dumps(msg['raw'])};"
    resp = exec_query(check_query)
    try:
        if resp and resp.json() and resp.json()[0].get('result'):
            print(f"[!] Duplicate message skipped: {msg['raw']}")
            return
    except Exception:
        pass

    # Insert
    insert_query = f"USE NS `{DB_NS}` DB `{DB_NAME}`; INSERT INTO `{TABLE_NAME}` CONTENT {json.dumps(msg)};"
    exec_query(insert_query)

# -------------------------------
# RabbitMQ consumption
# -------------------------------
def consume_rabbitmq():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declare queue with TTL
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={'x-message-ttl': QUEUE_TTL}
    )

    def callback(ch, method, properties, body):
        try:
            raw_msg = body.decode()
            parsed = parse_message(raw_msg)
            if parsed:
                insert_message(parsed)
            else:
                print(f"[!] Could not parse message: {raw_msg}")
        except Exception as e:
            print(f"[!] Error inserting message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    if not is_surrealdb_running():
        start_surrealdb()
    setup_database()
    consume_rabbitmq()
