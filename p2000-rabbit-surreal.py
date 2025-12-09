#!/usr/bin/env python3
import subprocess
import time
import json
import re
import pika
import requests
from datetime import datetime, timezone

# -----------------------------
# SurrealDB settings
# -----------------------------
SURREALDB_URL = "http://0.0.0.0:8000"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"
DB_NS = "p2000"
DB_NAME = "p2000"
TABLE_NAME = "messages"

HEADERS = {"Content-Type": "application/json"}

# -----------------------------
# RabbitMQ settings
# -----------------------------
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL_MS = 300000  # 5 minutes

# -----------------------------
# FLEX / P2000 Parsing
# -----------------------------
PRIO_REGEX = re.compile(r"\b(A|B|P|Prio|p)\s?([1-5])\b", re.IGNORECASE)
GRIP_REGEX = re.compile(r"\bGRIP\s?([1-4])\b", re.IGNORECASE)

def parse_flex_message(raw_msg):
    fields = raw_msg.split("|")
    parsed = {
        "raw": raw_msg,
        "service": fields[2] if len(fields) > 2 else None,
        "capcode": fields[4].split() if len(fields) > 4 else [],
        "time": fields[1] if len(fields) > 1 else None,
        "prio": None,
        "grip": None
    }

    # Extract prio from the last field
    if len(fields) > 5:
        prio_match = PRIO_REGEX.search(fields[5])
        if prio_match:
            parsed["prio"] = prio_match.group(1).upper() + prio_match.group(2)

        grip_match = GRIP_REGEX.search(fields[5])
        if grip_match:
            parsed["grip"] = int(grip_match.group(1))

    return parsed

# -----------------------------
# SurrealDB functions
# -----------------------------
def is_surrealdb_running():
    try:
        r = requests.get(f"{SURREALDB_URL}/sql", auth=(SURREALDB_USER, SURREALDB_PASS))
        return r.status_code in [200, 400]
    except requests.exceptions.ConnectionError:
        return False

def start_surrealdb():
    print("Starting SurrealDB...")
    subprocess.Popen([
        "surreal", "start", "rocksdb://./p2000-db",
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
    raise RuntimeError("Failed to start SurrealDB.")

def setup_database():
    try:
        queries = [
            f'USE NS {DB_NS} DB {DB_NAME};',
            f'CREATE TABLE IF NOT EXISTS {TABLE_NAME};',
            f'CREATE INDEX IF NOT EXISTS idx_time ON {TABLE_NAME} COLUMNS time;',
            f'CREATE INDEX IF NOT EXISTS idx_prio ON {TABLE_NAME} COLUMNS prio;',
            f'CREATE INDEX IF NOT EXISTS idx_grip ON {TABLE_NAME} COLUMNS grip;',
            f'CREATE INDEX IF NOT EXISTS idx_capcode ON {TABLE_NAME} COLUMNS capcode;',
            f'CREATE INDEX IF NOT EXISTS idx_raw ON {TABLE_NAME} COLUMNS raw;'
        ]
        for q in queries:
            resp = requests.post(f"{SURREALDB_URL}/sql", headers=HEADERS, data=json.dumps([q]),
                                 auth=(SURREALDB_USER, SURREALDB_PASS))
            if resp.status_code >= 400:
                print(f"Warning: Could not execute query: {q}\nResponse: {resp.text}")
        print("Database setup done.")
    except Exception as e:
        print("Warning: Could not setup database\n", e)

def insert_message(msg):
    parsed = parse_flex_message(msg['raw'])
    parsed['received_at'] = datetime.now(timezone.utc).isoformat()

    # Deduplication
    check_query = f'USE NS {DB_NS} DB {DB_NAME}; SELECT id FROM {TABLE_NAME} WHERE raw = {json.dumps(parsed["raw"])} LIMIT 1;'
    resp = requests.post(SURREALDB_URL, headers=HEADERS, data=json.dumps([check_query]),
                         auth=(SURREALDB_USER, SURREALDB_PASS))
    try:
        if resp.status_code < 400:
            result = resp.json()
            if result and len(result) > 0 and result[0].get('result'):
                print(f"[!] Duplicate message skipped: {parsed['raw']}")
                return
    except Exception:
        pass

    # Insert
    insert_query = f'USE NS {DB_NS} DB {DB_NAME}; INSERT INTO {TABLE_NAME} CONTENT {json.dumps(parsed)};'
    resp = requests.post(SURREALDB_URL, headers=HEADERS, data=json.dumps([insert_query]),
                         auth=(SURREALDB_USER, SURREALDB_PASS))
    if resp.status_code >= 400:
        print(f"[!] Error inserting message: {resp.text}")

# -----------------------------
# RabbitMQ consumption
# -----------------------------
def consume_rabbitmq():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Queue with TTL
    channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL_MS})

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body)
            insert_message(message)
        except json.JSONDecodeError:
            print("Invalid message format:", body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    if not is_surrealdb_running():
        start_surrealdb()
    setup_database()
    consume_rabbitmq()
