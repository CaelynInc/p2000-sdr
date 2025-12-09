#!/usr/bin/env python3
import subprocess
import time
import json
import pika
import requests
from datetime import datetime

# SurrealDB settings
SURREALDB_URL = "http://0.0.0.0:8000"  # Bind to all interfaces
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"
DB_NAME = "p2000"
TABLE_NAME = "messages"

# RabbitMQ settings
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"


def is_surrealdb_running():
    try:
        r = requests.get(f"{SURREALDB_URL}/sql", auth=(SURREALDB_USER, SURREALDB_PASS))
        return r.status_code in [200, 400]
    except requests.exceptions.ConnectionError:
        return False


def start_surrealdb():
    print("Starting SurrealDB...")
    subprocess.Popen([
        "surreal", "start", "p2000.db",
        "--bind", "0.0.0.0:8000",  # external access
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
    queries = [
        f"CREATE NS IF NOT EXISTS {DB_NAME};",
        f"USE NS {DB_NAME};",
        f"CREATE DB IF NOT EXISTS {DB_NAME};",
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME};",
        # Indexes for fast filtering
        f"DEFINE INDEX idx_time ON {TABLE_NAME} (time);",
        f"DEFINE INDEX idx_prio ON {TABLE_NAME} (prio);",
        f"DEFINE INDEX idx_grip ON {TABLE_NAME} (grip);",
        f"DEFINE INDEX idx_capcode ON {TABLE_NAME} (capcode);",
        f"DEFINE INDEX idx_raw ON {TABLE_NAME} (raw);"
    ]
    for q in queries:
        resp = requests.post(f"{SURREALDB_URL}/sql", data=q, auth=(SURREALDB_USER, SURREALDB_PASS))
        if resp.status_code >= 400:
            print(f"Warning: Could not execute query: {q}\nResponse: {resp.text}")


def insert_message(msg):
    # Add received timestamp
    msg['received_at'] = datetime.utcnow().isoformat() + "Z"

    # Deduplication: skip if same raw message exists
    check_query = f"SELECT * FROM {TABLE_NAME} WHERE raw = {json.dumps(msg['raw'])};"
    resp = requests.post(f"{SURREALDB_URL}/sql", data=check_query, auth=(SURREALDB_USER, SURREALDB_PASS))
    if resp.status_code < 400 and resp.json() and resp.json()[0]['result']:
        # Duplicate found, skip insertion
        print(f"[!] Duplicate message skipped: {msg['raw']}")
        return

    # Insert message
    insert_query = f"INSERT INTO {TABLE_NAME} CONTENT {json.dumps(msg)};"
    resp = requests.post(f"{SURREALDB_URL}/sql", data=insert_query, auth=(SURREALDB_USER, SURREALDB_PASS))
    if resp.status_code >= 400:
        print(f"Error inserting message: {resp.text}")


def consume_rabbitmq():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

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


if __name__ == "__main__":
    if not is_surrealdb_running():
        start_surrealdb()
    setup_database()
    consume_rabbitmq()
