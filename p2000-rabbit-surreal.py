#!/usr/bin/env python3
import subprocess
import time
import json
import pika
import requests
from datetime import datetime

# SurrealDB settings
SURREALDB_URL = "http://0.0.0.0:8000"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"
DB_PATH = "rocksdb:./p2000-db"
TABLE_NAME = "messages"

# RabbitMQ settings
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL = 300000  # 5 minutes in ms

HEADERS = {"Content-Type": "application/surrealql"}


def is_surrealdb_running():
    try:
        r = requests.get(f"{SURREALDB_URL}/sql", auth=(SURREALDB_USER, SURREALDB_PASS))
        return r.status_code in [200, 400]
    except requests.exceptions.ConnectionError:
        return False


def start_surrealdb():
    print("Starting SurrealDB...")
    subprocess.Popen([
        "surreal", "start",
        "--user", SURREALDB_USER,
        "--pass", SURREALDB_PASS,
        "--bind", "0.0.0.0:8000",
        DB_PATH
    ])
    for _ in range(10):
        if is_surrealdb_running():
            print("SurrealDB is running.")
            return
        time.sleep(1)
    raise RuntimeError("Failed to start SurrealDB.")


def setup_database():
    queries = [
        f"CREATE TABLE {TABLE_NAME};",
        f"CREATE INDEX idx_time ON {TABLE_NAME} COLUMNS time;",
        f"CREATE INDEX idx_prio ON {TABLE_NAME} COLUMNS prio;",
        f"CREATE INDEX idx_grip ON {TABLE_NAME} COLUMNS grip;",
        f"CREATE INDEX idx_capcode ON {TABLE_NAME} COLUMNS capcode;",
        f"CREATE INDEX idx_raw ON {TABLE_NAME} COLUMNS raw;"
    ]
    for q in queries:
        resp = requests.post(f"{SURREALDB_URL}/sql", data=q, headers=HEADERS,
                             auth=(SURREALDB_USER, SURREALDB_PASS))
        if resp.status_code >= 400:
            print(f"Warning: Could not execute query: {q}\nResponse: {resp.text}")


def insert_message(msg):
    msg['received_at'] = datetime.utcnow().isoformat() + "Z"

    # Deduplication
    check_query = f"SELECT * FROM {TABLE_NAME} WHERE raw = {json.dumps(msg['raw'])};"
    resp = requests.post(f"{SURREALDB_URL}/sql", data=check_query, headers=HEADERS,
                         auth=(SURREALDB_USER, SURREALDB_PASS))
    if resp.status_code < 400 and resp.json() and resp.json()[0].get('result'):
        print(f"[!] Duplicate message skipped: {msg['raw']}")
        return

    insert_query = f"INSERT INTO {TABLE_NAME} CONTENT {json.dumps(msg)};"
    resp = requests.post(f"{SURREALDB_URL}/sql", data=insert_query, headers=HEADERS,
                         auth=(SURREALDB_USER, SURREALDB_PASS))
    if resp.status_code >= 400:
        print(f"Error inserting message: {resp.text}")


def consume_rabbitmq():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Ensure queue is declared with TTL to match existing queue
    channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-message-ttl': QUEUE_TTL})

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
