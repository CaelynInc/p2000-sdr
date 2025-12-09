#!/usr/bin/env python3
import subprocess
import time
import json
import pika
import requests
from datetime import datetime, timezone

# SurrealDB settings
SURREALDB_URL = "http://0.0.0.0:8000/sql"
SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"
DB_NAMESPACE = "p2000"
DB_NAME = "p2000"
DB_PATH = "rocksdb:./p2000-db"
TABLE_NAME = "messages"

# RabbitMQ settings
RABBITMQ_URL = "amqp://p2000:Pi2000@vps.caelyn.nl:5672/%2F"
QUEUE_NAME = "p2000"
QUEUE_TTL = 300000  # 5 minutes


def is_surrealdb_running():
    try:
        r = requests.get(f"{SURREALDB_URL}?statements=[]", auth=(SURREALDB_USER, SURREALDB_PASS))
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
    statements = [
        f"USE NS {DB_NAMESPACE} DB {DB_NAME};",
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME};",
        f"CREATE INDEX IF NOT EXISTS idx_time ON {TABLE_NAME} COLUMNS time;",
        f"CREATE INDEX IF NOT EXISTS idx_prio ON {TABLE_NAME} COLUMNS prio;",
        f"CREATE INDEX IF NOT EXISTS idx_grip ON {TABLE_NAME} COLUMNS grip;",
        f"CREATE INDEX IF NOT EXISTS idx_capcode ON {TABLE_NAME} COLUMNS capcode;",
        f"CREATE INDEX IF NOT EXISTS idx_raw ON {TABLE_NAME} COLUMNS raw;"
    ]
    payload = {"statements": statements}
    resp = requests.post(SURREALDB_URL, auth=(SURREALDB_USER, SURREALDB_PASS),
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(payload))
    if resp.status_code >= 400:
        print(f"Warning: Could not setup database\nResponse: {resp.text}")


def insert_message(msg):
    msg['received_at'] = datetime.now(timezone.utc).isoformat()
    statements = [f"INSERT INTO {TABLE_NAME} CONTENT {json.dumps(msg)};"]
    payload = {"statements": statements}
    resp = requests.post(SURREALDB_URL, auth=(SURREALDB_USER, SURREALDB_PASS),
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(payload))
    if resp.status_code >= 400:
        print(f"Error inserting message: {resp.text}")


def consume_rabbitmq():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
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
