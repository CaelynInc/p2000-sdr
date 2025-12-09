#!/usr/bin/env python3
import pika, json, subprocess, time, requests, os, signal
from datetime import datetime, timezone

SURREALDB_PATH = "/usr/local/bin/surreal"
SURREALDB_DIR  = "./p2000-db"

SURREALDB_USER = "p2000"
SURREALDB_PASS = "Pi2000"

RABBITMQ_HOST = "localhost"
RABBITMQ_QUEUE = "p2000_messages"


# -------------------------
# Start SurrealDB locally
# -------------------------
def start_surrealdb():
    if not os.path.exists(SURREALDB_DIR):
        os.makedirs(SURREALDB_DIR)

    print("Starting SurrealDB...")
    return subprocess.Popen(
        [
            SURREALDB_PATH, "start",
            "--log", "debug",
            "--user", SURREALDB_USER,
            "--pass", SURREALDB_PASS,
            SURREALDB_DIR
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )


# -------------------------
# JSON-RPC client for SurrealDB
# -------------------------
def surreal(query):
    payload = {
        "id": "1",
        "method": "query",
        "params": [query]
    }
    return requests.post(
        "http://127.0.0.1:8000/rpc",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        auth=(SURREALDB_USER, SURREALDB_PASS)
    )


# -------------------------
# Prepare DB (namespace, DB, table, indexes)
# -------------------------
def setup_db():
    query = """
        DEFINE NAMESPACE p2000;
        DEFINE DATABASE p2000;

        USE NS p2000;
        USE DB p2000;

        DEFINE TABLE messages SCHEMALESS;

        DEFINE INDEX idx_raw ON TABLE messages COLUMNS raw UNIQUE;
        DEFINE INDEX idx_time ON TABLE messages COLUMNS time;
        DEFINE INDEX idx_prio ON TABLE messages COLUMNS prio;
        DEFINE INDEX idx_capcode ON TABLE messages COLUMNS capcode;
        DEFINE INDEX idx_grip ON TABLE messages COLUMNS grip;
    """

    r = surreal(query)
    try:
        resp = r.json()
        if "error" in resp:
            print("DB error:", json.dumps(resp, indent=2))
        else:
            print("Database setup complete.")
    except:
        print("DB unexpected:", r.text)


# -------------------------
# Insert message into SurrealDB
# -------------------------
def insert_message(msg):
    msg["received_at"] = datetime.now(timezone.utc).isoformat()

    doc = json.dumps(msg)

    query = f"""
        USE NS p2000;
        USE DB p2000;
        INSERT INTO messages {doc};
    """

    r = surreal(query)

    try:
        resp = r.json()
        if "error" in resp:
            print("Insert error:", json.dumps(resp, indent=2))
    except:
        print("Insert error:", r.text)


# -------------------------
# Parse P2000 message
# -------------------------
def parse_p2000(raw):
    parts = raw.split("|")
    msg = {
        "raw": raw,
        "prio": None,
        "capcode": None,
        "grip": None,
        "text": None,
        "time": None
    }

    if len(parts) >= 4:
        try:
            msg["time"] = parts[1]
            msg["capcode"] = parts[2].split("/")[0]
            msg["prio"] = parts[2].split("/")[3]
            msg["grip"] = parts[3]
        except:
            pass

    if len(parts) >= 5:
        msg["text"] = parts[4]

    return msg


# -------------------------
# RabbitMQ consumer
# -------------------------
def consume_rabbitmq():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)

    print("Waiting for messages… To exit press CTRL+C")

    def callback(ch, method, properties, body):
        raw = body.decode("utf-8").strip()
        msg = parse_p2000(raw)
        insert_message(msg)

    channel.basic_consume(
        queue=RABBITMQ_QUEUE,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    proc = start_surrealdb()
    time.sleep(2)

    setup_db()

    try:
        consume_rabbitmq()
    except KeyboardInterrupt:
        print("Stopping…")
        proc.send_signal(signal.SIGINT)
        proc.wait()
