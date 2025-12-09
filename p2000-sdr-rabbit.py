#!/usr/bin/env python3
import subprocess
import pika
import sys
import time

# -----------------------------
# RabbitMQ connection settings
# -----------------------------
RABBITMQ_HOST = "vps.caelyn.nl"      # e.g. "localhost" or "192.168.1.5"
RABBITMQ_USER = "p2000"
RABBITMQ_PASS = "Pi2000"
RABBITMQ_QUEUE = "p2000"

# -----------------------------
# RTL-SDR / multimon-ng settings
# -----------------------------
FREQUENCY = "169.65M"


def start_pipeline():
    """
    Starts rtl_fm → multimon-ng and returns a pipe that outputs decoded FLEX lines.
    """
    rtl_cmd = [
        "rtl_fm",
        "-f", FREQUENCY,
        "-M", "fm",
        "-s", "22050",
        "-g", "42"
    ]

    # Add -q for quiet mode to suppress startup messages
    multi_cmd = [
        "multimon-ng",
        "-a", "FLEX",
        "-t", "raw",
        "-q",
        "-"
    ]

    # rtl_fm process
    rtl_proc = subprocess.Popen(
        rtl_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    # multimon-ng process (reads from rtl_fm)
    multi_proc = subprocess.Popen(
        multi_cmd,
        stdin=rtl_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )

    return multi_proc


def connect_rabbit():
    """
    Keeps trying to connect to RabbitMQ until successful.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    credentials=credentials
                )
            )

            channel = connection.channel()
            # Match the existing queue TTL to avoid PRECONDITION_FAILED
            channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={'x-message-ttl': 300000}  # 5 minutes in ms
            )

            print("[+] Connected to RabbitMQ")
            return connection, channel

        except Exception as e:
            print(f"[-] RabbitMQ unavailable ({e}), retrying...")
            time.sleep(5)


def main():
    connection, channel = connect_rabbit()
    decoder = start_pipeline()

    print("[+] Listening for P2000 messages...")

    for line in decoder.stdout:
        line = line.strip()
        if not line:
            continue

        # Suppress multimon-ng startup message
        if line.startswith("Enabled demodulators:"):
            continue

        # Example FLEX line:
        # FLEX: Address:123456 Function:1 Alpha:TEST MESSAGE HERE
        print("[RX]", line)

        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=line.encode("utf8")
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping...")
        sys.exit(0)
