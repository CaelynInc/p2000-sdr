#!/usr/bin/env python3

import subprocess
import pika
import sys
import time

RABBITMQ_HOST = "vps.caelyn.nl"
RABBITMQ_QUEUE = "p2000"
FREQUENCY = "169.65M"

def start_pipeline():
    """
    Starts rtl_fm → multimon-ng and returns a pipe that outputs decoded lines.
    """
    rtl_cmd = [
        "rtl_fm",
        "-f", FREQUENCY,
        "-M", "fm",
        "-s", "22050",
        "-g", "42"
    ]

    multi_cmd = [
        "multimon-ng",
        "-a", "FLEX",
        "-t", "raw",
        "-"
    ]

    rtl_proc = subprocess.Popen(
        rtl_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    multi_proc = subprocess.Popen(
        multi_cmd,
        stdin=rtl_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )

    return multi_proc


def connect_rabbit():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            print("[+] Connected to RabbitMQ")
            return connection, channel
        except:
            print("[-] RabbitMQ unavailable, retrying...")
            time.sleep(5)


def main():
    connection, channel = connect_rabbit()
    decoder = start_pipeline()

    print("[+] Listening for P2000 messages...")

    for line in decoder.stdout:
        line = line.strip()
        if not line:
            continue

        # Example FLEX output: "FLEX: Address:123456 Function:1 Alpha:TEST MESSAGE"
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
