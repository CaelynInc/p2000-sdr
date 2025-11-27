#!/usr/bin/env python3
import sqlite3

def get_latest_message():
    conn = sqlite3.connect("p2000.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM p2000
        ORDER BY id DESC
        LIMIT 1
    """)

    row = cur.fetchone()
    conn.close()

    return row

def main():
    row = get_latest_message()

    if row is None:
        print("⚠ No messages in database yet.")
        return

    print("=== Latest P2000 Message ===")
    print(f"Timestamp: {row['timestamp']}")
    print(f"Capcodes : {row['capcodes']}")
    print(f"Message  : {row['message']}")
    print(f"Raw data : {row['raw']}")

if __name__ == "__main__":
    main()
