#!/usr/bin/env python3
import sqlite3
import os
import sys

DB_FILE = "p2000.db"

def connect():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def clear():
    os.system("clear" if os.name != "nt" else "cls")

def print_message(row):
    print("────────────────────────────────────────────")
    print(f"ID        : {row['id']}")
    print(f"Timestamp : {row['timestamp']}")
    print(f"Capcodes  : {row['capcodes']}")
    print(f"Message   : {row['message']}")
    print(f"Raw       : {row['raw']}")
    print("────────────────────────────────────────────\n")

def show_last_n(n):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM p2000 ORDER BY id DESC LIMIT ?", (n,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No messages found.")
        return

    for row in rows[::-1]:
        print_message(row)

def search_capcode(code):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM p2000 WHERE capcodes LIKE ? ORDER BY id DESC", (f"%{code}%",))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No messages found for that capcode.")
        return

    for row in rows[::-1]:
        print_message(row)

def search_text(query):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM p2000 WHERE message LIKE ? ORDER BY id DESC", (f"%{query}%",))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No messages match your query.")
        return

    for row in rows[::-1]:
        print_message(row)

def browse_all(page_size=20):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM p2000")
    total = cur.fetchone()[0]

    if total == 0:
        print("No messages in database.")
        conn.close()
        return

    page = 0

    while True:
        offset = page * page_size
        cur.execute("SELECT * FROM p2000 ORDER BY id DESC LIMIT ? OFFSET ?", (page_size, offset))
        rows = cur.fetchall()

        if not rows:
            print("End of list.")
            break

        clear()
        print(f"Showing page {page+1} (messages {offset+1}–{offset+len(rows)} of {total})\n")

        for row in rows[::-1]:
            print_message(row)

        print("[N]ext page | [P]revious page | [Q]uit")
        choice = input("> ").strip().lower()

        if choice == "n":
            page += 1
        elif choice == "p" and page > 0:
            page -= 1
        else:
            break

    conn.close()

def main_menu():
    while True:
        clear()
        print("P2000 Command-Line Browser")
        print("==========================\n")
        print("1) Show last 10 messages")
        print("2) Show last N messages")
        print("3) Search by capcode")
        print("4) Search by text")
        print("5) Browse all")
        print("6) Quit\n")

        choice = input("Choose an option: ").strip()

        if choice == "1":
            clear()
            show_last_n(10)
            input("Press ENTER to continue...")
        elif choice == "2":
            clear()
            n = int(input("How many messages? "))
            clear()
            show_last_n(n)
            input("Press ENTER to continue...")
        elif choice == "3":
            clear()
            code = input("Enter capcode: ").strip()
            clear()
            search_capcode(code)
            input("Press ENTER to continue...")
        elif choice == "4":
            clear()
            query = input("Enter search text: ").strip()
            clear()
            search_text(query)
            input("Press ENTER to continue...")
        elif choice == "5":
            clear()
            browse_all()
            input("Press ENTER to continue...")
        elif choice == "6":
            sys.exit(0)
        else:
            input("Invalid choice — press ENTER to try again...")

if __name__ == "__main__":
    main_menu()
