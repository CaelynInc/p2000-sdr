#!/usr/bin/env python3
import sys
import os
import time
import re
import sqlite3
from sqlite3 import Connection
from subprocess import Popen, PIPE

os.system('clear')

def coloriz(capcode):
    lfl = r'000120901|000923993|001420059|MMT|Traumaheli'
    fdp = r'00[0-9][0-9]0[0-9]{4}|^[Pp]\s?[12]|.*[Pp][Rr][Ii][Oo].*'
    ems = r'00[0-9][0-9]2[0-9]{4}|^A[12]|^B[12]'
    pdp = r'00[0-9][0-9]3[0-9]{4}|.*[Pp][Oo][Ll][Ii][Tt][Ii][Ee].*'

    if re.match(lfl, capcode):
        color = '\033[92m'
    elif re.match(fdp, capcode):
        color = '\033[91m'
    elif re.match(ems, capcode):
        color = '\033[93m'
    elif re.match(pdp, capcode):
        color = '\033[94m'
    else:
        color = '\033[0m'

    return color

def init_db():
    """Create database + table if missing."""
    conn = sqlite3.connect("p2000.db")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS p2000 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            capcodes TEXT,
            message TEXT,
            raw TEXT
        )
    """)
    conn.commit()
    return conn

def store_message(conn: Connection, timestamp, capcodes, message, raw):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO p2000 (timestamp, capcodes, message, raw) VALUES (?, ?, ?, ?)",
        (timestamp, " ".join(capcodes), message, raw)
    )
    conn.commit()

def main():
    command = 'rtl_fm -f 169.65M -M fm -s 22050 -g 42 | multimon-ng -a FLEX -t raw -'
    sdr = Popen(command, stdout=PIPE, stderr=open('error.log', 'a'), shell=True)
    caplist = {}

    # Load capcodes list
    with open('capcodes.dict', 'r') as fdh:
        for line in fdh:
            key, value = line.strip().split(' = ')
            caplist[key] = value

    # Initialize SQLite
    conn = init_db()

    try:
        while True:
            p2000 = sdr.stdout.readline().decode('utf-8')
            sdr.poll()

            if 'ALN' in p2000 and p2000.startswith('FLEX'):
                try:
                    message = p2000.strip().split('ALN|')[1]
                except IndexError:
                    continue  # skip corrupted messages

                capcodes = p2000[43:].split('|ALN|')[0].split()
                date = time.strftime('%d/%m/%Y %H:%M:%S')

                # Print output (same as before)
                print(f'\n\033[0mMelding van: {date}\a')
                print(f'{coloriz(message)}{message}\033[0m')

                for capcode in capcodes:
                    capdesc = caplist.get(capcode, 'Onbekende of persoonlijke capcode')
                    print(f'{coloriz(capcode)}[{capcode}]: {capdesc}')

                # Store to SQLite
                store_message(conn, date, capcodes, message, p2000.strip())

    except KeyboardInterrupt:
        os.kill(sdr.pid, 9)
        sys.exit(0)

if __name__ == "__main__":
    main()
