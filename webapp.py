#!/usr/bin/env python3
from flask import Flask, render_template, request
import sqlite3
import time


DB_FILE = "p2000.db"

app = Flask(__name__)

def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@app.route("/")
def index():
    t0 = time.time()

    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 50")
    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]

    render_time = time.time() - t0

    return render_template("index.html",
                           messages=messages,
                           total=total,
                           render_time=render_time)


@app.route("/message/<int:msg_id>")
def message_detail(msg_id):
    msg = query_db("SELECT * FROM p2000 WHERE id=?", (msg_id,), one=True)
    return render_template("message.html", msg=msg)

@app.route("/search")
def search():
    t0 = time.time()
    term = request.args.get("q", "").strip()

    if term == "":
        return render_template("search.html",
                               results=None,
                               term="",
                               total=None,
                               render_time=None)

    results = query_db("""
        SELECT * FROM p2000
        WHERE message LIKE ? OR capcodes LIKE ?
        ORDER BY id DESC
        LIMIT 200
    """, (f"%{term}%", f"%{term}%"))

    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]

    render_time = time.time() - t0

    return render_template("search.html",
                           results=results,
                           term=term,
                           total=total,
                           render_time=render_time)


    results = query_db("""
        SELECT * FROM p2000 
        WHERE message LIKE ? OR capcodes LIKE ?
        ORDER BY id DESC
        LIMIT 200
    """, (f"%{term}%", f"%{term}%"))

    return render_template("search.html", results=results, term=term)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
