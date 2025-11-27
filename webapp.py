#!/usr/bin/env python3
from flask import Flask, render_template, request, g, jsonify
import sqlite3
import time

DATABASE = "p2000.db"

app = Flask(__name__)


# ---------------------------
# Database helpers
# ---------------------------
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def query_db(query, args=(), one=False):
    db = get_db()
    cur = db.execute(query, args)
    rows = cur.fetchall()
    cur.close()
    return (rows[0] if rows else None) if one else rows


# ---------------------------
# Main page: browse/search
# ---------------------------
@app.route("/")
def index():
    search = request.args.get("q", "").strip()
    start = time.time()

    if search:
        messages = query_db(
            """SELECT * FROM p2000
               WHERE message LIKE ? OR capcodes LIKE ?
               ORDER BY id DESC""",
            (f"%{search}%", f"%{search}%")
        )
    else:
        messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")

    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000  # in milliseconds

    # Pass total + elapsed to template
    return render_template(
        "index.html",
        messages=messages,
        total=total,
        elapsed=elapsed,
        search=search
    )



# ---------------------------
# Live API endpoint
# ---------------------------
@app.route("/api/latest")
def api_latest():
    messages = query_db(
        "SELECT * FROM p2000 ORDER BY id DESC LIMIT 25"
    )
    return jsonify([dict(m) for m in messages])


# ---------------------------
# Live page (auto-updating)
# ---------------------------
@app.route("/live")
def live():
    start = time.time()
    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000
    return render_template("live.html", total=total, elapsed=elapsed)



# ---------------------------
# Start server
# ---------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
