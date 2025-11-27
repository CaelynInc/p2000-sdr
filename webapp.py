#!/usr/bin/env python3
from flask import Flask, render_template, request, g, jsonify
import sqlite3
import time
import logging
from logging.handlers import RotatingFileHandler

DATABASE = "p2000.db"

# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)

# ---------------------------
# Logging configuration
# ---------------------------
log_file = "webapp.log"

# Rotating file handler
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)

app_logger = logging.getLogger("p2000_webapp")
app_logger.setLevel(logging.INFO)
app_logger.addHandler(file_handler)

# Console handler for errors only
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(formatter)
app_logger.addHandler(console_handler)

# Flask internal logger also writes to the file
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

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
    app_logger.info(f"Executing DB query: {query} Args={args}")
    cur = db.execute(query, args)
    rows = cur.fetchall()
    cur.close()
    return (rows[0] if rows else None) if one else rows

# ---------------------------
# Routes
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
        app_logger.info(f"Home page requested with search: '{search}'")
    else:
        messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")
        app_logger.info("Home page requested without search")

    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000  # milliseconds

    return render_template(
        "index.html",
        messages=messages,
        total=total,
        elapsed=elapsed,
        search=search
    )

@app.route("/api/latest")
def api_latest():
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 25")
    app_logger.info("Live API requested (last 25 messages)")
    return jsonify([dict(m) for m in messages])

@app.route("/live")
def live():
    start = time.time()
    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000
    app_logger.info("Live page requested")
    return render_template("live.html", total=total, elapsed=elapsed)

# ---------------------------
# Run server
# ---------------------------
if __name__ == "__main__":
    try:
        app_logger.info("Starting Flask webapp on 0.0.0.0:8080")
        app.run(host="0.0.0.0", port=8080, debug=False)
    except Exception as e:
        app_logger.exception(f"Webapp failed to start: {e}")
        raise
