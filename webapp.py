#!/usr/bin/env python3
from flask import Flask, render_template, g, jsonify
import sqlite3
import time
import logging
from logging.handlers import RotatingFileHandler
import re

DATABASE = "p2000.db"
app = Flask(__name__)

# ---------------------------
# Logging configuration
# ---------------------------
log_file = "webapp.log"
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)

app_logger = logging.getLogger("p2000_webapp")
app_logger.setLevel(logging.INFO)
app_logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(formatter)
app_logger.addHandler(console_handler)

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
# Jinja2 custom filters
# ---------------------------
@app.template_filter('regex_search')
def regex_search(value, pattern):
    """Return True if regex pattern matches value"""
    if value is None:
        return False
    return bool(re.search(pattern, value))

# ---------------------------
# Routes
# ---------------------------

# Home / Live page
@app.route("/")
def index():
    start = time.time()
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")
    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000  # milliseconds
    app_logger.info("Home page requested")
    return render_template("index.html", messages=messages, total=total, elapsed=elapsed)

# Single message detail
@app.route("/message/<int:msg_id>")
def message_detail(msg_id):
    msg = query_db("SELECT * FROM p2000 WHERE id=?", (msg_id,), one=True)
    if not msg:
        return "Message not found", 404

    # Determine service class (matches JS logic)
    text = msg["message"].upper()
    cap = msg["capcodes"].upper()
    if re.search(r'000120901|000923993|001420059|MMT|TRAUMAHELI', cap) or re.search(r'MMT|TRAUMAHELI', text):
        service_class = "lfl"
    elif re.search(r'00\d\d0\d{4}', cap) or "PRIO" in text:
        service_class = "fdp"
    elif re.search(r'00\d\d2\d{4}', cap) or re.match(r'^A[12]|^B[12]', text):
        service_class = "ems"
    elif re.search(r'00\d\d3\d{4}', cap) or "POLITIE" in text:
        service_class = "pdp"
    else:
        service_class = ""

    # Determine severity class
    if re.search(r'\b(A1|PRIO\s*1|P\s*1|P1)\b', text):
        severity_class = "sev-high"
    elif re.search(r'\b(A2|PRIO\s*2|P\s*2|P2)\b', text):
        severity_class = "sev-med"
    elif re.search(r'\b(B1|B2|PRIO\s*3|P\s*3|P3)\b', text):
        severity_class = "sev-low"
    else:
        severity_class = "sev-none"

    return render_template(
        "message.html",
        message=msg,
        service_class=service_class,
        severity_class=severity_class
    )


# API for latest messages (live feed)
@app.route("/api/latest")
def api_latest():
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")
    app_logger.info("API latest messages requested")
    return jsonify([dict(m) for m in messages])

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
