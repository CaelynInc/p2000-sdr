#!/usr/bin/env python3
from flask import Flask, render_template, request, g, jsonify
import sqlite3
import time
import logging
from logging.handlers import RotatingFileHandler
import re

DATABASE = "p2000.db"

# ---------------------------
# Flask app
# ---------------------------
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
# Classification helpers
# ---------------------------
def classify_service(msg):
    text = msg["message"].upper()
    cap = msg["capcodes"].upper()
    if re.search(r"000120901|000923993|001420059|MMT|TRAUMAHELI", cap) or re.search(r"MMT|TRAUMAHELI", text):
        return "lfl", "MMT/Traumaheli"
    if re.search(r"00\d\d0\d{4}", cap) or "PRIO" in text:
        return "fdp", "Fire Department"
    if re.search(r"00\d\d2\d{4}", cap) or re.match(r"^A[12]|^B[12]", text):
        return "ems", "Ambulance"
    if re.search(r"00\d\d3\d{4}", cap) or "POLITIE" in text:
        return "pdp", "Police"
    return "", "Unknown"

def classify_severity(msg):
    t = msg["message"].upper()
    if re.search(r"\b(A1|PRIO\s*1|P\s*1|P1)\b", t):
        return "sev-high"
    if re.search(r"\b(A2|PRIO\s*2|P\s*2|P2)\b", t):
        return "sev-med"
    if re.search(r"\b(B1|B2|PRIO\s*3|P\s*3|P3)\b", t):
        return "sev-low"
    return ""

# ---------------------------
# Routes
# ---------------------------
@app.route("/")
def index():
    start = time.time()
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")
    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000

    # Generate dynamic legend
    services_seen = set()
    severities_seen = set()
    for msg in messages:
        svc_class, svc_name = classify_service(msg)
        sev_class = classify_severity(msg)
        if svc_class: services_seen.add((svc_class, svc_name))
        if sev_class: severities_seen.add((sev_class, sev_class.replace("sev-", "").capitalize()))

    services_seen = sorted(services_seen, key=lambda x: x[0])
    severities_seen = sorted(severities_seen, key=lambda x: x[0])

    return render_template(
        "index.html",
        messages=messages,
        total=total,
        elapsed=elapsed,
        services_seen=services_seen,
        severities_seen=severities_seen
    )

@app.route("/message/<int:msg_id>")
def message_detail(msg_id):
    message = query_db("SELECT * FROM p2000 WHERE id=?", (msg_id,), one=True)
    if not message:
        return "Message not found", 404

    svc_class, svc_name = classify_service(message)
    sev_class = classify_severity(message)

    return render_template(
        "message.html",
        message=message,
        service_class=svc_class,
        service_name=svc_name,
        severity_class=sev_class
    )

@app.route("/api/latest")
def api_latest():
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")
    app_logger.info("Live API requested (last 200 messages)")
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
