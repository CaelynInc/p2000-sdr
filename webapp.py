#!/usr/bin/env python3
from flask import Flask, render_template, g, jsonify
import sqlite3, time, re, logging, threading, requests
from logging.handlers import RotatingFileHandler

DATABASE = "p2000.db"
app = Flask(__name__)

# ---------------------------
# Disable Flask's default request logging to console
# ---------------------------
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# ---------------------------
# Logging
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
        g.db = sqlite3.connect(DATABASE, timeout=5)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db:
        db.close()

def query_db(query, args=(), one=False):
    db = get_db()
    cur = db.execute(query, args)
    rows = cur.fetchall()
    cur.close()
    return (rows[0] if rows else None) if one else rows

# ---------------------------
# Service and severity classification
# ---------------------------
def classify_service(msg):
    text = (msg["message"] or "").upper()
    caps = (msg["capcodes"] or "").upper()
    if re.search(r'000120901|000923993|001420059|MMT|TRAUMAHELI', caps) or re.search(r'MMT|TRAUMAHELI', text):
        return "lfl", "MMT / Traumaheli"
    if re.search(r'00\d\d0\d{4}', caps) or re.search(r'PRIO', text):
        return "fdp", "Fire Department"
    if re.search(r'00\d\d2\d{4}', caps) or re.match(r'^A[12]|^B[12]', text):
        return "ems", "Ambulance"
    if re.search(r'00\d\d3\d{4}', caps) or re.search(r'POLITIE', text):
        return "pdp", "Police"
    return "", "Unknown"

def classify_severity(msg):
    text = (msg["message"] or "").upper()
    if re.search(r'\b(A1|PRIO\s*1|P\s*1|P1)\b', text):
        return "sev-high"
    elif re.search(r'\b(A2|PRIO\s*2|P\s*2|P2)\b', text):
        return "sev-med"
    elif re.search(r'\b(B1|B2|PRIO\s*3|P\s*3|P3)\b', text):
        return "sev-low"
    return ""

# ---------------------------
# Periodic cleanup
# ---------------------------
def cleanup_db(max_rows=100000):
    with app.app_context():
        try:
            db = get_db()
            db.execute("""
                DELETE FROM p2000
                WHERE id NOT IN (
                    SELECT id FROM p2000 ORDER BY id DESC LIMIT ?
                )
            """, (max_rows,))
            db.commit()
            app_logger.info(f"Database cleanup completed, keeping last {max_rows} messages")
        except sqlite3.OperationalError as e:
            app_logger.error(f"Database cleanup failed: {e}")

def cleanup_worker():
    while True:
        cleanup_db()
        time.sleep(300)  # every 5 minutes

threading.Thread(target=cleanup_worker, daemon=True).start()

# ---------------------------
# Routes
# ---------------------------
@app.route("/")
def index():
    start = time.time()
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 200")
    total = query_db("SELECT COUNT(*) AS c FROM p2000", one=True)["c"]
    elapsed = (time.time() - start) * 1000
    return render_template("index.html", messages=messages, total=total, elapsed=elapsed)

@app.route("/api/latest")
def api_latest():
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 100")
    return jsonify([dict(m) for m in messages])

@app.route("/message/<int:msg_id>")
def message_page(msg_id):
    msg = query_db("SELECT * FROM p2000 WHERE id=?", (msg_id,), one=True)
    if not msg:
        return "Message not found", 404

    service_class, service_name = classify_service(msg)
    severity_class = classify_severity(msg)

    # ---------------------------
    # Attempt to extract address and geocode
    # ---------------------------
    address = None
    lat = lon = None
    match = re.search(r'\b([A-Z][a-zA-Z\s]+ \d{1,4}[a-zA-Z]?)\b', msg["message"])
    if match:
        address_candidate = match.group(1)
        try:
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": address_candidate, "format": "json", "limit": 1},
                headers={"User-Agent": "p2000-webapp"}
            )
            data = resp.json()
            if data:
                address = data[0]["display_name"]
                lat = data[0]["lat"]
                lon = data[0]["lon"]
            else:
                address = f"No results for '{address_candidate}'"
        except Exception as e:
            address = f"Geocoding failed: {e}"
    else:
        address = "No address detected"

    return render_template(
        "message.html",
        message=msg,
        service_class=service_class,
        service_name=service_name,
        severity_class=severity_class,
        address=address,
        lat=lat,
        lon=lon
    )

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
