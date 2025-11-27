#!/usr/bin/env python3
from flask import Flask, g, render_template, request, jsonify
import sqlite3

app = Flask(__name__)
DATABASE = 'p2000.db'

# --------------------------
# Database helpers
# --------------------------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# --------------------------
# Routes
# --------------------------

# Home page - live feed (last 50 messages)
@app.route("/")
def index():
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 50")
    return render_template("index.html", messages=messages)

# Live feed page (last 25 messages)
@app.route("/live")
def live():
    return render_template("live.html")

# API: latest 25 messages (for live feed)
@app.route("/api/latest")
def api_latest():
    messages = query_db("SELECT * FROM p2000 ORDER BY id DESC LIMIT 25")
    # Convert sqlite3.Row to dict
    result = [dict(m) for m in messages]
    return jsonify(result)

# Full database search
@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    messages = []
    if q:
        messages = query_db(
            """SELECT * FROM p2000
               WHERE message LIKE ? OR capcodes LIKE ?
               ORDER BY id DESC
               LIMIT 200""",
            (f"%{q}%", f"%{q}%")
        )
    return render_template("search.html", messages=messages, search=q)

# Single message view
@app.route("/message/<int:msg_id>")
def message_view(msg_id):
    m = query_db("SELECT * FROM p*_
