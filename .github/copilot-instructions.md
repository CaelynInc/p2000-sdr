## Purpose

This file gives immediate, actionable context for AI coding agents working on the `p2000-sdr` repository so they can be productive without manual onboarding.

## Big Picture

- **Capture & parse**: `p2000-sdr.py` (console) and `p2000-sdr-db.py` (DB writer) run an external pipeline: `rtl_fm -f 169.65M ... | multimon-ng -a FLEX -t raw -`. These scripts read lines starting with `FLEX` and containing `ALN`, extract the human-readable message and a list of capcodes, then either print (console) or insert into SQLite (`p2000.db`).
- **Storage**: `p2000.db` (SQLite) contains table `p2000` with columns `id, timestamp, capcodes, message, raw`. See `init_db()` in `p2000-sdr-db.py` for schema and timeout behaviors.
- **Web UI**: `webapp.py` is a small Flask app exposing `/` (index), `/api/latest` (JSON endpoint polled every second by client JS), and `/message/<id>` for detail. Templates live in `templates/` (notably `index.html`, `message.html`, `layout.html`).
- **CLI tools**: `browser.py` and `latest.py` provide simple command-line browsing and retrieval of latest message.

## How to run (developer quickstart)

- Requirements (discovered in `README.md`): `python3`, `librtlsdr`, `multimon-ng`, and an RTL-SDR dongle (Linux instructions are in README). Many runtime commands assume Linux; on Windows prefer WSL or adapt `rtl_*` tools accordingly.
- Start console capture (no DB): `./p2000-sdr.py`
- Start capture + DB writer: `./p2000-sdr-db.py`
- Run web UI: `python webapp.py` (default binds `0.0.0.0:8080`, logging goes to `webapp.log`)

Notes: `p2000-sdr-db.py` and `p2000-sdr.py` spawn `rtl_fm` + `multimon-ng` via `subprocess.Popen(..., shell=True)` and write child stderr to `error.log`.

## Key patterns & conventions (repo-specific)

- Message parsing: both capture scripts only process lines that `startwith('FLEX')` and contain `'ALN'`. They use string-splitting around `ALN|` and slice indexes (`p2000[43:]`) to extract capcodes — do not change parsing without verifying on live data.
- Capcode list: `capcodes.dict` contains lines like `000120901 = SomeService` — loader logic reads `key, value = line.strip().split(' = ')` and will crash on malformed lines.
- Time format: timestamps use `time.strftime('%d/%m/%Y %H:%M:%S')` across scripts. When adding features, keep this exact format to avoid display/DB mismatches.
- DB concurrency: `sqlite3.connect(..., timeout=5)` and retry loops are used in `p2000-sdr-db.py` to tolerate brief locks. If adding concurrent writers, follow the same retry/timeouts.
- Classification logic duplication: service/severity classification exists in Python (`webapp.py`) and duplicated in client-side JS (`templates/index.html`). When changing rules, update both locations.

## Integration points & external dependencies

- `rtl_fm` and `multimon-ng` (native binaries) — parsing depends on their textual output format. Tests that mock the pipeline should replicate the `FLEX ... ALN|` line format.
- Flask `webapp.py` uses `requests` to call Nominatim for geocoding in `/message/<id>`; this is network-dependent and has a simple `try/except` fallback.
- JS polling: `templates/index.html` polls `/api/latest` every 1000 ms. Avoid heavy work on that endpoint; it returns up to 100 rows as JSON.

## Files to edit for common tasks (examples)

- Add/change capcode descriptions: edit `capcodes.dict` (format: `CODE = Description`).
- Change DB schema: edit `init_db()` in `p2000-sdr-db.py` and migrate existing DBs manually (no migration tool exists).
- Update UI: edit `templates/index.html` and `templates/layout.html`; client classification helpers are in `index.html` JS.
- Adjust logging/rotation: `webapp.py` configures `RotatingFileHandler` writing to `webapp.log`.

## Small code examples (copyable)

- Query used by web UI to show messages: `SELECT * FROM p2000 ORDER BY id DESC LIMIT 200`
- Insert done by DB writer (see `store_message`): `INSERT INTO p2000 (timestamp, capcodes, message, raw) VALUES (?, ?, ?, ?)`

## Debugging tips

- If nothing appears in the DB, check `error.log` for child process errors from `rtl_fm`/`multimon-ng`.
- If Flask pages are empty or slow, check `webapp.log` and the Nominatim network calls in `/message/<id>`.
- To reproduce parsing issues, capture a single raw `FLEX ... ALN|` line and run the parsing logic interactively — parsing relies on fixed offsets, so malformed lines commonly cause IndexError.

## What an AI assistant should do first

- Prefer reading `p2000-sdr-db.py` and `webapp.py` together to understand data flow (capture → DB → API → client). Use the code examples above when proposing changes.
- When editing classification rules, update both the server (`webapp.py`) and client JS (`templates/index.html`) and run the webapp to check visual results.
- Avoid changing the parsing offset logic without sample data; if necessary, add robust defensive parsing (IndexError handling) and include unit tests.

## Request for feedback

If anything here is unclear or you want more examples (unit tests, sample raw inputs, or a migration script), tell me which area to expand and I will update this file.
