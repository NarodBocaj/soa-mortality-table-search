#!/usr/bin/env python3
"""
SOA Mortality Table Scraper

Usage:
  python3 scraper.py              # scrape first 10 valid tables (test mode)
  python3 scraper.py --full       # scrape all tables (3007+)
  python3 scraper.py --workers 20 # set concurrency
  python3 scraper.py --max-id 3500 # upper bound for ID scan
"""

import argparse
import json
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://mort.soa.org"
VIEW_URL = f"{BASE_URL}/ViewTable.aspx?TableIdentity={{id}}"
MAX_DURATIONS = 50   # raised from 25; some tables have 30–46 year select periods
DB_PATH = Path("mortality.db")
JSON_PATH = Path("data.json")
TOLERANCE = 1e-6


def make_session(pool_size: int = 25):
    s = requests.Session()
    s.headers.update({"User-Agent": "MortalityTableResearch/1.0 (academic)"})
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def setup_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tables (
            table_identity  INTEGER PRIMARY KEY,
            name            TEXT,
            content_type    TEXT,
            nation          TEXT,
            gender          TEXT,
            risk_class      TEXT,
            select_period   INTEGER,
            min_age         INTEGER,
            max_age         INTEGER,
            link            TEXT,
            ultimate_json   TEXT
        )
    """)
    dur_cols = ",\n            ".join(f"d{i} REAL" for i in range(1, MAX_DURATIONS + 1))
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS rates (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            table_identity  INTEGER NOT NULL,
            issue_age       INTEGER NOT NULL,
            {dur_cols},
            FOREIGN KEY (table_identity) REFERENCES tables(table_identity)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rates_table ON rates(table_identity)
    """)
    conn.commit()
    return conn


def already_scraped(conn: sqlite3.Connection, table_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM tables WHERE table_identity = ?", (table_id,)
    ).fetchone()
    return row is not None


def insert_table(conn: sqlite3.Connection, meta: dict, rate_rows: list):
    ultimate_json = json.dumps(meta.get("ultimate", {}), separators=(",", ":")) or None
    conn.execute(
        """INSERT OR REPLACE INTO tables
           (table_identity, name, content_type, nation, gender, risk_class,
            select_period, min_age, max_age, link, ultimate_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            meta["table_identity"], meta["name"], meta["content_type"],
            meta["nation"], meta["gender"], meta["risk_class"],
            meta["select_period"], meta["min_age"], meta["max_age"],
            meta["link"], ultimate_json,
        ),
    )
    if rate_rows:
        conn.execute(
            "DELETE FROM rates WHERE table_identity = ?", (meta["table_identity"],)
        )
        dur_col_names = ", ".join(f"d{i}" for i in range(1, MAX_DURATIONS + 1))
        placeholders = ", ".join(["?"] * (MAX_DURATIONS + 2))
        conn.executemany(
            f"INSERT INTO rates (table_identity, issue_age, {dur_col_names}) VALUES ({placeholders})",
            [
                (meta["table_identity"], r["age"]) + tuple(r["rates"])
                for r in rate_rows
            ],
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def extract_gender(name: str, description: str) -> str:
    text = (name + " " + description).lower()
    # Check combined/unisex signals first so "male and female combined" → Combined
    if ("male and female" in text or "combined" in text or "unisex" in text
            or "both sexes" in text or "aggregate" in text):
        return "Combined"
    if "female" in text or "women" in text or "woman" in text:
        return "Female"
    if "male" in text or " men " in text:
        return "Male"
    return "Combined"


def extract_risk_class(name: str) -> str:
    n = name.lower()
    parts = []
    if "ultra" in n or "super" in n:
        parts.append("Ultra")
    if "preferred" in n:
        parts.append("Preferred")
    elif "standard" in n:
        parts.append("Standard")
    if "non-smoker" in n or "nonsmoker" in n or "non smoker" in n:
        parts.append("Non-Smoker")
    elif "smoker" in n:
        parts.append("Smoker")
    return ", ".join(parts) if parts else "Unknown"


def parse_page(html: str, table_id: int):
    """Return (meta_dict, rate_rows) or (None, []) if invalid."""
    soup = BeautifulSoup(html, "html.parser")
    all_tables = soup.find_all("table")

    if len(all_tables) < 2:
        return None, []

    # ---- Global metadata from Table 0 ----
    meta_rows = {}
    for row in all_tables[0].find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            meta_rows[cells[0].get_text(strip=True)] = cells[1].get_text(strip=True)

    if "Table Identity" not in meta_rows:
        return None, []

    name = meta_rows.get("Table Name", "")
    content_type = meta_rows.get("Content Type", "")

    # ---- Find all data tables (Row\Column header) and their descriptions ----
    data_tables = []  # list of (sub_description, sub_nation, table_element)
    for i, tbl in enumerate(all_tables):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        first_cells = [c.get_text(strip=True) for c in rows[0].find_all(["td", "th"])]
        if not first_cells or first_cells[0] != "Row\\Column":
            continue

        sub_desc = ""
        sub_nation = "United States of America"
        if i > 0:
            for prev_row in all_tables[i - 1].find_all("tr"):
                pcells = prev_row.find_all(["td", "th"])
                if len(pcells) >= 2:
                    k = pcells[0].get_text(strip=True)
                    v = pcells[1].get_text(strip=True)
                    if k == "Table Description":
                        sub_desc = v
                    elif k == "Nation":
                        sub_nation = v

        data_tables.append((sub_desc, sub_nation, tbl))

    if not data_tables:
        return None, []

    # ---- Separate select vs ultimate sub-tables ----
    # Two description formats exist on the SOA site:
    #   new: "..., Select" / "..., Ultimate"  (ends with the word)
    #   old: "...Minimum Select Age: X..." / "...Minimum Ultimate Age: X..."
    # We cannot use a simple "Ultimate" in desc check because table names like
    # "Select and Ultimate Table" cause both sub-tables to match.
    def is_ultimate(desc):
        stripped = desc.strip()
        last_word = stripped.rsplit(None, 1)[-1] if stripped else ""
        return last_word == "Ultimate" or "Ultimate Age:" in desc

    select_parts = [(d, n, t) for d, n, t in data_tables if not is_ultimate(d)]
    ult_parts = [(d, n, t) for d, n, t in data_tables if is_ultimate(d)]

    # Pure ultimate table → include it (user wants to see it)
    parts_to_parse = select_parts if select_parts else ult_parts

    rate_rows = []
    max_durations = 0
    nation = "United States of America"

    for sub_desc, sub_nation, tbl in parts_to_parse:
        nation = sub_nation
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue

        header = [c.get_text(strip=True) for c in rows[0].find_all(["td", "th"])]
        n_dur = len(header) - 1  # exclude the 'Row\Column' label cell
        if n_dur > max_durations:
            max_durations = n_dur

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            try:
                age = int(cells[0].get_text(strip=True))
            except ValueError:
                continue

            rates = []
            for j in range(1, n_dur + 1):
                try:
                    rates.append(float(cells[j].get_text(strip=True)))
                except (ValueError, IndexError):
                    rates.append(None)

            # Clamp to MAX_DURATIONS then pad any remainder with None
            rates = rates[:MAX_DURATIONS]
            padded = rates + [None] * (MAX_DURATIONS - len(rates))
            rate_rows.append({"age": age, "rates": padded})

    if not rate_rows:
        return None, []

    # ---- Collect ultimate rates (attained_age → rate) for select+ultimate tables ----
    ultimate = {}
    if select_parts and ult_parts:
        for _, _, ult_tbl in ult_parts:
            ult_rows = ult_tbl.find_all("tr")
            for row in ult_rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                try:
                    att_age = int(cells[0].get_text(strip=True))
                    rate = float(cells[1].get_text(strip=True))
                    ultimate[att_age] = rate
                except ValueError:
                    continue

    ages = [r["age"] for r in rate_rows]
    meta = {
        "table_identity": table_id,
        "name": name,
        "content_type": content_type,
        "nation": nation,
        "gender": extract_gender(name, meta_rows.get("Table Description", "")),
        "risk_class": extract_risk_class(name),
        "select_period": max_durations,
        "min_age": min(ages),
        "max_age": max(ages),
        "link": f"{BASE_URL}/ViewTable.aspx?TableIdentity={table_id}",
        "ultimate": ultimate,
    }
    return meta, rate_rows


# ---------------------------------------------------------------------------
# Fetching with retries
# ---------------------------------------------------------------------------

def fetch_and_parse(table_id: int, session: requests.Session, retries: int = 3):
    url = VIEW_URL.format(id=table_id)
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                return None, []
            if resp.status_code == 500:
                time.sleep(2 ** attempt)
                continue
            meta, rates = parse_page(resp.text, table_id)
            return meta, rates
        except requests.RequestException as exc:
            log.warning("ID %d attempt %d failed: %s", table_id, attempt + 1, exc)
            time.sleep(2 ** attempt)
    return None, []


# ---------------------------------------------------------------------------
# JSON export
# ---------------------------------------------------------------------------

def export_json(conn: sqlite3.Connection, json_path: Path):
    log.info("Exporting to %s …", json_path)
    tables = []
    for row in conn.execute(
        "SELECT table_identity, name, content_type, nation, gender, risk_class, select_period, min_age, max_age, link, ultimate_json FROM tables ORDER BY table_identity"
    ):
        entry = {
            "id": row[0], "name": row[1], "content_type": row[2],
            "nation": row[3], "gender": row[4], "risk_class": row[5],
            "select_period": row[6], "min_age": row[7], "max_age": row[8],
            "link": row[9],
        }
        if row[10]:
            entry["ultimate"] = json.loads(row[10])
        tables.append(entry)

    dur_cols = ", ".join(f"d{i}" for i in range(1, MAX_DURATIONS + 1))
    rate_rows = []
    for row in conn.execute(
        f"SELECT table_identity, issue_age, {dur_cols} FROM rates ORDER BY table_identity, issue_age"
    ):
        tid, age = row[0], row[1]
        rates = list(row[2:])
        # Trim trailing Nones to keep JSON compact
        while rates and rates[-1] is None:
            rates.pop()
        rate_rows.append({"tid": tid, "age": age, "d": rates})

    data = {
        "meta": {"total_tables": len(tables), "tolerance": TOLERANCE},
        "tables": tables,
        "rates": rate_rows,
    }
    with open(json_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    # data.js — works with file:// protocol (no server needed)
    js_path = json_path.with_suffix(".js")
    with open(js_path, "w") as f:
        f.write("window.MORTALITY_DATA=")
        json.dump(data, f, separators=(",", ":"))
        f.write(";")

    log.info("Exported %d tables, %d rate rows → %s (%.1f MB) + %s",
             len(tables), len(rate_rows), json_path,
             json_path.stat().st_size / 1e6, js_path.name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape SOA mortality tables")
    parser.add_argument("--full", action="store_true",
                        help="Scrape all tables (default: first 10 only)")
    parser.add_argument("--limit", type=int, default=10,
                        help="Stop after N successfully scraped tables (ignored with --full)")
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of parallel HTTP workers")
    parser.add_argument("--max-id", type=int, default=3500,
                        help="Upper bound for table ID scan")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="Per-worker delay between requests (seconds)")
    args = parser.parse_args()

    limit = None if args.full else args.limit
    conn = setup_db(DB_PATH)
    session = make_session(pool_size=args.workers)

    log.info("Mode: %s | Workers: %d | Max ID: %d",
             "FULL" if args.full else f"TEST (first {limit})",
             args.workers, args.max_id)

    ids_to_try = range(1, args.max_id + 1)
    scraped_count = 0
    errors = 0

    # Pre-fetch already-scraped IDs so workers don't touch the DB
    scraped_ids = set(
        row[0] for row in conn.execute("SELECT table_identity FROM tables")
    )

    def worker(table_id):
        if table_id in scraped_ids:
            return table_id, None, None, "skip"
        time.sleep(args.delay)
        meta, rates = fetch_and_parse(table_id, session)
        return table_id, meta, rates, "ok"

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, tid): tid for tid in ids_to_try}
        pending = list(futures.keys())
        for fut in as_completed(pending):
            if limit is not None and scraped_count >= limit:
                for f in pending:
                    f.cancel()
                break
            table_id, meta, rates, status = fut.result()
            if status == "skip":
                scraped_count += 1
                continue
            if meta is None:
                errors += 1
                continue
            insert_table(conn, meta, rates)
            scraped_count += 1
            log.info("[%d] %s | gender=%s | ages=%s–%s | durations=%d | rates=%d",
                     table_id, meta["name"][:60], meta["gender"],
                     meta["min_age"], meta["max_age"],
                     meta["select_period"], len(rates))

    log.info("Done. %d tables scraped, %d IDs skipped/failed.", scraped_count, errors)
    export_json(conn, JSON_PATH)
    conn.close()


if __name__ == "__main__":
    main()
