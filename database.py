"""SQLite database — upload-based.
Stores: segments, accounts, account-segment mappings, data imports, weekly metrics.
"""
import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from config import SQLITE_DB_PATH, DEFAULT_SEGMENTS

logger = logging.getLogger("ga4.db")


SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    slug TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    color TEXT,
    icon TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    property_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    parent_account TEXT,
    last_seen_in_import TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS account_segments (
    property_id TEXT NOT NULL,
    segment_slug TEXT NOT NULL,
    PRIMARY KEY (property_id, segment_slug),
    FOREIGN KEY (property_id) REFERENCES accounts(property_id) ON DELETE CASCADE,
    FOREIGN KEY (segment_slug) REFERENCES segments(slug) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
    rows_imported INTEGER DEFAULT 0,
    properties_count INTEGER DEFAULT 0,
    weeks_count INTEGER DEFAULT 0,
    min_week TEXT,
    max_week TEXT,
    notes TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS weekly_data (
    property_id TEXT NOT NULL,
    week_start TEXT NOT NULL,
    sessions INTEGER DEFAULT 0,
    conversions REAL DEFAULT 0,
    conv_rate REAL DEFAULT 0,
    import_id INTEGER,
    PRIMARY KEY (property_id, week_start),
    FOREIGN KEY (property_id) REFERENCES accounts(property_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_weekly_week ON weekly_data(week_start);
CREATE INDEX IF NOT EXISTS idx_weekly_prop_week ON weekly_data(property_id, week_start);
"""


@contextmanager
def conn():
    c = sqlite3.connect(SQLITE_DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    try:
        yield c
        c.commit()
    finally:
        c.close()


def init_db():
    with conn() as c:
        c.executescript(SCHEMA)
        for s in DEFAULT_SEGMENTS:
            c.execute(
                "INSERT OR IGNORE INTO segments (slug, name, color, icon) VALUES (?, ?, ?, ?)",
                (s["slug"], s["name"], s["color"], s["icon"]),
            )
    logger.info("DB initialised")


# ─────────────── Segments ───────────────

def list_segments() -> list[dict]:
    with conn() as c:
        rows = c.execute("""
            SELECT s.*, COUNT(DISTINCT acs.property_id) AS account_count
            FROM segments s
            LEFT JOIN account_segments acs ON acs.segment_slug = s.slug
            GROUP BY s.slug
            ORDER BY s.name
        """).fetchall()
        return [dict(r) for r in rows]


def add_segment(slug: str, name: str, color: str = "#64748b", icon: str = "📦"):
    with conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO segments (slug, name, color, icon) VALUES (?, ?, ?, ?)",
            (slug, name, color, icon),
        )


def delete_segment(slug: str):
    with conn() as c:
        c.execute("DELETE FROM segments WHERE slug = ?", (slug,))


def assign_segment(property_id: str, slug: str):
    with conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO account_segments (property_id, segment_slug) VALUES (?, ?)",
            (property_id, slug),
        )


def remove_segment(property_id: str, slug: str):
    with conn() as c:
        c.execute(
            "DELETE FROM account_segments WHERE property_id = ? AND segment_slug = ?",
            (property_id, slug),
        )


def accounts_in_segment(slug: str) -> list[str]:
    with conn() as c:
        rows = c.execute(
            "SELECT property_id FROM account_segments WHERE segment_slug = ?",
            (slug,),
        ).fetchall()
        return [r["property_id"] for r in rows]


# ─────────────── Accounts ───────────────

def list_accounts() -> list[dict]:
    with conn() as c:
        rows = c.execute("""
            SELECT a.*,
                   GROUP_CONCAT(acs.segment_slug) AS segments_str
            FROM accounts a
            LEFT JOIN account_segments acs ON acs.property_id = a.property_id
            GROUP BY a.property_id
            ORDER BY a.display_name
        """).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["segments"] = (d.get("segments_str") or "").split(",") if d.get("segments_str") else []
            d["segments"] = [s for s in d["segments"] if s]
            d.pop("segments_str", None)
            out.append(d)
        return out


def upsert_account(property_id: str, display_name: str, parent_account: str, import_id: int):
    with conn() as c:
        c.execute("""
            INSERT INTO accounts (property_id, display_name, parent_account, last_seen_in_import, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(property_id) DO UPDATE SET
                display_name = excluded.display_name,
                parent_account = excluded.parent_account,
                last_seen_in_import = excluded.last_seen_in_import,
                updated_at = CURRENT_TIMESTAMP
        """, (property_id, display_name, parent_account, str(import_id)))


# ─────────────── Imports ───────────────

def create_import(filename: str, notes: str = "") -> int:
    with conn() as c:
        cur = c.execute(
            "INSERT INTO data_imports (filename, notes) VALUES (?, ?)",
            (filename, notes),
        )
        return cur.lastrowid


def update_import(import_id: int, **kwargs):
    if not kwargs: return
    fields = ", ".join(f"{k} = ?" for k in kwargs)
    with conn() as c:
        c.execute(f"UPDATE data_imports SET {fields} WHERE id = ?", (*kwargs.values(), import_id))


def list_imports() -> list[dict]:
    with conn() as c:
        rows = c.execute(
            "SELECT * FROM data_imports ORDER BY uploaded_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def latest_import() -> dict | None:
    with conn() as c:
        r = c.execute(
            "SELECT * FROM data_imports WHERE is_active = 1 ORDER BY uploaded_at DESC LIMIT 1"
        ).fetchone()
        return dict(r) if r else None


def delete_import(import_id: int):
    """Removes the import + all its weekly_data rows."""
    with conn() as c:
        c.execute("DELETE FROM weekly_data WHERE import_id = ?", (import_id,))
        c.execute("DELETE FROM data_imports WHERE id = ?", (import_id,))


# ─────────────── Weekly data ───────────────

def insert_weekly_rows(rows: list[dict], import_id: int):
    if not rows: return 0
    with conn() as c:
        c.executemany("""
            INSERT OR REPLACE INTO weekly_data
            (property_id, week_start, sessions, conversions, conv_rate, import_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            (r["property_id"], r["week_start"], r["sessions"], r["conversions"], r["conv_rate"], import_id)
            for r in rows
        ])
        return len(rows)


def query_weekly(property_ids: list[str] | None = None,
                  start: str | None = None, end: str | None = None) -> list[dict]:
    where = []
    args = []
    if property_ids:
        where.append(f"property_id IN ({','.join('?' for _ in property_ids)})")
        args.extend(property_ids)
    if start:
        where.append("week_start >= ?")
        args.append(start)
    if end:
        where.append("week_start <= ?")
        args.append(end)
    sql = "SELECT * FROM weekly_data"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY property_id, week_start"
    with conn() as c:
        rows = c.execute(sql, args).fetchall()
        return [dict(r) for r in rows]


def data_range() -> dict:
    """Return min/max week_start and counts."""
    with conn() as c:
        r = c.execute(
            "SELECT MIN(week_start) AS min_week, MAX(week_start) AS max_week, "
            "COUNT(*) AS rows, COUNT(DISTINCT property_id) AS properties "
            "FROM weekly_data"
        ).fetchone()
        return dict(r) if r else {"min_week": None, "max_week": None, "rows": 0, "properties": 0}


def reset_weekly_data():
    """Wipe all weekly data + imports (segments + accounts kept)."""
    with conn() as c:
        c.execute("DELETE FROM weekly_data")
        c.execute("DELETE FROM data_imports")
