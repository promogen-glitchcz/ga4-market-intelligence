"""SQLite layer for config + insights. DuckDB for time-series data warehouse."""
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb

from config import SQLITE_DB_PATH, DUCKDB_PATH, DEFAULT_SEGMENTS

logger = logging.getLogger("ga4.db")


# ─────────────── SQLite (config + insights) ───────────────

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    slug TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    color TEXT,
    icon TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS accounts (
    property_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    parent_account TEXT,
    parent_account_name TEXT,
    currency_code TEXT,
    time_zone TEXT,
    is_monitored INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS account_segments (
    property_id TEXT,
    segment_slug TEXT,
    PRIMARY KEY (property_id, segment_slug),
    FOREIGN KEY (property_id) REFERENCES accounts(property_id),
    FOREIGN KEY (segment_slug) REFERENCES segments(slug)
);

CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT,
    sync_type TEXT,
    started_at TEXT,
    finished_at TEXT,
    rows_synced INTEGER,
    status TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT DEFAULT (datetime('now')),
    scope TEXT,                    -- 'account', 'segment', 'cross-segment', 'global'
    scope_id TEXT,                 -- property_id or segment slug
    insight_type TEXT,             -- 'anomaly', 'trend', 'correlation', 'seasonal', 'forecast', 'briefing', 'hypothesis'
    severity TEXT,                 -- 'critical', 'warning', 'info', 'positive'
    title TEXT,
    body TEXT,
    confidence REAL,
    metric TEXT,
    period_start TEXT,
    period_end TEXT,
    metadata TEXT,                 -- JSON
    is_dismissed INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_insights_scope ON insights(scope, scope_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_insights_type ON insights(insight_type, created_at DESC);

CREATE TABLE IF NOT EXISTS market_health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at TEXT DEFAULT (datetime('now')),
    segment_slug TEXT,
    period_days INTEGER,
    period_end TEXT,
    score REAL,
    verdict TEXT,                  -- 'critical', 'poor', 'fair', 'good', 'excellent'
    components TEXT,               -- JSON breakdown
    accounts_in_segment INTEGER,
    accounts_declining INTEGER,
    summary TEXT
);
CREATE INDEX IF NOT EXISTS idx_health_seg ON market_health(segment_slug, computed_at DESC);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT DEFAULT (datetime('now')),
    scope TEXT,
    scope_id TEXT,
    alert_type TEXT,
    severity TEXT,
    title TEXT,
    message TEXT,
    metadata TEXT,
    is_read INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS agent_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT,
    agent_type TEXT,               -- 'sync', 'anomaly', 'correlation', 'forecast', 'briefing', 'hypothesis'
    scope TEXT,
    scope_id TEXT,
    status TEXT,                   -- 'running', 'success', 'error'
    findings_count INTEGER DEFAULT 0,
    summary TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS hypotheses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT DEFAULT (datetime('now')),
    question TEXT,
    scope TEXT,
    scope_id TEXT,
    period_start TEXT,
    period_end TEXT,
    answer TEXT,
    evidence TEXT,                 -- JSON
    confidence REAL,
    status TEXT DEFAULT 'pending'  -- 'pending', 'answered', 'failed'
);

CREATE TABLE IF NOT EXISTS daily_briefings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    briefing_date TEXT UNIQUE,
    generated_at TEXT DEFAULT (datetime('now')),
    headline TEXT,
    body TEXT,
    metadata TEXT
);
"""


@contextmanager
def sqlite_conn():
    conn = sqlite3.connect(SQLITE_DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_sqlite():
    with sqlite_conn() as c:
        c.executescript(SQLITE_SCHEMA)
        # Idempotent seed: insert any missing default segments
        for s in DEFAULT_SEGMENTS:
            c.execute(
                "INSERT OR IGNORE INTO segments (slug, name, color, icon) VALUES (?, ?, ?, ?)",
                (s["slug"], s["name"], s["color"], s["icon"]),
            )


# ── Account/segment management ──

def upsert_account(prop: dict):
    with sqlite_conn() as c:
        c.execute(
            """INSERT INTO accounts (property_id, display_name, parent_account, parent_account_name,
                       currency_code, time_zone, is_monitored)
               VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT is_monitored FROM accounts WHERE property_id = ?), 1))
               ON CONFLICT(property_id) DO UPDATE SET
                   display_name = excluded.display_name,
                   parent_account = excluded.parent_account,
                   parent_account_name = excluded.parent_account_name,
                   currency_code = excluded.currency_code,
                   time_zone = excluded.time_zone""",
            (prop["property_id"], prop["display_name"], prop.get("parent_account"),
             prop.get("parent_account_name"), prop.get("currency_code"), prop.get("time_zone"),
             prop["property_id"]),
        )


def list_accounts(monitored_only: bool = False) -> list[dict]:
    with sqlite_conn() as c:
        sql = """SELECT a.*,
                        GROUP_CONCAT(s.segment_slug) AS segments
                 FROM accounts a
                 LEFT JOIN account_segments s ON s.property_id = a.property_id"""
        if monitored_only:
            sql += " WHERE a.is_monitored = 1"
        sql += " GROUP BY a.property_id ORDER BY a.display_name"
        rows = c.execute(sql).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["segments"] = d["segments"].split(",") if d["segments"] else []
            out.append(d)
        return out


def set_account_monitored(property_id: str, monitored: bool):
    with sqlite_conn() as c:
        c.execute("UPDATE accounts SET is_monitored = ? WHERE property_id = ?",
                  (1 if monitored else 0, property_id))


def assign_segment(property_id: str, segment_slug: str):
    with sqlite_conn() as c:
        c.execute("INSERT OR IGNORE INTO account_segments VALUES (?, ?)",
                  (property_id, segment_slug))


def remove_segment(property_id: str, segment_slug: str):
    with sqlite_conn() as c:
        c.execute("DELETE FROM account_segments WHERE property_id = ? AND segment_slug = ?",
                  (property_id, segment_slug))


def list_segments() -> list[dict]:
    with sqlite_conn() as c:
        rows = c.execute("""
            SELECT s.*, COUNT(a.property_id) AS account_count
            FROM segments s
            LEFT JOIN account_segments a ON a.segment_slug = s.slug
            GROUP BY s.slug ORDER BY s.name
        """).fetchall()
        return [dict(r) for r in rows]


def add_segment(slug: str, name: str, color: str = "#64748b", icon: str = "📦"):
    with sqlite_conn() as c:
        c.execute("INSERT OR IGNORE INTO segments (slug, name, color, icon) VALUES (?, ?, ?, ?)",
                  (slug, name, color, icon))


def accounts_in_segment(segment_slug: str) -> list[str]:
    with sqlite_conn() as c:
        rows = c.execute(
            "SELECT property_id FROM account_segments WHERE segment_slug = ?",
            (segment_slug,)
        ).fetchall()
        return [r["property_id"] for r in rows]


# ── Insights ──

def save_insight(insight: dict) -> int:
    with sqlite_conn() as c:
        cur = c.execute(
            """INSERT INTO insights (scope, scope_id, insight_type, severity, title, body,
                                       confidence, metric, period_start, period_end, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (insight.get("scope"), insight.get("scope_id"), insight.get("insight_type"),
             insight.get("severity", "info"), insight.get("title"), insight.get("body"),
             insight.get("confidence", 0.5), insight.get("metric"),
             insight.get("period_start"), insight.get("period_end"),
             json.dumps(insight.get("metadata", {}), default=str)),
        )
        return cur.lastrowid


def list_insights(scope: str | None = None, scope_id: str | None = None,
                  insight_type: str | None = None, limit: int = 100) -> list[dict]:
    with sqlite_conn() as c:
        sql = "SELECT * FROM insights WHERE is_dismissed = 0"
        params: list[Any] = []
        if scope:
            sql += " AND scope = ?"; params.append(scope)
        if scope_id:
            sql += " AND scope_id = ?"; params.append(scope_id)
        if insight_type:
            sql += " AND insight_type = ?"; params.append(insight_type)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = c.execute(sql, params).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try: d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
            except: d["metadata"] = {}
            out.append(d)
        return out


def dismiss_insight(insight_id: int):
    with sqlite_conn() as c:
        c.execute("UPDATE insights SET is_dismissed = 1 WHERE id = ?", (insight_id,))


# ── Health scores ──

def save_health_score(score: dict):
    with sqlite_conn() as c:
        c.execute(
            """INSERT INTO market_health (segment_slug, period_days, period_end, score, verdict,
                                            components, accounts_in_segment, accounts_declining, summary)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (score["segment_slug"], score["period_days"], score["period_end"], score["score"],
             score["verdict"], json.dumps(score.get("components", {})),
             score.get("accounts_in_segment", 0), score.get("accounts_declining", 0),
             score.get("summary", "")),
        )


def latest_health_score(segment_slug: str, period_days: int = 30) -> dict | None:
    with sqlite_conn() as c:
        r = c.execute(
            "SELECT * FROM market_health WHERE segment_slug = ? AND period_days = ? "
            "ORDER BY computed_at DESC LIMIT 1",
            (segment_slug, period_days),
        ).fetchone()
        if not r: return None
        d = dict(r)
        try: d["components"] = json.loads(d["components"]) if d["components"] else {}
        except: d["components"] = {}
        return d


def health_score_history(segment_slug: str, period_days: int = 30, limit: int = 60) -> list[dict]:
    with sqlite_conn() as c:
        rows = c.execute(
            "SELECT * FROM market_health WHERE segment_slug = ? AND period_days = ? "
            "ORDER BY computed_at DESC LIMIT ?",
            (segment_slug, period_days, limit),
        ).fetchall()
        return [dict(r) for r in rows]


# ── Alerts ──

def save_alert(alert: dict):
    with sqlite_conn() as c:
        c.execute(
            """INSERT INTO alerts (scope, scope_id, alert_type, severity, title, message, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (alert.get("scope"), alert.get("scope_id"), alert.get("alert_type"),
             alert.get("severity", "warning"), alert.get("title"), alert.get("message"),
             json.dumps(alert.get("metadata", {}))),
        )


def list_alerts(unread_only: bool = False, limit: int = 50) -> list[dict]:
    with sqlite_conn() as c:
        sql = "SELECT * FROM alerts"
        if unread_only: sql += " WHERE is_read = 0"
        sql += " ORDER BY created_at DESC LIMIT ?"
        rows = c.execute(sql, (limit,)).fetchall()
        return [dict(r) for r in rows]


def mark_alert_read(alert_id: int):
    with sqlite_conn() as c:
        c.execute("UPDATE alerts SET is_read = 1 WHERE id = ?", (alert_id,))


# ── Agent activity ──

def start_agent_run(agent_type: str, scope: str = "", scope_id: str = "") -> int:
    with sqlite_conn() as c:
        cur = c.execute(
            "INSERT INTO agent_activity (agent_type, scope, scope_id, status) VALUES (?, ?, ?, 'running')",
            (agent_type, scope, scope_id),
        )
        return cur.lastrowid


def finish_agent_run(run_id: int, status: str = "success", findings: int = 0,
                     summary: str = "", error: str | None = None):
    with sqlite_conn() as c:
        c.execute(
            "UPDATE agent_activity SET finished_at = datetime('now'), status = ?, "
            "findings_count = ?, summary = ?, error = ? WHERE id = ?",
            (status, findings, summary, error, run_id),
        )


def list_agent_activity(limit: int = 100) -> list[dict]:
    with sqlite_conn() as c:
        rows = c.execute(
            "SELECT * FROM agent_activity ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Hypotheses ──

def save_hypothesis(question: str, scope: str = "", scope_id: str = "",
                    period_start: str | None = None, period_end: str | None = None) -> int:
    with sqlite_conn() as c:
        cur = c.execute(
            "INSERT INTO hypotheses (question, scope, scope_id, period_start, period_end) "
            "VALUES (?, ?, ?, ?, ?)",
            (question, scope, scope_id, period_start, period_end),
        )
        return cur.lastrowid


def update_hypothesis(hyp_id: int, answer: str, evidence: dict, confidence: float, status: str = "answered"):
    with sqlite_conn() as c:
        c.execute(
            "UPDATE hypotheses SET answer = ?, evidence = ?, confidence = ?, status = ? WHERE id = ?",
            (answer, json.dumps(evidence, default=str), confidence, status, hyp_id),
        )


def list_hypotheses(limit: int = 50) -> list[dict]:
    with sqlite_conn() as c:
        rows = c.execute("SELECT * FROM hypotheses ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try: d["evidence"] = json.loads(d["evidence"]) if d["evidence"] else {}
            except: d["evidence"] = {}
            out.append(d)
        return out


# ── Briefings ──

def save_briefing(date: str, headline: str, body: str, metadata: dict = None):
    with sqlite_conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO daily_briefings (briefing_date, headline, body, metadata) VALUES (?, ?, ?, ?)",
            (date, headline, body, json.dumps(metadata or {}, default=str)),
        )


def latest_briefing() -> dict | None:
    with sqlite_conn() as c:
        r = c.execute("SELECT * FROM daily_briefings ORDER BY briefing_date DESC LIMIT 1").fetchone()
        if not r: return None
        d = dict(r)
        try: d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
        except: d["metadata"] = {}
        return d


# ── Sync log ──

def log_sync(property_id: str, sync_type: str, started_at: str, finished_at: str,
             rows: int, status: str, error: str | None = None):
    with sqlite_conn() as c:
        c.execute(
            """INSERT INTO sync_log (property_id, sync_type, started_at, finished_at, rows_synced, status, error)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (property_id, sync_type, started_at, finished_at, rows, status, error),
        )


def last_sync_for(property_id: str) -> dict | None:
    with sqlite_conn() as c:
        r = c.execute(
            "SELECT * FROM sync_log WHERE property_id = ? AND status = 'success' "
            "ORDER BY finished_at DESC LIMIT 1",
            (property_id,),
        ).fetchone()
        return dict(r) if r else None


# ─────────────── DuckDB (data warehouse) ───────────────

DUCKDB_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_metrics (
    property_id VARCHAR,
    date DATE,
    sessions BIGINT,
    users BIGINT,
    new_users BIGINT,
    engaged_sessions BIGINT,
    bounce_rate DOUBLE,
    avg_session_duration DOUBLE,
    screen_page_views BIGINT,
    conversions DOUBLE,
    purchase_revenue DOUBLE,
    transactions BIGINT,
    PRIMARY KEY (property_id, date)
);

CREATE TABLE IF NOT EXISTS channel_daily (
    property_id VARCHAR,
    date DATE,
    channel_group VARCHAR,
    sessions BIGINT,
    users BIGINT,
    conversions DOUBLE,
    revenue DOUBLE,
    PRIMARY KEY (property_id, date, channel_group)
);

CREATE TABLE IF NOT EXISTS source_medium_daily (
    property_id VARCHAR,
    date DATE,
    source VARCHAR,
    medium VARCHAR,
    sessions BIGINT,
    users BIGINT,
    conversions DOUBLE,
    revenue DOUBLE,
    PRIMARY KEY (property_id, date, source, medium)
);

CREATE TABLE IF NOT EXISTS device_daily (
    property_id VARCHAR,
    date DATE,
    device_category VARCHAR,
    sessions BIGINT,
    users BIGINT,
    conversions DOUBLE,
    revenue DOUBLE,
    PRIMARY KEY (property_id, date, device_category)
);

CREATE TABLE IF NOT EXISTS country_daily (
    property_id VARCHAR,
    date DATE,
    country VARCHAR,
    sessions BIGINT,
    users BIGINT,
    revenue DOUBLE,
    PRIMARY KEY (property_id, date, country)
);

CREATE TABLE IF NOT EXISTS landing_page_daily (
    property_id VARCHAR,
    date DATE,
    landing_page VARCHAR,
    sessions BIGINT,
    bounce_rate DOUBLE,
    conversions DOUBLE,
    PRIMARY KEY (property_id, date, landing_page)
);

CREATE TABLE IF NOT EXISTS hourly_metrics (
    property_id VARCHAR,
    date DATE,
    hour INTEGER,
    sessions BIGINT,
    users BIGINT,
    conversions DOUBLE,
    PRIMARY KEY (property_id, date, hour)
);
"""


def get_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DUCKDB_PATH))


def init_duckdb():
    with get_duckdb() as conn:
        conn.execute(DUCKDB_SCHEMA)


def upsert_daily_metrics(rows: Iterable[dict]):
    rows = list(rows)
    if not rows: return 0
    with get_duckdb() as conn:
        for row in rows:
            conn.execute(
                """INSERT OR REPLACE INTO daily_metrics
                   (property_id, date, sessions, users, new_users, engaged_sessions, bounce_rate,
                    avg_session_duration, screen_page_views, conversions, purchase_revenue, transactions)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row["property_id"], row["date"], row.get("sessions", 0), row.get("users", 0),
                 row.get("new_users", 0), row.get("engaged_sessions", 0), row.get("bounce_rate", 0.0),
                 row.get("avg_session_duration", 0.0), row.get("screen_page_views", 0),
                 row.get("conversions", 0.0), row.get("purchase_revenue", 0.0),
                 row.get("transactions", 0)),
            )
    return len(rows)


def upsert_dimension_daily(table: str, rows: Iterable[dict], dim_columns: list[str]):
    rows = list(rows)
    if not rows: return 0
    if table == "landing_page_daily":
        cols = ["property_id", "date"] + dim_columns + ["sessions", "bounce_rate", "conversions"]
    elif table == "country_daily":
        cols = ["property_id", "date"] + dim_columns + ["sessions", "users", "revenue"]
    else:
        cols = ["property_id", "date"] + dim_columns + ["sessions", "users", "conversions", "revenue"]
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    with get_duckdb() as conn:
        for row in rows:
            values = [row.get(c) for c in cols]
            conn.execute(sql, values)
    return len(rows)


def query_daily_metrics(property_ids: list[str], start_date: str, end_date: str) -> list[dict]:
    if not property_ids: return []
    placeholders = ",".join(["?"] * len(property_ids))
    sql = f"""SELECT * FROM daily_metrics
              WHERE property_id IN ({placeholders})
                AND date >= ? AND date <= ?
              ORDER BY date, property_id"""
    with get_duckdb() as conn:
        result = conn.execute(sql, [*property_ids, start_date, end_date]).fetchall()
        cols = [d[0] for d in conn.description]
    return [dict(zip(cols, r)) for r in result]


def query_channel_breakdown(property_ids: list[str], start_date: str, end_date: str) -> list[dict]:
    if not property_ids: return []
    placeholders = ",".join(["?"] * len(property_ids))
    sql = f"""SELECT property_id, channel_group, SUM(sessions) AS sessions,
                     SUM(users) AS users, SUM(conversions) AS conversions, SUM(revenue) AS revenue
              FROM channel_daily
              WHERE property_id IN ({placeholders}) AND date >= ? AND date <= ?
              GROUP BY property_id, channel_group ORDER BY sessions DESC"""
    with get_duckdb() as conn:
        result = conn.execute(sql, [*property_ids, start_date, end_date]).fetchall()
        cols = [d[0] for d in conn.description]
    return [dict(zip(cols, r)) for r in result]


def init_db():
    init_sqlite()
    init_duckdb()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print("DB initialized.")
    print(f"  SQLite: {SQLITE_DB_PATH}")
    print(f"  DuckDB: {DUCKDB_PATH}")
    print(f"  Segments: {len(list_segments())}")
