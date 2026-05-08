"""Promogen Intelligence — upload-based GA4 viewer.
No automatic syncing. User uploads CSV from the ga4-export skill.
"""
import csv
import io
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from config import APP_NAME, APP_PORT, APP_HOST, UPLOAD_DIR
import database as db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    logger.info(f"{APP_NAME} starting on {APP_HOST}:{APP_PORT}")
    yield
    logger.info("App shutting down")


app = FastAPI(title=APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ─────────────── Frontend ───────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html", {"app_name": APP_NAME})


# ─────────────── Status ───────────────

@app.get("/api/status")
def api_status():
    rng = db.data_range()
    return {
        "app_name": APP_NAME,
        "accounts": len(db.list_accounts()),
        "segments": len(db.list_segments()),
        "data_range": rng,
        "imports": len(db.list_imports()),
    }


# ─────────────── Upload ───────────────

REQUIRED_COLS = {"property_id", "property_name", "parent_account", "week_start", "sessions", "conversions", "conv_rate"}


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    """Accept CSV from the ga4-export skill, parse + store."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Očekávám CSV soubor (.csv)")

    raw = await file.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("utf-8-sig")  # handle BOM

    # Save raw upload
    saved_path = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{file.filename}"
    saved_path.write_text(text, encoding="utf-8")

    reader = csv.DictReader(io.StringIO(text))
    cols = set(reader.fieldnames or [])
    missing = REQUIRED_COLS - cols
    if missing:
        raise HTTPException(400, f"Chybí povinné sloupce: {', '.join(sorted(missing))}. Najdou: {', '.join(cols)}")

    import_id = db.create_import(file.filename, notes=f"Saved as {saved_path.name}")

    accounts_seen: dict[str, dict] = {}
    weekly_rows: list[dict] = []
    weeks_seen: set = set()
    parse_errors = 0

    for row in reader:
        try:
            pid = (row.get("property_id") or "").strip()
            if not pid: continue
            week = (row.get("week_start") or "").strip()
            if not week: continue
            sessions = int(float(row.get("sessions") or 0))
            conversions = float(row.get("conversions") or 0)
            conv_rate = float(row.get("conv_rate") or 0)

            accounts_seen[pid] = {
                "display_name": row.get("property_name", "").strip() or pid,
                "parent_account": row.get("parent_account", "").strip(),
            }
            weekly_rows.append({
                "property_id": pid,
                "week_start": week,
                "sessions": sessions,
                "conversions": conversions,
                "conv_rate": conv_rate,
            })
            weeks_seen.add(week)
        except (ValueError, TypeError) as e:
            parse_errors += 1

    # Upsert accounts
    for pid, meta in accounts_seen.items():
        db.upsert_account(pid, meta["display_name"], meta["parent_account"], import_id)

    # Insert weekly data
    inserted = db.insert_weekly_rows(weekly_rows, import_id)

    db.update_import(
        import_id,
        rows_imported=inserted,
        properties_count=len(accounts_seen),
        weeks_count=len(weeks_seen),
        min_week=min(weeks_seen) if weeks_seen else None,
        max_week=max(weeks_seen) if weeks_seen else None,
    )

    return {
        "ok": True,
        "import_id": import_id,
        "rows": inserted,
        "properties": len(accounts_seen),
        "weeks": len(weeks_seen),
        "min_week": min(weeks_seen) if weeks_seen else None,
        "max_week": max(weeks_seen) if weeks_seen else None,
        "parse_errors": parse_errors,
    }


@app.get("/api/imports")
def api_imports():
    return {"imports": db.list_imports()}


@app.delete("/api/imports/{import_id}")
def api_delete_import(import_id: int):
    db.delete_import(import_id)
    return {"ok": True}


@app.post("/api/imports/reset")
def api_reset_imports():
    """Wipe all data (segments + accounts kept)."""
    db.reset_weekly_data()
    return {"ok": True}


# ─────────────── Accounts & segments ───────────────

@app.get("/api/accounts")
def api_accounts():
    return db.list_accounts()


@app.get("/api/segments")
def api_segments():
    return db.list_segments()


class SegmentCreateBody(BaseModel):
    slug: str
    name: str
    color: str = "#64748b"
    icon: str = "📦"


@app.post("/api/segments")
def api_create_segment(body: SegmentCreateBody):
    db.add_segment(body.slug, body.name, body.color, body.icon)
    return {"ok": True}


@app.delete("/api/segments/{slug}")
def api_delete_segment(slug: str):
    db.delete_segment(slug)
    return {"ok": True}


class SegmentAssignBody(BaseModel):
    segment_slug: str


@app.post("/api/accounts/{property_id}/segments")
def api_assign_segment(property_id: str, body: SegmentAssignBody):
    db.assign_segment(property_id, body.segment_slug)
    return {"ok": True}


@app.delete("/api/accounts/{property_id}/segments/{segment_slug}")
def api_remove_segment(property_id: str, segment_slug: str):
    db.remove_segment(property_id, segment_slug)
    return {"ok": True}


class BulkAssignBody(BaseModel):
    property_ids: list[str]
    segment_slug: str
    replace: bool = True


@app.post("/api/accounts/bulk_assign")
def api_bulk_assign(body: BulkAssignBody):
    moved = 0
    with db.conn() as c:
        for pid in body.property_ids:
            if body.replace:
                c.execute("DELETE FROM account_segments WHERE property_id = ?", (pid,))
            c.execute(
                "INSERT OR IGNORE INTO account_segments (property_id, segment_slug) VALUES (?, ?)",
                (pid, body.segment_slug),
            )
            moved += 1
    return {"ok": True, "moved": moved}


# ─────────────── Data queries ───────────────

@app.get("/api/data/timeseries")
def api_timeseries(property_ids: str, metric: str = "sessions",
                    start: str | None = None, end: str | None = None,
                    yoy: bool = False):
    """Returns one series per property_id with the requested metric."""
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    rows = db.query_weekly(pids, start, end)
    accounts = {a["property_id"]: a for a in db.list_accounts()}
    series_map: dict[str, dict] = {pid: {"property_id": pid,
                                           "display_name": accounts.get(pid, {}).get("display_name", pid),
                                           "data": []} for pid in pids}
    for r in rows:
        if r["property_id"] in series_map:
            series_map[r["property_id"]]["data"].append({
                "week_start": r["week_start"],
                "value": r.get(metric, 0),
            })

    result = {"metric": metric, "series": list(series_map.values()), "yoy": None}

    if yoy:
        # Shift the same window back 365 days
        from datetime import timedelta
        def shift(iso): return (datetime.fromisoformat(iso) - timedelta(days=365)).date().isoformat()
        yoy_start = shift(start) if start else None
        yoy_end = shift(end) if end else None
        yrows = db.query_weekly(pids, yoy_start, yoy_end)
        yoy_map: dict[str, list] = {pid: [] for pid in pids}
        for r in yrows:
            if r["property_id"] in yoy_map:
                # shift week_start forward by 1 year so it aligns with current
                shifted_week = (datetime.fromisoformat(r["week_start"]) + timedelta(days=365)).date().isoformat()
                yoy_map[r["property_id"]].append({
                    "week_start": shifted_week,
                    "value": r.get(metric, 0),
                })
        result["yoy"] = [{"property_id": pid,
                          "display_name": accounts.get(pid, {}).get("display_name", pid),
                          "data": yoy_map[pid]} for pid in pids]

    return result


@app.get("/api/data/account_strip")
def api_account_strip(property_ids: str, start: str | None = None, end: str | None = None):
    """KPIs per account for the period: sessions, conversions, conv_rate, sparkline."""
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    rows = db.query_weekly(pids, start, end)
    accounts = {a["property_id"]: a for a in db.list_accounts()}
    by_pid: dict[str, list] = {pid: [] for pid in pids}
    for r in rows:
        if r["property_id"] in by_pid:
            by_pid[r["property_id"]].append(r)

    out = []
    for pid in pids:
        prows = by_pid[pid]
        a = accounts.get(pid, {})
        if not prows:
            out.append({
                "property_id": pid,
                "display_name": a.get("display_name", pid),
                "parent_account": a.get("parent_account", ""),
                "no_data": True,
            })
            continue
        sessions_total = sum(r["sessions"] for r in prows)
        conv_total = sum(r["conversions"] for r in prows)
        avg_conv_rate = (conv_total / sessions_total * 100) if sessions_total else 0

        # Trend: compare last 4 weeks vs prior 4 weeks
        last4_s = sum(r["sessions"] for r in prows[-4:])
        prev4_s = sum(r["sessions"] for r in prows[-8:-4]) if len(prows) >= 5 else 0
        trend_pct = ((last4_s - prev4_s) / prev4_s * 100) if prev4_s else None

        out.append({
            "property_id": pid,
            "display_name": a.get("display_name", pid),
            "parent_account": a.get("parent_account", ""),
            "no_data": False,
            "kpis": {
                "sessions": sessions_total,
                "conversions": round(conv_total, 1),
                "conv_rate": round(avg_conv_rate, 2),
                "weeks": len(prows),
            },
            "trend_pct": round(trend_pct, 1) if trend_pct is not None else None,
            "sparkline": [r["sessions"] for r in prows],
            "weeks": [r["week_start"] for r in prows],
        })
    return {"accounts": out}


@app.get("/api/data/segment_rollup")
def api_segment_rollup(segment: str, start: str | None = None, end: str | None = None,
                        yoy: bool = False):
    """Sum metrics for all accounts in a segment, weekly."""
    pids = db.accounts_in_segment(segment)
    if not pids:
        return {"available": False, "reason": "no_accounts_in_segment"}
    rows = db.query_weekly(pids, start, end)
    by_week: dict[str, dict] = {}
    for r in rows:
        w = r["week_start"]
        if w not in by_week:
            by_week[w] = {"week_start": w, "sessions": 0, "conversions": 0}
        by_week[w]["sessions"] += r["sessions"]
        by_week[w]["conversions"] += r["conversions"]
    series = sorted(by_week.values(), key=lambda x: x["week_start"])
    for s in series:
        s["conv_rate"] = round((s["conversions"] / s["sessions"] * 100), 2) if s["sessions"] else 0

    yoy_series = None
    if yoy:
        from datetime import timedelta
        def shift(iso): return (datetime.fromisoformat(iso) - timedelta(days=365)).date().isoformat()
        ys = shift(start) if start else None
        ye = shift(end) if end else None
        yrows = db.query_weekly(pids, ys, ye)
        ymap: dict[str, dict] = {}
        for r in yrows:
            shifted_week = (datetime.fromisoformat(r["week_start"]) + timedelta(days=365)).date().isoformat()
            if shifted_week not in ymap:
                ymap[shifted_week] = {"week_start": shifted_week, "sessions": 0, "conversions": 0}
            ymap[shifted_week]["sessions"] += r["sessions"]
            ymap[shifted_week]["conversions"] += r["conversions"]
        yoy_series = sorted(ymap.values(), key=lambda x: x["week_start"])
        for s in yoy_series:
            s["conv_rate"] = round((s["conversions"] / s["sessions"] * 100), 2) if s["sessions"] else 0

    # Per-account breakdown for the period
    per_account = []
    accounts = {a["property_id"]: a for a in db.list_accounts()}
    for pid in pids:
        prows = [r for r in rows if r["property_id"] == pid]
        if not prows: continue
        s = sum(r["sessions"] for r in prows)
        c = sum(r["conversions"] for r in prows)
        per_account.append({
            "property_id": pid,
            "display_name": accounts.get(pid, {}).get("display_name", pid),
            "sessions": s,
            "conversions": round(c, 1),
            "conv_rate": round(c/s*100, 2) if s else 0,
            "weekly": [{"week_start": r["week_start"], "sessions": r["sessions"],
                         "conversions": r["conversions"], "conv_rate": r["conv_rate"]} for r in prows],
        })
    per_account.sort(key=lambda x: -x["sessions"])

    total_s = sum(s["sessions"] for s in series)
    total_c = sum(s["conversions"] for s in series)
    overall = {
        "sessions": total_s,
        "conversions": round(total_c, 1),
        "conv_rate": round(total_c/total_s*100, 2) if total_s else 0,
    }
    yoy_overall = None
    if yoy_series:
        ys = sum(s["sessions"] for s in yoy_series)
        yc = sum(s["conversions"] for s in yoy_series)
        yoy_overall = {"sessions": ys, "conversions": round(yc, 1),
                       "conv_rate": round(yc/ys*100, 2) if ys else 0}

    return {
        "available": True,
        "segment": segment,
        "n_accounts": len(per_account),
        "series": series,
        "yoy_series": yoy_series,
        "overall": overall,
        "yoy_overall": yoy_overall,
        "per_account": per_account,
    }


# ─────────────── Run ───────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=APP_HOST, port=APP_PORT, log_level="info")
