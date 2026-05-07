"""GA4 Market Intelligence — FastAPI main app.
Runs on http://localhost:8060 by default.
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from config import (APP_NAME, APP_PORT, APP_HOST, DATA_SYNC_INTERVAL,
                    ANALYSIS_INTERVAL, DEEP_ANALYSIS_INTERVAL,
                    INSIGHT_REFINE_INTERVAL, PATTERN_HUNT_INTERVAL,
                    DAILY_BRIEFING_HOUR)
import database as db
import sync as sync_mod
import agents
import analyzer as az
import intelligence as intel
import correlations as cor
from auth import has_valid_credentials
from ga4_api import days_ago, today_iso

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ga4.app")

# ─────────────── Background tasks ───────────────

_bg_tasks: dict[str, asyncio.Task] = {}


async def background_sync_loop():
    """Pulls fresh GA4 data every DATA_SYNC_INTERVAL."""
    while True:
        try:
            if has_valid_credentials():
                logger.info("Background sync starting...")
                await sync_mod.sync_all_async(deep=False)
                logger.info("Background sync done")
        except Exception as e:
            logger.exception(f"Background sync failed: {e}")
        await asyncio.sleep(DATA_SYNC_INTERVAL)


async def background_shallow_loop():
    """Fast 15-min loop: anomaly + health + top movers."""
    await asyncio.sleep(120)
    while True:
        try:
            if has_valid_credentials():
                logger.info("Shallow analysis cycle starting...")
                await asyncio.to_thread(agents.run_shallow_cycle)
                logger.info("Shallow analysis done")
        except Exception as e:
            logger.exception(f"Shallow analysis failed: {e}")
        await asyncio.sleep(ANALYSIS_INTERVAL)


async def background_deep_loop():
    """2h deep loop: cross-account correlation + pattern hunt + channel shifts + trends + forecasts."""
    await asyncio.sleep(300)
    while True:
        try:
            if has_valid_credentials():
                logger.info("Deep analysis cycle starting...")
                await asyncio.to_thread(agents.run_full_analysis_cycle)
                logger.info("Deep analysis done")
        except Exception as e:
            logger.exception(f"Deep analysis failed: {e}")
        await asyncio.sleep(DEEP_ANALYSIS_INTERVAL)


async def background_refine_loop():
    """6h: deduplicate, dismiss old insights."""
    await asyncio.sleep(900)
    while True:
        try:
            await asyncio.to_thread(agents.AGENTS["refine"])
        except Exception as e:
            logger.exception(f"Refine failed: {e}")
        await asyncio.sleep(INSIGHT_REFINE_INTERVAL)


async def background_publish_loop():
    """Auto-commit + push the insights DB every hour so the team sees latest data."""
    await asyncio.sleep(60 * 30)  # first run after 30 min
    while True:
        try:
            from auto_publish import publish
            result = await asyncio.to_thread(publish)
            if result.get("status") == "ok":
                logger.info(f"Auto-published insights DB to git: {result.get('ts')}")
            elif result.get("status") == "error":
                logger.warning(f"Auto-publish error at {result.get('step')}: {result.get('error')}")
        except Exception as e:
            logger.exception(f"Auto-publish failed: {e}")
        await asyncio.sleep(60 * 60)  # every hour


async def background_briefing_loop():
    """Daily briefing at DAILY_BRIEFING_HOUR local time."""
    while True:
        now = datetime.now()
        target = now.replace(hour=DAILY_BRIEFING_HOUR, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        logger.info(f"Next briefing at {target.isoformat()} (in {wait/3600:.1f}h)")
        await asyncio.sleep(wait)
        try:
            await asyncio.to_thread(agents.AGENTS["briefing"])
        except Exception as e:
            logger.exception(f"Briefing failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    logger.info(f"{APP_NAME} starting on {APP_HOST}:{APP_PORT}")

    if has_valid_credentials():
        # Discover accounts on startup (non-blocking)
        async def discover_init():
            try:
                n = await asyncio.to_thread(sync_mod.discover_and_register_accounts)
                logger.info(f"Discovered {n} GA4 accounts on startup")
            except Exception as e:
                logger.warning(f"Account discovery failed (will retry): {e}")
        asyncio.create_task(discover_init())

        _loop_factories["sync"]     = background_sync_loop
        _loop_factories["shallow"]  = background_shallow_loop
        _loop_factories["deep"]     = background_deep_loop
        _loop_factories["refine"]   = background_refine_loop
        _loop_factories["briefing"] = background_briefing_loop
        _loop_factories["publish"]  = background_publish_loop
        for name, fn in _loop_factories.items():
            _bg_tasks[name] = asyncio.create_task(fn())
    else:
        logger.warning("No valid OAuth credentials — run oauth_setup.py first")

    yield

    for t in _bg_tasks.values():
        t.cancel()
    logger.info("App shutting down")


app = FastAPI(title=APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ─────────────── Frontend routes ───────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html", {"app_name": APP_NAME})


# ─────────────── API: status & auth ───────────────

@app.get("/api/status")
def api_status():
    loops = []
    for name, t in _bg_tasks.items():
        loops.append({
            "name": name,
            "running": not t.done(),
            "cancelled": t.cancelled(),
        })
    return {
        "app_name": APP_NAME,
        "has_credentials": has_valid_credentials(),
        "accounts_total": len(db.list_accounts()),
        "accounts_monitored": len(db.list_accounts(monitored_only=True)),
        "segments": len(db.list_segments()),
        "recent_insights": len(db.list_insights(limit=10)),
        "background_loops": loops,
        "active_loops": sum(1 for l in loops if l["running"]),
    }


_loop_factories: dict = {}  # name -> async function


@app.post("/api/agents/loops/{name}/stop")
def api_loop_stop(name: str):
    t = _bg_tasks.get(name)
    if not t:
        raise HTTPException(404, f"Loop {name} not found")
    if t.done():
        return {"ok": True, "status": "already stopped"}
    t.cancel()
    return {"ok": True, "status": "cancelled"}


@app.post("/api/agents/loops/{name}/start")
def api_loop_start(name: str):
    if name not in _loop_factories:
        raise HTTPException(404, f"Loop {name} not registered")
    existing = _bg_tasks.get(name)
    if existing and not existing.done():
        return {"ok": True, "status": "already running"}
    _bg_tasks[name] = asyncio.create_task(_loop_factories[name]())
    return {"ok": True, "status": "started"}


@app.post("/api/agents/loops/stop_all")
def api_loops_stop_all():
    stopped = 0
    for name, t in _bg_tasks.items():
        if not t.done():
            t.cancel()
            stopped += 1
    return {"ok": True, "stopped": stopped}


@app.post("/api/agents/loops/start_all")
def api_loops_start_all():
    started = 0
    for name, factory in _loop_factories.items():
        t = _bg_tasks.get(name)
        if t and not t.done():
            continue
        _bg_tasks[name] = asyncio.create_task(factory())
        started += 1
    return {"ok": True, "started": started}


@app.get("/api/agents/loops")
def api_agent_loops():
    """Detailed status of background loops + next-run estimates."""
    from datetime import datetime
    schedules = {
        "sync":     {"interval_seconds": DATA_SYNC_INTERVAL,    "label": "Sync GA4 metrik", "human": "každých 30 min"},
        "shallow":  {"interval_seconds": ANALYSIS_INTERVAL,     "label": "Rychlá analýza (anomálie + zdraví + top hybatelé)", "human": "každých 15 min"},
        "deep":     {"interval_seconds": DEEP_ANALYSIS_INTERVAL,"label": "Hluboká analýza (korelace + vzorce + predikce)", "human": "každé 2 hodiny"},
        "refine":   {"interval_seconds": INSIGHT_REFINE_INTERVAL,"label": "Vylepšení insights (deduplikace + dismiss)", "human": "každých 6 hodin"},
        "briefing": {"interval_seconds": 86400, "label": f"Denní briefing", "human": f"každý den v {DAILY_BRIEFING_HOUR:02d}:00"},
        "publish":  {"interval_seconds": 3600, "label": "Auto-publish do GitHubu", "human": "každou hodinu"},
    }
    out = []
    for name, t in _bg_tasks.items():
        meta = schedules.get(name, {"label": name, "human": "?"})
        out.append({
            "name": name,
            "label": meta["label"],
            "frequency": meta["human"],
            "running": not t.done(),
            "cancelled": t.cancelled(),
        })
    return {"loops": out}


# ─────────────── API: accounts & segments ───────────────

@app.get("/api/accounts")
def api_accounts(monitored_only: bool = False):
    return db.list_accounts(monitored_only=monitored_only)


@app.post("/api/accounts/discover")
async def api_discover_accounts():
    if not has_valid_credentials():
        raise HTTPException(401, "Not authenticated")
    n = await asyncio.to_thread(sync_mod.discover_and_register_accounts)
    return {"discovered": n, "accounts": db.list_accounts()}


class AccountMonitorBody(BaseModel):
    monitored: bool


@app.put("/api/accounts/{property_id}/monitored")
def api_set_monitored(property_id: str, body: AccountMonitorBody):
    db.set_account_monitored(property_id, body.monitored)
    return {"ok": True, "property_id": property_id, "monitored": body.monitored}


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


# ─────────────── API: data ───────────────

@app.get("/api/metrics/daily")
def api_daily_metrics(property_ids: str, start: str, end: str):
    """Returns daily metrics for selected property_ids (comma-separated)."""
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    rows = db.query_daily_metrics(pids, start, end)
    return {"rows": rows, "count": len(rows)}


@app.get("/api/metrics/timeseries")
def api_timeseries(property_ids: str, metric: str, start: str, end: str):
    """Returns one series per property_id, indexed by date."""
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    rows = db.query_daily_metrics(pids, start, end)
    out: dict[str, dict[str, float]] = {pid: {} for pid in pids}
    for r in rows:
        d = r["date"]
        d_str = d.isoformat() if hasattr(d, "isoformat") else str(d)
        out[r["property_id"]][d_str] = float(r.get(metric, 0) or 0)
    accounts = {a["property_id"]: a for a in db.list_accounts()}
    series = []
    for pid in pids:
        series.append({
            "property_id": pid,
            "display_name": accounts.get(pid, {}).get("display_name", pid),
            "data": [{"date": d, "value": v} for d, v in sorted(out[pid].items())],
        })
    return {"metric": metric, "series": series}


@app.get("/api/metrics/channel")
def api_channel(property_ids: str, start: str, end: str):
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    return {"breakdown": db.query_channel_breakdown(pids, start, end)}


@app.get("/api/metrics/account_strip")
def api_account_strip(property_ids: str, start: str, end: str):
    """One row per account: KPIs + sparkline data."""
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    accounts = {a["property_id"]: a for a in db.list_accounts()}
    out = []
    for pid in pids:
        rows = db.query_daily_metrics([pid], start, end)
        if not rows:
            out.append({
                "property_id": pid,
                "display_name": accounts.get(pid, {}).get("display_name", pid),
                "currency": accounts.get(pid, {}).get("currency_code", ""),
                "no_data": True,
            })
            continue
        sessions = [r["sessions"] or 0 for r in rows]
        users = [r["users"] or 0 for r in rows]
        rev = [r["purchase_revenue"] or 0 for r in rows]
        conv = [r["conversions"] or 0 for r in rows]
        eng = [r["engaged_sessions"] or 0 for r in rows]

        s_total = sum(sessions); u_total = sum(users); r_total = sum(rev)
        c_total = sum(conv); e_total = sum(eng)
        cr = c_total / s_total * 100 if s_total else 0
        eng_rate = e_total / s_total * 100 if s_total else 0

        # Trend over period
        trend = az.linear_trend(az.to_series(rows, metric="sessions"))

        # YoY (use longer lookback)
        yoy_rows = db.query_daily_metrics([pid], days_ago(395), today_iso())
        yoy = az.yoy_change(az.to_series(yoy_rows, metric="sessions"), date.fromisoformat(end))

        # Compute health
        health = intel.compute_account_health(yoy_rows)

        out.append({
            "property_id": pid,
            "display_name": accounts.get(pid, {}).get("display_name", pid),
            "currency": accounts.get(pid, {}).get("currency_code", ""),
            "kpis": {
                "sessions": s_total,
                "users": u_total,
                "conversions": c_total,
                "revenue": r_total,
                "conv_rate": round(cr, 2),
                "engagement_rate": round(eng_rate, 1),
            },
            "trend": trend,
            "yoy_pct": yoy.get("pct_change"),
            "health_score": health.get("score"),
            "health_components": health.get("components", {}) if health.get("available") else {},
            "sparkline": [r["sessions"] or 0 for r in rows],
            "dates": [r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"]) for r in rows],
            "no_data": False,
        })
    return {"accounts": out}


@app.get("/api/metrics/segment_overview")
def api_segment_overview(segment: str, start: str, end: str):
    """Aggregate metrics + health for a segment over a period."""
    pids = db.accounts_in_segment(segment)
    if not pids:
        return {"available": False, "reason": "no_accounts_in_segment"}
    per_account = {pid: db.query_daily_metrics([pid], days_ago(395), today_iso())
                   for pid in pids}
    health = intel.compute_segment_health(segment, per_account)

    # Aggregate per-day across accounts
    aggregated_series = az.aggregate_segment_metrics(per_account, "sessions")
    series_data = []
    if len(aggregated_series):
        clipped = aggregated_series.loc[start:end]
        for d, v in clipped.items():
            series_data.append({"date": d.strftime("%Y-%m-%d"), "value": float(v)})

    return {
        "available": True,
        "segment": segment,
        "health": health,
        "aggregated_sessions": series_data,
        "n_accounts": len(pids),
    }


# ─────────────── API: insights, alerts, agents ───────────────

@app.get("/api/insights")
def api_insights(scope: str | None = None, scope_id: str | None = None,
                 insight_type: str | None = None, limit: int = 100):
    return {"insights": db.list_insights(scope, scope_id, insight_type, limit)}


@app.delete("/api/insights/{insight_id}")
def api_dismiss_insight(insight_id: int):
    db.dismiss_insight(insight_id)
    return {"ok": True}


@app.get("/api/alerts")
def api_alerts(unread_only: bool = False, limit: int = 50):
    return {"alerts": db.list_alerts(unread_only, limit)}


@app.put("/api/alerts/{alert_id}/read")
def api_mark_alert_read(alert_id: int):
    db.mark_alert_read(alert_id)
    return {"ok": True}


@app.get("/api/agents/activity")
def api_agent_activity(limit: int = 100):
    return {"activity": db.list_agent_activity(limit)}


@app.post("/api/agents/run/{agent_name}")
async def api_run_agent(agent_name: str):
    if agent_name == "all":
        result = await asyncio.to_thread(agents.run_full_analysis_cycle)
        return {"ok": True, "result": result}
    if agent_name not in agents.AGENTS:
        raise HTTPException(404, f"Unknown agent: {agent_name}")
    result = await asyncio.to_thread(agents.AGENTS[agent_name])
    return {"ok": True, "result": result}


@app.post("/api/sync/run")
async def api_run_sync(deep: bool = False):
    if not has_valid_credentials():
        raise HTTPException(401, "Not authenticated")
    results = await sync_mod.sync_all_async(deep=deep)
    return {"ok": True, "results": results}


@app.post("/api/sync/{property_id}")
async def api_sync_one(property_id: str, deep: bool = False):
    if not has_valid_credentials():
        raise HTTPException(401, "Not authenticated")
    result = await asyncio.to_thread(sync_mod.sync_property, property_id, deep)
    return result


# ─────────────── API: hypotheses ───────────────

class HypothesisBody(BaseModel):
    question: str
    scope: str = "global"
    scope_id: str = ""
    period_start: str | None = None
    period_end: str | None = None


@app.post("/api/hypothesis")
async def api_test_hypothesis(body: HypothesisBody):
    result = await asyncio.to_thread(
        agents.agent_test_hypothesis,
        body.question, body.scope, body.scope_id, body.period_start, body.period_end,
    )
    return result


@app.get("/api/hypothesis")
def api_list_hypotheses(limit: int = 50):
    return {"hypotheses": db.list_hypotheses(limit)}


# ─────────────── API: market health & briefing ───────────────

@app.get("/api/health/{segment_slug}")
def api_health(segment_slug: str, period_days: int = 30):
    latest = db.latest_health_score(segment_slug, period_days)
    history = db.health_score_history(segment_slug, period_days, limit=60)
    return {"latest": latest, "history": history}


@app.get("/api/briefing")
def api_briefing():
    b = db.latest_briefing()
    return {"briefing": b}


# ─────────────── API: correlations ───────────────

@app.get("/api/correlations/dow")
def api_dow(property_ids: str, start: str, end: str, metric: str = "sessions"):
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    rows = db.query_daily_metrics(pids, start, end)
    return cor.analyze_dow(rows, metric)


@app.get("/api/correlations/holidays")
def api_holidays(property_ids: str, start: str, end: str, metric: str = "sessions"):
    pids = [p.strip() for p in property_ids.split(",") if p.strip()]
    rows = db.query_daily_metrics(pids, start, end)
    return cor.analyze_holidays(rows, metric)


# ─────────────── DB info (where data is, what's in it) ───────────────

@app.get("/api/db/info")
def api_db_info():
    from config import SQLITE_DB_PATH, DUCKDB_PATH
    sqlite_size = SQLITE_DB_PATH.stat().st_size if SQLITE_DB_PATH.exists() else 0
    duckdb_size = DUCKDB_PATH.stat().st_size if DUCKDB_PATH.exists() else 0

    sqlite_tables = []
    with db.sqlite_conn() as c:
        for t in ["segments", "accounts", "account_segments", "insights", "market_health",
                  "alerts", "agent_activity", "hypotheses", "daily_briefings", "sync_log"]:
            try:
                n = c.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
                sqlite_tables.append({"table": t, "rows": n})
            except: pass

    duckdb_tables = []
    try:
        with db.get_duckdb() as conn:
            for t in ["daily_metrics", "channel_daily", "source_medium_daily",
                      "device_daily", "country_daily", "hourly_metrics"]:
                try:
                    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    duckdb_tables.append({"table": t, "rows": n})
                except: pass
    except: pass  # may be locked by sync

    return {
        "sqlite": {
            "path": str(SQLITE_DB_PATH),
            "size_bytes": sqlite_size,
            "size_mb": round(sqlite_size / 1024 / 1024, 2),
            "tables": sqlite_tables,
            "purpose": "Konfigurace + výstupy agentů (insights, health, briefings). Commitujeme do GitHubu.",
        },
        "duckdb": {
            "path": str(DUCKDB_PATH),
            "size_bytes": duckdb_size,
            "size_mb": round(duckdb_size / 1024 / 1024, 2),
            "tables": duckdb_tables,
            "purpose": "Surová GA4 data (sessions, kanály, zařízení, geo, hodinová). NEcommitujeme - lze regenerovat.",
        },
    }


# ─────────────── Run ───────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=APP_HOST, port=APP_PORT, log_level="info")
