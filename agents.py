"""Agent runner — autonomous analysis cycle.
Each agent type runs periodically, generates insights, stores in DB.
The 'Claude on PC' aspect: these are Python agents invoked on schedule.
For actual narrative reasoning we use the Claude Code session that started the app.
"""
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import database as db
import intelligence as intel
import analyzer as az
import correlations as cor
from ga4_api import days_ago, today_iso

logger = logging.getLogger("ga4.agents")


# ─────────────── Agent: Anomaly hunter ───────────────

def agent_anomaly_hunter(property_ids: list[str] | None = None) -> dict:
    """Scan for anomalies on every monitored account, save insights + alerts."""
    run_id = db.start_agent_run("anomaly")
    findings = 0
    summary_parts = []
    try:
        accounts = db.list_accounts(monitored_only=True)
        if property_ids:
            accounts = [a for a in accounts if a["property_id"] in property_ids]

        for a in accounts:
            rows = db.query_daily_metrics([a["property_id"]], days_ago(60), today_iso())
            if len(rows) < 30: continue
            ins_list = intel.generate_anomaly_insights(a["property_id"], rows, a["display_name"])
            for ins in ins_list:
                db.save_insight(ins)
                findings += 1
                if ins["severity"] == "critical":
                    db.save_alert({
                        "scope": "account", "scope_id": a["property_id"],
                        "alert_type": "anomaly", "severity": "critical",
                        "title": ins["title"], "message": ins["body"],
                        "metadata": ins.get("metadata", {}),
                    })
                    summary_parts.append(f"{a['display_name']}: critical anomaly")

        summary = f"Scanned {len(accounts)} accounts, found {findings} anomalies. " + "; ".join(summary_parts[:5])
        db.finish_agent_run(run_id, "success", findings, summary)
        return {"findings": findings, "scanned": len(accounts)}
    except Exception as e:
        logger.exception("Anomaly hunter failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Health scorer ───────────────

def agent_health_scorer() -> dict:
    """Compute Market Health Score for every segment."""
    run_id = db.start_agent_run("health")
    findings = 0
    try:
        segments = db.list_segments()
        scored = 0
        for seg in segments:
            account_ids = db.accounts_in_segment(seg["slug"])
            if not account_ids: continue
            account_data = {
                pid: db.query_daily_metrics([pid], days_ago(395), today_iso())
                for pid in account_ids
            }
            health = intel.compute_segment_health(seg["slug"], account_data)
            if not health.get("available"): continue
            db.save_health_score({
                "segment_slug": seg["slug"],
                "period_days": 30,
                "period_end": today_iso(),
                "score": health["score"],
                "verdict": health["verdict"],
                "components": health.get("components", {}),
                "accounts_in_segment": health.get("accounts_in_segment", 0),
                "accounts_declining": health.get("accounts_declining", 0),
                "summary": health.get("summary", ""),
            })
            for ins in intel.generate_insights_from_health(health):
                db.save_insight(ins)
                findings += 1
            scored += 1
        summary = f"Scored {scored}/{len(segments)} segments, generated {findings} insights"
        db.finish_agent_run(run_id, "success", findings, summary)
        return {"scored": scored, "insights": findings}
    except Exception as e:
        logger.exception("Health scorer failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Trend tracker ───────────────

def agent_trend_tracker() -> dict:
    run_id = db.start_agent_run("trend")
    findings = 0
    try:
        accounts = db.list_accounts(monitored_only=True)
        for a in accounts:
            rows = db.query_daily_metrics([a["property_id"]], days_ago(60), today_iso())
            if len(rows) < 30: continue
            for ins in intel.generate_trend_insights(a["property_id"], rows, a["display_name"]):
                db.save_insight(ins)
                findings += 1
        db.finish_agent_run(run_id, "success", findings, f"Tracked trends on {len(accounts)} accounts, {findings} insights")
        return {"insights": findings}
    except Exception as e:
        logger.exception("Trend tracker failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Forecaster ───────────────

def agent_forecaster() -> dict:
    run_id = db.start_agent_run("forecast")
    findings = 0
    try:
        accounts = db.list_accounts(monitored_only=True)
        for a in accounts:
            rows = db.query_daily_metrics([a["property_id"]], days_ago(180), today_iso())
            if len(rows) < 28: continue
            for ins in intel.generate_forecast_insights(a["property_id"], rows, a["display_name"]):
                db.save_insight(ins)
                findings += 1
        db.finish_agent_run(run_id, "success", findings, f"Generated {findings} forecasts")
        return {"forecasts": findings}
    except Exception as e:
        logger.exception("Forecaster failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Daily briefing ───────────────

def agent_daily_briefing() -> dict:
    """Compose 1-page market read for today, save to DB."""
    run_id = db.start_agent_run("briefing")
    today = date.today().isoformat()
    try:
        segments = db.list_segments()
        seg_lines = []
        critical_count = 0
        good_count = 0
        for seg in segments:
            h = db.latest_health_score(seg["slug"])
            if not h: continue
            score = h["score"]
            verdict = h["verdict"]
            if verdict == "critical": critical_count += 1
            elif verdict in ("good", "excellent"): good_count += 1
            seg_lines.append(f"  {seg['icon']} **{seg['name']}**: {score}/100 ({intel.verdict_text(verdict)})")

        total_accounts = sum(s["account_count"] for s in segments)
        latest_alerts = db.list_alerts(limit=5)

        if not seg_lines:
            headline = "Žiadne segmenty s dátami — pridaj účty a otaguj segmenty"
            body = "Stav: čaká sa na sync GA4 dát. Otvor /accounts a priraď účty segmentom."
        else:
            if critical_count >= 2:
                headline = f"⚠️ {critical_count} segmentov v kritickom stave"
            elif good_count >= max(1, len(segments) // 2):
                headline = f"✅ Trh stabilný — {good_count} segmentov v dobrej kondícii"
            else:
                headline = f"🔍 Zmiešané signály — {len(segments)} segmentov skenovaných"

            body_parts = ["**Stav segmentov:**"] + seg_lines
            if latest_alerts:
                body_parts.append("\n**Posledné alerty:**")
                for a in latest_alerts[:5]:
                    body_parts.append(f"  • {a['title']}")
            body_parts.append(f"\n**Celkom účtov pod monitoringom:** {total_accounts}")
            body = "\n".join(body_parts)

        db.save_briefing(today, headline, body, {
            "critical_segments": critical_count,
            "good_segments": good_count,
            "total_segments": len(segments),
            "total_accounts": total_accounts,
        })
        db.finish_agent_run(run_id, "success", 1, f"Briefing: {headline}")
        return {"headline": headline}
    except Exception as e:
        logger.exception("Daily briefing failed")
        db.finish_agent_run(run_id, "error", 0, "", str(e))
        raise


# ─────────────── Agent: Hypothesis tester ───────────────

def agent_test_hypothesis(question: str, scope: str = "global", scope_id: str = "",
                           period_start: str | None = None, period_end: str | None = None) -> dict:
    """Test a user-provided hypothesis using available data."""
    hyp_id = db.save_hypothesis(question, scope, scope_id, period_start, period_end)
    run_id = db.start_agent_run("hypothesis", scope, scope_id)
    try:
        period_start = period_start or days_ago(60)
        period_end = period_end or today_iso()

        # Naive analysis: pull data for scope, run trend + correlations
        if scope == "segment":
            account_ids = db.accounts_in_segment(scope_id)
        elif scope == "account":
            account_ids = [scope_id]
        else:
            accounts = db.list_accounts(monitored_only=True)
            account_ids = [a["property_id"] for a in accounts]

        evidence = {"period": [period_start, period_end], "accounts": account_ids}
        per_account = {}
        for pid in account_ids:
            rows = db.query_daily_metrics([pid], period_start, period_end)
            series = az.to_series(rows, metric="sessions")
            if len(series) < 7: continue
            trend = az.linear_trend(series)
            yoy = az.yoy_change(series)
            per_account[pid] = {
                "trend": trend["trend"],
                "pct_per_day": trend["pct_per_day"],
                "yoy_pct": yoy.get("pct_change"),
            }
        evidence["per_account"] = per_account

        # Compose answer
        falling = sum(1 for v in per_account.values() if v["trend"] == "falling")
        rising = sum(1 for v in per_account.values() if v["trend"] == "rising")
        n = len(per_account)
        if n == 0:
            answer = "Nedostatok dát pre vyhodnotenie."
            confidence = 0.0
        elif falling >= n * 0.7:
            answer = f"DATA POTVRDZUJÚ POKLES: {falling}/{n} účtov má klesajúci trend v období {period_start}..{period_end}."
            confidence = 0.8
        elif rising >= n * 0.7:
            answer = f"DATA UKAZUJÚ RAST: {rising}/{n} účtov rastie v období {period_start}..{period_end}."
            confidence = 0.8
        else:
            answer = f"ZMIEŠANÉ SIGNÁLY: {falling} padá, {rising} rastie, zvyšok stagnuje (z {n} účtov)."
            confidence = 0.5

        db.update_hypothesis(hyp_id, answer, evidence, confidence, "answered")
        db.finish_agent_run(run_id, "success", 1, answer[:200])
        return {"hypothesis_id": hyp_id, "answer": answer, "confidence": confidence,
                "evidence": evidence}
    except Exception as e:
        logger.exception("Hypothesis test failed")
        db.update_hypothesis(hyp_id, f"Error: {e}", {}, 0.0, "failed")
        db.finish_agent_run(run_id, "error", 0, "", str(e))
        raise


# ─────────────── Agent registry ───────────────

try:
    from agents_advanced import ADVANCED_AGENTS
except ImportError:
    ADVANCED_AGENTS = {}


AGENTS: dict[str, Callable] = {
    "anomaly": agent_anomaly_hunter,
    "health": agent_health_scorer,
    "trend": agent_trend_tracker,
    "forecast": agent_forecaster,
    "briefing": agent_daily_briefing,
    **ADVANCED_AGENTS,
}


def run_full_analysis_cycle() -> dict:
    """Run all stateless agents end-to-end. Used by scheduler + manual trigger."""
    out = {}
    for name, fn in AGENTS.items():
        try:
            out[name] = fn()
        except Exception as e:
            out[name] = {"error": str(e)}
    return out


def run_shallow_cycle() -> dict:
    """Fast pass: just anomaly + health + top movers. Runs every 15min."""
    fast = ["anomaly", "health", "top_movers"]
    out = {}
    for name in fast:
        if name in AGENTS:
            try: out[name] = AGENTS[name]()
            except Exception as e: out[name] = {"error": str(e)}
    return out


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) > 1:
        agent_name = sys.argv[1]
        if agent_name == "all":
            print(run_full_analysis_cycle())
        elif agent_name in AGENTS:
            print(AGENTS[agent_name]())
        else:
            print(f"Unknown agent. Available: {list(AGENTS.keys())} or 'all'")
    else:
        print(f"Available agents: {list(AGENTS.keys())} or 'all'")
