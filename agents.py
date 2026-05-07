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
import analyzer as analyzer  # alias for briefing
import correlations as cor
from ga4_api import days_ago, today_iso

def _days_ago_str(n): return days_ago(n)
def _today_str(): return today_iso()

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
    """Compose detailed market read with best/worst account per segment."""
    run_id = db.start_agent_run("briefing")
    today = date.today().isoformat()
    try:
        segments = db.list_segments()
        seg_blocks = []
        critical_count = 0
        good_count = 0

        accounts_map = {a["property_id"]: a for a in db.list_accounts()}

        for seg in segments:
            if seg.get("account_count", 0) == 0:
                continue
            h = db.latest_health_score(seg["slug"])
            score = h["score"] if h else None
            verdict = h["verdict"] if h else "unknown"
            if verdict == "critical": critical_count += 1
            elif verdict in ("good", "excellent"): good_count += 1

            # For each segment compute YoY-like rankings: best riser, worst faller
            account_ids = db.accounts_in_segment(seg["slug"])
            movers = []
            for pid in account_ids:
                rows = db.query_daily_metrics([pid], analyzer.days_ago(60), analyzer.today_iso()) if hasattr(analyzer, 'days_ago') else db.query_daily_metrics([pid], _days_ago_str(60), _today_str())
                if len(rows) < 14: continue
                series = az.to_series(rows, metric="sessions")
                if len(series) < 14: continue
                cur7 = series.tail(7).sum()
                prev7 = series.iloc[-14:-7].sum()
                if prev7 < 5: continue
                pct = (cur7 - prev7) / prev7 * 100
                rev_series = az.to_series(rows, metric="purchase_revenue")
                cur_rev = float(rev_series.tail(7).sum()) if len(rev_series) else 0
                movers.append({
                    "pid": pid,
                    "name": accounts_map.get(pid, {}).get("display_name", pid),
                    "wow_pct": pct,
                    "cur_sessions": int(cur7),
                    "prev_sessions": int(prev7),
                    "cur_revenue": cur_rev,
                })

            if movers:
                movers.sort(key=lambda x: x["wow_pct"])
                worst = movers[0] if movers[0]["wow_pct"] < 0 else None
                best = movers[-1] if movers[-1]["wow_pct"] > 0 else None
                total_sessions = sum(m["cur_sessions"] for m in movers)
                total_rev = sum(m["cur_revenue"] for m in movers)
                rising = sum(1 for m in movers if m["wow_pct"] > 5)
                falling = sum(1 for m in movers if m["wow_pct"] < -5)

                lines = [f"\n{seg['icon']} **{seg['name']}** — Health: **{score}/100** ({intel.verdict_text(verdict)})"]
                lines.append(f"  • {len(movers)} aktivních účtů · WoW: {rising} rostou, {falling} padají, {len(movers)-rising-falling} stagnuje")
                lines.append(f"  • Týdenní součet: **{total_sessions:,}** sessions, **{total_rev:,.0f} Kč** tržby")
                if best:
                    lines.append(f"  • 📈 Nejlepší: **{best['name']}** ({best['wow_pct']:+.1f}% WoW, {best['cur_sessions']:,} sessions)")
                if worst:
                    lines.append(f"  • 📉 Nejhorší: **{worst['name']}** ({worst['wow_pct']:+.1f}% WoW, {worst['cur_sessions']:,} sessions)")
                seg_blocks.append("\n".join(lines))
            else:
                seg_blocks.append(f"\n{seg['icon']} **{seg['name']}** — {seg['account_count']} účtů, čekám na data")

        latest_alerts = db.list_alerts(limit=8)
        total_accounts = sum(s["account_count"] for s in segments if s.get("account_count", 0) > 0)
        active_segments = sum(1 for s in segments if s.get("account_count", 0) > 0)

        if not seg_blocks:
            headline = "Žádné segmenty s daty — přiřaď účty"
            body = 'Otevři **Účty** a klikni "+ přidat" u každého účtu.'
        else:
            if critical_count >= 2:
                headline = f"⚠️ {critical_count} segmentů v kritickém stavu"
            elif good_count >= max(1, active_segments // 2):
                headline = f"✅ Trh stabilní — {good_count} segmentů v dobré kondici"
            else:
                headline = f"🔍 Smíšené signály — {active_segments} segmentů aktivních"

            body_parts = [f"**Sledováno {active_segments} segmentů, {total_accounts} účtů**"]
            body_parts.extend(seg_blocks)
            if latest_alerts:
                body_parts.append("\n**🚨 Poslední kritické alerty:**")
                for a in latest_alerts[:8]:
                    body_parts.append(f"  • {a['title']}")
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
