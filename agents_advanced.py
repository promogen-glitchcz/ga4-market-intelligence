"""Advanced 24/7 agents — cross-account correlation, pattern hunter, insight refiner.
These run continuously alongside the basic agents, accumulating intelligence.
"""
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import numpy as np
import pandas as pd

import database as db
import analyzer as az
import correlations as cor
import intelligence as intel
from ga4_api import days_ago, today_iso

logger = logging.getLogger("ga4.agents.advanced")


# ─────────────── Agent: Cross-account correlation hunter ───────────────

def agent_cross_account_correlation() -> dict:
    """For each segment, find pairs of accounts that move together (or oppose).
    Strong correlations → market-wide signals; anti-correlations → competition."""
    run_id = db.start_agent_run("correlation")
    findings = 0
    try:
        segments = db.list_segments()
        for seg in segments:
            account_ids = db.accounts_in_segment(seg["slug"])
            if len(account_ids) < 2: continue
            series_dict = {}
            accounts = {a["property_id"]: a for a in db.list_accounts()}
            for pid in account_ids:
                rows = db.query_daily_metrics([pid], days_ago(60), today_iso())
                if len(rows) < 14: continue
                series_dict[pid] = az.to_series(rows, metric="sessions")
            if len(series_dict) < 2: continue

            corr = az.correlation_matrix(series_dict, min_overlap=14)
            if corr.empty: continue

            # Find strong pairs
            pids = list(corr.columns)
            for i, p1 in enumerate(pids):
                for p2 in pids[i+1:]:
                    val = corr.loc[p1, p2]
                    if pd.isna(val): continue
                    if abs(val) > 0.75:
                        n1 = accounts.get(p1, {}).get("display_name", p1)
                        n2 = accounts.get(p2, {}).get("display_name", p2)
                        kind = "spolu rastú/padajú" if val > 0 else "opačný pohyb"
                        sev = "info" if val > 0 else "positive"
                        title = f"[{seg['name']}] Silná korelácia: {n1} ↔ {n2} ({val:+.2f})"
                        body = (f"Účty sa silne {'-' if val>0 else 'anti-'}korelujú za posledných 60d "
                                f"(r={val:.3f}). {kind}. "
                                f"{'Pravdepodobne spoločný trhový signál' if val>0 else 'Možno si konkurujú v rovnakom segmente'}.")
                        db.save_insight({
                            "scope": "segment", "scope_id": seg["slug"],
                            "insight_type": "correlation",
                            "severity": sev,
                            "title": title, "body": body,
                            "confidence": min(1.0, abs(val)),
                            "metric": "sessions",
                            "period_start": days_ago(60),
                            "period_end": today_iso(),
                            "metadata": {"prop_a": p1, "prop_b": p2, "correlation": float(val),
                                         "name_a": n1, "name_b": n2},
                        })
                        findings += 1

        db.finish_agent_run(run_id, "success", findings,
                             f"Cross-account corr scanned {len(segments)} segments, {findings} strong pairs")
        return {"strong_pairs": findings}
    except Exception as e:
        logger.exception("Cross-account correlation failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Pattern hunter (DoW, holidays per segment) ───────────────

def agent_pattern_hunter() -> dict:
    """Find day-of-week patterns and holiday effects per segment, save as insights."""
    run_id = db.start_agent_run("pattern")
    findings = 0
    try:
        segments = db.list_segments()
        for seg in segments:
            account_ids = db.accounts_in_segment(seg["slug"])
            if not account_ids: continue
            rows = db.query_daily_metrics(account_ids, days_ago(180), today_iso())
            if len(rows) < 30: continue

            # DoW
            dow = cor.analyze_dow(rows, "sessions")
            if dow.get("available"):
                best = dow["best_day"]
                worst = dow["worst_day"]
                if best and worst and abs(best["vs_overall_pct"] - worst["vs_overall_pct"]) > 30:
                    db.save_insight({
                        "scope": "segment", "scope_id": seg["slug"],
                        "insight_type": "seasonal",
                        "severity": "info",
                        "title": f"[{seg['name']}] Týždenný pattern: najlepší {best['day']} (+{best['vs_overall_pct']:.1f}%), najhorší {worst['day']} ({worst['vs_overall_pct']:+.1f}%)",
                        "body": f"Day-of-week analýza za 180d: {best['day']} ({best['avg_value']:.0f} priem. sessions) výrazne nadpriemerný, {worst['day']} ({worst['avg_value']:.0f}) podpriemerný. Reklamné rozpočty plánuj podľa toho.",
                        "confidence": 0.75,
                        "metric": "sessions",
                        "period_end": today_iso(),
                        "metadata": dow,
                    })
                    findings += 1

            # Holidays
            hol = cor.analyze_holidays(rows, "sessions")
            if hol.get("available") and hol.get("pct_diff") is not None:
                pct = hol["pct_diff"]
                if abs(pct) > 15:
                    direction = "vyššia" if pct > 0 else "nižšia"
                    db.save_insight({
                        "scope": "segment", "scope_id": seg["slug"],
                        "insight_type": "seasonal",
                        "severity": "info",
                        "title": f"[{seg['name']}] Sviatky: aktivita {direction} o {abs(pct):.1f}%",
                        "body": f"Cez sviatky (CZ+SK) je traffic {direction} oproti bežným dňom o {pct:+.1f}%. "
                                f"Plánuj kampane podľa kalendára.",
                        "confidence": 0.7,
                        "metric": "sessions",
                        "period_end": today_iso(),
                        "metadata": {"pct_diff": pct, "holiday_avg": hol.get("holiday_avg"),
                                     "non_holiday_avg": hol.get("non_holiday_avg")},
                    })
                    findings += 1

        db.finish_agent_run(run_id, "success", findings, f"Pattern hunter found {findings} patterns")
        return {"patterns": findings}
    except Exception as e:
        logger.exception("Pattern hunter failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Channel mix shift detector ───────────────

def agent_channel_shift_detector() -> dict:
    """Detect when an account's channel mix has materially shifted."""
    run_id = db.start_agent_run("channel_shift")
    findings = 0
    try:
        accounts = db.list_accounts(monitored_only=True)
        from database import get_duckdb
        for a in accounts:
            with get_duckdb() as conn:
                rows = conn.execute(
                    """SELECT date, channel_group, sessions FROM channel_daily
                       WHERE property_id = ? AND date >= ?
                       ORDER BY date""",
                    [a["property_id"], days_ago(60)]
                ).fetchall()
            if len(rows) < 14: continue
            df = pd.DataFrame(rows, columns=["date", "channel_group", "sessions"])
            df["date"] = pd.to_datetime(df["date"])
            split = pd.Timestamp(date.today() - timedelta(days=14))
            pre = df[df["date"] < split].groupby("channel_group")["sessions"].sum()
            post = df[df["date"] >= split].groupby("channel_group")["sessions"].sum()
            pre_total = pre.sum() or 1
            post_total = post.sum() or 1
            for ch in set(pre.index) | set(post.index):
                pre_share = pre.get(ch, 0) / pre_total * 100
                post_share = post.get(ch, 0) / post_total * 100
                delta = post_share - pre_share
                if abs(delta) > 8:  # 8 percentage points shift
                    direction = "vzrástol" if delta > 0 else "klesol"
                    db.save_insight({
                        "scope": "account", "scope_id": a["property_id"],
                        "insight_type": "trend",
                        "severity": "warning" if abs(delta) > 15 else "info",
                        "title": f"{a['display_name']}: kanál {ch} {direction} o {abs(delta):.1f} bodov",
                        "body": f"Posledných 14d vs predtým: podiel kanálu {ch} {direction} z {pre_share:.1f}% na {post_share:.1f}%. Skontroluj konfiguráciu/kampane.",
                        "confidence": 0.7,
                        "metric": "channel_share",
                        "period_end": today_iso(),
                        "metadata": {"channel": ch, "pre_share": pre_share, "post_share": post_share, "delta": delta},
                    })
                    findings += 1
        db.finish_agent_run(run_id, "success", findings, f"Channel shift: {findings} flagged")
        return {"shifts": findings}
    except Exception as e:
        logger.exception("Channel shift detector failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Agent: Insight refiner (deduplicates + promotes) ───────────────

def agent_insight_refiner() -> dict:
    """Suppress near-duplicate insights, boost confidence on repeating patterns.
    Keeps the insight panel clean as the system runs 24/7.
    """
    run_id = db.start_agent_run("refine")
    actions = 0
    try:
        # Find duplicates: same scope+type+title in last 7 days
        with db.sqlite_conn() as c:
            rows = c.execute(
                """SELECT id, scope, scope_id, insight_type, title, created_at, confidence
                   FROM insights WHERE is_dismissed = 0
                     AND created_at > datetime('now', '-7 days')
                   ORDER BY scope, scope_id, insight_type, title, created_at"""
            ).fetchall()
            seen: dict[tuple, list[int]] = {}
            for r in rows:
                key = (r["scope"], r["scope_id"], r["insight_type"], r["title"])
                seen.setdefault(key, []).append(r["id"])
            for key, ids in seen.items():
                if len(ids) > 1:
                    # Keep the newest, mark older as dismissed
                    keep = ids[-1]
                    for old_id in ids[:-1]:
                        c.execute("UPDATE insights SET is_dismissed = 1 WHERE id = ?", (old_id,))
                        actions += 1
                    # Boost confidence on the one we kept (signal repeats = stronger)
                    c.execute(
                        "UPDATE insights SET confidence = MIN(1.0, confidence + ?) WHERE id = ?",
                        (0.05 * (len(ids) - 1), keep),
                    )

            # Auto-dismiss insights older than 30 days
            res = c.execute(
                "UPDATE insights SET is_dismissed = 1 "
                "WHERE is_dismissed = 0 AND created_at < datetime('now', '-30 days')"
            )
            actions += res.rowcount or 0

        db.finish_agent_run(run_id, "success", actions, f"Refined {actions} insight records")
        return {"refined": actions}
    except Exception as e:
        logger.exception("Insight refiner failed")
        db.finish_agent_run(run_id, "error", actions, "", str(e))
        raise


# ─────────────── Agent: Top movers ───────────────

def agent_top_movers() -> dict:
    """Identify accounts with the largest WoW changes (positive and negative)."""
    run_id = db.start_agent_run("top_movers")
    findings = 0
    try:
        accounts = db.list_accounts(monitored_only=True)
        movers = []
        for a in accounts:
            rows = db.query_daily_metrics([a["property_id"]], days_ago(14), today_iso())
            if len(rows) < 14: continue
            series = az.to_series(rows, metric="sessions")
            cur = series.tail(7).sum()
            prev = series.head(7).sum()
            if prev <= 10: continue
            pct = (cur - prev) / prev * 100
            movers.append({
                "property_id": a["property_id"], "display_name": a["display_name"],
                "wow_pct": pct, "current": cur, "previous": prev,
            })
        movers.sort(key=lambda x: abs(x["wow_pct"]), reverse=True)
        top = movers[:10]
        for m in top:
            if abs(m["wow_pct"]) < 25: continue
            sev = "warning" if m["wow_pct"] < -25 else "positive" if m["wow_pct"] > 25 else "info"
            db.save_insight({
                "scope": "account", "scope_id": m["property_id"],
                "insight_type": "trend",
                "severity": sev,
                "title": f"{m['display_name']}: WoW {m['wow_pct']:+.1f}%",
                "body": f"Posledný týždeň {m['current']:.0f} sessions vs {m['previous']:.0f} pred-týždeň ({m['wow_pct']:+.1f}%). Patrí do top 10 najpohyblivejších účtov.",
                "confidence": 0.7,
                "metric": "sessions",
                "period_end": today_iso(),
                "metadata": m,
            })
            findings += 1
        db.finish_agent_run(run_id, "success", findings, f"Top movers: {findings} accounts flagged")
        return {"top_movers": findings, "all_movers_count": len(movers)}
    except Exception as e:
        logger.exception("Top movers failed")
        db.finish_agent_run(run_id, "error", findings, "", str(e))
        raise


# ─────────────── Registry ───────────────

ADVANCED_AGENTS = {
    "correlation": agent_cross_account_correlation,
    "pattern": agent_pattern_hunter,
    "channel_shift": agent_channel_shift_detector,
    "refine": agent_insight_refiner,
    "top_movers": agent_top_movers,
}


def run_advanced_cycle() -> dict:
    out = {}
    for name, fn in ADVANCED_AGENTS.items():
        try: out[name] = fn()
        except Exception as e: out[name] = {"error": str(e)}
    return out
