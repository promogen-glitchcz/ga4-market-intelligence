"""Market Health Score + intelligence/insight generation.
Composite score (0-100) per segment that captures market state.
"""
import logging
from datetime import date, datetime, timedelta
from typing import Iterable

import numpy as np
import pandas as pd

from config import HEALTH_WEIGHTS, DEFAULT_LOOKBACK_DAYS
import database as db
import analyzer as az
import correlations as cor

logger = logging.getLogger("ga4.intelligence")


# ─────────────── Health score ───────────────

def _normalize_metric(value: float, low: float, high: float) -> float:
    """Map value to 0..100. Above high=100, below low=0, linear in between."""
    if value is None or pd.isna(value): return 50.0
    if value <= low: return 0.0
    if value >= high: return 100.0
    return float((value - low) / (high - low) * 100)


def _normalize_pct_change(pct: float | None) -> float:
    """YoY/MoM pct change to score: -50% = 0, 0% = 50, +50% = 100."""
    if pct is None or pd.isna(pct): return 50.0
    return float(max(0, min(100, 50 + pct)))


def compute_account_health(daily_rows: list[dict], end_date: date | None = None) -> dict:
    """Score one account 0-100 based on YoY, MoM, trend, engagement."""
    if not daily_rows:
        return {"score": None, "available": False}

    sessions = az.to_series(daily_rows, metric="sessions")
    rev = az.to_series(daily_rows, metric="purchase_revenue")
    conv = az.to_series(daily_rows, metric="conversions")
    users = az.to_series(daily_rows, metric="users")
    eng = az.to_series(daily_rows, metric="engaged_sessions")

    if end_date is None and len(sessions):
        end_date = sessions.index.max().date()

    s_yoy = az.yoy_change(sessions, end_date)["pct_change"]
    s_mom = az.mom_change(sessions, end_date)["pct_change"]
    cr_yoy = None
    if len(sessions) and len(conv):
        cur_s = sessions.tail(30).sum() or 1
        cur_c = conv.tail(30).sum()
        prev_s = sessions.iloc[-395:-365].sum() if len(sessions) > 395 else None
        prev_c = conv.iloc[-395:-365].sum() if len(conv) > 395 else None
        if prev_s and prev_c:
            cr_now = cur_c / cur_s
            cr_prev = prev_c / prev_s if prev_s else 0
            cr_yoy = az.safe_pct_change(cr_now, cr_prev) if cr_prev else None
    rev_yoy = az.yoy_change(rev, end_date)["pct_change"]

    eng_rate = None
    if len(sessions) and len(eng):
        s_30 = sessions.tail(30).sum() or 1
        e_30 = eng.tail(30).sum()
        eng_rate = e_30 / s_30 * 100

    trend_30 = az.linear_trend(sessions.tail(30))

    components = {
        "sessions_yoy": _normalize_pct_change(s_yoy),
        "sessions_mom": _normalize_pct_change(s_mom),
        "conv_rate_yoy": _normalize_pct_change(cr_yoy) if cr_yoy is not None else 50.0,
        "revenue_yoy": _normalize_pct_change(rev_yoy),
        "engagement_rate": _normalize_metric(eng_rate, 30, 75) if eng_rate is not None else 50.0,
        "trend_30d": _normalize_pct_change(trend_30["pct_per_day"] * 30),
    }
    total = sum(components[k] * HEALTH_WEIGHTS[k] for k in HEALTH_WEIGHTS)

    return {
        "score": round(float(total), 1),
        "components": components,
        "raw": {
            "sessions_yoy_pct": s_yoy,
            "sessions_mom_pct": s_mom,
            "conv_rate_yoy_pct": cr_yoy,
            "revenue_yoy_pct": rev_yoy,
            "engagement_rate": eng_rate,
            "trend_30d_pct_per_day": trend_30["pct_per_day"],
        },
        "available": True,
    }


def verdict_from_score(score: float) -> str:
    if score is None: return "unknown"
    if score >= 80: return "excellent"
    if score >= 60: return "good"
    if score >= 40: return "fair"
    if score >= 20: return "poor"
    return "critical"


def verdict_text(verdict: str) -> str:
    return {
        "excellent": "Trh frčí — všetko nadpriemerne",
        "good": "Trh stabilný / mierne rastúci",
        "fair": "Trh kolíše — zmiešané signály",
        "poor": "Trh padá — väčšina účtov pod plán",
        "critical": "Trh je v riti — všetky účty výrazne zaostávajú",
        "unknown": "Nedostatok dát",
    }.get(verdict, "?")


# ─────────────── Segment health ───────────────

def compute_segment_health(segment_slug: str, account_data: dict[str, list[dict]],
                            period_days: int = 30) -> dict:
    """Aggregate health across all accounts in a segment."""
    if not account_data:
        return {
            "segment_slug": segment_slug,
            "score": None,
            "verdict": "unknown",
            "summary": "Nie sú priradené žiadne účty",
            "available": False,
        }

    account_scores = {}
    declining = 0
    for prop_id, rows in account_data.items():
        h = compute_account_health(rows)
        account_scores[prop_id] = h
        raw = h.get("raw", {}) if h.get("available") else {}
        if raw.get("sessions_yoy_pct") is not None and raw["sessions_yoy_pct"] < -10:
            declining += 1

    valid_scores = [v["score"] for v in account_scores.values() if v.get("available") and v["score"] is not None]
    if not valid_scores:
        return {
            "segment_slug": segment_slug,
            "score": None,
            "verdict": "unknown",
            "summary": "Nedostatok historických dát",
            "available": False,
        }

    # Aggregate components by mean
    aggregated_components = {}
    for k in HEALTH_WEIGHTS.keys():
        vals = [v["components"][k] for v in account_scores.values()
                if v.get("available") and k in v.get("components", {})]
        if vals: aggregated_components[k] = float(np.mean(vals))

    seg_score = float(np.mean(valid_scores))
    verdict = verdict_from_score(seg_score)
    n_accounts = len(account_data)

    # Build summary
    parts = []
    if declining == n_accounts and n_accounts >= 2:
        parts.append(f"Všetky {n_accounts}/{n_accounts} účtov padajú medziročne >10%")
    elif declining == 0 and seg_score > 60:
        parts.append(f"Žiadny z {n_accounts} účtov nie je v poklese")
    else:
        parts.append(f"{declining}/{n_accounts} účtov v medziročnom poklese >10%")

    co_mov = az.co_movement({pid: az.to_series(rows, metric="sessions").tail(60)
                             for pid, rows in account_data.items()})
    if co_mov.get("avg_correlation") is not None:
        ac = co_mov["avg_correlation"]
        if ac > 0.6: parts.append("vysoká koherencia – signál celého trhu")
        elif ac < 0.2: parts.append("účty sa hýbu nezávisle – problém je u jednotlivcov")

    summary = ". ".join(parts) + "."

    return {
        "segment_slug": segment_slug,
        "score": round(seg_score, 1),
        "verdict": verdict,
        "verdict_text": verdict_text(verdict),
        "components": aggregated_components,
        "accounts_in_segment": n_accounts,
        "accounts_declining": declining,
        "co_movement": co_mov,
        "per_account_scores": {pid: v["score"] for pid, v in account_scores.items() if v.get("available")},
        "summary": summary,
        "period_days": period_days,
        "available": True,
    }


# ─────────────── Insight generation ───────────────

def generate_insights_from_health(health: dict) -> list[dict]:
    """Turn a health computation into stored insights."""
    out = []
    if not health.get("available"): return out

    seg = health["segment_slug"]
    score = health["score"]
    end = date.today().isoformat()

    severity = (
        "critical" if score < 25
        else "warning" if score < 45
        else "info" if score < 70
        else "positive"
    )

    out.append({
        "scope": "segment",
        "scope_id": seg,
        "insight_type": "health_score",
        "severity": severity,
        "title": f"Market Health: {score}/100 ({verdict_text(health['verdict'])})",
        "body": health.get("summary", ""),
        "confidence": 0.9,
        "metric": "health_score",
        "period_end": end,
        "metadata": {"components": health.get("components", {}),
                     "accounts_declining": health.get("accounts_declining"),
                     "accounts_in_segment": health.get("accounts_in_segment")},
    })

    # Co-movement insight
    cm = health.get("co_movement", {})
    if cm.get("avg_correlation") is not None and cm.get("n_accounts", 0) >= 2:
        ac = cm["avg_correlation"]
        if ac > 0.7:
            out.append({
                "scope": "segment", "scope_id": seg,
                "insight_type": "correlation",
                "severity": "info",
                "title": f"Vysoká koherencia účtov v segmente ({ac:.2f})",
                "body": f"Účty sa hýbu spolu — pravdepodobne ide o trhový signál, nie individuálne problémy. {cm['n_accounts']} účtov, priemerná korelácia {ac:.2f}.",
                "confidence": 0.8,
                "metric": "co_movement",
                "period_end": end,
                "metadata": cm,
            })

    return out


def generate_anomaly_insights(property_id: str, daily_rows: list[dict], display_name: str) -> list[dict]:
    """Detect anomalies on key metrics, return insight dicts."""
    out = []
    end = date.today().isoformat()
    for metric_label, metric in [("sessions", "sessions"), ("conversions", "conversions"),
                                  ("revenue", "purchase_revenue")]:
        series = az.to_series(daily_rows, metric=metric)
        if len(series) < 30: continue
        anomalies = az.detect_anomalies_zscore(series.tail(60), window=21, z_thresh=2.5)
        for _, a in anomalies.iterrows():
            direction = "▼ pokles" if a["direction"] == "down" else "▲ vzostup"
            severity = "critical" if abs(a["z_score"]) > 3.5 else "warning"
            out.append({
                "scope": "account", "scope_id": property_id,
                "insight_type": "anomaly",
                "severity": severity if a["direction"] == "down" else "info",
                "title": f"{display_name}: anomália {direction} v {metric_label} ({a['date'].strftime('%Y-%m-%d')})",
                "body": (f"Hodnota {a['value']:.0f} vs očakávané {a['expected']:.0f} "
                         f"(z-score {a['z_score']:.2f}). Odchýlka {((a['value']-a['expected'])/a['expected']*100):+.1f}%."),
                "confidence": min(1.0, abs(a["z_score"]) / 4),
                "metric": metric,
                "period_start": a["date"].strftime("%Y-%m-%d"),
                "period_end": a["date"].strftime("%Y-%m-%d"),
                "metadata": {"z_score": float(a["z_score"]),
                             "value": float(a["value"]),
                             "expected": float(a["expected"])},
            })
    return out


def generate_trend_insights(property_id: str, daily_rows: list[dict], display_name: str) -> list[dict]:
    """Trend analysis insights."""
    out = []
    end = date.today().isoformat()
    series = az.to_series(daily_rows, metric="sessions")
    if len(series) < 30: return out
    trend = az.linear_trend(series.tail(30))
    pct_30 = trend["pct_per_day"] * 30
    if abs(pct_30) > 15 and trend["r_squared"] > 0.3:
        direction = "rastie" if trend["trend"] == "rising" else "padá"
        sev = "warning" if trend["trend"] == "falling" and pct_30 < -25 else "info"
        out.append({
            "scope": "account", "scope_id": property_id,
            "insight_type": "trend",
            "severity": sev,
            "title": f"{display_name}: 30d trend — {direction} {pct_30:+.1f}%",
            "body": (f"Lineárny trend posledných 30 dní: {pct_30:+.1f}% (R²={trend['r_squared']:.2f}). "
                     f"Sklon {trend['pct_per_day']:+.2f}% / deň."),
            "confidence": min(1.0, trend["r_squared"]),
            "metric": "sessions",
            "period_end": end,
            "metadata": trend,
        })
    return out


def generate_forecast_insights(property_id: str, daily_rows: list[dict], display_name: str) -> list[dict]:
    """Forecast next 30 days, save as insight."""
    out = []
    series = az.to_series(daily_rows, metric="sessions")
    if len(series) < 28: return out
    fc = az.forecast_holt_winters(series, periods=30)
    if not fc.get("available"): return out
    fc_total = sum(p["forecast"] for p in fc["forecast"])
    last_30 = series.tail(30).sum()
    delta_pct = az.safe_pct_change(fc_total, last_30) if last_30 else 0
    out.append({
        "scope": "account", "scope_id": property_id,
        "insight_type": "forecast",
        "severity": "warning" if delta_pct < -15 else "info",
        "title": f"{display_name}: forecast 30d → {fc_total:,.0f} sessions ({delta_pct:+.1f}% vs posledných 30d)",
        "body": f"Holt-Winters forecast: {fc_total:,.0f} sessions za nasledujúcich 30 dní. "
                f"Trend per day: {fc['trend_per_day']:+.2f}.",
        "confidence": 0.65,
        "metric": "sessions",
        "period_end": fc["forecast"][-1]["date"],
        "metadata": {"forecast_points": fc["forecast"][:10],  # sample
                     "method": fc.get("method")},
    })
    return out
