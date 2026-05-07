"""Correlation analyses: day-of-week, hour-of-day, holidays, weather.
Reveals patterns hidden in the daily data.
"""
import logging
from datetime import date, datetime
from typing import Iterable

import numpy as np
import pandas as pd
import holidays

from config import HOLIDAY_COUNTRIES

logger = logging.getLogger("ga4.correlations")


def _to_df(rows: list[dict], date_col: str = "date") -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty: return df
    df[date_col] = pd.to_datetime(df[date_col])
    return df


# ─────────────── Day of week ───────────────

def analyze_dow(rows: list[dict], metric: str = "sessions") -> dict:
    """Average performance by day of week."""
    df = _to_df(rows)
    if df.empty or metric not in df.columns:
        return {"available": False}
    df["dow"] = df["date"].dt.dayofweek
    avg = df.groupby("dow")[metric].mean()
    overall = df[metric].mean()
    days = ["Pondělí", "Úterý", "Středa", "Čtvrtek", "Pátek", "Sobota", "Neděle"]
    out = []
    for i in range(7):
        if i in avg.index:
            v = avg[i]
            out.append({
                "day": days[i],
                "day_idx": i,
                "avg_value": float(v),
                "vs_overall_pct": float((v - overall) / overall * 100) if overall else 0.0,
            })
    best = max(out, key=lambda x: x["avg_value"]) if out else None
    worst = min(out, key=lambda x: x["avg_value"]) if out else None
    return {
        "available": True,
        "by_day": out,
        "best_day": best,
        "worst_day": worst,
        "metric": metric,
    }


# ─────────────── Hour of day ───────────────

def analyze_hod(hourly_rows: list[dict], metric: str = "sessions") -> dict:
    """Hourly performance pattern."""
    df = _to_df(hourly_rows)
    if df.empty or metric not in df.columns or "hour" not in df.columns:
        return {"available": False}
    avg = df.groupby("hour")[metric].mean()
    overall = df[metric].mean()
    out = []
    for h in range(24):
        if h in avg.index:
            v = avg[h]
            out.append({
                "hour": h,
                "avg_value": float(v),
                "vs_overall_pct": float((v - overall) / overall * 100) if overall else 0.0,
            })
    peak = max(out, key=lambda x: x["avg_value"]) if out else None
    return {
        "available": True,
        "by_hour": out,
        "peak_hour": peak,
        "metric": metric,
    }


# ─────────────── Holidays ───────────────

def get_holiday_dates(start_year: int, end_year: int, countries: list[str] = None) -> dict[str, list[str]]:
    """Returns {date_str: [country_codes]}."""
    countries = countries or HOLIDAY_COUNTRIES
    out: dict[str, list[str]] = {}
    for cc in countries:
        try:
            hol = holidays.country_holidays(cc, years=range(start_year, end_year + 1))
            for d, name in hol.items():
                key = d.isoformat()
                out.setdefault(key, []).append(f"{cc}: {name}")
        except Exception as e:
            logger.warning(f"holidays for {cc} failed: {e}")
    return out


def analyze_holidays(rows: list[dict], metric: str = "sessions") -> dict:
    """Compare metric on holidays vs non-holidays."""
    df = _to_df(rows)
    if df.empty: return {"available": False}
    years = (df["date"].dt.year.min(), df["date"].dt.year.max())
    hol_map = get_holiday_dates(years[0], years[1])
    df["is_holiday"] = df["date"].dt.strftime("%Y-%m-%d").isin(hol_map.keys())
    df["holiday_name"] = df["date"].dt.strftime("%Y-%m-%d").map(lambda d: ", ".join(hol_map.get(d, [])))

    h_avg = df[df["is_holiday"]][metric].mean()
    n_avg = df[~df["is_holiday"]][metric].mean()

    return {
        "available": True,
        "metric": metric,
        "holiday_avg": float(h_avg) if not pd.isna(h_avg) else None,
        "non_holiday_avg": float(n_avg) if not pd.isna(n_avg) else None,
        "pct_diff": float((h_avg - n_avg) / n_avg * 100) if n_avg else None,
        "individual_holidays": [
            {
                "date": r["date"].strftime("%Y-%m-%d"),
                "name": r["holiday_name"],
                "value": float(r[metric]),
                "vs_normal_pct": float((r[metric] - n_avg) / n_avg * 100) if n_avg else None,
            }
            for _, r in df[df["is_holiday"]].iterrows()
        ][-30:],  # most recent 30
    }


# ─────────────── Channel mix shifts ───────────────

def channel_mix_shift(channel_rows: list[dict], split_date: str) -> dict:
    """Compare channel composition before vs after split_date."""
    df = _to_df(channel_rows)
    if df.empty: return {"available": False}
    split = pd.Timestamp(split_date)
    pre = df[df["date"] < split].groupby("channel_group")["sessions"].sum()
    post = df[df["date"] >= split].groupby("channel_group")["sessions"].sum()
    pre_total = pre.sum() or 1
    post_total = post.sum() or 1
    out = []
    for ch in set(pre.index) | set(post.index):
        pre_share = pre.get(ch, 0) / pre_total * 100
        post_share = post.get(ch, 0) / post_total * 100
        out.append({
            "channel": ch,
            "pre_share_pct": float(pre_share),
            "post_share_pct": float(post_share),
            "delta_pct_pts": float(post_share - pre_share),
        })
    out.sort(key=lambda x: abs(x["delta_pct_pts"]), reverse=True)
    return {"available": True, "shifts": out, "split_date": split_date}


# ─────────────── Cross-segment co-movement ───────────────

def co_movement_strength(segment_health_history: dict[str, pd.Series]) -> dict:
    """How strongly do health scores of different segments move together?"""
    if len(segment_health_history) < 2:
        return {"available": False}
    df = pd.DataFrame(segment_health_history).dropna()
    if len(df) < 7: return {"available": False}
    corr = df.corr()
    # Average pairwise correlation (ex-diagonal)
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    avg = float(upper.stack().mean()) if not upper.stack().empty else None
    return {
        "available": True,
        "average_correlation": avg,
        "matrix": corr.to_dict(),
        "interpretation": (
            "wholeMarketMoving" if avg and avg > 0.6
            else "partialAlignment" if avg and avg > 0.3
            else "independentMovements"
        ),
    }
