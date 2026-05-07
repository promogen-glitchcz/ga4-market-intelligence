"""Time-series analyzers: anomaly detection, trends, seasonality, forecasting.
Pure numpy/pandas - no scipy/prophet (Python 3.14 compatibility).
"""
import logging
import math
from datetime import datetime, timedelta, date
from typing import Iterable

import numpy as np
import pandas as pd

logger = logging.getLogger("ga4.analyzer")


# ─────────────── Helpers ───────────────

def safe_pct_change(current: float, previous: float) -> float:
    if previous == 0 or previous is None:
        return 0.0 if current == 0 else 100.0
    return (current - previous) / previous * 100.0


def to_series(rows: list[dict], date_col: str = "date", metric: str = "sessions") -> pd.Series:
    if not rows: return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col)[metric].astype(float)
    df = df.asfreq("D", fill_value=0)
    return df


# ─────────────── Anomaly detection ───────────────

def detect_anomalies_zscore(series: pd.Series, window: int = 28, z_thresh: float = 2.5) -> pd.DataFrame:
    """Z-score on rolling window; flags points with |z| > thresh."""
    if len(series) < window + 1:
        return pd.DataFrame(columns=["date", "value", "expected", "z_score", "direction"])
    rolling = series.rolling(window=window, min_periods=max(7, window // 2))
    mean = rolling.mean().shift(1)
    std = rolling.std().shift(1).replace(0, np.nan)
    z = (series - mean) / std
    anomalies = pd.DataFrame({
        "date": series.index,
        "value": series.values,
        "expected": mean.values,
        "z_score": z.values,
    })
    anomalies = anomalies.dropna()
    anomalies = anomalies[anomalies["z_score"].abs() > z_thresh]
    anomalies["direction"] = anomalies["z_score"].apply(lambda v: "up" if v > 0 else "down")
    return anomalies.reset_index(drop=True)


def detect_anomalies_iqr(series: pd.Series, window: int = 28) -> pd.DataFrame:
    """IQR-based detection - more robust to skewed distributions."""
    if len(series) < window + 1:
        return pd.DataFrame()
    rolling = series.rolling(window=window, min_periods=max(7, window // 2))
    q1 = rolling.quantile(0.25).shift(1)
    q3 = rolling.quantile(0.75).shift(1)
    iqr = (q3 - q1).replace(0, np.nan)
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    out = pd.DataFrame({
        "date": series.index, "value": series.values,
        "lower": lower.values, "upper": upper.values,
    }).dropna()
    out = out[(out["value"] < out["lower"]) | (out["value"] > out["upper"])]
    out["direction"] = out.apply(lambda r: "down" if r["value"] < r["lower"] else "up", axis=1)
    return out.reset_index(drop=True)


# ─────────────── Trend analysis ───────────────

def linear_trend(series: pd.Series) -> dict:
    """Simple linear regression on time series. Returns slope (per day), intercept, r2."""
    if len(series) < 7:
        return {"slope": 0.0, "intercept": 0.0, "r_squared": 0.0, "trend": "flat"}
    x = np.arange(len(series))
    y = series.values.astype(float)
    mask = ~np.isnan(y)
    if mask.sum() < 7:
        return {"slope": 0.0, "intercept": 0.0, "r_squared": 0.0, "trend": "flat"}
    x, y = x[mask], y[mask]
    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    avg = y.mean()
    pct_per_day = (slope / avg * 100) if avg > 0 else 0.0
    if pct_per_day > 0.5: trend = "rising"
    elif pct_per_day < -0.5: trend = "falling"
    else: trend = "flat"
    return {
        "slope": float(slope),
        "intercept": float(intercept),
        "r_squared": float(r2),
        "pct_per_day": float(pct_per_day),
        "trend": trend,
    }


def yoy_change(series: pd.Series, end_date: date | None = None, days: int = 30) -> dict:
    """Year-over-year comparison: last `days` vs same period 365 days earlier."""
    if end_date is None:
        end_date = series.index.max().date() if len(series) else date.today()
    end = pd.Timestamp(end_date)
    cur_start = end - pd.Timedelta(days=days - 1)
    prev_end = end - pd.Timedelta(days=365)
    prev_start = prev_end - pd.Timedelta(days=days - 1)

    cur = series.loc[cur_start:end].sum() if cur_start >= series.index.min() else None
    prev = series.loc[prev_start:prev_end].sum() if prev_start >= series.index.min() else None
    if cur is None or prev is None or prev == 0:
        return {"current": cur, "previous": prev, "pct_change": None, "available": False}
    return {
        "current": float(cur),
        "previous": float(prev),
        "pct_change": float(safe_pct_change(cur, prev)),
        "available": True,
    }


def mom_change(series: pd.Series, end_date: date | None = None, days: int = 30) -> dict:
    """Month-over-month: last `days` vs prior `days`."""
    if end_date is None:
        end_date = series.index.max().date() if len(series) else date.today()
    end = pd.Timestamp(end_date)
    cur_start = end - pd.Timedelta(days=days - 1)
    prev_end = cur_start - pd.Timedelta(days=1)
    prev_start = prev_end - pd.Timedelta(days=days - 1)

    cur = series.loc[cur_start:end].sum() if cur_start >= series.index.min() else None
    prev = series.loc[prev_start:prev_end].sum() if prev_start >= series.index.min() else None
    if cur is None or prev is None or prev == 0:
        return {"current": cur, "previous": prev, "pct_change": None, "available": False}
    return {
        "current": float(cur),
        "previous": float(prev),
        "pct_change": float(safe_pct_change(cur, prev)),
        "available": True,
    }


# ─────────────── Seasonality (weekly + monthly cycle) ───────────────

def weekly_seasonality(series: pd.Series) -> dict:
    """Average each weekday across all weeks. Returns Mon..Sun pattern."""
    if len(series) < 14: return {}
    df = pd.DataFrame({"value": series, "dow": series.index.dayofweek})
    avg_by_dow = df.groupby("dow")["value"].mean()
    overall = df["value"].mean()
    out = {}
    days = ["Po", "Út", "St", "Čt", "Pá", "So", "Ne"]
    for i, name in enumerate(days):
        if i in avg_by_dow.index:
            out[name] = {
                "avg": float(avg_by_dow[i]),
                "vs_overall_pct": float(safe_pct_change(avg_by_dow[i], overall)),
            }
    return out


def monthly_seasonality(series: pd.Series) -> dict:
    """Average each month across years."""
    if len(series) < 60: return {}
    df = pd.DataFrame({"value": series, "month": series.index.month})
    avg_by_month = df.groupby("month")["value"].mean()
    overall = df["value"].mean()
    months = ["Jan", "Feb", "Mar", "Apr", "Máj", "Jún", "Júl", "Aug", "Sep", "Okt", "Nov", "Dec"]
    out = {}
    for i, name in enumerate(months, 1):
        if i in avg_by_month.index:
            out[name] = {
                "avg": float(avg_by_month[i]),
                "vs_overall_pct": float(safe_pct_change(avg_by_month[i], overall)),
            }
    return out


def deseasonalize(series: pd.Series) -> pd.Series:
    """Remove weekly seasonality - useful before trend analysis."""
    if len(series) < 14: return series
    dow_mean = series.groupby(series.index.dayofweek).transform("mean")
    overall = series.mean()
    return series - dow_mean + overall


# ─────────────── Forecasting (Holt-Winters double exponential smoothing) ───────────────

def forecast_holt_winters(series: pd.Series, periods: int = 30, seasonal_period: int = 7) -> dict:
    """Triple exponential smoothing (additive). Returns forecast + 80% confidence interval."""
    if len(series) < seasonal_period * 2:
        return {"forecast": [], "available": False, "reason": "insufficient_history"}

    y = series.values.astype(float)
    n = len(y)

    # Initialize level, trend, seasonal
    L = y[:seasonal_period].mean()
    T = (y[seasonal_period:2*seasonal_period].mean() - y[:seasonal_period].mean()) / seasonal_period
    S = np.array([y[i] - L for i in range(seasonal_period)])

    # Smoothing parameters (reasonable defaults)
    alpha, beta, gamma = 0.4, 0.1, 0.3

    levels = []
    trends = []
    seasons = list(S)
    for t in range(n):
        s_idx = t - seasonal_period
        s_val = seasons[s_idx] if s_idx >= 0 else S[t % seasonal_period]
        new_L = alpha * (y[t] - s_val) + (1 - alpha) * (L + T)
        new_T = beta * (new_L - L) + (1 - beta) * T
        new_S = gamma * (y[t] - new_L) + (1 - gamma) * s_val
        levels.append(new_L)
        trends.append(new_T)
        seasons.append(new_S)
        L, T = new_L, new_T

    # Compute residuals for confidence interval
    fitted = []
    for t in range(n):
        s_idx = t - seasonal_period
        if s_idx >= 0:
            s_val = seasons[s_idx]
        else:
            s_val = S[t % seasonal_period]
        fit = (levels[t-1] if t > 0 else L) + (trends[t-1] if t > 0 else T) + s_val
        fitted.append(fit)
    residuals = y - np.array(fitted)
    sigma = float(np.std(residuals[seasonal_period:]))

    # Forecast
    forecast = []
    last_date = series.index[-1]
    for h in range(1, periods + 1):
        s_val = seasons[n + h - 1 - seasonal_period] if n + h - 1 - seasonal_period < len(seasons) else seasons[-(seasonal_period - (h - 1) % seasonal_period)]
        fc = L + h * T + s_val
        ci_width = 1.28 * sigma * math.sqrt(h)
        forecast.append({
            "date": (last_date + pd.Timedelta(days=h)).strftime("%Y-%m-%d"),
            "forecast": float(max(0, fc)),
            "lower": float(max(0, fc - ci_width)),
            "upper": float(max(0, fc + ci_width)),
        })

    return {
        "forecast": forecast,
        "available": True,
        "method": "holt_winters_additive",
        "sigma": sigma,
        "level": float(L),
        "trend_per_day": float(T),
    }


# ─────────────── Cross-account correlation ───────────────

def correlation_matrix(account_series: dict[str, pd.Series], min_overlap: int = 14) -> pd.DataFrame:
    """Pairwise Pearson correlation. Series must share dates."""
    if len(account_series) < 2:
        return pd.DataFrame()
    df = pd.DataFrame(account_series).dropna()
    if len(df) < min_overlap: return pd.DataFrame()
    return df.corr()


def co_movement(account_series: dict[str, pd.Series], min_overlap: int = 14) -> dict:
    """How aligned are accounts? Returns avg pairwise correlation."""
    corr = correlation_matrix(account_series, min_overlap)
    if corr.empty:
        return {"avg_correlation": None, "n_accounts": len(account_series)}
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    avg = float(upper.stack().mean()) if not upper.stack().empty else None
    return {
        "avg_correlation": avg,
        "n_accounts": len(account_series),
        "matrix": corr.to_dict(),
    }


# ─────────────── Aggregation utility ───────────────

def aggregate_segment_metrics(per_account: dict[str, list[dict]], metric: str = "sessions") -> pd.Series:
    """Sum metric across all accounts in segment, aligned by date."""
    series_dict = {pid: to_series(rows, metric=metric) for pid, rows in per_account.items()}
    if not series_dict: return pd.Series(dtype=float)
    df = pd.DataFrame(series_dict).fillna(0)
    return df.sum(axis=1)
