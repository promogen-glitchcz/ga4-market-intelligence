"""GA4 Data API + Admin API client.
Uses google-analytics-data SDK for data, raw HTTP for Admin API.
"""
import logging
from datetime import date, datetime, timedelta
from typing import Iterable

import requests
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, OrderBy, FilterExpression,
)

from auth import get_credentials, access_token

logger = logging.getLogger("ga4.api")

ADMIN_BASE = "https://analyticsadmin.googleapis.com/v1beta"


# ─────────────── Admin API (list properties) ───────────────

def list_account_summaries() -> list[dict]:
    """Returns all GA4 accounts + properties the user has access to."""
    tok = access_token()
    out: list[dict] = []
    page_token: str | None = None
    while True:
        url = f"{ADMIN_BASE}/accountSummaries?pageSize=200"
        if page_token: url += f"&pageToken={page_token}"
        r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
        r.raise_for_status()
        data = r.json()
        for account in data.get("accountSummaries", []):
            for prop in account.get("propertySummaries", []):
                out.append({
                    "property_id": prop["property"].split("/")[-1],
                    "property_resource": prop["property"],
                    "display_name": prop["displayName"],
                    "parent_account": account.get("name", ""),
                    "parent_account_name": account.get("displayName", ""),
                    "property_type": prop.get("propertyType", ""),
                })
        page_token = data.get("nextPageToken")
        if not page_token: break
    return out


def get_property_details(property_id: str) -> dict:
    """Fetch single property metadata (currency, time zone, etc.)."""
    tok = access_token()
    url = f"{ADMIN_BASE}/properties/{property_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    p = r.json()
    return {
        "property_id": property_id,
        "display_name": p.get("displayName", ""),
        "currency_code": p.get("currencyCode", ""),
        "time_zone": p.get("timeZone", ""),
        "create_time": p.get("createTime"),
        "industry_category": p.get("industryCategory", ""),
    }


# ─────────────── Data API client ───────────────

class GA4Client:
    def __init__(self):
        self._client: BetaAnalyticsDataClient | None = None

    @property
    def client(self) -> BetaAnalyticsDataClient:
        if self._client is None:
            self._client = BetaAnalyticsDataClient(credentials=get_credentials())
        return self._client

    def _run(self, property_id: str, dims: list[str], metrics: list[str],
             start: str, end: str, order_by: str | None = None,
             limit: int = 100_000) -> list[dict]:
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dims],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            limit=limit,
        )
        if order_by:
            req.order_bys = [OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name=order_by))]
        resp = self.client.run_report(req)
        rows = []
        for row in resp.rows:
            rec = {}
            for i, d in enumerate(dims):
                rec[d] = row.dimension_values[i].value
            for i, m in enumerate(metrics):
                v = row.metric_values[i].value
                try: rec[m] = float(v) if "." in v else int(v)
                except (ValueError, TypeError): rec[m] = v
            rows.append(rec)
        return rows

    # ── Daily core metrics ──
    def daily_metrics(self, property_id: str, start: str, end: str) -> list[dict]:
        rows = self._run(
            property_id,
            dims=["date"],
            metrics=["sessions", "totalUsers", "newUsers", "engagedSessions",
                    "bounceRate", "averageSessionDuration", "screenPageViews",
                    "conversions", "purchaseRevenue", "transactions"],
            start=start, end=end, order_by="date",
        )
        out = []
        for r in rows:
            d = r["date"]
            iso_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append({
                "property_id": property_id,
                "date": iso_date,
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("totalUsers", 0)),
                "new_users": int(r.get("newUsers", 0)),
                "engaged_sessions": int(r.get("engagedSessions", 0)),
                "bounce_rate": float(r.get("bounceRate", 0.0)),
                "avg_session_duration": float(r.get("averageSessionDuration", 0.0)),
                "screen_page_views": int(r.get("screenPageViews", 0)),
                "conversions": float(r.get("conversions", 0.0)),
                "purchase_revenue": float(r.get("purchaseRevenue", 0.0)),
                "transactions": int(r.get("transactions", 0)),
            })
        return out

    def channel_daily(self, property_id: str, start: str, end: str) -> list[dict]:
        rows = self._run(
            property_id,
            dims=["date", "sessionDefaultChannelGroup"],
            metrics=["sessions", "totalUsers", "conversions", "purchaseRevenue"],
            start=start, end=end,
        )
        out = []
        for r in rows:
            d = r["date"]
            iso_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append({
                "property_id": property_id,
                "date": iso_date,
                "channel_group": r.get("sessionDefaultChannelGroup", "(unknown)"),
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("totalUsers", 0)),
                "conversions": float(r.get("conversions", 0.0)),
                "revenue": float(r.get("purchaseRevenue", 0.0)),
            })
        return out

    def source_medium_daily(self, property_id: str, start: str, end: str) -> list[dict]:
        rows = self._run(
            property_id,
            dims=["date", "sessionSource", "sessionMedium"],
            metrics=["sessions", "totalUsers", "conversions", "purchaseRevenue"],
            start=start, end=end,
        )
        out = []
        for r in rows:
            d = r["date"]
            iso_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append({
                "property_id": property_id, "date": iso_date,
                "source": r.get("sessionSource", "(none)"),
                "medium": r.get("sessionMedium", "(none)"),
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("totalUsers", 0)),
                "conversions": float(r.get("conversions", 0.0)),
                "revenue": float(r.get("purchaseRevenue", 0.0)),
            })
        return out

    def device_daily(self, property_id: str, start: str, end: str) -> list[dict]:
        rows = self._run(
            property_id,
            dims=["date", "deviceCategory"],
            metrics=["sessions", "totalUsers", "conversions", "purchaseRevenue"],
            start=start, end=end,
        )
        out = []
        for r in rows:
            d = r["date"]
            iso_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append({
                "property_id": property_id, "date": iso_date,
                "device_category": r.get("deviceCategory", "(unknown)"),
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("totalUsers", 0)),
                "conversions": float(r.get("conversions", 0.0)),
                "revenue": float(r.get("purchaseRevenue", 0.0)),
            })
        return out

    def country_daily(self, property_id: str, start: str, end: str) -> list[dict]:
        rows = self._run(
            property_id,
            dims=["date", "country"],
            metrics=["sessions", "totalUsers", "purchaseRevenue"],
            start=start, end=end,
        )
        out = []
        for r in rows:
            d = r["date"]
            iso_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append({
                "property_id": property_id, "date": iso_date,
                "country": r.get("country", "(not set)"),
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("totalUsers", 0)),
                "revenue": float(r.get("purchaseRevenue", 0.0)),
            })
        return out

    def hourly_metrics(self, property_id: str, start: str, end: str) -> list[dict]:
        rows = self._run(
            property_id,
            dims=["date", "hour"],
            metrics=["sessions", "totalUsers", "conversions"],
            start=start, end=end,
        )
        out = []
        for r in rows:
            d = r["date"]
            iso_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append({
                "property_id": property_id, "date": iso_date,
                "hour": int(r.get("hour", 0)),
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("totalUsers", 0)),
                "conversions": float(r.get("conversions", 0.0)),
            })
        return out


def days_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).isoformat()


def today_iso() -> str:
    return date.today().isoformat()
