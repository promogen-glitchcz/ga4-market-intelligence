"""Background data sync from GA4 -> DuckDB warehouse.
Pulls daily metrics + breakdowns for all monitored accounts.
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable

from ga4_api import GA4Client, days_ago, today_iso, list_account_summaries, get_property_details
from config import DEEP_LOOKBACK_DAYS, DEFAULT_LOOKBACK_DAYS
import database as db

logger = logging.getLogger("ga4.sync")


def discover_and_register_accounts() -> int:
    """Fetch all GA4 properties user has access to, save to DB."""
    summaries = list_account_summaries()
    n = 0
    for s in summaries:
        try:
            details = get_property_details(s["property_id"])
            db.upsert_account({
                "property_id": s["property_id"],
                "display_name": s["display_name"],
                "parent_account": s["parent_account"],
                "parent_account_name": s["parent_account_name"],
                "currency_code": details.get("currency_code", ""),
                "time_zone": details.get("time_zone", ""),
            })
            n += 1
        except Exception as e:
            logger.warning(f"  Failed to register {s['property_id']}: {e}")
    return n


def sync_property(property_id: str, deep: bool = False) -> dict:
    """Pull GA4 data for one property and store in DuckDB."""
    started = datetime.now(timezone.utc).isoformat()
    days = DEEP_LOOKBACK_DAYS if deep else DEFAULT_LOOKBACK_DAYS
    start = days_ago(days)
    end = today_iso()

    client = GA4Client()
    total_rows = 0
    error: str | None = None

    try:
        # Daily core metrics
        rows = client.daily_metrics(property_id, start, end)
        n = db.upsert_daily_metrics(rows)
        total_rows += n
        logger.info(f"  [{property_id}] daily: {n} rows")

        # Channel breakdown
        rows = client.channel_daily(property_id, start, end)
        n = db.upsert_dimension_daily("channel_daily", rows, ["channel_group"])
        total_rows += n
        logger.info(f"  [{property_id}] channel: {n} rows")

        # Source/medium
        rows = client.source_medium_daily(property_id, days_ago(min(days, 60)), end)
        n = db.upsert_dimension_daily("source_medium_daily", rows, ["source", "medium"])
        total_rows += n
        logger.info(f"  [{property_id}] source/medium: {n} rows")

        # Device
        rows = client.device_daily(property_id, start, end)
        n = db.upsert_dimension_daily("device_daily", rows, ["device_category"])
        total_rows += n
        logger.info(f"  [{property_id}] device: {n} rows")

        # Country
        rows = client.country_daily(property_id, days_ago(min(days, 60)), end)
        n = db.upsert_dimension_daily("country_daily", rows, ["country"])
        total_rows += n
        logger.info(f"  [{property_id}] country: {n} rows")

    except Exception as e:
        logger.exception(f"  [{property_id}] sync failed: {e}")
        error = str(e)

    finished = datetime.now(timezone.utc).isoformat()
    status = "success" if error is None else "error"
    db.log_sync(property_id, "full" if deep else "incremental", started, finished, total_rows, status, error)

    return {"property_id": property_id, "rows_synced": total_rows,
            "status": status, "error": error,
            "started": started, "finished": finished}


def sync_all_monitored(deep: bool = False) -> list[dict]:
    """Sync every monitored account."""
    accounts = db.list_accounts(monitored_only=True)
    logger.info(f"Syncing {len(accounts)} monitored accounts...")
    results = []
    for a in accounts:
        results.append(sync_property(a["property_id"], deep=deep))
    n_success = sum(1 for r in results if r["status"] == "success")
    logger.info(f"Sync complete: {n_success}/{len(accounts)} success")
    return results


async def sync_property_async(property_id: str, deep: bool = False) -> dict:
    return await asyncio.to_thread(sync_property, property_id, deep)


async def sync_all_async(deep: bool = False) -> list[dict]:
    accounts = db.list_accounts(monitored_only=True)
    tasks = [sync_property_async(a["property_id"], deep) for a in accounts]
    return await asyncio.gather(*tasks, return_exceptions=False)
