"""Auto-segmentation: tag GA4 accounts to segments based on keywords in name/parent.
Heuristic, not bulletproof — user can override via UI.
"""
import logging
import re

import database as db

logger = logging.getLogger("ga4.auto_segment")


# Keyword → segment slug rules (Czech/Slovak/English combined)
RULES: list[tuple[str, str]] = [
    # Disabled — moje generic keyword rules robia príliš veľa falošných pozitív.
    # Lepšie je nechať agent_segment_discovery (cez GA4 Admin API industryCategory)
    # tagnúť čo vie, a zvyšok nechať v "nezarazeno" — užívateľ to bulk-presunie.
]


def classify_account(name: str, parent_name: str = "") -> list[str]:
    """Return list of segment slugs (lowercase) that match this account.
    With RULES empty, returns ["nezarazeno"] for everything — manual review needed."""
    text = (name + " " + parent_name).lower()
    matches = set()
    for pattern, slug in RULES:
        if re.search(pattern, text, re.IGNORECASE):
            matches.add(slug)
    return list(matches) or ["nezarazeno"]


def auto_segment_all(reclassify: bool = False) -> dict:
    """Tag every account in DB. If reclassify=True, override existing tags first.
    Returns counts per segment."""
    accounts = db.list_accounts()
    counts = {}
    for a in accounts:
        existing = a.get("segments") or []
        if existing and not reclassify:
            continue
        slugs = classify_account(a["display_name"], a.get("parent_account_name", ""))
        # If reclassifying, remove old "ostatni" tags so the more specific ones can replace it
        if reclassify and "ostatni" in existing and slugs != ["ostatni"]:
            db.remove_segment(a["property_id"], "ostatni")
        for slug in slugs:
            db.assign_segment(a["property_id"], slug)
            counts[slug] = counts.get(slug, 0) + 1
    logger.info(f"Auto-segmented: {counts}")
    return counts


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    db.init_sqlite()
    reclassify = "--reclassify" in sys.argv
    counts = auto_segment_all(reclassify=reclassify)
    total = sum(counts.values())
    print(f"\nAuto-tagged {total} account-segment pairs (reclassify={reclassify}):")
    for slug, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {slug}: {n}")
