"""Auto-segmentation: tag GA4 accounts to segments based on keywords in name/parent.
Heuristic, not bulletproof — user can override via UI.
"""
import logging
import re

import database as db

logger = logging.getLogger("ga4.auto_segment")


# Keyword → segment slug rules (Czech/Slovak/English combined)
RULES: list[tuple[str, str]] = [
    # kola / cycling
    (r"\b(kola|cycle|cycl|bike|ski|lyz|lyž|spot[-_]shop|cross[-_]?domain)\b", "kola"),
    # sport
    (r"\b(sport|fitness|trenirk|trenink|running|fitn|gym)\b", "sport"),
    # zahrada
    (r"\b(zahra|garden|drev|wood|palivov)\b", "zahrada"),
    # uklid
    (r"\b(uklid|clean|čisti|cisti)\b", "uklid"),
    # moda
    (r"\b(moda|fashion|jeans|underwear|stylestyle|kenvelo|baagl|bonek|byvm|under[-_]?armour)\b", "moda"),
    # potraviny / drink
    (r"\b(food|drink|rum|cokolada|kafe|kafest|cokol|janský|jansk[ýy])\b", "potraviny"),
    # elektro
    (r"\b(mobil|elektro|tech|digital[-_]?boss|pcmobil|profi[-_]?webyo|fotov)\b", "elektro"),
    # domacnost
    (r"\b(home|nábyt|nabyt|domác|stick|kanyl|kalisek|sevt|svit|kanc)\b", "domacnost"),
]


def classify_account(name: str, parent_name: str = "") -> list[str]:
    """Return list of segment slugs (lowercase) that match this account."""
    text = (name + " " + parent_name).lower()
    matches = set()
    for pattern, slug in RULES:
        if re.search(pattern, text, re.IGNORECASE):
            matches.add(slug)
    return list(matches) or ["ostatni"]


def auto_segment_all() -> dict:
    """Tag every account in DB. Returns counts per segment."""
    accounts = db.list_accounts()
    counts = {}
    for a in accounts:
        # Don't override existing tags
        if a.get("segments"): continue
        slugs = classify_account(a["display_name"], a.get("parent_account_name", ""))
        for slug in slugs:
            db.assign_segment(a["property_id"], slug)
            counts[slug] = counts.get(slug, 0) + 1
    logger.info(f"Auto-segmented: {counts}")
    return counts


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    db.init_sqlite()  # only sqlite, leave duckdb alone (running app holds lock)
    counts = auto_segment_all()
    total = sum(counts.values())
    print(f"\nAuto-tagged {total} account-segment pairs:")
    for slug, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {slug}: {n}")
