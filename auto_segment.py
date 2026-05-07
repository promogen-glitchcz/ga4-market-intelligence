"""Auto-segmentation: tag GA4 accounts to segments based on keywords in name/parent.
Heuristic, not bulletproof — user can override via UI.
"""
import logging
import re

import database as db

logger = logging.getLogger("ga4.auto_segment")


# Keyword → segment slug rules (Czech/Slovak/English combined)
RULES: list[tuple[str, str]] = [
    # Hry / gaming
    (r"\b(hern[ií]|hry|gaming|game|hraj)\b", "hry"),
    # papierníctvo / kanc. potreby
    (r"\b(papír|papir|sevt|pavlik|originalniknihy|tisk|print|stickies)\b", "papierenstvo"),
    # knihy
    (r"\b(kniha|knih|book|donativo|dobre[-_]?knih)\b", "knihy"),
    # auto / mobilita
    (r"\b(auto|esa|car|caravan|mobil[\W_]?aplikac|kola[-_]?radotin)\b", "auto"),
    # kola / cycling / ski
    (r"\b(cycle|cycl|bike|ski[\W_]|lyz|lyž|spot[-_]shop|cross[-_]?domain|skicen|skibi)\b", "kola"),
    # sport
    (r"\b(sport|fitness|trenirk|trening|trenink|running|fitn|gym|underarmour)\b", "sport"),
    # zahrada / drevo
    (r"\b(zahra|garden|drev|wood|palivov|nvdrev|nvpalivov)\b", "zahrada"),
    # uklid
    (r"\b(uklid|clean|čisti|cisti)\b", "uklid"),
    # tisk / print
    (r"\b(tisk|print|admasys|tisk[a-z]*)\b", "tisk"),
    # moda
    (r"\b(moda|fashion|jeans|underwear|stylestyle|kenvelo|baagl|bonek|byvm|enjoy[-_]?style|trenyrk|nedeto|exe[-_]?jeans|timeoutjeans)\b", "moda"),
    # potraviny / drink
    (r"\b(food|drink|rum|čokolád|cokolad|kafe|kafest|jansk[ýy]|24daysofrum|united[-_]?drinks|eateebowl|varime|dobroty)\b", "potraviny"),
    # elektro
    (r"\b(mobil|elektro|tech|digital[-_]?boss|pcmobil|profi[-_]?webyo|fotov|fotospin|fizual|premium[-_]?candles)\b", "elektro"),
    # detské
    (r"\b(děts|detsk|deti|dětsk|baagl|baby|minilove|chcipiska|ella[-_]?a[-_]?max|warehouse1|miláčk|milack)\b", "deti"),
    # kosmetika / zdraví
    (r"\b(kosmet|cosmet|nailzz|olivie|nafigate|bellocosm|bellagreen|nejenleky|aquapeeling|liftera|kanyl|destov|aquanix|ocni|kamyk|pragomed|profichondro|profifyto|prouro|probioform|galmed|warehouse[\W_]?1|menstr|modibodi)\b", "kosmetika"),
    # domacnost / nábytok
    (r"\b(home|nábyt|nabyt|domác|domalenka|svit|kanc|veneckyjanecek|cendulka|nefertitis|maluna|kalisek)\b", "domacnost"),
]


def classify_account(name: str, parent_name: str = "") -> list[str]:
    """Return list of segment slugs (lowercase) that match this account."""
    text = (name + " " + parent_name).lower()
    matches = set()
    for pattern, slug in RULES:
        if re.search(pattern, text, re.IGNORECASE):
            matches.add(slug)
    return list(matches) or ["ostatni"]


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
