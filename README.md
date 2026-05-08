# Promogen Intelligence

Lokálna upload-based viewer GA4 dát naprieč všetkými účtami agentúry.

## Architektúra

**Dva nezávislé nástroje:**

1. **`ga4-export` skill** (Claude Code skill) — stiahne CSV z GA4 API
2. **Promogen Intelligence app** (FastAPI lokálka, port 8060) — viewer pre uploadovaný CSV

Žiadne automatické agentmi. Žiadne 24/7 background loops. Iba: vyexportuj → nahraj → prezri.

## Rýchly štart

### 1. Spustenie appky

```bash
cd ~/Downloads/claude/ga4-market-intelligence
source venv/bin/activate
python3 app.py
# → http://localhost:8060
```

### 2. Vyexportovanie GA4 dát (jeden raz, kedykoľvek chceš čerstvé dáta)

V Claude Code napíš jedno z:
- „export ga4"
- „stiahni ga4 data"
- „vyexportuj ga4 účty"

Skill spustí `python3 ~/.claude/skills/ga4-export/export.py` ktorý:
- Použije OAuth credentials z `~/.google_tokens.json`
- Stiahne všetky GA4 properties (Admin API)
- Pre každú vyžiada týždenné dáta za 56 týždňov (≈13 mesiacov)
- Metriky: **sessions, conversions, conv_rate**
- Uloží do `~/Downloads/ga4-weekly-YYYY-MM-DD.csv`

### 3. Nahranie CSV do appky

Otvor http://localhost:8060 → klikni „Nahrát CSV" v sidebare → drag&drop CSV súbor.
App parsne, uloží do SQLite, zobrazí dáta.

### 4. Použitie

- **Přehled:** Vyber účty alebo segment → vidíš KPI strip + trendový graf
- **Segmenty:** Klikni „Otevřít detail" na segmente → tabuľka účtov + graf súčtu
- **Účty:** Tabuľka všetkých účtov + filter + bulk reassignment do segmentov
- **Nahrát CSV:** Upload + history předchádzajúcich importov

## CSV formát

```csv
property_id,property_name,parent_account,week_start,sessions,conversions,conv_rate
267083701,sevt.cz – GA4,SEVT,2025-04-07,12345,234,1.89
267083701,sevt.cz – GA4,SEVT,2025-04-14,11200,198,1.77
...
```

Long-format: 1 riadok = 1 property × 1 týždeň. `week_start` je pondelok týždňa.

## Funkcionalita

| Vrstva | Čo umie |
|---|---|
| Filtre | Segment, Období (4/13/26/52 týdnů, YTD, all), výber účtov, YoY toggle |
| Account Strip | KPI + sparkline + 4-week trend % per účet |
| Trend graf | Multi-line chart, súčet, lineárny trendline, voliteľne YoY overlay |
| Segment detail | Súčet segmentu + per-účet krivky, YoY porovnanie |
| Bulk reassign | Označ viac účtov → preraď do iného segmentu (jeden klik) |

## Lokálne dáta

- `data/ga4_intel.db` — SQLite (segmenty, účty, importy, týždenné metriky)
- `data/uploads/` — záloha nahraných CSV súborov

## Stack

- **Backend:** FastAPI + SQLite (žiadny DuckDB, žiadne agentmi)
- **Frontend:** Vanilla JS + Chart.js + date-fns adapter
- **Skill:** Standalone Python script (urllib only, žiadne GA SDK)
- **Auth:** OAuth Desktop client v `~/.google_tokens.json`

## Endpointy

| Path | Účel |
|---|---|
| `GET /` | dashboard UI |
| `GET /api/status` | stav (počty + rozsah dát) |
| `POST /api/upload` | nahranie CSV |
| `GET /api/imports` | história importov |
| `DELETE /api/imports/{id}` | zmazanie importu + jeho dát |
| `POST /api/imports/reset` | wipe všetkých dát |
| `GET /api/accounts` | účty + ich segmenty |
| `GET /api/segments` | segmenty + počty účtov |
| `POST /api/accounts/bulk_assign` | bulk reassignment |
| `GET /api/data/timeseries` | týždenné metriky pre graf |
| `GET /api/data/account_strip` | KPI per účet |
| `GET /api/data/segment_rollup` | súčty + per-účet pre segment |

## OAuth setup (jeden raz)

```bash
python3 oauth_setup.py
```

Otvorí browser, prihlasiš sa do `google@promogen.cz`, povoliš scope `analytics.readonly`. Tokeny sa uložia do `~/.google_tokens.json`.

Ak Google Analytics Admin API ešte nie je zapnuté:
- https://console.developers.google.com/apis/api/analyticsadmin.googleapis.com/overview?project=964285332420
- https://console.developers.google.com/apis/api/analyticsdata.googleapis.com/overview?project=964285332420
