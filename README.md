# GA4 Market Intelligence

Lokálna FastAPI appka na hĺbkovú analýzu **Google Analytics 4** dát naprieč mnohými účtami. Inšpirovaná `meta-ads-monitor`, ale pre GA4.

## Čo robí

- **Multi-account dashboard** — vyberieš si účty + obdobie + segment, dostaneš lineárny graf, account strip (1 riadok = 1 účet) s KPI a sparklines
- **Market Health Score** (0–100) per segment — composite skóre z YoY/MoM, conv rate, revenue, engagement, trend; verdict od *trh frčí* po *trh je v riti*
- **24/7 agenti** ktorí neustále skenujú dáta:
  - **Anomaly hunter** (10 min) — z-score na sessions/conversions/revenue
  - **Health scorer** (15 min) — Market Health pre každý segment
  - **Trend tracker** — lineárny trend na 30d
  - **Forecaster** — Holt-Winters predikcia 30 dní vpred
  - **Cross-account correlation** (2h) — nájde dvojice účtov ktoré sa hýbu spolu (trhový signál) alebo proti sebe (konkurencia)
  - **Pattern hunter** — DoW efekty, sviatky
  - **Channel shift detector** — flagne keď sa zmenil podiel kanálov
  - **Top movers** — top 10 najpohyblivejších účtov WoW
  - **Insight refiner** (6h) — deduplikuje a auto-dismissne staré
  - **Daily briefing** (08:00) — 1-stránkový "stav trhu"
- **Hypotézy** — spýtaš sa otázku, agent odpovie s evidenciou
- **Lokálna DB** (DuckDB + SQLite) — žiadne cloud, plná kontrola dát

## Rýchly štart

```bash
# 1. Inštalácia
cd ga4-market-intelligence
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. OAuth (jeden raz) — pripojí Google Analytics
# Vytvor Desktop OAuth client v https://console.cloud.google.com/apis/credentials
# Vlož client_id + secret do ~/.google_tokens.json (formát: { "default": { "client_id": "...", "client_secret": "..." }})
python3 oauth_setup.py

# 3. Zapni potrebné API v GCP
# https://console.developers.google.com/apis/api/analyticsadmin.googleapis.com/overview
# https://console.developers.google.com/apis/api/analyticsdata.googleapis.com/overview

# 4. Spustenie
python3 app.py
# → http://localhost:8060
```

## 24/7 mód (macOS)

```bash
./install_autostart.sh
```

Toto pridá macOS LaunchAgent ktorý:
- Spustí appku pri logine
- Auto-reštart pri páde
- Loguje do `data/launchagent.log`
- Agenti pracujú stále, kým je PC zapnutý

## Architektúra

```
config.py            ─ paths, intervaly, váhy Health Score
database.py          ─ SQLite (config + insights) + DuckDB (warehouse)
auth.py              ─ OAuth credentials cache + refresh
ga4_api.py           ─ GA4 Data API + Admin API klient
sync.py              ─ stiahne daily/channel/source/device/country/hourly metrics
analyzer.py          ─ z-score, trend, YoY/MoM, Holt-Winters, korelácie
correlations.py      ─ DoW, hour, holidays, channel mix shifts
intelligence.py      ─ Market Health Score, insight generators
agents.py            ─ základní agenti
agents_advanced.py   ─ pokročilí (correlation, pattern, refiner, top-movers)
auto_segment.py      ─ heuristika ktorá tagne účty k segmentom podľa názvu
app.py               ─ FastAPI hlavný app + background loops
templates/index.html ─ frontend (sidebar, view switching, filtre)
static/css/style.css ─ tmavý dashboard štýl
static/js/app.js     ─ Chart.js, fetch API, view rendering
```

## Endpointy

| Path | Účel |
|---|---|
| `GET  /` | dashboard UI |
| `GET  /api/status` | OAuth + počty |
| `GET  /api/accounts` | zoznam GA4 properties |
| `POST /api/accounts/discover` | znova pull zo zo Admin API |
| `PUT  /api/accounts/{id}/monitored` | zapni/vypni sync |
| `POST /api/accounts/{id}/segments` | priraď segment |
| `GET  /api/segments` | zoznam segmentov |
| `GET  /api/metrics/account_strip` | KPI + sparkline pre vybrané účty |
| `GET  /api/metrics/timeseries?metric=sessions&...` | denné série pre multi-line chart |
| `GET  /api/metrics/channel` | channel breakdown |
| `GET  /api/correlations/dow` | day-of-week pattern |
| `GET  /api/correlations/holidays` | holiday effect |
| `GET  /api/insights` | filtrovateľný feed insights |
| `GET  /api/alerts` | unread alerty |
| `POST /api/agents/run/{name}` | manuál spustenie agenta |
| `POST /api/sync/run` | manuál sync všetkých |
| `POST /api/hypothesis` | otestovanie hypotézy |
| `GET  /api/health/{segment}` | Market Health Score history |
| `GET  /api/briefing` | aktuálny daily briefing |

## Roadmap (čo treba dorobiť)

- Vertical/segment auto-classifier cez GA4 Industry Category (lepší než kľúčové slová)
- Weather correlation (reuse z meta-ads-monitor)
- Geographic heatmap
- Cross-segment co-movement (keď padne kola, padá aj sport?)
- Native LLM hypothesis answering cez Claude API key
- Slack/email alert sink
- Side-by-side účet komparátor
