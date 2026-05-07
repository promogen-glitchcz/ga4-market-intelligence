"""Project configuration - paths, API endpoints, defaults."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# Storage
SQLITE_DB_PATH = DATA_DIR / "ga4_intel.db"
DUCKDB_PATH = DATA_DIR / "ga4_warehouse.duckdb"

# Auth
TOKENS_PATH = Path.home() / ".google_tokens.json"

# OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/adwords",
]

# App
APP_NAME = "GA4 Market Intelligence"
APP_PORT = 8060
APP_HOST = "0.0.0.0"

# Sync cadences (seconds) — agents run continuously while PC is on
DATA_SYNC_INTERVAL = 60 * 30          # 30-min: pull fresh GA4 metrics
ANALYSIS_INTERVAL = 60 * 15           # 15-min: shallow analysis pass (anomaly + health)
DEEP_ANALYSIS_INTERVAL = 60 * 60 * 2  # 2h: deep cross-account + correlation discovery
DAILY_BRIEFING_HOUR = 8               # 08:00 local time
ALERT_CHECK_INTERVAL = 60 * 10        # 10-min: anomaly scan
INSIGHT_REFINE_INTERVAL = 60 * 60 * 6 # 6h: review old insights, suppress duplicates, boost confident ones
PATTERN_HUNT_INTERVAL = 60 * 60       # 1h: hunt for new patterns across segments

# Default windows
DEFAULT_LOOKBACK_DAYS = 90
DEEP_LOOKBACK_DAYS = 730              # 2y for seasonality

# Anomaly thresholds
ANOMALY_Z_SCORE = 2.5
HEALTH_SCORE_DROP_ALERT = 15          # alert if score drops 15+ pts week-over-week

# Holidays
HOLIDAY_COUNTRIES = ["CZ", "SK"]

# Default segment seeds (Matus typical verticals)
DEFAULT_SEGMENTS = [
    {"slug": "kola", "name": "Kola / cyklistika", "color": "#3b82f6", "icon": "🚴"},
    {"slug": "zahrada", "name": "Zahrada", "color": "#22c55e", "icon": "🌱"},
    {"slug": "uklid", "name": "Úklid / čištění", "color": "#06b6d4", "icon": "🧽"},
    {"slug": "domacnost", "name": "Domácnost / nábytek", "color": "#a855f7", "icon": "🏠"},
    {"slug": "elektro", "name": "Elektro / technika", "color": "#f59e0b", "icon": "💡"},
    {"slug": "moda", "name": "Móda / oděvy", "color": "#ec4899", "icon": "👕"},
    {"slug": "potraviny", "name": "Potraviny / nápoje", "color": "#84cc16", "icon": "🥗"},
    {"slug": "sport", "name": "Sport / fitness", "color": "#ef4444", "icon": "💪"},
    {"slug": "ostatni", "name": "Ostatní", "color": "#64748b", "icon": "📦"},
]

# Health score weights (must sum to 1.0)
HEALTH_WEIGHTS = {
    "sessions_yoy": 0.20,
    "sessions_mom": 0.15,
    "conv_rate_yoy": 0.20,
    "revenue_yoy": 0.20,
    "engagement_rate": 0.10,
    "trend_30d": 0.15,
}
