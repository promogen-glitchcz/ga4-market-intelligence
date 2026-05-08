"""Project configuration — paths, app port. The app is now upload-based:
no automatic GA4 syncing. Users upload CSVs from the ga4-export skill.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR = DATA_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

SQLITE_DB_PATH = DATA_DIR / "ga4_intel.db"

APP_NAME = "Promogen Intelligence"
APP_PORT = 8060
APP_HOST = "0.0.0.0"

# Default segments — user can add/remove via UI
DEFAULT_SEGMENTS = [
    {"slug": "kola", "name": "Kola / cyklistika", "color": "#3b82f6", "icon": "🚴"},
    {"slug": "zahrada", "name": "Zahrada", "color": "#22c55e", "icon": "🌱"},
    {"slug": "uklid", "name": "Úklid / čištění", "color": "#06b6d4", "icon": "🧽"},
    {"slug": "domacnost", "name": "Domácnost / nábytek", "color": "#a855f7", "icon": "🏠"},
    {"slug": "elektro", "name": "Elektro / technika", "color": "#f59e0b", "icon": "💡"},
    {"slug": "moda", "name": "Móda / oděvy", "color": "#ec4899", "icon": "👕"},
    {"slug": "potraviny", "name": "Potraviny / nápoje", "color": "#84cc16", "icon": "🥗"},
    {"slug": "sport", "name": "Sport / fitness", "color": "#ef4444", "icon": "💪"},
    {"slug": "papierenstvo", "name": "Papírnictví", "color": "#fbbf24", "icon": "📚"},
    {"slug": "tisk", "name": "Tisk / tiskárny", "color": "#7c3aed", "icon": "🖨️"},
    {"slug": "kosmetika", "name": "Kosmetika / zdraví", "color": "#f472b6", "icon": "💄"},
    {"slug": "deti", "name": "Děti", "color": "#fb923c", "icon": "👶"},
    {"slug": "auto", "name": "Auto / mobilita", "color": "#0ea5e9", "icon": "🚗"},
    {"slug": "knihy", "name": "Knihy", "color": "#8b5cf6", "icon": "📖"},
    {"slug": "hry", "name": "Hry / hraní", "color": "#10b981", "icon": "🎮"},
    {"slug": "nezarazeno", "name": "Nezařazeno", "color": "#475569", "icon": "❓"},
]
