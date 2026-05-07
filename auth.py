"""Google OAuth helper - manages token refresh, returns Credentials."""
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.oauth2.credentials import Credentials

from config import TOKENS_PATH, SCOPES

logger = logging.getLogger("ga4.auth")

_lock = threading.Lock()
_cached_creds: Credentials | None = None
_cached_at: float = 0


def _load_tokens() -> dict:
    if not TOKENS_PATH.exists():
        raise RuntimeError(f"Tokens file missing: {TOKENS_PATH}. Run oauth_setup.py first.")
    return json.loads(TOKENS_PATH.read_text())["default"]


def _save_tokens(updates: dict):
    data = json.loads(TOKENS_PATH.read_text())
    data["default"].update(updates)
    TOKENS_PATH.write_text(json.dumps(data, indent=2))


def get_credentials(force_refresh: bool = False) -> Credentials:
    """Returns Google OAuth Credentials, refreshing access token when needed."""
    global _cached_creds, _cached_at

    with _lock:
        if not force_refresh and _cached_creds and (time.time() - _cached_at) < 50 * 60:
            return _cached_creds

        tok = _load_tokens()
        if not tok.get("refresh_token"):
            raise RuntimeError("No refresh token. Run oauth_setup.py first.")

        creds = Credentials(
            token=tok.get("access_token"),
            refresh_token=tok["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=tok["client_id"],
            client_secret=tok["client_secret"],
            scopes=tok.get("scopes_granted", SCOPES),
        )

        # Force refresh to get fresh token
        from google.auth.transport.requests import Request as GoogleRequest
        creds.refresh(GoogleRequest())

        _save_tokens({
            "access_token": creds.token,
            "expiry": creds.expiry.isoformat() if creds.expiry else None,
        })

        _cached_creds = creds
        _cached_at = time.time()
        logger.debug(f"Refreshed access token, expires {creds.expiry}")
        return creds


def access_token() -> str:
    return get_credentials().token


def has_valid_credentials() -> bool:
    try:
        if not TOKENS_PATH.exists(): return False
        tok = _load_tokens()
        return bool(tok.get("refresh_token"))
    except Exception:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    creds = get_credentials(force_refresh=True)
    print(f"Token: {creds.token[:30]}... (truncated)")
    print(f"Scopes: {creds.scopes}")
    print(f"Expires: {creds.expiry}")
