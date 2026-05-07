"""Quick OAuth + GA4 connectivity test.
Verifies the saved refresh token can fetch an access token and list GA4 properties.
"""
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

TOKENS_PATH = Path.home() / ".google_tokens.json"


def get_access_token() -> dict:
    creds = json.loads(TOKENS_PATH.read_text())["default"]
    data = urllib.parse.urlencode({
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": creds["refresh_token"],
        "grant_type": "refresh_token",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"OAuth HTTP {e.code}: {body}", file=sys.stderr)
        raise


def list_ga4_account_summaries(access_token: str) -> dict:
    req = urllib.request.Request(
        "https://analyticsadmin.googleapis.com/v1beta/accountSummaries?pageSize=200",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"Admin API HTTP {e.code}: {body}", file=sys.stderr)
        raise


if __name__ == "__main__":
    print("[1/2] Refreshing access token...")
    tok = get_access_token()
    print(f"  scopes granted: {tok.get('scope', '(none)')}")
    if "analytics" not in tok.get("scope", ""):
        print("  ! refresh token has NO analytics scope — need re-auth")
        sys.exit(2)
    print(f"  access_token len: {len(tok['access_token'])}, expires_in: {tok.get('expires_in')}s")

    print("\n[2/2] Listing GA4 account summaries...")
    summaries = list_ga4_account_summaries(tok["access_token"])
    accounts = summaries.get("accountSummaries", [])
    print(f"  found {len(accounts)} GA4 accounts")
    total_props = 0
    for a in accounts:
        props = a.get("propertySummaries", [])
        total_props += len(props)
        print(f"  - {a.get('displayName', '?')} ({a.get('account', '?')}): {len(props)} properties")
        for p in props[:5]:
            print(f"      • {p.get('displayName', '?')} ({p.get('property', '?')})")
        if len(props) > 5:
            print(f"      ... +{len(props) - 5} more")
    print(f"\nTOTAL: {len(accounts)} accounts, {total_props} properties")
