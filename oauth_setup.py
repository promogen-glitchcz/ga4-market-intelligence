"""Interactive OAuth flow to obtain a refresh token with GA4 + Google Ads scopes.
Uses local server flow on port 8765. Saves the resulting token to ~/.google_tokens.json.
"""
import json
import sys
import webbrowser
from pathlib import Path

from google_auth_oauthlib.flow import Flow

TOKENS_PATH = Path.home() / ".google_tokens.json"
SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/adwords",
]
PORT = 8765
REDIRECT_URI = f"http://localhost:{PORT}/"


def main() -> int:
    creds = json.loads(TOKENS_PATH.read_text())["default"]
    client_config = {
        "installed": {
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [REDIRECT_URI],
        }
    }
    flow = Flow.from_client_config(client_config, SCOPES, redirect_uri=REDIRECT_URI)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )

    print(f"\nRedirect URI in use: {REDIRECT_URI}")
    print(f"\nIf Google rejects with redirect_uri_mismatch:")
    print(f"  -> Open https://console.cloud.google.com/apis/credentials")
    print(f"  -> Find your OAuth client (Web app)")
    print(f"  -> Add EXACTLY this URI to 'Authorized redirect URIs': {REDIRECT_URI}")
    print(f"  -> Save and wait 30s for propagation, then re-run\n")
    print(f"Opening browser...")

    # Local server to catch the redirect
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import urlparse, parse_qs

    received_code = {"code": None, "error": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            qs = parse_qs(urlparse(self.path).query)
            if "code" in qs:
                received_code["code"] = qs["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    b"<html><body style='font-family:system-ui;padding:60px;text-align:center'>"
                    b"<h1 style='color:#22c55e'>Hotovo!</h1>"
                    b"<p>Authorization complete. You can close this window.</p>"
                    b"</body></html>"
                )
            elif "error" in qs:
                received_code["error"] = qs.get("error_description", qs["error"])[0]
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f"Error: {received_code['error']}".encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *a, **kw): pass  # silence

    server = HTTPServer(("localhost", PORT), Handler)
    webbrowser.open(auth_url)
    print(f"If browser doesn't open, visit:\n{auth_url}\n")
    print(f"Listening on {REDIRECT_URI}...")
    while received_code["code"] is None and received_code["error"] is None:
        server.handle_request()
    server.server_close()

    if received_code["error"]:
        print(f"\nError: {received_code['error']}", file=sys.stderr)
        return 1

    flow.fetch_token(code=received_code["code"])
    cred = flow.credentials

    saved = json.loads(TOKENS_PATH.read_text())
    saved["default"]["refresh_token"] = cred.refresh_token
    saved["default"]["access_token"] = cred.token
    saved["default"]["scopes_granted"] = cred.scopes
    saved["default"]["expiry"] = cred.expiry.isoformat() if cred.expiry else None
    TOKENS_PATH.write_text(json.dumps(saved, indent=2))
    print(f"\nSaved new refresh token to {TOKENS_PATH}")
    print(f"Scopes: {cred.scopes}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
