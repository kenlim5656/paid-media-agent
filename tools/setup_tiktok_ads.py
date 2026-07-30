# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

#!/usr/bin/env python3
"""
TikTok Ads OAuth setup helper — Phase 3 (Task 20).

Guides a local practitioner through the TikTok Marketing API OAuth 2.0
authorization flow, exchanges the auth code for a long-lived access token,
and writes ~/tiktok-ads.yaml for use by tools/tiktok_ads_client.py (Simple Mode).

Usage:
    python tools/setup_tiktok_ads.py

Prerequisites:
    pip install pyyaml httpx

What this script does:
    1. Prompts for TikTok App ID and App Secret (from TikTok Ads Manager → API).
    2. Constructs the authorization URL and opens it in your browser.
    3. Starts a local redirect server on localhost:8080 to capture the auth_code.
    4. Exchanges the auth_code for a long-lived access token via the token endpoint.
    5. Lists accessible advertiser IDs returned by TikTok.
    6. Writes ~/tiktok-ads.yaml with the credentials (chmod 600).
    7. Verifies that tiktok-ads.yaml is listed in .gitignore and .claudeignore.

This script is for local use only. For Cloud Run / headless environments, set
TIKTOK_ACCESS_TOKEN and TIKTOK_ADVERTISER_IDS as environment variables instead
(see tools/tiktok_ads_client.py — Full Mode).

Token lifespan: TikTok long-term access tokens are valid for approximately 1 year.
Set a reminder to re-run this script before expiry.
"""
from __future__ import annotations

import http.server
import json
import secrets
import sys
import threading
import urllib.parse
import webbrowser
from pathlib import Path

YAML_FILENAME = "tiktok-ads.yaml"
REDIRECT_PORT = 8080
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/"

TIKTOK_AUTH_URL = "https://ads.tiktok.com/marketing_api/auth"
TIKTOK_TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
TIKTOK_ADVERTISER_INFO_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/"


# ── Dependency check ───────────────────────────────────────────────────────────

def _check_deps() -> None:
    missing = []
    try:
        import yaml  # noqa: F401
    except ImportError:
        missing.append("pyyaml")
    try:
        import httpx  # noqa: F401
    except ImportError:
        missing.append("httpx")
    if missing:
        print(f"\n[setup_tiktok_ads] Missing dependencies: {', '.join(missing)}")
        print(f"Install with: pip install {' '.join(missing)}")
        sys.exit(1)


# ── Git safeguard ──────────────────────────────────────────────────────────────

GITIGNORE_ENTRIES = [YAML_FILENAME]


def _ensure_git_safeguards() -> None:
    """
    Programmatically confirm tiktok-ads.yaml is in .gitignore and .claudeignore.
    Adds entries if missing.
    """
    repo_root = Path(__file__).parent.parent.resolve()

    for ignore_file in [".gitignore", ".claudeignore"]:
        ignore_path = repo_root / ignore_file
        if not ignore_path.exists():
            print(f"  [warn] {ignore_file} not found at {ignore_path} — skipping safeguard check.")
            continue

        content = ignore_path.read_text()
        added = []
        for entry in GITIGNORE_ENTRIES:
            if entry not in content:
                with ignore_path.open("a") as f:
                    f.write(f"\n# TikTok Ads local credentials — never commit\n{entry}\n")
                added.append(entry)

        if added:
            print(f"  [safeguard] Added to {ignore_file}: {', '.join(added)}")
        else:
            print(f"  [safeguard] {ignore_file}: {YAML_FILENAME} already listed. ✓")


# ── Prompt helpers ─────────────────────────────────────────────────────────────

def _prompt(label: str, secret: bool = False, required: bool = True) -> str:
    import getpass
    while True:
        value = (getpass.getpass if secret else input)(f"  {label}: ").strip()
        if value or not required:
            return value
        print("  [error] This field is required.")


def _print_section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Local redirect server ──────────────────────────────────────────────────────

class _AuthCodeCapture:
    """Thread-safe container for the captured auth_code."""
    def __init__(self) -> None:
        self.auth_code: str | None = None
        self.error: str | None = None
        self.done = threading.Event()


def _start_redirect_server(capture: _AuthCodeCapture) -> http.server.HTTPServer:
    """
    Start a one-shot HTTP server on localhost:REDIRECT_PORT.
    Captures the ?auth_code= query param from the TikTok redirect and signals done.
    """
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)

            if "auth_code" in params:
                capture.auth_code = params["auth_code"][0]
                body = (
                    b"<html><body><h2>TikTok authorization successful!</h2>"
                    b"<p>You can close this tab and return to the terminal.</p></body></html>"
                )
                self.send_response(200)
            elif "error" in params:
                capture.error = params.get("error_description", ["Unknown error"])[0]
                body = (
                    b"<html><body><h2>Authorization failed.</h2>"
                    b"<p>Check the terminal for details.</p></body></html>"
                )
                self.send_response(400)
            else:
                body = b"<html><body><p>Waiting for TikTok redirect...</p></body></html>"
                self.send_response(200)

            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(body)

            if capture.auth_code or capture.error:
                capture.done.set()

        def log_message(self, *args: object) -> None:
            pass  # suppress request logs

    try:
        server = http.server.HTTPServer(("localhost", REDIRECT_PORT), Handler)
    except OSError as exc:
        if "Address already in use" in str(exc):
            print(f"\n  [error] Port {REDIRECT_PORT} is already in use.")
            print(f"  Stop any service on port {REDIRECT_PORT} and re-run this script.")
            sys.exit(1)
        raise

    thread = threading.Thread(target=lambda: server.serve_forever(), daemon=True)
    thread.start()
    return server


# ── OAuth flow ─────────────────────────────────────────────────────────────────

def _build_auth_url(app_id: str, state: str) -> str:
    """Construct the TikTok authorization URL."""
    params = {
        "app_id":       app_id,
        "state":        state,
        "redirect_uri": REDIRECT_URI,
        "scope":        "user.info.basic,ad.read,ad.write,audience.write",
    }
    return f"{TIKTOK_AUTH_URL}?{urllib.parse.urlencode(params)}"


def _exchange_auth_code(
    app_id: str,
    app_secret: str,
    auth_code: str,
) -> dict:
    """
    Exchange the authorization code for an access token via the token endpoint.
    Returns the full data payload from the TikTok response.
    """
    import httpx

    resp = httpx.post(
        TIKTOK_TOKEN_URL,
        headers={"Content-Type": "application/json"},
        json={
            "app_id":     app_id,
            "secret":     app_secret,
            "auth_code":  auth_code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    code = body.get("code", -1)
    if code != 0:
        raise RuntimeError(
            f"Token exchange failed (code {code}): {body.get('message', 'unknown')}. "
            f"request_id: {body.get('request_id', 'n/a')}"
        )
    return body.get("data", {})


def _get_advertiser_list(access_token: str, app_id: str, app_secret: str) -> list[str]:
    """
    Retrieve the list of advertiser IDs accessible via this access token.
    Uses the advertiser/get/ endpoint with app-level credentials.
    """
    import httpx

    resp = httpx.get(
        TIKTOK_ADVERTISER_INFO_URL,
        headers={"Access-Token": access_token},
        params={
            "app_id": app_id,
            "secret": app_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    code = body.get("code", -1)
    if code != 0:
        print(f"  [warn] Could not fetch advertiser list (code {code}): {body.get('message')}. "
              "You can add advertiser IDs manually to ~/tiktok-ads.yaml.")
        return []
    data = body.get("data", {})
    return [str(adv.get("advertiser_id")) for adv in data.get("list", []) if adv.get("advertiser_id")]


# ── YAML writer ────────────────────────────────────────────────────────────────

def _write_yaml(
    access_token: str,
    app_id: str,
    app_secret: str,
    advertiser_ids: list[str],
) -> Path:
    """Write ~/tiktok-ads.yaml with the collected credentials."""
    import yaml  # type: ignore[import]

    yaml_path = Path.home() / YAML_FILENAME
    config = {
        "access_token":    access_token,
        "app_id":          app_id,
        "app_secret":      app_secret,
        "advertiser_ids":  advertiser_ids,
    }
    with yaml_path.open("w") as f:
        yaml.dump(config, f, default_flow_style=False)

    # Lock permissions: owner read/write only
    yaml_path.chmod(0o600)
    return yaml_path


# ── Verification ───────────────────────────────────────────────────────────────

def _verify_connection(access_token: str, advertiser_ids: list[str]) -> bool:
    """Make a lightweight API call to confirm the token is valid."""
    import httpx

    if not advertiser_ids:
        print("  [warn] No advertiser IDs to verify against. Skipping connection test.")
        return False

    advertiser_id = advertiser_ids[0]
    try:
        resp = httpx.get(
            "https://business-api.tiktok.com/open_api/v1.3/campaign/get/",
            headers={"Access-Token": access_token},
            params={
                "advertiser_id": advertiser_id,
                "page":          1,
                "page_size":     1,
            },
            timeout=20,
        )
        body = resp.json()
        code = body.get("code", -1)
        if code == 0:
            total = body.get("data", {}).get("page_info", {}).get("total_number", "unknown")
            print(f"\n  Connection verified for advertiser {advertiser_id}.")
            print(f"  Accessible campaigns: {total}")
            return True
        else:
            print(f"\n  [warn] Connection test returned code {code}: {body.get('message')}.")
            return False
    except Exception as exc:
        print(f"\n  [warn] Connection verification failed: {exc}")
        print("  The YAML was written but the credentials may not be fully active yet.")
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "=" * 60)
    print("  TikTok Ads Marketing API — Local Setup (Simple Mode)")
    print("  Generates ~/tiktok-ads.yaml for tools/tiktok_ads_client.py")
    print("=" * 60)
    print("\n  This tool is for LOCAL use only.")
    print("  For Cloud Run / production, set TIKTOK_ACCESS_TOKEN and")
    print("  TIKTOK_ADVERTISER_IDS as env vars instead.\n")

    _check_deps()

    # ── Step 1: Git safeguards ───────────────────────────────────────────────
    _print_section("Step 1 — Git safeguards")
    _ensure_git_safeguards()

    # ── Step 2: Collect credentials ──────────────────────────────────────────
    _print_section("Step 2 — App credentials")
    print("  You will need your TikTok Marketing API app credentials.")
    print("  Get them at: https://ads.tiktok.com/marketing_api/apps\n")
    print("  Create an app (or use an existing one) with these permissions:")
    print("    • user.info.basic")
    print("    • ad.read, ad.write")
    print("    • audience.write\n")
    print("  Add http://localhost:8080/ as a redirect URI in the app settings.\n")

    app_id = _prompt("App ID")
    app_secret = _prompt("App Secret", secret=True)

    # ── Step 3: OAuth flow ───────────────────────────────────────────────────
    _print_section("Step 3 — OAuth authorization")
    print("  A browser window will open to TikTok's consent screen.")
    print("  Log in with a TikTok account that has access to your Ads Manager.")
    print("  After you authorize the app, this script captures the auth code automatically.\n")
    input("  Press Enter to open the browser...")

    # Generate a random state value for CSRF protection
    state = secrets.token_urlsafe(16)
    auth_url = _build_auth_url(app_id, state)

    capture = _AuthCodeCapture()
    server = _start_redirect_server(capture)

    print(f"\n  Opening: {auth_url}\n")
    webbrowser.open(auth_url)

    print("  Waiting for TikTok redirect on localhost:8080 ...")
    capture.done.wait(timeout=300)   # 5-minute window
    server.shutdown()

    if capture.error:
        print(f"\n  [error] TikTok authorization failed: {capture.error}")
        sys.exit(1)

    if not capture.auth_code:
        print("\n  [error] No auth code received within 5 minutes.")
        print("  Ensure the redirect URI http://localhost:8080/ is configured in your app.")
        sys.exit(1)

    print("  Auth code received. ✓")

    # ── Step 4: Token exchange ───────────────────────────────────────────────
    _print_section("Step 4 — Exchanging auth code for access token")
    try:
        token_data = _exchange_auth_code(app_id, app_secret, capture.auth_code)
    except Exception as exc:
        print(f"\n  [error] Token exchange failed: {exc}")
        sys.exit(1)

    access_token = token_data.get("access_token", "")
    if not access_token:
        print("\n  [error] No access token in response. Check App ID / Secret and retry.")
        print(f"  Full response: {json.dumps(token_data, indent=2)}")
        sys.exit(1)

    print("  Access token obtained. ✓")

    # TikTok may return advertiser IDs directly in the token response
    advertiser_ids_from_token: list[str] = [
        str(aid) for aid in token_data.get("advertiser_ids", []) if aid
    ]

    # ── Step 5: Fetch advertiser list ────────────────────────────────────────
    _print_section("Step 5 — Fetching accessible advertiser accounts")
    advertiser_ids = _get_advertiser_list(access_token, app_id, app_secret)

    # Merge — token response and API list may differ
    all_ids = list(dict.fromkeys(advertiser_ids_from_token + advertiser_ids))

    if all_ids:
        print(f"\n  Accessible advertisers ({len(all_ids)}):")
        for aid in all_ids:
            print(f"    {aid}")
    else:
        print("\n  [warn] No advertiser IDs found automatically.")
        print("  You can add them manually to ~/tiktok-ads.yaml after setup.")
        manual = _prompt(
            "Enter advertiser ID(s) manually (comma-separated, or press Enter to skip)",
            required=False,
        )
        if manual:
            all_ids = [aid.strip() for aid in manual.split(",") if aid.strip()]

    # ── Step 6: Write YAML ───────────────────────────────────────────────────
    _print_section("Step 6 — Writing ~/tiktok-ads.yaml")
    yaml_path = _write_yaml(access_token, app_id, app_secret, all_ids)
    print(f"  Written to: {yaml_path}  (permissions: 600 — owner read/write only) ✓")

    # ── Step 7: Verify connection ────────────────────────────────────────────
    _print_section("Step 7 — Verifying API connection")
    _verify_connection(access_token, all_ids)

    # ── Done ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Setup complete.")
    print(f"  tiktok-ads.yaml: {yaml_path}")
    print("  The TikTok client will use this file automatically in Simple Mode.")
    print("\n  Next steps:")
    print("    1. Set TIKTOK_ADVERTISER_ID in .env to your primary advertiser ID.")
    print("    2. Run `python -c \"from tools.tiktok_ads_client import _get_context; print(_get_context())\"` to confirm.")
    print("    3. TikTok access tokens expire in ~1 year. Re-run this script before then.")
    print("    4. Never commit ~/tiktok-ads.yaml to version control.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
