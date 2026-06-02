# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

#!/usr/bin/env python3
"""
Google Ads OAuth setup helper — Phase 4 (Task 21).

Guides a local practitioner through the OAuth 2.0 authorization grant flow,
exchanges the authorization code for a long-lived refresh token, and writes
~/google-ads.yaml for use by tools/google_ads_client.py (Simple Mode).

Usage:
    python tools/setup_google_ads.py

Prerequisites:
    pip install google-auth-oauthlib pyyaml

What this script does:
    1. Prompts for developer token, client ID, client secret, and optional
       manager account (MCC) ID.
    2. Opens a browser window to the Google authorization consent screen.
    3. Starts a local redirect server on localhost:8080 to capture the auth code.
    4. Exchanges the auth code for a refresh token via the token endpoint.
    5. Writes ~/google-ads.yaml with the credentials.
    6. Verifies that google-ads.yaml is listed in .gitignore and .claudeignore.

This script is for local use only. For Cloud Run / headless environments, set
the environment variables described in tools/google_ads_client.py (Full Mode).
"""
from __future__ import annotations

import os
import sys
import webbrowser
from pathlib import Path

# ── Dependency check ──────────────────────────────────────────────────────────

def _check_deps() -> None:
    missing = []
    try:
        import google_auth_oauthlib  # noqa: F401
    except ImportError:
        missing.append("google-auth-oauthlib")
    try:
        import yaml  # noqa: F401
    except ImportError:
        missing.append("pyyaml")
    if missing:
        print(f"\n[setup_google_ads] Missing dependencies: {', '.join(missing)}")
        print(f"Install with: pip install {' '.join(missing)}")
        sys.exit(1)


# ── Git safeguard ─────────────────────────────────────────────────────────────

YAML_FILENAME = "google-ads.yaml"
GITIGNORE_ENTRIES = [YAML_FILENAME]


def _ensure_git_safeguards() -> None:
    """
    Programmatically confirm google-ads.yaml is in .gitignore and .claudeignore.
    Adds entries if missing. Prints a clear warning if either file is not found.
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
                    f.write(f"\n# Google Ads local credentials — never commit\n{entry}\n")
                added.append(entry)

        if added:
            print(f"  [safeguard] Added to {ignore_file}: {', '.join(added)}")
        else:
            print(f"  [safeguard] {ignore_file}: {YAML_FILENAME} already listed. ✓")


# ── Prompt helpers ────────────────────────────────────────────────────────────

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


# ── OAuth flow ────────────────────────────────────────────────────────────────

OAUTH_SCOPES = ["https://www.googleapis.com/auth/adwords"]
REDIRECT_PORT = 8080


def _run_oauth_flow(client_id: str, client_secret: str) -> str:
    """
    Execute the OAuth 2.0 installed-app flow.
    Opens a browser to the consent screen. Starts a local server on
    localhost:8080 to capture the redirect and exchange for a refresh token.

    Returns: the refresh_token string.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore[import]

    client_config = {
        "installed": {
            "client_id":     client_id,
            "client_secret": client_secret,
            "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
            "token_uri":     "https://oauth2.googleapis.com/token",
            "redirect_uris": [f"http://localhost:{REDIRECT_PORT}"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=OAUTH_SCOPES)

    print(f"\n  Opening browser for Google authorization...")
    print(f"  If the browser does not open automatically, visit the URL printed below.\n")

    try:
        # run_local_server opens the browser and captures the auth code automatically.
        # open_browser=True triggers webbrowser.open() — same library, consistent behavior.
        credentials = flow.run_local_server(
            port=REDIRECT_PORT,
            open_browser=True,
            prompt="consent",
            access_type="offline",
        )
    except OSError as exc:
        if "Address already in use" in str(exc):
            print(f"\n  [error] Port {REDIRECT_PORT} is already in use.")
            print(f"  Stop any service using port {REDIRECT_PORT} and re-run this script.")
            sys.exit(1)
        raise

    if not credentials.refresh_token:
        print("\n  [error] No refresh token returned by Google.")
        print("  This can happen if you previously authorized this app and Google")
        print("  did not re-issue a refresh token. Fix: go to")
        print("  https://myaccount.google.com/permissions → revoke this app → re-run.")
        sys.exit(1)

    return credentials.refresh_token


# ── YAML writer ───────────────────────────────────────────────────────────────

def _write_yaml(
    developer_token: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    login_customer_id: str,
) -> Path:
    """Write ~/google-ads.yaml with the collected credentials."""
    import yaml  # type: ignore[import]

    yaml_path = Path.home() / YAML_FILENAME
    config: dict = {
        "developer_token": developer_token,
        "client_id":       client_id,
        "client_secret":   client_secret,
        "refresh_token":   refresh_token,
        "use_proto_plus":  True,
    }
    if login_customer_id:
        # Store as plain digits — no dashes
        config["login_customer_id"] = login_customer_id.replace("-", "")

    with yaml_path.open("w") as f:
        yaml.dump(config, f, default_flow_style=False)

    # Lock permissions: owner read/write only
    yaml_path.chmod(0o600)
    return yaml_path


# ── Verification ──────────────────────────────────────────────────────────────

def _verify_connection(yaml_path: Path) -> bool:
    """
    Attempt to instantiate a GoogleAdsClient from the written yaml and make
    a lightweight API call to confirm credentials are valid.
    """
    try:
        from google.ads.googleads.client import GoogleAdsClient  # type: ignore[import]
        from tools.google_ads_client import GOOGLE_ADS_API_VERSION
        client = GoogleAdsClient.load_from_storage(str(yaml_path), version=GOOGLE_ADS_API_VERSION)
        # CustomerService.list_accessible_customers is the standard connectivity probe
        customer_service = client.get_service("CustomerService")
        response = customer_service.list_accessible_customers()
        accounts = list(response.resource_names)
        print(f"\n  Connection verified. Accessible accounts: {len(accounts)}")
        for acc in accounts[:5]:
            print(f"    {acc}")
        if len(accounts) > 5:
            print(f"    ... and {len(accounts) - 5} more")
        return True
    except Exception as exc:
        print(f"\n  [warn] Connection verification failed: {exc}")
        print("  The yaml was written but the credentials may not be valid yet.")
        print("  Common causes: wrong developer_token, token not yet active,")
        print("  or account not linked to the OAuth app.")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "=" * 60)
    print("  Google Ads API — Local Setup (Simple Mode)")
    print("  Generates ~/google-ads.yaml for tools/google_ads_client.py")
    print("=" * 60)
    print("\n  This tool is for LOCAL use only.")
    print("  For Cloud Run / production, set env vars instead (see tools/google_ads_client.py).\n")

    _check_deps()

    # ── Step 1: Check git safeguards first ───────────────────────────────────
    _print_section("Step 1 — Git safeguards")
    _ensure_git_safeguards()

    # ── Step 2: Collect credentials ──────────────────────────────────────────
    _print_section("Step 2 — Credentials")
    print("  You will need:")
    print("    • Developer token (from your Google Ads Manager Account → API Center)")
    print("    • OAuth 2.0 Client ID and Secret (from Google Cloud Console → Credentials)")
    print("    • Optional: Manager account (MCC) ID if your token is MCC-scoped\n")

    developer_token    = _prompt("Developer token", secret=True)
    client_id          = _prompt("OAuth Client ID")
    client_secret      = _prompt("OAuth Client Secret", secret=True)
    login_customer_id  = _prompt("Manager (MCC) account ID (optional — press Enter to skip)", required=False)

    # ── Step 3: OAuth flow ───────────────────────────────────────────────────
    _print_section("Step 3 — OAuth authorization")
    print("  A browser window will open to Google's consent screen.")
    print("  Sign in with the Google account that has access to your Google Ads account.")
    print("  After you authorize the app, this script captures the refresh token automatically.\n")
    input("  Press Enter to open the browser...")

    refresh_token = _run_oauth_flow(client_id, client_secret)
    print("\n  Refresh token obtained. ✓")

    # ── Step 4: Write yaml ───────────────────────────────────────────────────
    _print_section("Step 4 — Writing ~/google-ads.yaml")
    yaml_path = _write_yaml(developer_token, client_id, client_secret, refresh_token, login_customer_id)
    print(f"  Written to: {yaml_path}  (permissions: 600 — owner read/write only) ✓")

    # ── Step 5: Verify ───────────────────────────────────────────────────────
    _print_section("Step 5 — Verifying connection")
    _verify_connection(yaml_path)

    # ── Done ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Setup complete.")
    print(f"  google-ads.yaml: {yaml_path}")
    print("  The Google Ads client will use this file automatically in Simple Mode.")
    print("\n  Next steps:")
    print("    1. Run `python -c \"from tools.google_ads_client import _check_credentials; _check_credentials()\"` to confirm.")
    print("    2. Set GOOGLE_ADS_CUSTOMER_ID in .env to your default advertiser account ID.")
    print("    3. Never commit ~/google-ads.yaml to version control.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
