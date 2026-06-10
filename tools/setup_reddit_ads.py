# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

#!/usr/bin/env python3
"""
Reddit Ads API local setup helper — Task 33.

Guides a local practitioner through obtaining Reddit Ads OAuth 2.0 credentials
and writes ~/reddit-ads.yaml for use by tools/reddit_ads_client.py (Simple Mode).

Usage:
    python tools/setup_reddit_ads.py

Prerequisites:
    pip install pyyaml httpx

What this script does:
    1. Prompts for Client ID and Client Secret from the Reddit Ads developer portal.
    2. Prompts for an optional Refresh Token (obtained via the Reddit developer portal
       or by completing a one-time authorization flow for ad account scopes).
    3. Prompts for ad account IDs (t2_xxx / a2_xxx prefix format — validated).
    4. Verifies connectivity by hitting the accounts list endpoint.
    5. Writes ~/reddit-ads.yaml with the credentials (chmod 600).
    6. Verifies that reddit-ads.yaml is listed in .gitignore and .claudeignore.

For Cloud Run / headless environments:
    Set REDDIT_ADS_CLIENT_ID, REDDIT_ADS_CLIENT_SECRET, REDDIT_ADS_REFRESH_TOKEN,
    and REDDIT_ADS_ACCOUNT_IDS as environment variables instead (see Full Mode
    documentation in tools/reddit_ads_client.py).

Reddit developer app registration:
    https://ads.reddit.com → Tools → API Access → Create App
    Scopes needed: ads:read, ads:write
    Type: script or web application (web preferred for refresh token support)
"""
from __future__ import annotations

import sys
from pathlib import Path

YAML_FILENAME = "reddit-ads.yaml"
_VALID_PREFIXES = ("t2_", "a2_")


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
        print(f"\n[setup_reddit_ads] Missing dependencies: {', '.join(missing)}")
        print(f"Install with: pip install {' '.join(missing)}")
        sys.exit(1)


# ── Git safeguard ──────────────────────────────────────────────────────────────

def _ensure_git_safeguards() -> None:
    repo_root = Path(__file__).parent.parent.resolve()
    for ignore_file in [".gitignore", ".claudeignore"]:
        ignore_path = repo_root / ignore_file
        if not ignore_path.exists():
            print(f"  [warn] {ignore_file} not found — skipping safeguard check.")
            continue
        content = ignore_path.read_text()
        if YAML_FILENAME not in content:
            with ignore_path.open("a") as f:
                f.write(f"\n# Reddit Ads local credentials — never commit\n{YAML_FILENAME}\n")
            print(f"  [safeguard] Added {YAML_FILENAME} to {ignore_file}. ✓")
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


def _section(title: str) -> None:
    print(f"\n{'─' * 60}\n  {title}\n{'─' * 60}")


# ── Account ID validation ──────────────────────────────────────────────────────

def _validate_account_id(account_id: str) -> bool:
    return bool(account_id) and any(account_id.startswith(p) for p in _VALID_PREFIXES)


# ── Connection verification ────────────────────────────────────────────────────

def _verify_connection(client_id: str, client_secret: str, refresh_token: str | None, account_ids: list[str]) -> bool:
    from base64 import b64encode

    import httpx

    credentials = b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "User-Agent":    "paid-media-agent:v1.0 (by u/setup-script)",
        "Content-Type":  "application/x-www-form-urlencoded",
    }
    data = (
        {"grant_type": "refresh_token", "refresh_token": refresh_token}
        if refresh_token
        else {"grant_type": "client_credentials"}
    )

    try:
        resp = httpx.post("https://www.reddit.com/api/v1/access_token",
                          headers=headers, data=data, timeout=20)
        body = resp.json()
        token = body.get("access_token", "")
        if not token:
            print(f"\n  [warn] Token exchange failed: {body}")
            return False

        print(f"\n  OAuth token obtained (expires_in: {body.get('expires_in', '?')}s) ✓")

        # Quick accounts probe
        if account_ids:
            aid = account_ids[0]
            probe = httpx.get(
                f"https://ads-api.reddit.com/api/v3/ad_accounts/{aid}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "User-Agent":    "paid-media-agent:v1.0 (by u/setup-script)",
                },
                timeout=15,
            )
            if probe.status_code == 200:
                print(f"  Ad account {aid} accessible. ✓")
            else:
                print(f"  [warn] Ad account probe returned {probe.status_code} — verify account ID.")
        return True
    except Exception as exc:
        print(f"\n  [warn] Connection verification failed: {exc}")
        return False


# ── YAML writer ────────────────────────────────────────────────────────────────

def _write_yaml(client_id: str, client_secret: str, refresh_token: str, account_ids: list[str]) -> Path:
    import yaml  # type: ignore[import]
    yaml_path = Path.home() / YAML_FILENAME
    config = {
        "client_id":     client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "account_ids":   account_ids,
    }
    with yaml_path.open("w") as f:
        yaml.dump(config, f, default_flow_style=False)
    yaml_path.chmod(0o600)
    return yaml_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "=" * 60)
    print("  Reddit Ads API — Local Setup (Simple Mode)")
    print("  Generates ~/reddit-ads.yaml for tools/reddit_ads_client.py")
    print("=" * 60)
    print("\n  This tool is for LOCAL use only.")
    print("  For Cloud Run / production, set REDDIT_ADS_CLIENT_ID,")
    print("  REDDIT_ADS_CLIENT_SECRET, REDDIT_ADS_REFRESH_TOKEN, and")
    print("  REDDIT_ADS_ACCOUNT_IDS as environment variables instead.\n")

    _check_deps()

    _section("Step 1 — Git safeguards")
    _ensure_git_safeguards()

    _section("Step 2 — App credentials")
    print("  Get credentials at: https://ads.reddit.com → Tools → API Access")
    print("  Create a 'script' or 'web application' type app.")
    print("  Required scopes: ads:read, ads:write\n")
    client_id     = _prompt("Client ID")
    client_secret = _prompt("Client Secret", secret=True)

    _section("Step 3 — Refresh Token (optional but recommended)")
    print("  A refresh token gives persistent access to ad account data.")
    print("  Obtain it from the Reddit Ads developer portal OAuth consent flow.")
    print("  Leave blank to use Client Credentials grant (limited scope).\n")
    refresh_token = _prompt("Refresh Token (press Enter to skip)", required=False)

    _section("Step 4 — Ad Account IDs")
    print("  Account IDs must begin with 't2_' or 'a2_'.")
    print("  Find them in Reddit Ads Manager → Account → Account ID.")
    print("  Enter multiple IDs separated by commas.\n")

    account_ids: list[str] = []
    while True:
        raw = _prompt("Account ID(s) (e.g. t2_abc123 or t2_abc123,t2_def456)")
        ids = [aid.strip() for aid in raw.split(",") if aid.strip()]
        invalid = [aid for aid in ids if not _validate_account_id(aid)]
        if invalid:
            print(f"  [error] Invalid account IDs (must start with t2_ or a2_): {invalid}")
            continue
        account_ids = ids
        break

    _section("Step 5 — Verifying connection")
    _verify_connection(client_id, client_secret, refresh_token or None, account_ids)

    _section("Step 6 — Writing ~/reddit-ads.yaml")
    yaml_path = _write_yaml(client_id, client_secret, refresh_token or "", account_ids)
    print(f"  Written to: {yaml_path}  (permissions: 600) ✓")

    print("\n" + "=" * 60)
    print("  Setup complete.")
    print(f"  reddit-ads.yaml: {yaml_path}")
    print("  The Reddit Ads client will use this file in Simple Mode.")
    print("\n  Next steps:")
    print("    1. Set REDDIT_ADS_ACCOUNT_ID in .env for a default account.")
    print("    2. Run a test extraction:")
    print("       python -c \"from tools.reddit_ads_client import _get_context; print(_get_context())\"")
    print("    3. Never commit ~/reddit-ads.yaml to version control.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
