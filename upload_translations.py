#!/usr/bin/env python3
"""
Upload translations from a CSV to the Shopify store via the Admin API.

Reads a CSV with columns: resource_type, resource_id, resource_handle, field_key,
digest, plus one column per target locale (from config.json). Pushes target-locale
columns to the store using translationsRegister (requires write_translations scope).
Source locale and other extra columns are not sent.

Uses SHOPIFY_SHOP, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET (or NEW_* equivalents) from .env.

Rate limits: GraphQL Admin API uses a cost bucket. Optional delay between requests
and retries with backoff on 429. Use --delay to throttle.

Usage:
    python upload_translations.py <translation_entries.csv>
    python upload_translations.py --dry-run <translation_entries.csv>
    python upload_translations.py --delay 0.5 <translation_entries.csv>
"""

import csv
import os
import sys
import time
from pathlib import Path

import requests
from typing import List, Dict, Optional, Tuple, Any

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# Default delay between API calls (seconds) to reduce rate limit risk
DEFAULT_DELAY_SECONDS = 0.2
# Retry on 429: wait this long, then double each time (capped at 32s)
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 32.0
MAX_RETRIES_429 = 5


def get_access_token(shop: str, client_id: str, client_secret: str) -> Optional[str]:
    token_url = f"https://{shop}/admin/oauth/access_token"
    try:
        response = requests.post(
            token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"Error getting access token: {e}")
        return None


def _throttle_status(data: dict) -> Optional[Dict[str, Any]]:
    """Extract throttle status from GraphQL response extensions.cost.throttleStatus."""
    try:
        return (data.get("extensions") or {}).get("cost", {}).get("throttleStatus")
    except Exception:
        return None


def upload_translations(
    shop: str,
    access_token: str,
    resource_id: str,
    field_key: str,
    digest: str,
    translations: List[Dict[str, str]],
    dry_run: bool = False,
    delay_after_ok: float = 0,
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """
    Call translationsRegister for one resource + key.
    translations: list of { "locale": "en", "value": "..." }
    Returns (success, error_message, throttle_status from response).
    """
    if not digest or not field_key or not translations:
        return False, "Missing digest, field_key, or translations", None
    api_version = "2025-10"
    url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }
    # TranslationInput: key, locale, value, translatableContentDigest
    inputs = [
        {
            "key": field_key,
            "locale": t["locale"],
            "value": t["value"],
            "translatableContentDigest": digest,
        }
        for t in translations
    ]
    query = """
    mutation translationsRegister($resourceId: ID!, $translations: [TranslationInput!]!) {
      translationsRegister(resourceId: $resourceId, translations: $translations) {
        userErrors {
          field
          message
        }
        translations {
          locale
          key
        }
      }
    }
    """
    if dry_run:
        print(f"  [dry-run] would register {len(inputs)} translation(s) for {resource_id} key={field_key}")
        return True, None, None
    payload = {
        "query": query,
        "variables": {"resourceId": resource_id, "translations": inputs},
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        data = response.json() if response.text else {}
        throttle = _throttle_status(data)

        if response.status_code == 429:
            return False, "429 Too Many Requests (rate limited)", throttle

        response.raise_for_status()
        if data.get("errors"):
            return False, "; ".join(e.get("message", str(e)) for e in data["errors"]), throttle
        result = data.get("data", {}).get("translationsRegister", {})
        errors = result.get("userErrors", [])
        if errors:
            return False, "; ".join(e.get("message", str(e)) for e in errors), throttle
        if delay_after_ok > 0:
            time.sleep(delay_after_ok)
        return True, None, throttle
    except requests.exceptions.RequestException as e:
        return False, str(e), None


def upload_with_retry(
    shop: str,
    access_token: str,
    resource_id: str,
    field_key: str,
    digest: str,
    translations: List[Dict[str, str]],
    dry_run: bool = False,
    delay_after_ok: float = 0,
    max_retries_429: int = MAX_RETRIES_429,
) -> Tuple[bool, Optional[str]]:
    """Call upload_translations with retry on 429 (rate limit). Returns (success, error)."""
    last_err: Optional[str] = None
    backoff = INITIAL_BACKOFF_SECONDS
    for attempt in range(max_retries_429 + 1):
        success, err, throttle = upload_translations(
            shop, access_token, resource_id, field_key, digest, translations,
            dry_run=dry_run, delay_after_ok=delay_after_ok if attempt == 0 else 0,
        )
        if success:
            return True, None
        last_err = err
        if "429" not in (err or "") or dry_run or attempt == max_retries_429:
            return False, last_err
        if throttle:
            available = throttle.get("currentlyAvailable")
            restore_rate = throttle.get("restoreRate")
            print(f"    Rate limited (available={available}, restoreRate={restore_rate}); waiting {backoff:.1f}s...", file=sys.stderr)
        else:
            print(f"    Rate limited; waiting {backoff:.1f}s before retry {attempt + 1}/{max_retries_429}...", file=sys.stderr)
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
    return False, last_err


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Upload translations from CSV to Shopify.")
    parser.add_argument("csv_file", help="Path to CSV (e.g. translation_entries_changed.csv from process_translations.py)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be sent, do not call API.")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        metavar="SECONDS",
        help=f"Delay in seconds between successful API calls (default: {DEFAULT_DELAY_SECONDS}). Use 0.5 or 1 to avoid rate limits.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES_429,
        metavar="N",
        help=f"Max retries on 429 rate limit (default: {MAX_RETRIES_429}).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parent / "config.json",
        help="Path to config.json (default: config.json in script directory).",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.csv_file):
        print(f"Error: file not found: {args.csv_file}")
        sys.exit(1)
    if not str(args.csv_file).lower().endswith(".csv"):
        print(f"Error: expected a CSV file (e.g. push_to_python.csv), got: {args.csv_file}")
        sys.exit(1)

    # Load config for target locales (column name -> Shopify locale code)
    from lib.locale_config import load_config, normalize_config, locale_to_column
    config = normalize_config(load_config(args.config))
    # normalize_config sets default target_locales when missing; see lib.locale_config
    target_locales = config.get("target_locales") or []
    locale_column_to_code = {locale_to_column(t): t for t in target_locales if t}

    shop = os.getenv("SHOPIFY_SHOP") or os.getenv("NEW_SHOP")
    client_id = os.getenv("SHOPIFY_CLIENT_ID") or os.getenv("NEW_CLIENT_ID")
    client_secret = os.getenv("SHOPIFY_CLIENT_SECRET") or os.getenv("NEW_SECRET")
    if not all([shop, client_id, client_secret]):
        print("Error: set SHOPIFY_SHOP, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET (or NEW_* equivalents) in .env")
        sys.exit(1)

    shop = shop.rstrip("/").replace("https://", "").replace("http://", "")
    if not shop.endswith(".myshopify.com"):
        shop = f"{shop}.myshopify.com"

    print("Getting access token...")
    access_token = get_access_token(shop, client_id, client_secret)
    if not access_token:
        print("Error: failed to get access token")
        sys.exit(1)
    print("✓ Access token obtained")

    rows: List[Dict[str, str]] = []
    with open(args.csv_file, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r is None:
                continue
            rows.append({
                (k or "").strip(): (v.strip() if isinstance(v, str) else "")
                for k, v in r.items()
                if (k or "").strip()  # skip None or blank keys
            })

    if not rows:
        print("No rows in CSV.")
        sys.exit(0)

    print(f"Read {len(rows)} row(s) from {args.csv_file}")
    if args.dry_run:
        print("Dry run: no API calls will be made.")

    ok = 0
    fail = 0
    failed_rows: List[tuple[int, str]] = []  # (row_number, reason)
    for i, row in enumerate(rows):
        resource_id = (row.get("resource_id") or "").strip()
        field_key = (row.get("field_key") or "").strip()
        digest = (row.get("digest") or "").strip()
        if not resource_id:
            fail += 1
            failed_rows.append((i + 1, "no resource_id"))
            continue
        if not field_key:
            fail += 1
            failed_rows.append((i + 1, "no field_key"))
            continue
        if not digest:
            fail += 1
            failed_rows.append((i + 1, "no digest"))
            continue
        translations = []
        for col, locale_code in locale_column_to_code.items():
            val = (row.get(col) or "").strip()
            if val:
                translations.append({"locale": locale_code, "value": val})
        if not translations:
            fail += 1
            failed_rows.append((i + 1, "no target locale values"))
            continue
        success, err = upload_with_retry(
            shop, access_token, resource_id, field_key, digest, translations,
            dry_run=args.dry_run,
            delay_after_ok=args.delay,
            max_retries_429=args.max_retries,
        )
        if success:
            ok += 1
            print(f"  Row {i + 1}: ✓ {resource_id} {field_key} ({len(translations)} locales)")
        else:
            fail += 1
            failed_rows.append((i + 1, err or "upload failed"))

    print(f"\nDone: {ok} succeeded, {fail} failed/skipped.")
    if failed_rows:
        print("\nFailed/skipped rows:")
        for row_num, reason in failed_rows:
            print(f"  Row {row_num}: {reason}")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
