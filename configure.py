#!/usr/bin/env python3
"""
Interactive configuration for Shopify translation tools.
Creates config.json (and optionally .env) so any store can run export/process/upload
without editing JSON by hand. Run this as the first step after cloning the repo.

Usage:
    python configure.py
"""
import os
import sys
from pathlib import Path

# Load .env from current directory so we can read existing credentials
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.shopify_client import load_dotenv_from_script_dir
load_dotenv_from_script_dir()

from lib.locale_config import (
    build_default_config,
    create_dictionary_csv,
    DEFAULT_RESOURCE_TYPES,
    locale_to_column,
    write_config,
)
from lib.shopify_client import fetch_shop_locales, get_access_token


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config.json"
ENV_KEYS = ("SHOPIFY_SHOP", "SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET")


def prompt(text: str, default: str = "") -> str:
    """Read a line from stdin; return default if empty."""
    if default:
        line = input(f"{text} [{default}]: ").strip()
        return line if line else default
    return input(f"{text}: ").strip()


def prompt_yes_no(text: str, default: bool = True) -> bool:
    """Prompt y/n; default True = yes."""
    d = "Y/n" if default else "y/N"
    line = input(f"{text} [{d}]: ").strip().lower()
    if not line:
        return default
    return line in ("y", "yes")


def main() -> int:
    print("Shopify Translation Tools — Configuration\n")
    config_path = SCRIPT_DIR / "config.json"
    env_path = SCRIPT_DIR / ".env"

    # --- Shop and credentials ---
    shop = os.environ.get("SHOPIFY_SHOP") or os.environ.get("NEW_SHOP") or ""
    client_id = os.environ.get("SHOPIFY_CLIENT_ID") or os.environ.get("NEW_CLIENT_ID") or ""
    client_secret = os.environ.get("SHOPIFY_CLIENT_SECRET") or os.environ.get("NEW_SECRET") or ""

    if not shop:
        shop = prompt("Shop domain (e.g. mystore.myshopify.com)", "")
    if not shop:
        print("Shop is required. Create a Shopify app with read_translations, write_translations, and optionally read_locales.")
        return 1
    shop = shop.rstrip("/").replace("https://", "").replace("http://", "")
    if not shop.endswith(".myshopify.com"):
        shop = f"{shop}.myshopify.com"

    if not client_id:
        client_id = prompt("Shopify app Client ID", "")
    if not client_secret:
        client_secret = prompt("Shopify app Client secret", "")

    has_creds = bool(client_id and client_secret)
    shop_locales_list: list = []
    if has_creds:
        print("\nFetching shop locales...")
        token = get_access_token(shop, client_id, client_secret)
        if token:
            shop_locales_list, err = fetch_shop_locales(shop, token, published_only=True)
            if err:
                print(f"  Could not fetch shopLocales: {err}")
                print("  You can still enter source and target locales manually.")
            elif shop_locales_list:
                print("  Enabled (published) locales on your shop:")
                for sl in shop_locales_list:
                    primary = " (primary)" if sl.get("primary") else ""
                    print(f"    - {sl.get('locale')}: {sl.get('name', '')}{primary}")
        else:
            print("  Could not get access token; enter locales manually.")

    # --- Source locale ---
    if shop_locales_list:
        locs = [s["locale"] for s in shop_locales_list]
        primary_locale = next((s["locale"] for s in shop_locales_list if s.get("primary")), locs[0])
        source_locale = prompt(
            "Source locale (ISO code, the language of your content)",
            primary_locale,
        )
        if source_locale not in locs:
            print(f"  Note: '{source_locale}' may not be enabled on your shop. Only shopLocales are valid for the Translations API.")
    else:
        source_locale = prompt("Source locale (Shopify ISO, e.g. pt-BR or en)", "pt-BR")

    # --- Target locales ---
    if shop_locales_list:
        others = [s["locale"] for s in shop_locales_list if s["locale"] != source_locale]
        default_targets = ",".join(others) if others else "en,es,it,fr"
        target_line = prompt(
            "Target locales (comma-separated ISO codes, to translate into)",
            default_targets,
        )
    else:
        target_line = prompt("Target locales (comma-separated, e.g. en,es,it,fr)", "en,es,it,fr")
    target_locales = [t.strip() for t in target_line.split(",") if t.strip()]

    # --- Resource types (optional) ---
    print("\nExport can limit to certain resource types (products, collections, etc.).")
    use_all_resources = prompt_yes_no("Export all resource types?", True)
    resource_types: list | None = [] if use_all_resources else None
    if not use_all_resources:
        example = "PRODUCT,COLLECTION,PAGE"
        rline = prompt(f"Resource types (comma-separated, e.g. {example})", example)
        resource_types = [r.strip().upper() for r in rline.split(",") if r.strip()]
        # Validate against known types
        valid = set(DEFAULT_RESOURCE_TYPES)
        unknown = [r for r in resource_types if r not in valid]
        if unknown:
            print(f"  Warning: unknown types {unknown}; they may still be sent to the API.")

    # --- Build config ---
    config = build_default_config(
        source_locale=source_locale,
        target_locales=target_locales,
        resource_types=resource_types if resource_types is not None else [],
    )
    write_config(config_path, config)
    print(f"\nWrote config to {config_path}")
    print(f"  source_locale: {source_locale}")
    print(f"  target_locales: {target_locales}")
    print(f"  resource_types: {'all' if not resource_types else resource_types}")

    # Create dictionary.csv with correct columns if it doesn't exist
    dictionary_path = SCRIPT_DIR / "dictionary.csv"
    existed = dictionary_path.exists()
    create_dictionary_csv(dictionary_path, config, overwrite=False)
    if not existed and dictionary_path.exists():
        print(f"  Created dictionary.csv with columns: Digest, {locale_to_column(source_locale)}, {', '.join(locale_to_column(t) for t in target_locales)}")

    # --- Optional .env ---
    if has_creds:
        write_env = prompt_yes_no("Write .env with shop and credentials?", False)
        if write_env:
            lines = [
                f"SHOPIFY_SHOP={shop}",
                f"SHOPIFY_CLIENT_ID={client_id}",
                f"SHOPIFY_CLIENT_SECRET={client_secret}",
                "",
                "# Optional: for AI translation (process_translations.py --translate)",
                "# OPENROUTER_API_KEY=sk-...",
            ]
            env_path.write_text("\n".join(lines), encoding="utf-8")
            print(f"Wrote {env_path} (add OPENROUTER_API_KEY if you use --translate).")
        else:
            print("Skipped .env. Set SHOPIFY_SHOP, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET in your environment or .env.")

    print("\nNext steps: run export, then process, then upload.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
