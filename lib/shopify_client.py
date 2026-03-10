"""
Minimal Shopify Admin API client for config CLI (shopLocales) and shared auth.
"""
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# API version used by export/upload; keep in sync.
SHOPIFY_API_VERSION = "2025-10"


def get_access_token(shop: str, client_id: str, client_secret: str) -> Optional[str]:
    """Obtain Admin API access token via client credentials."""
    shop = shop.rstrip("/").replace("https://", "").replace("http://", "")
    if not shop.endswith(".myshopify.com"):
        shop = f"{shop}.myshopify.com"
    url = f"https://{shop}/admin/oauth/access_token"
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"Error getting access token: {e}")
        return None


def fetch_shop_locales(
    shop: str, access_token: str, published_only: bool = True
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Call shopLocales query. Returns (list of {locale, name, primary, published}, error).
    """
    shop = shop.rstrip("/").replace("https://", "").replace("http://", "")
    if not shop.endswith(".myshopify.com"):
        shop = f"{shop}.myshopify.com"
    url = f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    query = """
    query ShopLocales($published: Boolean) {
      shopLocales(published: $published) {
        locale
        name
        primary
        published
      }
    }
    """
    try:
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": access_token,
            },
            json={"query": query, "variables": {"published": published_only}},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            msg = "; ".join(e.get("message", str(e)) for e in data["errors"])
            return [], msg
        nodes = (data.get("data") or {}).get("shopLocales") or []
        return nodes, None
    except requests.RequestException as e:
        return [], str(e)


def load_dotenv_from_script_dir() -> None:
    """Load .env from the directory containing the script (e.g. configure.py or project root)."""
    try:
        from dotenv import load_dotenv
        # When running configure.py or from project root, load .env in cwd
        load_dotenv(Path.cwd() / ".env")
    except ImportError:
        pass
