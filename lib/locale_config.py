"""
Locale and config helpers for Shopify translation tools.
- locale_to_column / column_to_locale: map Shopify ISO codes to CSV column names.
- load_config / normalize_config: load config.json with legacy key fallback.
- create_dictionary_csv: create an empty dictionary.csv with columns from config.
"""
import csv
import json
from pathlib import Path
from typing import Any, List, Tuple


def locale_to_column(iso_code: str) -> str:
    """Convert Shopify locale ISO code to CSV column name (e.g. pt-BR -> pt_br)."""
    if not iso_code:
        return ""
    return iso_code.strip().lower().replace("-", "_")


def column_to_locale(column: str) -> str:
    """Convert CSV column name back to Shopify locale ISO code (e.g. pt_br -> pt-BR)."""
    if not column:
        return ""
    # Common case: pt_br -> pt-BR (second segment capitalized for region)
    parts = column.strip().lower().replace("-", "_").split("_")
    if len(parts) == 1:
        return parts[0]
    # e.g. pt_br -> pt-BR
    return f"{parts[0]}-{parts[1].upper()}"


# Default TranslatableResourceType list (full set from Shopify Admin API).
DEFAULT_RESOURCE_TYPES = [
    "PRODUCT",
    "COLLECTION",
    "PAGE",
    "ARTICLE",
    "BLOG",
    "METAOBJECT",
    "METAFIELD",
    "MEDIA_IMAGE",
    "COLLECTION_IMAGE",
    "ARTICLE_IMAGE",
    "PRODUCT_OPTION",
    "PRODUCT_OPTION_VALUE",
    "SHOP",
    "SHOP_POLICY",
    "MENU",
    "LINK",
    "FILTER",
    "SELLING_PLAN_GROUP",
    "SELLING_PLAN",
    "PAYMENT_GATEWAY",
    "DELIVERY_METHOD_DEFINITION",
    "PACKING_SLIP_TEMPLATE",
    "EMAIL_TEMPLATE",
    "ONLINE_STORE_THEME",
    "ONLINE_STORE_THEME_JSON_TEMPLATE",
    "ONLINE_STORE_THEME_SECTION_GROUP",
    "ONLINE_STORE_THEME_APP_EMBED",
    "ONLINE_STORE_THEME_LOCALE_CONTENT",
    "ONLINE_STORE_THEME_SETTINGS_CATEGORY",
    "ONLINE_STORE_THEME_SETTINGS_DATA_SECTIONS",
]


def load_config(config_path: Path) -> dict:
    """Load config JSON; return empty dict if file missing or invalid."""
    if not config_path.exists():
        return {}
    try:
        with open(config_path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def normalize_config(config: dict) -> dict:
    """
    Normalize config to canonical keys: source_locale, target_locales (Shopify ISO).
    Legacy: source_of_truth_locale / target_languages (column names) -> convert to ISO.
    When keys are missing, sets example defaults (pt-BR, en/es/it/fr); run configure.py
    or edit config.json to set your store's locales.
    """
    out = dict(config)
    # Legacy: source_of_truth_locale (column) -> source_locale (ISO)
    if "source_locale" not in out and "source_of_truth_locale" in out:
        col = (out.get("source_of_truth_locale") or "").strip()
        out["source_locale"] = column_to_locale(col) if col else "pt-BR"
    if "source_locale" not in out:
        out.setdefault("source_locale", "pt-BR")
    # Legacy: target_languages (columns) -> target_locales (ISO)
    if "target_locales" not in out and "target_languages" in out:
        cols = out.get("target_languages") or []
        out["target_locales"] = [column_to_locale(c) for c in cols if c]
    if "target_locales" not in out:
        out.setdefault("target_locales", ["en", "es", "it", "fr"])
    return out


def get_locale_columns(config: dict) -> Tuple[str, List[str], List[str]]:
    """
    From normalized config return (source_column, target_columns, locale_columns).
    locale_columns = [source_column] + target_columns.
    """
    source_iso = (config.get("source_locale") or "pt-BR").strip()
    target_isos = config.get("target_locales") or []
    source_col = locale_to_column(source_iso)
    target_cols = [locale_to_column(t) for t in target_isos if t]
    locale_cols = [source_col] + target_cols
    return source_col, target_cols, locale_cols


def get_dictionary_columns(config: dict) -> Tuple[str, ...]:
    """Digest + source column + target columns for dictionary CSV."""
    source_col, target_cols, _ = get_locale_columns(config)
    return ("Digest", source_col, *target_cols)


def get_resource_types(config: dict) -> List[str]:
    """Resource types for export; empty or missing means use DEFAULT_RESOURCE_TYPES."""
    rt = config.get("resource_types")
    if not rt:
        return DEFAULT_RESOURCE_TYPES
    return [str(x).strip().upper() for x in rt if x]


def write_config(config_path: Path, config: dict) -> None:
    """Write config JSON with sensible indentation."""
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


def build_default_config(
    source_locale: str,
    target_locales: List[str],
    resource_types: List[str] | None = None,
    **extra: Any,
) -> dict:
    """Build a config dict with canonical keys plus any extra keys."""
    out: dict = {
        "source_locale": source_locale.strip(),
        "target_locales": [t.strip() for t in target_locales if t],
        "resource_types": resource_types if resource_types is not None else [],
    }
    out.update(extra)
    return out


def create_dictionary_csv(
    path: Path,
    config: dict,
    overwrite: bool = False,
) -> Path:
    """
    Create a dictionary CSV with columns Digest + source + target locales from config.
    Writes only the header row (no data). Uses UTF-8 BOM for Excel.
    Returns the path written. If overwrite is False and the file exists, does nothing and returns path.
    """
    path = Path(path)
    if path.exists() and not overwrite:
        return path
    columns = get_dictionary_columns(config)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
    return path
