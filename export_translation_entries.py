#!/usr/bin/env python3
"""
Export translatable entries from the Shopify store to a CSV.

Uses the Translations API (translatableResources) to pull existing translations
for the locales defined in config.json (source_locale + target_locales). Requires
read_translations scope. If that scope is missing, falls back to exporting source
content only (source locale column filled).

Pulls products, collections, pages, metaobjects, theme, etc. CSV columns:
resource_type, resource_id, resource_handle, field_key, digest, plus one column
per locale (from config).

Uses SHOPIFY_SHOP, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET (or NEW_* equivalents) from .env.

Usage:
    python export_translation_entries.py
    python export_translation_entries.py --output translation_entries.csv
    python export_translation_entries.py --theme-id THEME_ID   # only that theme; omit for store default
    python export_translation_entries.py --incomplete-only     # only rows with at least one missing locale
    python export_translation_entries.py --condense            # merge repeated source values (dictionary-style CSV)
"""

import csv
import os
import time
from pathlib import Path

import requests
from typing import List, Dict, Optional, Tuple, Any

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# Retry when Shopify returns rate limit (429 / Throttled)
THROTTLE_RETRIES = 5
THROTTLE_SLEEP_SECONDS = 10


# Metafield type names that hold translatable text
TEXT_METAFIELD_TYPES = {
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
}


def _locale_to_graphql_alias(locale: str) -> str:
    """Convert Shopify locale ISO to safe GraphQL alias suffix (e.g. pt-BR -> pt_BR)."""
    return (locale or "").strip().replace("-", "_")


def _normalize_for_csv(s: Optional[str]) -> str:
    """Return a clean string for CSV; empty if None or blank."""
    if s is None:
        return ""
    t = str(s).strip()
    return t if t else ""


def _all_locales_filled(row: Dict[str, str], locale_columns: List[str]) -> bool:
    """True if all locale columns are non-empty."""
    return all(_normalize_for_csv(row.get(c)) for c in locale_columns)


def _has_missing_translation(row: Dict[str, str], locale_columns: List[str]) -> bool:
    """True if any locale column is empty (row needs translation)."""
    return not _all_locales_filled(row, locale_columns)


def _all_locales_empty(row: Dict[str, str], locale_columns: List[str]) -> bool:
    """True if all locale columns are empty."""
    return not any(_normalize_for_csv(row.get(c)) for c in locale_columns)


def condense_translation_rows(
    rows: List[Dict[str, str]], locale_columns: List[str], source_column: str
) -> List[Dict[str, str]]:
    """
    Prepare a translation dictionary from full export:
    1. Remove rows where source locale value is unique (keep only repeated source values).
    2. Remove rows where all locale columns are already filled.
    3. Condense to one row per repeated source value, merging any locale values from the group.
    Output is for filling empty cells later (dictionary: source column -> target columns).
    Digest/resource details are not preserved; goal is dictionary rows only.
    """
    if not rows:
        return []
    groups: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        key = _normalize_for_csv(r.get(source_column))
        if not key:
            continue
        groups.setdefault(key, []).append(r)
    repeated = {k for k, g in groups.items() if len(g) > 1}
    if not repeated:
        return []
    out: List[Dict[str, str]] = []
    for src_val in repeated:
        group = groups[src_val]
        incomplete = [r for r in group if not _all_locales_filled(r, locale_columns)]
        if not incomplete:
            continue
        merged: Dict[str, str] = {
            "resource_type": "dictionary",
            "resource_id": "",
            "resource_handle": "",
            "field_key": src_val[:50] + ("..." if len(src_val) > 50 else ""),
            "digest": "",
            **{c: "" for c in locale_columns},
        }
        merged[source_column] = src_val
        for col in locale_columns:
            for r in incomplete:
                v = _normalize_for_csv(r.get(col))
                if v and not merged.get(col):
                    merged[col] = v
        out.append(merged)
    return out


def _row(
    resource_type: str,
    resource_id: str,
    resource_handle: str,
    field_key: str,
    digest: str,
    values_by_col: Dict[str, str],
    locale_columns: List[str],
) -> Dict[str, str]:
    out = {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "resource_handle": resource_handle,
        "field_key": field_key,
        "digest": _normalize_for_csv(digest),
    }
    for c in locale_columns:
        out[c] = _normalize_for_csv(values_by_col.get(c, ""))
    return out


class ShopifyTranslationExporter:
    def __init__(self, shop: str, access_token: str, locales: List[str]):
        """
        locales: list of Shopify locale ISO codes (e.g. [source_locale] + target_locales).
        """
        from lib.locale_config import locale_to_column
        from lib.shopify_client import SHOPIFY_API_VERSION
        self.shop = shop.rstrip("/").replace("https://", "").replace("http://", "")
        if not self.shop.endswith(".myshopify.com"):
            self.shop = f"{self.shop}.myshopify.com"
        self.access_token = access_token
        self.api_version = SHOPIFY_API_VERSION
        self.graphql_url = f"https://{self.shop}/admin/api/{self.api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token,
        }
        self.locales = [l for l in locales if l]
        self.locale_columns = [locale_to_column(l) for l in self.locales]

    def execute_graphql_query(
        self, query: str, variables: Optional[Dict] = None
    ) -> Tuple[Optional[Dict], Optional[str]]:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        last_error: Optional[str] = None
        for attempt in range(THROTTLE_RETRIES):
            try:
                response = requests.post(
                    self.graphql_url,
                    headers=self.headers,
                    json=payload,
                    timeout=60,
                )
                if response.status_code == 429:
                    last_error = "Throttled"
                    if attempt < THROTTLE_RETRIES - 1:
                        time.sleep(THROTTLE_SLEEP_SECONDS)
                        continue
                    return None, last_error
                response.raise_for_status()
                data = response.json()
                if "errors" in data:
                    msgs = [e.get("message", str(e)) for e in data["errors"]]
                    err_text = "; ".join(msgs)
                    if "hrottl" in err_text.lower() and attempt < THROTTLE_RETRIES - 1:
                        last_error = err_text
                        time.sleep(THROTTLE_SLEEP_SECONDS)
                        continue
                    return None, err_text
                return data.get("data"), None
            except requests.exceptions.RequestException as e:
                resp = getattr(e, "response", None)
                if resp is not None and resp.status_code == 429 and attempt < THROTTLE_RETRIES - 1:
                    time.sleep(THROTTLE_SLEEP_SECONDS)
                    continue
                return None, f"Request error: {str(e)}"
            except Exception as e:
                return None, str(e)
        return None, last_error or "Throttled"

    def _translation_fragment(self, prefix: str = "") -> str:
        """GraphQL fragment for translations in configured locales (dynamic aliases)."""
        lines = []
        for locale in self.locales:
            alias = "translations_" + _locale_to_graphql_alias(locale)
            lines.append(f'{prefix}{alias}: translations(locale: "{locale}") {{ key value }}')
        return "\n        " + "\n        ".join(lines)

    def fetch_translatable_resources(
        self,
        resource_type: str,
        page_size: int = 50,
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch translatable resources with content and translations for all target locales.
        Uses translatableResources query (requires read_translations scope).
        resource_type: PRODUCT, COLLECTION, PAGE, METAOBJECT, etc.
        """
        all_nodes: List[Dict] = []
        cursor = None
        trans = self._translation_fragment()
        nested_trans = self._translation_fragment(prefix="")
        query = f"""
        query GetTranslatable($first: Int!, $after: String) {{
          translatableResources(first: $first, after: $after, resourceType: {resource_type}) {{
            edges {{
              node {{
                resourceId
                translatableContent {{
                  key
                  value
                  locale
                  digest
                }}
                {trans}
                nestedTranslatableResources(first: 200) {{
                  nodes {{
                    resourceId
                    translatableContent {{ key value locale digest }}
                    {nested_trans}
                  }}
                }}
              }}
            }}
            pageInfo {{ hasNextPage endCursor }}
          }}
        }}
        """
        while True:
            variables: Dict = {"first": page_size}
            if cursor:
                variables["after"] = cursor
            data, error = self.execute_graphql_query(query, variables)
            if error:
                return [], error
            conn = data.get("translatableResources") if data else None
            if not conn:
                break
            for edge in conn.get("edges", []):
                all_nodes.append(edge.get("node", {}))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_nodes, None

    def fetch_translatable_resources_by_ids(
        self,
        resource_ids: List[str],
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch translatable resources by exact resource IDs (e.g. a specific theme).
        Uses translatableResourcesByIds query. Node shape matches fetch_translatable_resources.
        """
        if not resource_ids:
            return [], None
        trans = self._translation_fragment()
        nested_trans = self._translation_fragment(prefix="")
        query = f"""
        query GetTranslatableByIds($resourceIds: [ID!]!, $first: Int!) {{
          translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {{
            edges {{
              node {{
                resourceId
                translatableContent {{
                  key
                  value
                  locale
                  digest
                }}
                {trans}
                nestedTranslatableResources(first: 200) {{
                  nodes {{
                    resourceId
                    translatableContent {{ key value locale digest }}
                    {nested_trans}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        variables: Dict = {"resourceIds": resource_ids, "first": max(1, len(resource_ids))}
        data, error = self.execute_graphql_query(query, variables)
        if error:
            return [], error
        conn = data.get("translatableResourcesByIds") if data else None
        if not conn:
            return [], None
        nodes = [edge.get("node", {}) for edge in conn.get("edges", [])]
        return nodes, None

    def _value_for_key(self, translations_list: List[Dict], key: str) -> str:
        """Get translation value for a key from a list of { key, value }."""
        if not translations_list:
            return ""
        for t in translations_list:
            if (t.get("key") or "").strip() == key:
                return _normalize_for_csv(t.get("value"))
        return ""

    def _rows_from_translatable_node(
        self,
        node: Dict,
        resource_type: str,
    ) -> List[Dict[str, str]]:
        """
        Build CSV rows from one translatable resource node.
        Fills locale columns from translatableContent (source) and translations.
        """
        rows: List[Dict[str, str]] = []
        resource_id = node.get("resourceId") or ""
        content_list = node.get("translatableContent") or []
        if not content_list:
            return rows

        # Build handle from content key "handle" (for products, collections, pages)
        handle = ""
        for c in content_list:
            if (c.get("key") or "").strip() == "handle":
                handle = _normalize_for_csv(c.get("value"))
                break

        # Per-key: fill values_by_col from translation aliases and translatableContent
        locale_to_col = dict(zip(self.locales, self.locale_columns))
        seen_keys: set = set()
        for content in content_list:
            key = (content.get("key") or "").strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            source_value = _normalize_for_csv(content.get("value"))
            source_locale = (content.get("locale") or "").strip()

            values_by_col: Dict[str, str] = {col: "" for col in self.locale_columns}
            for loc, col in zip(self.locales, self.locale_columns):
                alias = "translations_" + _locale_to_graphql_alias(loc)
                val = self._value_for_key(node.get(alias) or [], key)
                if val:
                    values_by_col[col] = val
            if source_value and source_locale and locale_to_col.get(source_locale):
                col = locale_to_col[source_locale]
                if not values_by_col.get(col):
                    values_by_col[col] = source_value
            if source_value and self.locale_columns and not values_by_col.get(self.locale_columns[0]):
                values_by_col[self.locale_columns[0]] = source_value

            type_label = resource_type.lower().replace("_", " ")
            digest = _normalize_for_csv(content.get("digest"))
            rows.append(
                _row(
                    resource_type=type_label,
                    resource_id=resource_id,
                    resource_handle=handle,
                    field_key=key,
                    digest=digest,
                    values_by_col=values_by_col,
                    locale_columns=self.locale_columns,
                )
            )

        # Nested resources (e.g. product variants, metafields, options)
        for nested in node.get("nestedTranslatableResources", {}).get("nodes", []) or []:
            rows.extend(self._rows_from_translatable_node(nested, resource_type))

        return rows

    # Theme resource types: when --theme-id is set we fetch that theme by ID instead of API default (live).
    THEME_RESOURCE_TYPES = frozenset({
        "ONLINE_STORE_THEME",
        "ONLINE_STORE_THEME_JSON_TEMPLATE",
        "ONLINE_STORE_THEME_SECTION_GROUP",
        "ONLINE_STORE_THEME_APP_EMBED",
        "ONLINE_STORE_THEME_LOCALE_CONTENT",
        "ONLINE_STORE_THEME_SETTINGS_CATEGORY",
        "ONLINE_STORE_THEME_SETTINGS_DATA_SECTIONS",
    })

    def fetch_all_translation_rows(
        self,
        resource_types: List[str],
        theme_id: Optional[str] = None,
    ) -> Tuple[List[Dict[str, str]], Optional[str]]:
        """
        Fetch translatable resources for the given resource_types with translations for configured locales.
        theme_id: if set, only that theme's entries are returned (no products/collections/etc.).
        """
        all_rows: List[Dict[str, str]] = []

        if theme_id:
            # Normalize: allow numeric ID or full GID
            tid = theme_id.strip()
            if tid.isdigit():
                theme_gid = f"gid://shopify/OnlineStoreTheme/{tid}"
            else:
                theme_gid = tid
            nodes, err = self.fetch_translatable_resources_by_ids([theme_gid])
            if err:
                print(f"  ⚠ Theme (id={theme_id}): {err[:80]}...")
            else:
                count = 0
                for node in nodes:
                    rows = self._rows_from_translatable_node(node, "ONLINE_STORE_THEME")
                    all_rows.extend(rows)
                    count += len(rows)
                if nodes:
                    print(f"  ✓ Theme (id={theme_id}): {len(nodes)} resources, {count} rows")
            # When theme_id is set, return only this theme's entries (no products, collections, etc.)
            return all_rows, None

        for rtype in resource_types:
            if theme_id and rtype in self.THEME_RESOURCE_TYPES:
                continue  # already fetched by theme ID above
            nodes, err = self.fetch_translatable_resources(rtype)
            if err:
                print(f"  ⚠ {rtype}: {err[:80]}...")
                continue
            count = 0
            for node in nodes:
                rows = self._rows_from_translatable_node(node, rtype)
                all_rows.extend(rows)
                count += len(rows)
            if nodes:
                print(f"  ✓ {rtype}: {len(nodes)} resources, {count} rows")
        return all_rows, None

    def fetch_all_products(self) -> Tuple[List[Dict], Optional[str]]:
        """Fetch all products with title, description, metafields, variants, options."""
        all_products = []
        cursor = None
        query = """
        query GetProducts($first: Int!, $after: String) {
          products(first: $first, after: $after) {
            edges {
              node {
                id
                handle
                title
                description
                descriptionHtml
                options {
                  id
                  name
                  position
                  values
                }
                variants(first: 250) {
                  edges {
                    node {
                      id
                      title
                    }
                  }
                }
                metafields(first: 250) {
                  edges {
                    node {
                      namespace
                      key
                      value
                      type
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        while True:
            variables = {"first": 250}
            if cursor:
                variables["after"] = cursor
            data, error = self.execute_graphql_query(query, variables)
            if error:
                return [], error
            conn = data.get("products") if data else None
            if not conn:
                break
            for edge in conn.get("edges", []):
                all_products.append(edge.get("node", {}))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_products, None

    def fetch_all_collections(self) -> Tuple[List[Dict], Optional[str]]:
        """Fetch all collections with title, description, metafields."""
        all_collections = []
        cursor = None
        query = """
        query GetCollections($first: Int!, $after: String) {
          collections(first: $first, after: $after) {
            edges {
              node {
                id
                handle
                title
                description
                descriptionHtml
                metafields(first: 250) {
                  edges {
                    node {
                      namespace
                      key
                      value
                      type
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        while True:
            variables = {"first": 250}
            if cursor:
                variables["after"] = cursor
            data, error = self.execute_graphql_query(query, variables)
            if error:
                return [], error
            conn = data.get("collections") if data else None
            if not conn:
                break
            for edge in conn.get("edges", []):
                all_collections.append(edge.get("node", {}))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_collections, None

    def fetch_all_pages(self) -> Tuple[List[Dict], Optional[str]]:
        """Fetch all pages with title and body."""
        all_pages = []
        cursor = None
        query = """
        query GetPages($first: Int!, $after: String) {
          pages(first: $first, after: $after) {
            edges {
              node {
                id
                handle
                title
                body
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        while True:
            variables = {"first": 250}
            if cursor:
                variables["after"] = cursor
            data, error = self.execute_graphql_query(query, variables)
            if error:
                return [], error
            conn = data.get("pages") if data else None
            if not conn:
                break
            for edge in conn.get("edges", []):
                all_pages.append(edge.get("node", {}))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_pages, None

    def fetch_metaobject_definitions(self) -> Tuple[List[Dict], Optional[str]]:
        """Fetch metaobject definitions to get types and text field keys."""
        all_defs = []
        cursor = None
        query = """
        query GetMetaobjectDefinitions($first: Int!, $after: String) {
          metaobjectDefinitions(first: $first, after: $after) {
            edges {
              node {
                id
                type
                name
                fieldDefinitions {
                  key
                  name
                  type { name category }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        while True:
            variables = {"first": 250}
            if cursor:
                variables["after"] = cursor
            data, error = self.execute_graphql_query(query, variables)
            if error:
                return [], error
            conn = data.get("metaobjectDefinitions") if data else None
            if not conn:
                break
            for edge in conn.get("edges", []):
                all_defs.append(edge.get("node", {}))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_defs, None

    def fetch_metaobjects_by_type(self, metaobject_type: str) -> Tuple[List[Dict], Optional[str]]:
        """Fetch all metaobjects of a given type with their fields."""
        all_objs = []
        cursor = None
        query = """
        query GetMetaobjects($type: String!, $first: Int!, $after: String) {
          metaobjects(type: $type, first: $first, after: $after) {
            edges {
              node {
                id
                handle
                type
                fields {
                  key
                  value
                  type
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        while True:
            variables = {"type": metaobject_type, "first": 250}
            if cursor:
                variables["after"] = cursor
            data, error = self.execute_graphql_query(query, variables)
            if error:
                return [], error
            conn = data.get("metaobjects") if data else None
            if not conn:
                break
            for edge in conn.get("edges", []):
                all_objs.append(edge.get("node", {}))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_objs, None

    def _source_only_row(
        self, resource_type: str, resource_id: str, handle: str, field_key: str, value: str
    ) -> Dict[str, str]:
        """Build a row with only the source locale filled (content-only fallback)."""
        values_by_col = {c: value if c == self.locale_columns[0] else "" for c in self.locale_columns}
        return _row(
            resource_type, resource_id, handle, field_key, "", values_by_col, self.locale_columns
        )

    def build_translation_rows(
        self,
        products: List[Dict],
        collections: List[Dict],
        pages: List[Dict],
        metaobject_definitions: List[Dict],
    ) -> List[Dict[str, str]]:
        """Build CSV rows from all resources; source locale filled, target locales empty."""
        rows: List[Dict[str, str]] = []

        for p in products:
            gid = p.get("id", "")
            handle = p.get("handle", "")
            title = _normalize_for_csv(p.get("title"))
            if title:
                rows.append(self._source_only_row("product", gid, handle, "title", title))
            desc = _normalize_for_csv(p.get("description") or p.get("descriptionHtml"))
            if desc:
                rows.append(self._source_only_row("product", gid, handle, "description", desc))
            for edge in p.get("metafields", {}).get("edges", []):
                node = edge.get("node", {})
                if (node.get("type") or "").strip() not in TEXT_METAFIELD_TYPES:
                    continue
                val = _normalize_for_csv(node.get("value"))
                if not val:
                    continue
                field_key = f"metafield.{node.get('namespace', '')}.{node.get('key', '')}"
                rows.append(self._source_only_row("product", gid, handle, field_key, val))
            for opt in p.get("options", []) or []:
                opt_name = _normalize_for_csv(opt.get("name"))
                if opt_name:
                    rows.append(self._source_only_row("product", gid, handle, f"option.{opt_name}", opt_name))
                for v in opt.get("values", []) or []:
                    v_str = _normalize_for_csv(v)
                    if v_str:
                        rows.append(
                            self._source_only_row(
                                "product", gid, handle,
                                f"option_value.{opt.get('name', '')}.{v_str}", v_str,
                            )
                        )
            seen_variant_titles = set()
            for edge in p.get("variants", {}).get("edges", []):
                node = edge.get("node", {})
                vt = _normalize_for_csv(node.get("title"))
                if vt and vt not in seen_variant_titles:
                    seen_variant_titles.add(vt)
                    rows.append(self._source_only_row("product", gid, handle, "variant.title", vt))

        for c in collections:
            gid = c.get("id", "")
            handle = c.get("handle", "")
            title = _normalize_for_csv(c.get("title"))
            if title:
                rows.append(self._source_only_row("collection", gid, handle, "title", title))
            desc = _normalize_for_csv(c.get("description") or c.get("descriptionHtml"))
            if desc:
                rows.append(self._source_only_row("collection", gid, handle, "description", desc))
            for edge in c.get("metafields", {}).get("edges", []):
                node = edge.get("node", {})
                if (node.get("type") or "").strip() not in TEXT_METAFIELD_TYPES:
                    continue
                val = _normalize_for_csv(node.get("value"))
                if not val:
                    continue
                field_key = f"metafield.{node.get('namespace', '')}.{node.get('key', '')}"
                rows.append(self._source_only_row("collection", gid, handle, field_key, val))

        for page in pages:
            gid = page.get("id", "")
            handle = page.get("handle", "")
            title = _normalize_for_csv(page.get("title"))
            if title:
                rows.append(self._source_only_row("page", gid, handle, "title", title))
            body = _normalize_for_csv(page.get("body"))
            if body:
                rows.append(self._source_only_row("page", gid, handle, "body", body))

        for defn in metaobject_definitions:
            obj_type = defn.get("type", "")
            text_keys = set()
            for fd in defn.get("fieldDefinitions", []) or []:
                t = fd.get("type") or {}
                type_name = (t.get("name") or "").strip()
                if type_name in TEXT_METAFIELD_TYPES:
                    text_keys.add(fd.get("key", ""))
            objs, err = self.fetch_metaobjects_by_type(obj_type)
            if err:
                continue
            for obj in objs:
                gid = obj.get("id", "")
                handle = obj.get("handle", "")
                for f in obj.get("fields", []) or []:
                    key = f.get("key", "")
                    ftype = (f.get("type") or "").strip()
                    is_text = key in text_keys or ftype in TEXT_METAFIELD_TYPES
                    if not is_text:
                        continue
                    val = _normalize_for_csv(f.get("value"))
                    if not val:
                        continue
                    field_key = f"metaobject.{obj_type}.{key}"
                    rows.append(self._source_only_row("metaobject", gid, handle, field_key, val))

        return rows

    def export_csv(self, rows: List[Dict[str, str]], filename: str) -> None:
        """Write rows to CSV with UTF-8 BOM for Excel."""
        fieldnames = [
            "resource_type",
            "resource_id",
            "resource_handle",
            "field_key",
            "digest",
            *self.locale_columns,
        ]
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(f"✓ Wrote {len(rows)} rows to {filename}")


def main():
    import argparse
    from lib.locale_config import (
        load_config,
        normalize_config,
        get_locale_columns,
        get_resource_types,
    )
    from lib.shopify_client import get_access_token

    parser = argparse.ArgumentParser(
        description="Export translatable entries to CSV (locales from config.json)."
    )
    parser.add_argument(
        "--output", "-o",
        default="translation_entries.csv",
        help="Output CSV path (default: translation_entries.csv)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parent / "config.json",
        help="Path to config.json (default: config.json in script directory).",
    )
    parser.add_argument(
        "--theme-id",
        default=None,
        metavar="ID",
        help="Theme ID to pull theme translations from (e.g. 140041060436 for dev). "
             "If not set, theme entries come from the store's live/main theme.",
    )
    parser.add_argument(
        "--condense",
        action="store_true",
        help="Build a dictionary CSV: keep only repeated source values, drop fully translated rows.",
    )
    parser.add_argument(
        "--incomplete-only",
        action="store_true",
        help="Export only rows with at least one empty and one non-empty locale.",
    )
    args = parser.parse_args()

    config = normalize_config(load_config(args.config))
    source_col, target_cols, locale_cols = get_locale_columns(config)
    source_locale = config.get("source_locale") or "pt-BR"
    target_locales = config.get("target_locales") or ["en", "es", "it", "fr"]
    locales = [source_locale] + target_locales
    resource_types = get_resource_types(config)

    shop = os.getenv("SHOPIFY_SHOP") or os.getenv("NEW_SHOP")
    client_id = os.getenv("SHOPIFY_CLIENT_ID") or os.getenv("NEW_CLIENT_ID")
    client_secret = os.getenv("SHOPIFY_CLIENT_SECRET") or os.getenv("NEW_SECRET")
    if not all([shop, client_id, client_secret]):
        print("Error: Set SHOPIFY_SHOP, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET (or NEW_*) in .env")
        return

    shop_domain = shop.rstrip("/").replace("https://", "").replace("http://", "")
    if not shop_domain.endswith(".myshopify.com"):
        shop_domain = f"{shop_domain}.myshopify.com"

    print("Getting access token...")
    access_token = get_access_token(shop_domain, client_id, client_secret)
    if not access_token:
        print("Error: Failed to get access token")
        return
    print("✓ Access token obtained")

    exporter = ShopifyTranslationExporter(shop_domain, access_token, locales)

    if args.theme_id:
        print(f"Theme: using id {args.theme_id} — exporting only this theme's entries.")
    else:
        print("Theme: using store default (live theme) for theme entries.")
    print(f"Fetching translatable resources and translations for {locales}...")
    rows, err = exporter.fetch_all_translation_rows(resource_types, theme_id=args.theme_id)
    if err:
        print(f"Error: {err}")
        return
    if not rows:
        print("  No rows from Translations API (check read_translations scope). Using content-only export.")
        print("Fetching products...")
        products, err = exporter.fetch_all_products()
        if err:
            print(f"Error: {err}")
            return
        print(f"  {len(products)} products")
        print("Fetching collections...")
        collections, err = exporter.fetch_all_collections()
        if err:
            print(f"Error: {err}")
            return
        print("Fetching pages...")
        pages, err = exporter.fetch_all_pages()
        if err:
            print(f"Error: {err}")
            return
        print("Fetching metaobject definitions...")
        meta_defs, err = exporter.fetch_metaobject_definitions()
        if err:
            print(f"Error: {err}")
            return
        print("Building translation rows (source only)...")
        rows = exporter.build_translation_rows(products, collections, pages, meta_defs)
    else:
        print(f"  Got {len(rows)} translatable entries with locale data.")

    if args.condense:
        before = len(rows)
        rows = condense_translation_rows(rows, locale_cols, source_col)
        print(f"  Condensed: {before} → {len(rows)} rows (repeated source only, incomplete; for dictionary).")

    if args.incomplete_only:
        before = len(rows)
        rows = [r for r in rows if _has_missing_translation(r, locale_cols) and not _all_locales_empty(r, locale_cols)]
        print(f"  Incomplete-only filter: {before} → {len(rows)} rows.")

    exporter.export_csv(rows, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
