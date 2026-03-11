#!/usr/bin/env python3
"""
Process Shopify translation entries CSV: apply dictionary, translate via API, check-handles, sanitize.

Input CSV columns: resource_type, resource_id, resource_handle, field_key, digest, plus locale columns (e.g. pt_br, en, es, it, fr).
Source locale and target locales come from config.json.

What you can do (one mode per run):

  1. Apply dictionary (default)
     Fill target locales from dictionary.csv where digest matches. Output: only rows that changed.
     Example: python process_translations.py -i entries.csv -o changed.csv
     Use --resource-type TYPE to limit to one resource type.

  2. Translate missing locales (--translate)
     For rows missing any target locale: call OpenRouter to translate source → target locales.
     Digests already in dictionary (with all target locales) are filled from it; only missing digests call the API.
     Requires OPENROUTER_API_KEY in .env. Output default: {input_stem}_translated.csv.
     Example: python process_translations.py --translate -i entries.csv
     Use --dry-run to see what would be translated; use --test [N] to limit to first N digests.
     Can be combined with --check-handles: translate first, then run handle-check on the translated file.

  3. Check handles (--check-handles)
     For rows with field_key=handle only: normalize locale columns (accents, spaces→hyphens) and keep only those
     with all locales present and valid URL slugs. All other rows are left unchanged.
     Output: {stem}_check_handles.csv (valid handle rows + all non-handle rows) and {stem}_check_handles_removed.csv.
     Example: python process_translations.py --check-handles -i entries.csv

  4. Sanitize dictionary (--sanitize CSV [CSV ...])
     Collect digests from the given CSV(s) and remove those rows from dictionary.csv.
     Example: python process_translations.py --sanitize check_handles_removed.csv
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path

import requests

# Allow large fields (e.g. HTML in translation entries)
csv.field_size_limit(max(csv.field_size_limit(), 2**20))

# Load .env from script directory (project root when run as main)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# Default paths relative to script directory
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENTRIES = SCRIPT_DIR / "translation_entries.csv"
DEFAULT_DICTIONARY = SCRIPT_DIR / "dictionary.csv"
DEFAULT_CONFIG = SCRIPT_DIR / "config.json"
DEFAULT_OUTPUT = SCRIPT_DIR / "translation_entries_changed.csv"
DEFAULT_SYSTEM_PROMPT = SCRIPT_DIR / "translation_system_prompt.md"

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")

# OpenRouter rate limit backoff
OPENROUTER_RETRY_COUNT = 3
OPENROUTER_RETRY_SLEEP = 10


def _translation_response_schema(target_columns: list[str]) -> dict:
    """Build OpenRouter structured output schema for target locale columns."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "translation_result",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {col: {"type": "string", "description": f"Translation for {col}"} for col in target_columns},
                "required": target_columns,
                "additionalProperties": False,
            },
        },
    }


def load_system_prompt(path: Path | None = None) -> str:
    """Load translation system prompt from markdown file."""
    path = path or DEFAULT_SYSTEM_PROMPT
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


# Cap dictionary rows in the system prompt so the prompt stays under API context limits (e.g. 128k)
MAX_DICTIONARY_ROWS_FOR_PROMPT = 250


def format_dictionary_for_prompt(
    dictionary_path: Path,
    source_column: str,
    target_columns: list[str],
    max_rows: int = MAX_DICTIONARY_ROWS_FOR_PROMPT,
) -> str:
    """
    Load dictionary.csv (Digest, source_column, target_columns) and return a block of text
    to append to the system prompt so the model uses these preferred term mappings.
    Only the first max_rows entries are included to keep the prompt under context limits.
    """
    if not dictionary_path.exists():
        return ""
    locale_cols = [source_column] + target_columns
    rows: list[dict[str, str]] = []
    with open(dictionary_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw = reader.fieldnames or []
        fieldnames = [k.strip().lstrip("\ufeff") for k in raw]
        for row in reader:
            row = {k.strip().lstrip("\ufeff"): v for k, v in row.items()}
            src = (row.get(source_column) or "").strip()
            if not src:
                continue
            if not any((row.get(c) or "").strip() for c in target_columns if c in row):
                continue
            rows.append({c: (row.get(c) or "").strip() for c in locale_cols})
            if len(rows) >= max_rows:
                break
    if not rows:
        return ""
    header = " | ".join(locale_cols)
    sep = "------|" + "---|" * len(locale_cols)
    lines = [
        "",
        "---",
        "## Preferred product/UI term mappings (dictionary)",
        "Use these exact translations when the source text contains the term (exact or as part of a phrase) to keep consistency across the store:",
        "",
        header,
        sep,
    ]
    for r in rows:
        lines.append(" | ".join(r.get(c, "") for c in locale_cols))
    if len(rows) == max_rows:
        lines.append("")
        lines.append(f"(… first {max_rows} entries only; full dictionary used for exact digest matches.)")
    return "\n".join(lines)


def _extract_json_from_response(text: str) -> dict:
    """Extract a JSON object from model response (may be wrapped in markdown or extra text)."""
    text = text.strip()
    # Strip optional markdown code fence
    if text.startswith("```"):
        first = text.find("\n")
        if first != -1:
            text = text[first + 1 :]
        if text.endswith("```"):
            text = text[:-3].strip()
    # Find first { and last }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("No JSON object found in response")


def translate_source_to_targets(
    source_value: str,
    api_key: str,
    system_prompt: str,
    target_columns: list[str],
    response_schema: dict,
    model: str = OPENROUTER_MODEL,
) -> dict[str, str]:
    """
    Call OpenRouter API to translate one source string into target locales.
    Returns {col: "..." for col in target_columns}.
    """
    source_value = (source_value or "").strip()
    if not source_value:
        return {loc: "" for loc in target_columns}

    keys_str = ", ".join(target_columns)
    user_content = (
        f"Translate the following string into the target language(s). "
        f"Output only a single JSON object with keys: {keys_str}. No other text or markdown.\n\n"
        f"Source:\n{source_value}"
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.2,
        "response_format": response_schema,
    }
    # Reasoning tokens only for models that support it (e.g. o1, gpt-oss); 4o-mini does not
    if "o1" in model or "gpt-oss" in model:
        payload["reasoning"] = {"enabled": True}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    last_error = None
    for attempt in range(OPENROUTER_RETRY_COUNT):
        try:
            resp = requests.post(
                OPENROUTER_URL,
                headers=headers,
                data=json.dumps(payload),
                timeout=120,
            )
            if resp.status_code == 429:
                # Rate limited: use Retry-After if present, else default
                retry_after = resp.headers.get("Retry-After")
                wait = OPENROUTER_RETRY_SLEEP
                if retry_after and retry_after.isdigit():
                    wait = int(retry_after)
                if attempt < OPENROUTER_RETRY_COUNT - 1:
                    time.sleep(wait)
                    continue
                last_error = requests.HTTPError(f"429 Too Many Requests (rate limited) after {OPENROUTER_RETRY_COUNT} attempts")
                continue
            if not resp.ok:
                body = resp.text
                try:
                    err = resp.json()
                    body = err.get("error", {}).get("message", body) if isinstance(err.get("error"), dict) else json.dumps(err)[:500]
                except Exception:
                    body = (body or "(empty)")[:800]
                print(f"OpenRouter HTTP {resp.status_code}: {body}", file=sys.stderr)
            resp.raise_for_status()
            data = resp.json()
            choice = data.get("choices")
            if not choice:
                raise ValueError("No choices in OpenRouter response")
            message = choice[0].get("message") or {}
            content = (message.get("content") or "").strip()
            # With reasoning enabled, final answer may be in content or in reasoning_details
            if not content and message.get("reasoning_details"):
                rd = message["reasoning_details"]
                if isinstance(rd, dict) and "parts" in rd:
                    parts = rd["parts"]
                    if parts and isinstance(parts[-1], dict) and "content" in parts[-1]:
                        content = (parts[-1].get("content") or "").strip()
                elif isinstance(rd, list) and rd and isinstance(rd[-1], dict):
                    content = (rd[-1].get("content") or rd[-1].get("text") or "").strip()
            if not content:
                raise ValueError("Empty response content from API (no content or reasoning_details)")
            # Structured output may return raw JSON; otherwise extract from text
            try:
                out = json.loads(content)
            except json.JSONDecodeError:
                try:
                    out = _extract_json_from_response(content)
                except ValueError:
                    # Log a snippet so user can see what the model returned
                    snippet = content[:400] + ("..." if len(content) > 400 else "")
                    raise ValueError(f"No JSON in response. Content snippet: {snippet!r}") from None
            result = {}
            for loc in target_columns:
                result[loc] = (out.get(loc) or "").strip() if isinstance(out.get(loc), str) else ""
            return result
        except (requests.RequestException, ValueError, json.JSONDecodeError) as e:
            last_error = e
            if attempt < OPENROUTER_RETRY_COUNT - 1:
                time.sleep(OPENROUTER_RETRY_SLEEP)
    raise RuntimeError(f"OpenRouter translation failed after {OPENROUTER_RETRY_COUNT} attempts: {last_error}")


def load_config(path: Path) -> dict:
    """Load and normalize config JSON (source_locale, target_locales, legacy keys)."""
    from lib.locale_config import load_config as load_config_file, normalize_config
    return normalize_config(load_config_file(path))


def get_locale_columns(config: dict) -> tuple[str, list[str], list[str]]:
    """Return (source_column, target_columns, locale_columns) from config."""
    from lib.locale_config import get_locale_columns as _get
    return _get(config)


def _normalize_headers(row: dict[str, str]) -> dict[str, str]:
    """Strip BOM and whitespace from header keys."""
    return {k.strip().lstrip("\ufeff"): v for k, v in row.items()}


def load_dictionary(path: Path, target_columns: list[str]) -> dict[str, dict[str, str]]:
    """
    Load dictionary.csv: digest -> {col: value} for target_columns.
    Only includes target columns that have a non-empty value.
    """
    result: dict[str, dict[str, str]] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = _normalize_headers(row)
            digest = (row.get("Digest") or "").strip()
            if not digest:
                continue
            result[digest] = {}
            for col in target_columns:
                val = (row.get(col) or "").strip()
                if val:
                    result[digest][col] = val
    return result


def load_entries(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    """Load translation entries CSV into list of dicts; returns (rows, fieldnames)."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_fieldnames = reader.fieldnames or []
        fieldnames = [k.strip().lstrip("\ufeff") for k in raw_fieldnames]
        rows = []
        for row in reader:
            rows.append(_normalize_headers(row))
        return rows, fieldnames


def apply_dictionary(
    rows: list[dict[str, str]],
    dictionary: dict[str, dict[str, str]],
    resource_type_filter: str | None,
) -> list[dict[str, str]]:
    """
    Apply dictionary translations to entries.
    Returns only rows that were modified (at least one target locale changed).
    """
    changed = []
    for row in rows:
        if resource_type_filter is not None:
            rt = (row.get("resource_type") or "").strip()
            if rt != resource_type_filter:
                continue
        digest = (row.get("digest") or "").strip()
        if digest not in dictionary:
            continue
        trans = dictionary[digest]
        row_modified = False
        for loc, new_val in trans.items():
            if loc not in row:
                continue
            old_val = (row.get(loc) or "").strip()
            if old_val != new_val:
                row[loc] = new_val
                row_modified = True
        if row_modified:
            changed.append(row)
    return changed


def write_csv(rows: list[dict[str, str]], path: Path, fieldnames: list[str]) -> None:
    """Write rows to CSV with given fieldnames. Writes header only when rows is empty."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        if rows:
            writer.writerows(rows)


# Check handles: field_key value for product/locale handles
FIELD_KEY_HANDLE = "handle"
# Valid handle = URL slug only: lowercase, digits, hyphens, underscores; no spaces or prefixes
_VALID_HANDLE_RE = re.compile(r"^[a-z0-9_-]+$")


def _output_paths_from_input(input_path: Path, kept_suffix: str, removed_suffix: str) -> tuple[Path, Path]:
    """From input path (e.g. translation_entries.csv), return (stem_kept_suffix.csv, stem_removed_suffix.csv)."""
    stem = input_path.stem
    parent = input_path.parent
    return (
        parent / f"{stem}{kept_suffix}.csv",
        parent / f"{stem}{removed_suffix}.csv",
    )


def remove_accents(s: str) -> str:
    """Return the string with accented characters normalized to ASCII equivalents (e.g. é → e, ñ → n)."""
    if not s:
        return s
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def normalize_handle_value(s: str) -> str:
    """Normalize a handle for URL slug: remove accents, replace runs of whitespace with a single hyphen, strip."""
    if not s:
        return s
    s = remove_accents((s or "").strip())
    s = re.sub(r"\s+", "-", s).strip("-")
    return s


def _is_valid_handle_value(value: str) -> bool:
    """Return True if value is a valid URL-slug handle (no spaces, no hex prefix, only [a-z0-9_-])."""
    if not value:
        return False
    s = value.strip()
    if not s or " " in s:
        return False
    return bool(_VALID_HANDLE_RE.match(s))


def check_handles_csv(
    input_path: Path,
    locale_columns: list[str],
    field_key_column: str = "field_key",
) -> tuple[Path, Path, int, int]:
    """
    For rows where field_key = "handle" only: normalize locale columns (remove accents, spaces → hyphens)
    and keep only those with all locale columns present and valid URL slugs. All other rows are left
    unchanged and always kept.

    Writes to {stem}_check_handles.csv (valid handle rows + all non-handle rows) and
    {stem}_check_handles_removed.csv (handle rows that failed the check).
    Returns (path_kept, path_removed, n_kept, n_removed).
    """
    out_kept, out_removed = _output_paths_from_input(input_path, "_check_handles", "_check_handles_removed")
    rows, fieldnames = load_entries(input_path)
    kept: list[dict[str, str]] = []
    removed: list[dict[str, str]] = []
    for row in rows:
        is_handle_row = (row.get(field_key_column) or "").strip() == FIELD_KEY_HANDLE
        if not is_handle_row:
            kept.append(row)
            continue
        # Handle row only: normalize locale columns (accents + spaces → hyphens), then validate
        for loc in locale_columns:
            if loc in row and row.get(loc):
                row[loc] = normalize_handle_value(row[loc])
        all_present = True
        all_valid = True
        for loc in locale_columns:
            val = (row.get(loc) or "").strip()
            if not val:
                all_present = False
                break
            if not _is_valid_handle_value(val):
                all_valid = False
                break
        if all_present and all_valid:
            kept.append(row)
        else:
            removed.append(row)
    write_csv(kept, out_kept, fieldnames)
    write_csv(removed, out_removed, fieldnames)
    return out_kept, out_removed, len(kept), len(removed)


def collect_digests_from_csv(path: Path, digest_column: str = "digest") -> set[str]:
    """Load a CSV and return the set of non-empty digest values from the digest column."""
    digests: set[str] = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_fieldnames = reader.fieldnames or []
        fieldnames = [k.strip().lstrip("\ufeff") for k in raw_fieldnames]
        # Support both 'digest' and 'Digest'
        key = digest_column if digest_column in fieldnames else (
            "Digest" if "Digest" in fieldnames else "digest"
        )
        for row in reader:
            row = _normalize_headers(row)
            d = (row.get(key) or "").strip()
            if d:
                digests.add(d)
    return digests


def sanitize_dictionary(
    dictionary_path: Path,
    digests_to_remove: set[str],
) -> tuple[int, int]:
    """
    Remove from dictionary.csv any row whose Digest is in digests_to_remove.
    Writes back to dictionary_path. Returns (rows_before, rows_removed).
    """
    with open(dictionary_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_fieldnames = reader.fieldnames or []
        fieldnames = [k.strip().lstrip("\ufeff") for k in raw_fieldnames]
        digest_key = "Digest" if "Digest" in fieldnames else "digest"
        rows = []
        for row in reader:
            rows.append(_normalize_headers(row))
    n_before = len(rows)
    kept = [r for r in rows if (r.get(digest_key) or "").strip() not in digests_to_remove]
    n_removed = n_before - len(kept)
    write_csv(kept, dictionary_path, fieldnames)
    return n_before, n_removed


def run_sanitize(input_paths: list[Path], dictionary_path: Path) -> tuple[int, int]:
    """
    Collect all digests from the given CSV files, then remove those digests from dictionary.csv.
    Returns (total_digests_collected, rows_removed_from_dictionary).
    """
    all_digests: set[str] = set()
    for path in input_paths:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        all_digests |= collect_digests_from_csv(path)
    n_before, n_removed = sanitize_dictionary(dictionary_path, all_digests)
    return len(all_digests), n_removed


def append_one_translation_to_dictionary(
    dictionary_path: Path,
    digest: str,
    source_column: str,
    target_columns: list[str],
    source_value: str,
    result: dict[str, str],
) -> None:
    """Append a single digest + source and target locale values to dictionary.csv."""
    dictionary_columns = ("Digest", source_column, *target_columns)
    write_header = not dictionary_path.exists()
    row = {"Digest": digest, source_column: source_value}
    for col in target_columns:
        row[col] = result.get(col, "")
    with open(dictionary_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(dictionary_columns))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def run_translate(
    input_path: Path,
    output_path: Path,
    system_prompt_path: Path,
    api_key: str,
    model: str,
    resource_type_filter: str | None,
    source_column: str,
    target_columns: list[str],
    dry_run: bool = False,
    test_limit: int | None = None,
    dictionary_path: Path | None = None,
) -> int:
    """
    Load entries, translate source_column → target_columns via OpenRouter for rows missing
    any target locale; write only successfully translated rows to output_path.
    If dry_run: only report what would be translated, no API calls, no write.
    If test_limit: only translate the first test_limit digests.
    If dictionary_path: digests already in the dictionary (with all target columns) are filled from it; only missing digests call the API.
    """
    rows, fieldnames = load_entries(input_path)
    if not rows:
        print("No rows in input.", file=sys.stderr)
        return 0

    system_prompt = load_system_prompt(system_prompt_path)
    if not system_prompt:
        print("Error: system prompt file not found or empty.", file=sys.stderr)
        return 1

    response_schema = _translation_response_schema(target_columns)

    def needs_translation(row: dict[str, str]) -> bool:
        pt = (row.get(source_column) or "").strip()
        if not pt:
            return False
        if resource_type_filter is not None:
            if (row.get("resource_type") or "").strip() != resource_type_filter:
                return False
        for loc in target_columns:
            if loc not in row:
                continue
            if not (row.get(loc) or "").strip():
                return True
        return False

    # Group by digest so we call API once per digest
    digest_to_rows: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        if not needs_translation(row):
            continue
        digest = (row.get("digest") or "").strip()
        if not digest:
            continue
        digest_to_rows.setdefault(digest, []).append(row)

    if not digest_to_rows:
        print("No rows need translation (all target locales already set).", file=sys.stderr)
        if not dry_run:
            write_csv([], output_path, fieldnames)
        return 0

    # Optional limit for --test (e.g. first 10 digests). Limit is by digest, not by CSV row.
    if test_limit is not None and test_limit >= 0:
        digest_to_rows = dict(list(digest_to_rows.items())[: test_limit])
        n_rows_affected = sum(len(g) for g in digest_to_rows.values())
        print(f"Test mode: limiting to first {len(digest_to_rows)} digest(s) ({n_rows_affected} CSV rows).", file=sys.stderr)

    # Load dictionary: digests already here skip API and use these translations
    dictionary: dict[str, dict[str, str]] = {}
    if dictionary_path and dictionary_path.exists():
        dictionary = load_dictionary(dictionary_path, target_columns)
    n_total = len(digest_to_rows)
    n_from_dict = sum(
        1
        for digest, group in digest_to_rows.items()
        if digest in dictionary and all(loc in dictionary[digest] for loc in target_columns)
    )
    n_need_api = n_total - n_from_dict

    if dry_run:
        print(f"[DRY RUN] {n_total} unique digest(s): {n_from_dict} from dictionary, {n_need_api} would call OpenRouter ({model}).", file=sys.stderr)
        print(f"[DRY RUN] Output would be written to {output_path}.", file=sys.stderr)
        for i, (digest, group) in enumerate(list(digest_to_rows.items())[:5], 1):
            src_val = (group[0].get(source_column) or "").strip()
            preview = (src_val[:60] + "…") if len(src_val) > 60 else src_val
            from_where = "dictionary" if digest in dictionary and all(loc in dictionary[digest] for loc in target_columns) else "API"
            print(f"  {i}. digest {digest[:16]}... → {preview!r} ({from_where})", file=sys.stderr)
        if n_total > 5:
            print(f"  ... and {n_total - 5} more.", file=sys.stderr)
        return 0

    print("(Only successfully translated rows are written to the output file.)", file=sys.stderr)
    if "o1" in model or "gpt-oss" in model:
        print("(First request may take 30–60s with reasoning enabled.)", file=sys.stderr)

    BAR_WIDTH = 24
    failed_digests: list[tuple[str, str]] = []
    rows_to_write: list[dict[str, str]] = []
    api_calls_done = 0
    dict_used = 0
    n_appended_to_dict = 0
    for i, (digest, group) in enumerate(digest_to_rows.items(), 1):
        src_val = (group[0].get(source_column) or "").strip()
        use_dictionary = (
            digest in dictionary and all(loc in dictionary[digest] for loc in target_columns)
        )
        try:
            if use_dictionary:
                result = {loc: dictionary[digest].get(loc, "") for loc in target_columns}
                dict_used += 1
            else:
                result = translate_source_to_targets(
                    src_val, api_key, system_prompt, target_columns, response_schema, model=model
                )
                api_calls_done += 1
                if dictionary_path and dictionary_path.suffix.lower() == ".csv":
                    append_one_translation_to_dictionary(
                        dictionary_path,
                        digest,
                        source_column,
                        target_columns,
                        src_val,
                        result,
                    )
                    n_appended_to_dict += 1
                    dictionary[digest] = {loc: result.get(loc, "") for loc in target_columns}
            for row in group:
                for loc in target_columns:
                    if loc in row and result.get(loc):
                        row[loc] = result[loc]
                rows_to_write.append(row)
            pct = int(100 * i / n_total) if n_total else 0
            filled = int(BAR_WIDTH * i / n_total) if n_total else 0
            bar = "\u2588" * filled + "\u2591" * (BAR_WIDTH - filled)
            line = f"  [{bar}] {pct:3}%  {i}/{n_total}  \u00b7  {dict_used} dict, {api_calls_done} API  "
            print(f"\r{line}", end="", file=sys.stderr)
            sys.stderr.flush()
            if not use_dictionary and i < n_total:
                time.sleep(0.5)
        except Exception as e:
            print(file=sys.stderr)  # newline so error message doesn't overwrite progress
            failed_digests.append((digest, str(e)))
            print(f"  Error digest {digest[:16]}...: {e}", file=sys.stderr)
    print(file=sys.stderr)  # newline after progress line

    write_csv(rows_to_write, output_path, fieldnames)
    if test_limit is not None and test_limit >= 0:
        print(f"Wrote {len(rows_to_write)} rows (from {len(digest_to_rows)} digests) to {output_path}", file=sys.stderr)
    else:
        print(f"Wrote {len(rows_to_write)} successfully translated rows to {output_path}", file=sys.stderr)
    if n_appended_to_dict:
        print(f"Appended {n_appended_to_dict} new digest(s) to {dictionary_path.name}.", file=sys.stderr)
    if failed_digests:
        print(f"Failed: {len(failed_digests)}/{n_total} digest(s) (see errors above). Re-run with same input to retry only missing ones.)", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Process Shopify translation entries using dictionary.csv; output only changed rows. Use --translate to fill missing locales via OpenRouter API."
    )
    parser.add_argument(
        "--resource-type",
        dest="resource_type",
        metavar="TYPE",
        help="Only process entries with this resource_type (e.g. 'online store theme'). If omitted, process all entries.",
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=DEFAULT_ENTRIES,
        help=f"Input translation entries CSV (default: {DEFAULT_ENTRIES.name})",
    )
    parser.add_argument(
        "--dictionary",
        "-d",
        type=Path,
        default=DEFAULT_DICTIONARY,
        help=f"Dictionary CSV digest -> translations (default: {DEFAULT_DICTIONARY.name})",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output CSV. With --translate: default is {input_stem}_translated.csv. Otherwise default is translation_entries_changed.csv.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to config.json (default: config.json in script directory).",
    )
    parser.add_argument(
        "--translate",
        action="store_true",
        help="Call OpenRouter API to translate source → target locales for rows missing any target. Requires OPENROUTER_API_KEY in .env.",
    )
    parser.add_argument(
        "--system-prompt",
        type=Path,
        default=DEFAULT_SYSTEM_PROMPT,
        help=f"Path to system prompt markdown for translation (default: {DEFAULT_SYSTEM_PROMPT.name})",
    )
    parser.add_argument(
        "--model",
        default=OPENROUTER_MODEL,
        help=f"OpenRouter model (default: {OPENROUTER_MODEL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --translate: do not call the API or write output; only report how many digests would be translated.",
    )
    parser.add_argument(
        "--test",
        nargs="?",
        const=10,
        type=int,
        default=None,
        metavar="N",
        help="With --translate: run only on the first N digests (unique source strings). Each digest can have multiple CSV rows, so output rows may exceed N. E.g. --test or --test 100.",
    )
    parser.add_argument(
        "--check-handles",
        action="store_true",
        help="For field_key=handle rows only: normalize locale columns and keep valid slug rows. Other rows unchanged. With --translate: runs on translated file. Writes {stem}_check_handles.csv and {stem}_check_handles_removed.csv.",
    )
    parser.add_argument(
        "--sanitize",
        nargs="+",
        type=Path,
        metavar="CSV",
        help="Collect digests from each CSV and remove those digests from dictionary. E.g. --sanitize check_handles_removed.csv",
    )
    args = parser.parse_args()

    # Default output: with --translate use {input_stem}_translated.csv, else DEFAULT_OUTPUT
    if args.output is None:
        args.output = (
            args.input.parent / f"{args.input.stem}_translated.csv"
            if args.translate
            else DEFAULT_OUTPUT
        )

    # Load config and derive locale columns for all modes
    config = load_config(args.config)
    source_col, target_cols, locale_cols = get_locale_columns(config)

    if args.sanitize:
        if not args.dictionary.exists():
            print(f"Error: dictionary file not found: {args.dictionary}", file=sys.stderr)
            return 1
        try:
            n_digests, n_removed = run_sanitize(args.sanitize, args.dictionary)
            print(
                f"Sanitize: collected {n_digests} digest(s) from {len(args.sanitize)} file(s), "
                f"removed {n_removed} row(s) from {args.dictionary}"
            )
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        return 0

    if args.translate:
        api_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
        if not api_key and not args.dry_run:
            print("Error: OPENROUTER_API_KEY not set. Add it to .env or environment.", file=sys.stderr)
            return 1
        if not args.input.exists():
            print(f"Error: input file not found: {args.input}", file=sys.stderr)
            return 1
        rc = run_translate(
            args.input,
            args.output,
            args.system_prompt,
            api_key or "",
            args.model,
            args.resource_type,
            source_col,
            target_cols,
            dry_run=args.dry_run,
            test_limit=args.test,
            dictionary_path=args.dictionary,
        )
        if rc != 0:
            return rc
        # If both --translate and --check-handles: run handle-check on the translated output
        if args.check_handles and args.output.exists():
            out_kept, out_removed, n_keep, n_removed = check_handles_csv(args.output, locale_cols)
            print(
                f"Check handles (on translated file): {n_keep} rows → {out_kept}, {n_removed} handle rows removed → {out_removed}"
            )
        return 0

    if args.check_handles:
        if not args.input.exists():
            print(f"Error: input file not found: {args.input}", file=sys.stderr)
            return 1
        out_kept, out_removed, n_keep, n_removed = check_handles_csv(args.input, locale_cols)
        print(
            f"Check handles: {n_keep} rows → {out_kept}, {n_removed} handle rows removed → {out_removed}"
        )
        return 0

    if not args.input.exists():
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        return 1
    if not args.dictionary.exists():
        print(f"Error: dictionary file not found: {args.dictionary}", file=sys.stderr)
        return 1

    dictionary = load_dictionary(args.dictionary, target_cols)
    if not dictionary:
        print("Warning: dictionary is empty; no rows will be changed.", file=sys.stderr)

    rows, fieldnames = load_entries(args.input)
    if not rows:
        print("No rows in input.", file=sys.stderr)
        return 0

    changed = apply_dictionary(rows, dictionary, args.resource_type)

    write_csv(changed, args.output, fieldnames)
    print(f"Processed {len(rows)} entries; {len(changed)} rows changed and written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
