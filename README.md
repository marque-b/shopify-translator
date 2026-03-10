Use this tool to save money when translating a Shopify Store by using your own api and model of choice (via OpenRouter). When running for the first time, I suggest using `export_translation_entries.py --condense` to pull a table with entries that appear more than once in your store, so you can copy some of them to the dictionary. `export_translation_entries.py --incomplete-only` is useful for subsequent processing.

**Digest.** Each translatable string is identified by a **digest** (a hash of the content). The same source text in many places (e.g. "Add to cart" on several products) shares one digest. Use the **dictionary** to store one translation per digest; the process step then fills every row that shares that digest from the dictionary, so you translate repeated phrases once. Build the dictionary by copying rows from a `--condense` export or by running `--translate` (new digests get translated and appended to the dictionary).

## Prerequisites

- Python 3.10+
- A Shopify app with Admin API access and the following scopes:
  - `read_translations` and `write_translations` (required for export and upload)
  - `read_locales` or `read_markets_home` (required: used by `configure.py` to list enabled shop locales)
- Optional: [OpenRouter](https://openrouter.ai/) API key for AI translation (`process_translations.py --translate`)

## First-time setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Create configuration**

   Run the interactive config to generate `config.json` and optionally `.env`:

   ```bash
   python configure.py
   ```

   You will be prompted for:

   - Shop domain (e.g. `mystore.myshopify.com`)
   - Shopify app Client ID and Client secret
   - Source locale (e.g. `pt-BR` or `en`) and target locales (e.g. `en`, `es`, `it`, `fr`)
   - Whether to export all resource types or a subset

   If credentials are valid, the script can fetch your shop’s enabled locales via the API so you can pick from the list. Valid translation locales are only those returned by [shopLocales](https://shopify.dev/docs/api/admin-graphql/latest/queries/shopLocales).

   Alternatively, copy `.env.example` to `.env` and edit `config.json` by hand.

3. **Prepare system instructions** (if using AI translation)

   Before running `process_translations.py --translate`, edit `translation_system_prompt.md`: replace the placeholders (e.g. source/target languages, brand name, tone) with your store’s context. The AI uses this file as the system prompt; good instructions improve translation quality and consistency.

## Configuration

- **config.json** – Main config. Important keys:
  - `source_locale` – Shopify ISO code of the language your content is in (e.g. `"pt-BR"`, `"en"`).
  - `target_locales` – List of Shopify ISO codes to translate into (e.g. `["en", "es", "it", "fr"]`).
  - `resource_types` – Optional. List of TranslatableResourceType values to export (e.g. `["PRODUCT", "COLLECTION", "PAGE"]`). If missing or empty, all supported types are used.
- **.env** – Secrets (do not commit). Use `SHOPIFY_SHOP`, `SHOPIFY_CLIENT_ID`, `SHOPIFY_CLIENT_SECRET`. For AI translation (`--translate`): `OPENROUTER_API_KEY` (required); `OPENROUTER_MODEL` (optional, default: `openai/gpt-4o-mini`).


## Workflow

1. **Init** (once): `python configure.py`
2. **Export**: `python export_translation_entries.py` → produces `translation_entries.csv`
3. **Process**: apply dictionary, translate missing locales, check-handles, etc. (see below)
4. **Upload**: `python upload_translations.py translation_entries_changed.csv`

### Export

```bash
python export_translation_entries.py
python export_translation_entries.py --output my_entries.csv
python export_translation_entries.py --theme-id THEME_ID   # only theme entries
python export_translation_entries.py --incomplete-only         # only rows with at least one missing locale
python export_translation_entries.py --condense                 # dictionary-style CSV (repeated source values only)
```

Uses `config.json` for `source_locale`, `target_locales`, and `resource_types`. CSV columns are `resource_type`, `resource_id`, `resource_handle`, `field_key`, `digest`, plus one column per locale (e.g. `pt_br`, `en`, `es`, `it`, `fr`).

### Process

```bash
# Apply dictionary (fill target locales from dictionary.csv where digest matches)
python process_translations.py -i translation_entries.csv

# Translate missing locales via OpenRouter (requires OPENROUTER_API_KEY; set OPENROUTER_MODEL in .env)
python process_translations.py --translate -i translation_entries.csv

# Check handles (normalize all locale columns, then keep only handle rows with all locales present and valid slugs)
python process_translations.py --check-handles -i translation_entries.csv

# Sanitize dictionary (remove digests that appear in given CSVs)
python process_translations.py --sanitize check_handles_removed.csv
```

Uses `config.json` for source and target locale columns. Dictionary CSV must have columns: `Digest`, then the source column, then target columns (e.g. `pt_br`, `en`, `es`, `it`, `fr`).

### Upload

```bash
python upload_translations.py translation_entries_changed.csv
python upload_translations.py --dry-run translation_entries_changed.csv
python upload_translations.py --delay 0.5 translation_entries_changed.csv
```

Pushes only target-locale columns from the CSV to the store via `translationsRegister`. Uses `config.json` for which columns are target locales.

## Dictionary and system prompt

- **dictionary.csv** – Columns: `Digest`, `<source_column>`, `<target_columns>`. Used to fill translations by digest and (optionally) to inject term mappings into the AI system prompt when using `--translate`.
- **translation_system_prompt.md** – System prompt for the translation model. **Prepare this file before using `--translate`:** replace placeholders with your source/target languages, brand, tone, and any rules (e.g. `{{variable}}`, `%{variable}`). The default is generic; edit it for your store to get better results.

## How to contribute

- Open an issue to report a bug, ask a question, or suggest a feature.
- Fork the repo, make your changes, and open a pull request. Keep PRs focused and include a short description of what changed.
- Ensure your code follows the existing style and that the scripts still run as expected with the documented workflow.

## Disclaimer

This project is provided **as is**, without warranty of any kind. Use it at your own risk. The authors are not responsible for any damage or data loss resulting from the use of this software, including but not limited to translations pushed to your Shopify store. Always review changes (e.g. with `--dry-run` where available) and keep backups before modifying store data.

## License

MIT. See [LICENSE](LICENSE).

