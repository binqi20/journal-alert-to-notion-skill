# Journal Alert to Notion

`journal-alert-to-notion` is a Codex skill for ingesting journal alert emails from Gmail, verifying paper metadata on official publisher pages, and writing duplicate-safe records to Notion.

## Highlights

- Gmail lookup with robust fallback strategy (strict query -> relaxed query -> inbox/all-mail crawl).
- Publisher-first metadata verification (ScienceDirect, SAGE, Wiley, INFORMS, AOM, and similar).
- Anti-bot-aware extraction flow with challenge handling and retries.
- DOI normalization + duplicate-safe Notion payload planning.
- Structured diagnostics for reproducible runs (`search_ladder`, `attempts`, warnings).

## Recent Improvements (v0.1.1)

- AOM/Atypon (`journals.aom.org`) extraction support with `dc.*` metadata parsing + DOM fallbacks.
- AOM tracked-link resolution support (`el.aom.org`) and fast exclusion for non-article/account/privacy links.
- ScienceDirect-aware verification remains curl-first (`auto`) with browser fallback only when metadata is incomplete.
- Gmail subject matching now tolerates trailing punctuation differences (for example copied subject without final period).
- Gmail UI fallback uses selector-first readiness checks before short `networkidle` fallback (faster on dynamic Gmail pages).
- `build_notion_payload.py` now supports `--existing -` / `--existing-stdin` and `--save-existing` for duplicate-safe reruns.
- Added local AOM/Atypon fixture regression tests (`npm test`).
- `v0.1.1` was validated end-to-end against a live Academy of Management ToC alert before release.

## Repository Contents

- [`SKILL.md`](./SKILL.md): full workflow and policy guide.
- [`scripts/find_gmail_message.py`](./scripts/find_gmail_message.py): Gmail message discovery helper.
- [`scripts/verify_publisher_record.mjs`](./scripts/verify_publisher_record.mjs): publisher verification helper.
- [`scripts/build_notion_payload.py`](./scripts/build_notion_payload.py): Notion payload + dedupe helper.

## Quick Start

### 1. Prerequisites

- Python 3.10+
- Node.js 18+
- `playwright` installed for browser automation
- Authenticated Gmail browser session
- Notion MCP access

Install dependencies:

```bash
uv pip install browser-cookie3 playwright
npm install
```

### 2. Run the workflow helpers

```bash
python3 scripts/find_gmail_message.py \
  --subject "New articles for Management Science are available online" \
  --sender "alerts@informs.org" \
  --received-at "2026-01-19 17:18" \
  --timezone "America/New_York" \
  --inject-browser-cookies \
  --session-fallback \
  --max-pages 8 \
  --date-window-days 1 \
  --output /tmp/journal_email_match.json

node scripts/verify_publisher_record.mjs \
  --input /tmp/journal_email_links.json \
  --sciencedirect-mode auto \
  --output /tmp/journal_verified_records.json

python3 scripts/build_notion_payload.py \
  --records /tmp/journal_verified_records.json \
  --existing /tmp/notion_existing_rows.json \
  --require-existing \
  --data-source-id "<collection-id>" \
  --output /tmp/notion_create_payload.json

# Optional duplicate-safe pipeline when piping raw Notion view JSON:
# cat /tmp/notion_query_view_raw.json | \
# python3 scripts/build_notion_payload.py \
#   --records /tmp/journal_verified_records.json \
#   --existing-stdin \
#   --save-existing /tmp/notion_existing_rows.json \
#   --require-existing \
#   --data-source-id "<collection-id>" \
#   --output /tmp/notion_create_payload.json
```

## Testing

Run local parser regression tests (AOM/Atypon fixtures):

```bash
npm test
```

These tests validate recent parser improvements, including AOM journal inference and APA author formatting cleanup.

## Troubleshooting

- Gmail subject mismatch by punctuation:
  Gmail may display/store a subject with a trailing period while the copied subject omits it. The Gmail helper now normalizes trailing punctuation, but keep the received timestamp for exact targeting.
- Gmail search appears to stay in Inbox:
  The helper already validates search-state and downgrades strategy when Gmail silently fails a query route. Review `attempts` diagnostics in the JSON output.
- Protected publisher pages (Cloudflare / anti-bot):
  Prefer running with a normal authenticated Chrome session and attach via `--cdp-url` when verification is blocked.
- ScienceDirect verification challenge:
  Use the default `--sciencedirect-mode auto`; it prefers a fast curl path and falls back to browser only if required fields are incomplete.

## Privacy & Safety

- Do not commit cookies, storage state files, or auth tokens.
- Treat publisher landing pages and DOI targets as the source of truth.
- Unknown fields must be marked `[Not verified]` (no fabrication).
- Use user-authenticated sessions only; no CAPTCHA-solving bypass behavior.

## Contributing

Issues and pull requests are welcome. If reporting an extraction issue, include:

- journal name,
- alert subject + timestamp,
- failing URL pattern,
- sanitized logs or JSON output artifacts.

## Contact

- Email: `tangbinqi@gmail.com`
- Preferred support channel: open a GitHub Issue in this repository.
