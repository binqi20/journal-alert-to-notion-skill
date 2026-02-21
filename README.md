# Journal Alert to Notion

`journal-alert-to-notion` is a Codex skill for ingesting journal alert emails from Gmail, verifying paper metadata on official publisher pages, and writing duplicate-safe records to Notion.

## Highlights

- Gmail lookup with robust fallback strategy (strict query -> relaxed query -> inbox/all-mail crawl).
- Publisher-first metadata verification (ScienceDirect, SAGE, Wiley, INFORMS, AOM, and similar).
- Anti-bot-aware extraction flow with challenge handling and retries.
- DOI normalization + duplicate-safe Notion payload planning.
- Structured diagnostics for reproducible runs (`search_ladder`, `attempts`, warnings).

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
```

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
