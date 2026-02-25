# Journal Alert to Notion

`journal-alert-to-notion` is a Codex skill for ingesting journal alert emails from Gmail, verifying paper metadata on official publisher pages, and writing duplicate-safe records to Notion.

## Highlights

- Gmail lookup with robust fallback strategy (strict query -> relaxed query -> inbox/all-mail crawl).
- Publisher-first metadata verification (ScienceDirect, SAGE, Wiley, INFORMS, AOM, and similar).
- Anti-bot-aware extraction flow with challenge handling and retries.
- DOI normalization + duplicate-safe Notion payload planning.
- Structured diagnostics for reproducible runs (`search_ladder`, `attempts`, warnings).

## Recent Improvements (unreleased / post-v0.1.5)

- Gmail helper adds list-row hydration retries and diagnostics (`ui_probe`, `list_hydration`) to avoid false `0`-row misses when the Gmail shell renders before message rows attach.
- Gmail search validation now treats shell-present + `0` rows as an inconclusive UI state (not a trustworthy empty search) and can refresh the view once before downgrading a strategy.
- `find_gmail_message.py` adds `--row-hydration-timeout-ms` and `--zero-row-retries` for tuning Gmail row hydration behavior.
- `find_gmail_message.py` now auto-expands Gmail clipped-message webview links (`[Message clipped] View entire message`) and merges hidden paper links/body text before verification.
- `verify_publisher_record.mjs` adds explicit Springer Link (`link.springer.com`) policy support and Springernature tracker-host support for `links.springernature.com`.
- Verifier article-type classification now uses publisher raw-type precedence and emits trace fields (`articleTypeClassificationSource`, `articleTypeMatchedHint`) so Springer `Original Paper` is not misclassified by title heuristics (for example `Perspective`).
- Added Springer fixture regression tests and Python unit tests for Gmail helper hydration/search-validation logic.
- Validated live against a Journal of Business Ethics ToC alert (Feb 24, 2026, 4:09 PM) without manual Gmail probing or manual Springer article-type normalization.
- Validated recovery of two clipped JBE papers from Gmail `view=lg&permmsgid` webview expansion (`Ethical Tools...`, `Navigating Ethical Waters...`) and imported them via Springer verification.

## Recent Improvements (v0.1.3)

- Added explicit safety guards to block `unsubscribe`, `removeAlert`, and manage-alert/preferences links before verification browser navigation (including tracker links when Gmail anchor text is available).
- Gmail extraction now blocks unsupported non-HTTP(S) schemes (for example `mailto:`) from verification candidates and records blocked-link `reason` labels in diagnostics.
- Verifier now accepts the full `find_gmail_message.py` `*_match.json` output directly (reads `candidates[*]`) instead of requiring a manually extracted links-only JSON.
- Gmail search validation now checks first-page row content (not URL state alone) to detect false `#search/...` views that still show inbox-like rows.
- Gmail crawl/search pagination now uses stronger `Older` selectors/click fallbacks and emits a truncation warning when page 1 returns the max row count but no usable `Older` control is found.
- Added Wiley tracker-host support for `el.wiley.com` and improved tracking resolution audit fields for tracker-heavy alert emails.
- Added a Wiley `cookieAbsent` fallback path so pre-resolution false redirects do not suppress real article verification (browser navigation retries the original tracker URL).
- Added final-URL dedupe within a verification batch to avoid reprocessing duplicate TOC/unsubscribe links that share the same resolved destination.
- Gmail helper now emits `link_details` (`href` + anchor text) alongside `links` to enable cheaper pre-filtering and better diagnostics.
- Validated live against a Strategic Management Journal Early View alert (Feb 16, 2026) with Wiley tracker links.

## Recent Improvements (v0.1.2)

- Fixed incomplete/truncated abstracts on Wiley article pages (including SMJ) by preferring full DOM abstract sections over teaser meta descriptions.
- Verifier now re-evaluates publisher policy after browser navigation/redirects (for example DOI -> Wiley final URL), so publisher-specific selectors are applied on the actual page.
- Added a Wiley/SMJ fixture regression test for abstract selection (`npm test`).
- Validated live by re-verifying and backfilling 9 Strategic Management Journal (Vol. 47, No. 3) records with full abstracts in Notion.

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

# Date-only mode (accept any time on the date):
# python3 scripts/find_gmail_message.py \
#   --subject "Early View Alert: Strategic Management Journal" \
#   --sender "WileyOnlineLibrary@wiley.com" \
#   --received-on "2026-02-08" \
#   --session-fallback --inject-browser-cookies \
#   --output /tmp/journal_email_match.json

node scripts/verify_publisher_record.mjs \
  --input /tmp/journal_email_match.json \
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

Recommended order for large alerts:
- Precheck obvious duplicates (exact/normalized DOI) from existing Notion rows first.
- Run publisher verification only for remaining candidates.
- Run `build_notion_payload.py` with `--require-existing` as the final duplicate-safe write gate.

## Testing

Run local parser regression tests (AOM/Atypon, Wiley, Springer fixtures):

```bash
npm test
```

Run Gmail helper logic unit tests:

```bash
python3 -m unittest discover -s tests -p 'test_find_gmail_message_helpers.py'
```

These tests validate recent parser improvements plus Gmail zero-row hydration/search-validation decision logic.

## Troubleshooting

- Gmail subject mismatch by punctuation:
  Gmail may display/store a subject with a trailing period while the copied subject omits it. The Gmail helper now normalizes trailing punctuation, but keep the received timestamp for exact targeting.
- Gmail search appears to stay in Inbox:
  The helper already validates search-state and downgrades strategy when Gmail silently fails a query route. Review `attempts` diagnostics in the JSON output.
- Gmail helper shows `0` rows but the email exists:
  Inspect `attempts[*].ui_probe` and `attempts[*].list_hydration` (and `<output>.partial.json` if present). The helper now retries row hydration and may refresh the view once before downgrading a search/crawl attempt.
- Gmail helper looks stalled during a long Playwright run:
  If `--output` is set, inspect `<output>.partial.json` to see the latest phase (`list_hydration_*`, `candidate_row_match`, `candidate_opened`, `candidate_extracted`) and current strategy.
- Gmail email body is clipped and missing paper links:
  The helper now auto-expands Gmail webview links (`view=lg&permmsgid`) and merges hidden links into `all_link_details`/`links`. Check `match.gmail_webview_expansion` in the JSON output to confirm expansion succeeded and how many links were added.
- Protected publisher pages (Cloudflare / anti-bot):
  Prefer running with a normal authenticated Chrome session and attach via `--cdp-url` when verification is blocked.
- Wiley / SMJ challenge in headless verification:
  The verifier now auto-retries Wiley challenge failures with a local CDP browser if available, otherwise a headed Chrome retry; inspect `fallbackRuns` in output JSON for the path used.
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
