---
name: journal-alert-to-notion
description: Extract research papers and editorials from Gmail journal alert emails, verify metadata directly on publisher pages (SAGE, Wiley, INFORMS, ScienceDirect, AOM, and similar), and add accurate entries to a Notion database. Includes Gmail access fallback paths, anti-bot/Cloudflare handling, and duplicate-safe Notion ingestion.
---

# Journal Alert to Notion

Use this workflow to ingest papers from journal alert emails with strict source verification, anti-bot-aware extraction, and idempotent Notion writes.

## Required Inputs

- Gmail target account (or active logged-in browser session)
- Email selector (exact subject preferred; received timestamp strongly recommended)
- Target Notion database URL
- Inclusion/exclusion rule set (default below if not provided)

## Required Capabilities

- Browser automation for Gmail and protected publisher pages (Playwright preferred)
- Notion MCP tools for database fetch/query/create
- Local JSON artifact capture for traceability

## Bundled Helper Scripts

Use these scripts under `scripts/` to standardize the workflow:

1. `scripts/find_gmail_message.py`
   - Finds an exact Gmail message by subject + received minute.
   - Accepts exact subject matching with trailing punctuation normalization (for example user input omits a terminal period shown in Gmail UI/Atom).
   - Supports optional sender validation (`--sender`) in both Atom and Playwright fallback paths.
   - Uses Atom feed with browser cookies first, then optional Playwright session fallback.
   - Implements a query ladder in Playwright fallback:
     - strict exact subject + day window + sender,
     - exact subject only,
     - subject stem (for alerts like `Alert 02 February` -> `Alert`),
     - sender + broad date window,
     - inbox crawl,
     - all-mail crawl.
   - Supports search-input fallback mode when hash search routes are unreliable.
   - Validates whether Gmail search was actually applied (not silently left in inbox-like state).
   - Supports pagination via `--max-pages` and relaxed date fallback via `--date-window-days`.
   - Handles Gmail localized timestamp formats (including narrow-space AM/PM strings).
   - Supports cross-domain Google cookie loading (`.google.com`, `mail.google.com`, `accounts.google.com`) and direct cookie injection for fallback.
   - Emits structured diagnostics (`search_ladder`, `attempts`, sampled rows, selected strategy, warnings).
   - Returns both `links` (href strings, backward compatible) and `link_details` (`{href,text}`) for cheaper pre-filtering of unsubscribe/account links.
   - Dependencies:
     - `browser-cookie3` for cookie extraction (`uv pip install browser-cookie3`)
     - Python Playwright only when using `--session-fallback` (`uv pip install playwright`)
2. `scripts/verify_publisher_record.mjs`
   - Verifies metadata from publisher pages with domain policy and challenge detection/retries.
   - Resolves tracked email links (for example `click.skem1.com`, `el.aom.org`, `el.wiley.com`) to final article URLs before verification.
   - Re-evaluates domain policy after browser navigation/redirects (for example DOI -> Wiley final URL) so publisher-specific selectors still apply on the actual article page.
   - Includes AOM/Atypon (`journals.aom.org`) metadata extraction support using `dc.*` tags + DOM fallbacks (journal breadcrumb, online date, article type).
   - Strips academic honorifics (for example `Dr.`, `Professor`) before APA author formatting.
   - Chooses the best abstract candidate across DOM + meta tags, penalizing truncated teaser snippets (for example `...`) and common issue/TOC prompts.
   - Fast-excludes known non-article links (unsubscribe/account/privacy/technology-partner) after tracked-link resolution to avoid unnecessary browser retries.
   - Deduplicates repeated tracker links that converge on the same final URL within a run (for example duplicate Wiley TOC/unsubscribe links).
   - Uses a ScienceDirect-aware strategy (`--sciencedirect-mode auto|curl|browser`, default `auto`):
     - `auto`: curl-first extraction from official ScienceDirect page HTML (including `__PRELOADED_STATE__`) with browser fallback only if metadata is incomplete.
     - `curl`: force ScienceDirect curl path only.
     - `browser`: force browser extraction only.
   - Waits/polls for Cloudflare challenge pages to clear before failing on browser paths.
   - In `--cdp-url` mode, reuses the existing Chrome profile context (instead of always creating a fresh context) for better challenge/cookie carryover.
   - Emits normalized `articleType`, `ingestDecision`, and `ingestReason` for policy-safe ingestion.
   - Dependency:
     - Node Playwright (`npm i playwright`) or `playwright-core`
3. `scripts/build_notion_payload.py`
   - Normalizes DOI URLs and builds duplicate-safe Notion `create-pages` parameters.
   - Enforces default research/editorial inclusion rules and non-research exclusion by article type.
   - Supports strict duplicate mode with `--require-existing`.
   - Accepts existing Notion view payload from stdin (`--existing -` or `--existing-stdin`) and can save it locally with `--save-existing` for reproducible duplicate-safe reruns.

### Local Regression Tests (AOM/Atypon Fixtures)

- Redacted AOM/Atypon HTML fixtures live under `tests/fixtures/`.
- Run parser regression checks with:

```bash
npm test
```

Example sequence:

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
  --cdp-url http://127.0.0.1:9222 \
  --challenge-wait-ms 45000 \
  --output /tmp/journal_verified_records.json

python3 scripts/build_notion_payload.py \
  --records /tmp/journal_verified_records.json \
  --existing /tmp/notion_existing_rows.json \
  --require-existing \
  --data-source-id "<collection-id>" \
  --output /tmp/notion_create_payload.json

# Example when piping a raw Notion query-database-view JSON payload:
# cat /tmp/notion_query_view_raw.json | \
# python3 scripts/build_notion_payload.py \
#   --records /tmp/journal_verified_records.json \
#   --existing-stdin \
#   --save-existing /tmp/notion_existing_rows.json \
#   --require-existing \
#   --data-source-id "<collection-id>" \
#   --output /tmp/notion_create_payload.json
```

## Default Inclusion Rules

Include:

- Research articles
- Research papers
- Editorials (for example, "From the Editors")

Exclude:

- Book reviews
- Discussions/commentaries
- Commentaries
- Corrigenda/errata/retractions
- Interviews
- Calls for papers
- News/announcements and other non-research items

If item type is unclear after opening the article landing page, classify as `[Not verified]` and do not ingest automatically.

## Source-of-Truth Policy

- Treat publisher landing pages and DOI targets as authoritative.
- Do not use memory for bibliographic data.
- Do not generate missing fields.
- If any required field cannot be confirmed from source, write `[Not verified]`.
- Treat email-side author shorthands such as “and others” as insufficient for citation-quality metadata.

## Execution Workflow

1. Locate the target Gmail message.
2. Extract full email body text and all candidate links.
3. Classify each item using inclusion/exclusion rules.
4. Verify each included item on its publisher/DOI page.
5. Build APA 7 citation from verified source fields only.
6. Fetch Notion database schema and map properties.
7. Deduplicate against existing DOI URLs.
8. Create one Notion page per verified, non-duplicate item.
9. Re-query database and report added/skipped/gaps.

## Gmail Access Playbook

Use this escalation path; stop at the first reliable method.

### Path A: Gmail Web UI via Authenticated Browser (Primary)

- Use browser automation in the user’s logged-in context.
- Use a query ladder, stopping at first exact match:
  1. `subject:"<exact subject>" after:YYYY/MM/DD before:YYYY/MM/DD from:<sender>`
  2. `subject:"<exact subject>"`
  3. `subject:"<subject stem>"` (for example strip alert date suffix)
  4. `from:<sender>` + broader date window (`--date-window-days`)
  5. inbox crawl (paginate with `--max-pages`)
  6. all-mail crawl (paginate with `--max-pages`)
- Validate search application:
  - Confirm navigation is in Gmail search mode (not inbox-like fallback).
  - If query route looks applied but rows are clearly inbox-like, downgrade to next strategy.
- If timestamp is given, confirm the exact row datetime before opening the conversation.
- Treat terminal subject punctuation differences as a valid match only when the received minute (and sender, if provided) also matches.
- Open the exact conversation and extract:
  - Subject
  - Sender
  - Received datetime
  - Full body text
  - All article links (anchor `href`s)

### Path B: Gmail Atom Feed Sanity Check (Secondary)

- Use authenticated Gmail cookies and query `/mail/u/0/feed/atom`.
- Confirm target subject/date appears.
- Use this only as confirmation; still extract full body from Gmail web UI.
- If local cert trust causes SSL verification failures:
  - the script auto-retries Atom in insecure TLS mode once and records a warning,
  - then continues to Playwright fallback if still unresolved.

### Path C: Selector Hardening (When Gmail DOM Is Dynamic)

- Prefer resilient selectors (`role`, `aria-label`, semantic landmarks) over brittle class names.
- Wait for stable render (`networkidle` plus explicit content checks).
- Prefer concrete Gmail surface readiness checks (rows/message header/search input) before `networkidle`; use `networkidle` only as a short fallback because Gmail often keeps background requests alive.
- If needed, switch between hash-route search and search-input (`Enter`) fallback mode.
- Persist diagnostics for each attempt:
  - strategy name,
  - query used,
  - URL before/after search,
  - pages/rows scanned,
  - sampled row subjects/timestamps.

## Publisher Verification Playbook

### Domain Strategy

- For unprotected domains, HTTP fetch + parser is acceptable.
- For protected domains (often INFORMS, SAGE, Wiley), default to browser-rendered extraction.
- When source links are DOI/tracking URLs, derive extraction policy from the navigated final page URL (not only the original URL) so Wiley/Atypon/etc. abstract selectors are applied correctly.
- For AOM/Atypon (`journals.aom.org`), treat as protected and prefer browser extraction (CDP-attached Chrome when available). The pages often expose `dc.*` metadata that is sufficient for accurate journal/year/article-type extraction once rendered.
- For ScienceDirect links, default to the script’s curl-first path (`--sciencedirect-mode auto`) and only fall back to browser when required metadata is incomplete.
- For Oxford Academic (`academic.oup.com`), treat as protected and expect a temporary Cloudflare interstitial.

### ScienceDirect Fast Path

- Trigger: source/resolved URL on `sciencedirect.com`, Elsevier tracking URL resolving to ScienceDirect, or DOI patterns such as `10.1016/...`.
- Steps:
  1. Resolve tracked link to final target.
  2. Canonicalize to `/science/article/pii/<PII>` when available.
  3. Fetch HTML via `curl -L` with browser-like user-agent.
  4. Extract metadata from:
     - citation/prism meta tags,
     - JSON-LD,
     - `window.__PRELOADED_STATE__` (authors, abstract, publication fields).
  5. Build APA citation and enforce required-field checks.
- Fallback:
  - In `auto` mode, if citation-critical fields are incomplete, retry via browser policy flow.
  - In `curl` mode, do not browser-fallback.

### Cloudflare / Anti-Bot Handling

Challenge signals:

- Page title includes `Just a moment`, `Access denied`, or `Attention Required`
- URL includes challenge paths (e.g., `/cdn-cgi/`)
- Expected article metadata/abstract is missing and interstitial text is present
- Wiley/other publisher pages returning only teaser abstract meta descriptions (for example, truncated `...`) when a full DOM abstract exists indicates selector/policy mismatch or incomplete render.

Recovery sequence:

1. Retry URL with backoff (for example, 2s, 5s, 10s).
2. Open canonical DOI URL and resolved publisher URL in fresh tab/context.
3. Attach automation to a normal authenticated Chrome session (CDP) if available; prefer reusing the existing profile/browser context to preserve cookies/challenge state.
4. Reduce per-domain concurrency to 1.
5. Wait/poll challenge pages for a bounded window (for example, 30-45s) before marking blocked.

If still blocked, mark missing fields as `[Not verified]` and do not auto-ingest.

### Tracked Link Resolution

- Journal alert emails may use redirect trackers (for example `click.skem1.com`, `el.aom.org`, `el.wiley.com`).
- Resolve tracked links to final destinations before metadata extraction.
- For `el.wiley.com`, unauthenticated pre-resolution may land on `onlinelibrary.wiley.com/action/cookieAbsent`; treat that as a non-authoritative gate and fall back to browser navigation of the original tracker URL in the authenticated browser session.
- Filter to article/DOI destinations and ignore account, privacy, unsubscribe, and global marketing links.
- If tracked-link resolution lands on known non-article endpoints (for example AOM account/login/privacy/Atypon partner pages), classify as `exclude` immediately and skip expensive metadata extraction.
- For Wiley alerts, treat issue/TOC pages (`/toc/...`), journal home pages (`/journal/...`), and alert-management endpoints (`/action/removeAlert`) as non-article links and classify as `exclude`.

### Metadata Extraction Order

1. Structured metadata (`citation_*` tags, JSON-LD).
2. Publisher citation export, when complete.
3. Visible article page content cross-check.
4. Abstract: copy only article abstract section verbatim (trim navigation/legal boilerplate).

### Required Verified Fields for Auto-Ingest

- `title`
- `doiUrl` (canonical DOI URL)
- `journal`
- `year`
- `abstract`
- `citation`

If any required field is missing, set `[Not verified]` and skip ingestion.

## APA 7 Construction Rules

- Prefer publisher-provided citation export if available on the page and complete.
- Otherwise construct APA 7 manually from verified fields:
  - `Author, A. A., & Author, B. B. (Year). Title of article. Journal Name, volume(issue), pages. https://doi.org/...`
- Use `Advance online publication` only if explicitly stated by source.
- If any citation-critical field is missing and cannot be verified, set citation to `[Not verified]`.

## DOI Normalization and Deduplication

- Normalize DOI to canonical form: `https://doi.org/<doi>`.
- Normalize variants before duplicate checks:
  - `doi:...`
  - `http://doi.org/...`
  - `https://dx.doi.org/...`
  - mixed case DOI suffixes
- Query existing Notion records and skip exact DOI matches.
- Never create a second page for the same canonical DOI.

## Notion Mapping Rules

- Always fetch the database first to read property names and types.
- For `query-database-view`, use the `view://...` URL returned by database fetch (not a public `https://notion.so/...` URL).
- For page creation, use parent object with `data_source_id` from `collection://...`.
- Use semantic matching to map fields.
- Title -> title property.
- DOI URL -> url property (including names like `URL` or `userDefined:URL`).
- Citation -> rich_text/text property (for example `Citation`).
- Abstract -> `Abstract` property when present.
- If no abstract property exists, place abstract in page body.
- For deduplication, prefer database-view rows/property checks over broad semantic search.
- Avoid duplicates by checking existing entries for matching DOI URL (exact or normalized).
- Prefer one database-view pull + local DOI normalization over repeated semantic searches.
- If `query-database-view` rejects a copied public Notion URL, retry with the `view://...` identifier from `fetch`; as a temporary fallback, run exact DOI searches against the target data source before writing.

## Notion Rate-Limit Handling

- Expect transient `429 rate_limited` responses during high-frequency reads/writes.
- On Notion `429`, retry with bounded backoff (for example 2s, 5s, 10s, 20s) and cap attempts.
- Avoid parallel bursts of Notion search calls; batch writes and verify with a single `query-database-view` when possible.
- Keep create calls chunked (for example 10-20 pages per request) for large ingestion runs.

## Operational Reliability Rules

- Save intermediate artifacts for auditability:
  - extracted email body text
  - extracted candidate links
  - verified per-paper metadata JSON
  - Notion write payload
  - Notion write results
- Use bounded retries for network/browser failures.
- Keep extraction and ingestion steps separate so reruns can reuse verified metadata.
- Parallelize publisher verification conservatively; lower concurrency on protected domains.

## Recommended Function Units

Use these logical units when building helper scripts or reusable automation:

- `find_gmail_message(subject, received_datetime, sender)` -> exact Gmail thread/message target.
- `extract_email_candidates(message_html_or_text)` -> titles, candidate URLs, DOI links.
- `classify_item_type(candidate)` -> include/exclude/`[Not verified]`.
- `verify_publisher_record(url, domain_policy)` -> source-verified metadata payload.
- `normalize_doi(doi_or_url)` -> canonical `https://doi.org/<doi>`.
- `build_apa7(record)` -> APA citation from verified fields only.
- `fetch_notion_schema(database_url)` -> canonical property map.
- `query_existing_dois(data_source)` -> duplicate index for idempotent writes.
- `create_notion_pages(records)` -> write only verified, non-duplicate records.
- `report_ingestion(added, skipped, gaps)` -> final user-facing output contract.

## Output Contract

After ingestion, return:

1. Added items: `Title — DOI URL`
2. Skipped items with reason (excluded type, duplicate, or `[Not verified]`)
3. Any unresolved verification gaps
4. If Gmail lookup required fallback, include strategy diagnostics (`attempts`, selected strategy, and warnings)

## Non-Negotiables

- No fabrication.
- No silent fallback to memory.
- Mark unknowns explicitly as `[Not verified]`.
- Do not attempt CAPTCHA solving or unauthorized bypass; use user-authenticated sessions only.
