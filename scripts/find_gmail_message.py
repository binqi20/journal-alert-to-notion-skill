#!/usr/bin/env python3
"""Find an exact Gmail message by subject and received minute.

Primary strategy:
1) Gmail Atom feed with authenticated browser cookies.
2) Playwright Gmail session fallback when feed lookup misses or cannot authenticate.

The script prints JSON to stdout and optionally writes JSON to --output.
"""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import http.cookiejar
import json
import re
import ssl
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import HTTPCookieProcessor, HTTPSHandler, Request, build_opener
from zoneinfo import ZoneInfo


ATOM_NS = "{http://purl.org/atom/ns#}"
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _log(msg: str, *, enabled: bool) -> None:
    if enabled:
        print(f"[find_gmail_message] {msg}", file=sys.stderr)


def _die(message: str, code: int = 1) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(code)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", required=True, help="Exact Gmail subject line to match.")
    parser.add_argument(
        "--sender",
        help=(
            "Optional sender filter (email or display name). "
            "Examples: alerts@journal.com or 'Journal Alerts'."
        ),
    )
    parser.add_argument(
        "--received-at",
        required=True,
        help=(
            "Target received datetime. Examples: "
            "'2026-01-19 17:18', '2026-01-19T17:18:00-05:00', "
            "'Jan 19, 2026, 5:18 PM'."
        ),
    )
    parser.add_argument(
        "--timezone",
        help="IANA timezone used when --received-at has no timezone (example: America/New_York).",
    )
    parser.add_argument("--mailbox", default="0", help="Gmail mailbox index (default: 0).")
    parser.add_argument(
        "--atom-timeout-seconds",
        type=float,
        default=20.0,
        help="Timeout per Atom request in seconds.",
    )
    parser.add_argument(
        "--atom-insecure",
        action="store_true",
        help="Disable TLS certificate verification for Atom lookup (last resort).",
    )
    parser.add_argument(
        "--browser",
        default="chrome",
        choices=["chrome", "chromium", "edge", "firefox", "brave", "opera"],
        help="Browser cookie source for browser_cookie3 (default: chrome).",
    )
    parser.add_argument(
        "--cookie-file",
        type=Path,
        help="Optional Netscape/Mozilla cookies.txt file path.",
    )
    parser.add_argument(
        "--cookie-header",
        help="Optional raw Cookie header for mail.google.com.",
    )
    parser.add_argument(
        "--skip-atom",
        action="store_true",
        help="Skip Atom feed lookup and go directly to session fallback.",
    )
    parser.add_argument(
        "--session-fallback",
        action="store_true",
        help="Enable Playwright Gmail UI fallback if Atom lookup misses.",
    )
    parser.add_argument(
        "--inject-browser-cookies",
        action="store_true",
        help=(
            "Inject browser cookies into Playwright fallback context "
            "(uses --browser or --cookie-file/--cookie-header inputs)."
        ),
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Playwright fallback in headless mode.",
    )
    parser.add_argument(
        "--playwright-channel",
        default="chrome",
        help="Playwright browser channel for fallback (default: chrome).",
    )
    parser.add_argument(
        "--storage-state",
        type=Path,
        help="Playwright storage state JSON for authenticated Gmail session.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=40,
        help="Max Gmail rows to inspect in Playwright fallback.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=6,
        help="Max Gmail result/list pages to inspect per fallback strategy.",
    )
    parser.add_argument(
        "--date-window-days",
        type=int,
        default=1,
        help=(
            "Extra day window for relaxed fallback queries around --received-at. "
            "Used only after strict search misses."
        ),
    )
    parser.add_argument(
        "--include-body",
        action="store_true",
        help="Include message body text for matched session results.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional file path for JSON output.",
    )
    parser.add_argument(
        "--exit-nonzero-on-miss",
        action="store_true",
        help="Exit with code 2 when no exact match is found.",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose stderr logs.")
    return parser.parse_args()


def _timezone_from_args(name: str | None) -> dt.tzinfo:
    if name:
        try:
            return ZoneInfo(name)
        except Exception as exc:  # pragma: no cover - defensive
            _die(f"Invalid timezone '{name}': {exc}")
    return dt.datetime.now().astimezone().tzinfo or dt.timezone.utc


def _parse_received_datetime(raw: str, fallback_tz: dt.tzinfo) -> dt.datetime:
    value = raw.strip()

    # ISO-like first.
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=fallback_tz)
        return parsed
    except ValueError:
        pass

    fmt_candidates = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %I:%M %p",
        "%b %d, %Y, %I:%M %p",
        "%B %d, %Y, %I:%M %p",
        "%a, %b %d, %Y, %I:%M %p",
    ]
    for fmt in fmt_candidates:
        try:
            parsed = dt.datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=fallback_tz)
        except ValueError:
            continue

    parsed_email = email.utils.parsedate_to_datetime(value)
    if parsed_email is not None:
        if parsed_email.tzinfo is None:
            return parsed_email.replace(tzinfo=fallback_tz)
        return parsed_email

    _die(
        "Could not parse --received-at. Use ISO format like "
        "'2026-01-19T17:18:00-05:00' or provide --timezone."
    )
    raise AssertionError("unreachable")


def _same_minute(a: dt.datetime, b: dt.datetime, tz: dt.tzinfo) -> bool:
    aa = a.astimezone(tz)
    bb = b.astimezone(tz)
    return (
        aa.year == bb.year
        and aa.month == bb.month
        and aa.day == bb.day
        and aa.hour == bb.hour
        and aa.minute == bb.minute
    )


def _normalized_sender_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


def _sender_matches(expected_sender: str | None, sender_name: str, sender_email: str) -> bool:
    if not expected_sender:
        return True
    expected_name, expected_email = email.utils.parseaddr(expected_sender)
    expected_name = _normalized_sender_text(expected_name)
    expected_email = _normalized_sender_text(expected_email)
    expected_raw = _normalized_sender_text(expected_sender)
    sender_name_norm = _normalized_sender_text(sender_name)
    sender_email_norm = _normalized_sender_text(sender_email)

    if expected_email:
        if sender_email_norm == expected_email:
            return True
        return expected_email in sender_email_norm

    token = expected_name or expected_raw
    if not token:
        return True
    return token in sender_name_norm or token in sender_email_norm


def _subject_stem(subject: str) -> str:
    normalized = re.sub(r"\s+", " ", subject).strip()
    patterns = [
        r"^(.*?:\s*Alert)\s+\d{1,2}\s+[A-Za-z]+$",
        r"^(.*?:\s*Alert)\s+\d{1,2}\s+[A-Za-z]+\s+\d{4}$",
    ]
    for pattern in patterns:
        matched = re.match(pattern, normalized, flags=re.IGNORECASE)
        if matched:
            return matched.group(1).strip()
    return normalized


def _sender_query_token(sender: str | None) -> str | None:
    if not sender:
        return None
    _, parsed_email = email.utils.parseaddr(sender)
    token = (parsed_email or sender).strip().strip('"')
    return token or None


def _escape_query_phrase(value: str) -> str:
    return value.replace('"', '\\"')


def _normalize_subject_text(value: str | None) -> str:
    text = (value or "").replace("\u202f", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _strip_terminal_subject_punctuation(value: str) -> str:
    return re.sub(r"[\s.!?。！？…]+$", "", value).strip()


def _subject_matches_requested(observed: str | None, requested: str | None) -> bool:
    observed_norm = _normalize_subject_text(observed)
    requested_norm = _normalize_subject_text(requested)
    if not observed_norm or not requested_norm:
        return False
    if observed_norm == requested_norm:
        return True
    # Gmail subjects in the UI/feed may differ only by trailing punctuation.
    return _strip_terminal_subject_punctuation(observed_norm) == _strip_terminal_subject_punctuation(
        requested_norm
    )


def _build_search_query(
    *,
    subject: str | None,
    target_dt: dt.datetime,
    tz: dt.tzinfo,
    sender: str | None = None,
    window_days: int = 0,
) -> str:
    local_date = target_dt.astimezone(tz).date()
    window = max(0, int(window_days))
    start_date = local_date - dt.timedelta(days=window)
    before_date = local_date + dt.timedelta(days=window + 1)

    parts: list[str] = []
    if subject:
        safe_subject = _escape_query_phrase(subject)
        parts.append(f'subject:"{safe_subject}"')
    parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
    parts.append(f"before:{before_date.strftime('%Y/%m/%d')}")

    sender_token = _sender_query_token(sender)
    if sender_token:
        parts.append(f"from:{sender_token}")
    return " ".join(parts)


def _build_search_ladder(
    *,
    subject: str,
    target_dt: dt.datetime,
    tz: dt.tzinfo,
    sender: str | None,
    date_window_days: int,
) -> list[dict[str, Any]]:
    safe_window = max(1, int(date_window_days))
    sender_window = max(2, safe_window)
    stem = _subject_stem(subject)

    strict_query = _build_search_query(
        subject=subject,
        target_dt=target_dt,
        tz=tz,
        sender=sender,
        window_days=0,
    )
    strategies: list[dict[str, Any]] = [
        {
            "name": "search_strict_exact_subject",
            "mode": "search",
            "query": strict_query,
        }
    ]
    strategies.append(
        {
            "name": "search_exact_subject_only",
            "mode": "search",
            "query": f'subject:"{_escape_query_phrase(subject)}"',
        }
    )

    if stem and stem != subject:
        strategies.append(
            {
                "name": "search_subject_stem_only",
                "mode": "search",
                "query": f'subject:"{_escape_query_phrase(stem)}"',
            }
        )
        strategies.append(
            {
                "name": "search_input_subject_stem_only",
                "mode": "search_input",
                "query": f'subject:"{_escape_query_phrase(stem)}"',
            }
        )
        strategies.append(
            {
                "name": "search_subject_stem_window",
                "mode": "search",
                "query": _build_search_query(
                    subject=stem,
                    target_dt=target_dt,
                    tz=tz,
                    sender=sender,
                    window_days=safe_window,
                ),
            }
        )

    relaxed_subject_query = _build_search_query(
        subject=subject,
        target_dt=target_dt,
        tz=tz,
        sender=None,
        window_days=safe_window,
    )
    if relaxed_subject_query != strict_query:
        strategies.append(
            {
                "name": "search_subject_window_no_sender",
                "mode": "search",
                "query": relaxed_subject_query,
            }
        )

    sender_token = _sender_query_token(sender)
    if sender_token:
        broad_sender_query = _build_search_query(
            subject=None,
            target_dt=target_dt,
            tz=tz,
            sender=sender,
            window_days=sender_window,
        )
        journal_hint = subject.split(":")[0].strip()
        if journal_hint:
            broad_sender_query = f'"{journal_hint}" {broad_sender_query}'
        strategies.append(
            {
                "name": "search_sender_broad_window",
                "mode": "search",
                "query": broad_sender_query,
            }
        )

    strategies.append({"name": "crawl_inbox", "mode": "crawl", "folder": "inbox"})
    strategies.append({"name": "crawl_all_mail", "mode": "crawl", "folder": "all"})
    return strategies


def _cookiejar_from_header(cookie_header: str) -> http.cookiejar.CookieJar:
    jar = http.cookiejar.CookieJar()
    for segment in cookie_header.split(";"):
        if "=" not in segment:
            continue
        name, value = segment.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookie = http.cookiejar.Cookie(
            version=0,
            name=name,
            value=value,
            port=None,
            port_specified=False,
            domain=".mail.google.com",
            domain_specified=True,
            domain_initial_dot=True,
            path="/",
            path_specified=True,
            secure=True,
            expires=None,
            discard=True,
            comment=None,
            comment_url=None,
            rest={},
            rfc2109=False,
        )
        jar.set_cookie(cookie)
    return jar


def _load_cookie_file(path: Path) -> http.cookiejar.CookieJar:
    jar = http.cookiejar.MozillaCookieJar(str(path))
    jar.load(ignore_discard=True, ignore_expires=True)
    return jar


def _load_browser_cookie3(browser: str) -> http.cookiejar.CookieJar:
    try:
        import browser_cookie3  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "browser_cookie3 is required for browser cookie lookup. "
            "Install with: uv pip install browser-cookie3"
        ) from exc

    loaders: dict[str, Any] = {
        "chrome": browser_cookie3.chrome,
        "chromium": browser_cookie3.chromium,
        "edge": browser_cookie3.edge,
        "firefox": browser_cookie3.firefox,
        "brave": browser_cookie3.brave,
        "opera": browser_cookie3.opera,
    }
    loader = loaders.get(browser)
    if loader is None:
        raise RuntimeError(f"Unsupported browser for cookie extraction: {browser}")
    domains = [".google.com", "mail.google.com", "accounts.google.com"]
    merged = http.cookiejar.CookieJar()
    seen: set[tuple[str, str, str]] = set()
    for domain in domains:
        jar = loader(domain_name=domain)
        for cookie in jar:
            key = (cookie.domain, cookie.path, cookie.name)
            if key in seen:
                continue
            seen.add(key)
            merged.set_cookie(cookie)
    return merged


def _fetch_atom_feed(
    jar: http.cookiejar.CookieJar,
    mailbox: str,
    timeout_seconds: float,
    verbose: bool,
    insecure: bool,
) -> str:
    feed_urls = [
        f"https://mail.google.com/mail/u/{mailbox}/feed/atom",
        "https://mail.google.com/mail/feed/atom",
    ]
    handlers: list[Any] = [HTTPCookieProcessor(jar)]
    if insecure:
        _log("Atom lookup using insecure TLS mode.", enabled=verbose)
        handlers.append(HTTPSHandler(context=ssl._create_unverified_context()))
    opener = build_opener(*handlers)
    last_error: Exception | None = None

    for feed_url in feed_urls:
        _log(f"Trying Atom URL: {feed_url}", enabled=verbose)
        request = Request(
            feed_url,
            headers={
                "User-Agent": DEFAULT_UA,
                "Accept": "application/atom+xml,text/xml,*/*",
            },
        )
        try:
            with opener.open(request, timeout=timeout_seconds) as response:
                payload = response.read().decode("utf-8", errors="replace")
                if "<feed" in payload:
                    return payload
                last_error = RuntimeError("Atom response did not include <feed> payload.")
        except Exception as exc:  # pragma: no cover - runtime network condition
            last_error = exc
            _log(f"Atom URL failed: {exc}", enabled=verbose)

    if last_error is None:
        raise RuntimeError("Atom feed lookup failed with no diagnostic.")
    raise RuntimeError(f"Atom feed lookup failed: {last_error}")


def _parse_atom_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    try:
        return dt.datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        parsed = email.utils.parsedate_to_datetime(cleaned)
        return parsed


def _parse_atom_entries(feed_xml: str) -> list[dict[str, Any]]:
    root = ET.fromstring(feed_xml)
    entries: list[dict[str, Any]] = []
    for entry in root.findall(f"{ATOM_NS}entry"):
        title = (entry.findtext(f"{ATOM_NS}title") or "").strip()
        entry_id = (entry.findtext(f"{ATOM_NS}id") or "").strip()
        issued = (entry.findtext(f"{ATOM_NS}issued") or "").strip()
        modified = (entry.findtext(f"{ATOM_NS}modified") or "").strip()
        summary = (entry.findtext(f"{ATOM_NS}summary") or "").strip()
        author_name = ""
        author_email = ""
        author = entry.find(f"{ATOM_NS}author")
        if author is not None:
            author_name = (author.findtext(f"{ATOM_NS}name") or "").strip()
            author_email = (author.findtext(f"{ATOM_NS}email") or "").strip()
        entries.append(
            {
                "title": title,
                "id": entry_id,
                "issued": issued,
                "modified": modified,
                "summary": summary,
                "author_name": author_name,
                "author_email": author_email,
            }
        )
    return entries


def _select_atom_match(
    entries: list[dict[str, Any]],
    subject: str,
    target_dt: dt.datetime,
    tz: dt.tzinfo,
    sender: str | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    for entry in entries:
        if not _subject_matches_requested(str(entry.get("title") or ""), subject):
            continue
        issued_dt = _parse_atom_datetime(entry.get("issued")) or _parse_atom_datetime(
            entry.get("modified")
        )
        candidate = dict(entry)
        if issued_dt is not None:
            candidate["issued_local"] = issued_dt.astimezone(tz).isoformat()
            candidate["minute_match"] = _same_minute(issued_dt, target_dt, tz)
        else:
            candidate["issued_local"] = None
            candidate["minute_match"] = False
        candidate["sender_match"] = _sender_matches(
            sender,
            str(candidate.get("author_name") or ""),
            str(candidate.get("author_email") or ""),
        )
        candidates.append(candidate)
        if candidate["minute_match"] and candidate["sender_match"]:
            return candidate, candidates
    return None, candidates


def _parse_gmail_datetime(value: str, fallback_tz: dt.tzinfo) -> dt.datetime | None:
    raw = value.strip()
    if not raw:
        return None
    cleaned = re.sub(r"\s+\([^)]*\)\s*$", "", raw).strip()
    cleaned = cleaned.replace("\u202f", " ").replace("\xa0", " ")

    # Gmail header title samples:
    # "Mon, Jan 19, 2026, 5:18 PM"
    # "Jan 19, 2026, 5:18 PM"
    # "Mon, Jan 19, 2026 at 5:18 PM"
    format_candidates = [
        "%a, %b %d, %Y, %I:%M %p",
        "%b %d, %Y, %I:%M %p",
        "%a, %b %d, %Y at %I:%M %p",
        "%b %d, %Y at %I:%M %p",
    ]
    for fmt in format_candidates:
        try:
            parsed = dt.datetime.strptime(cleaned, fmt)
            return parsed.replace(tzinfo=fallback_tz)
        except ValueError:
            continue

    try:
        parsed_email = email.utils.parsedate_to_datetime(cleaned)
    except (TypeError, ValueError):
        return None
    if parsed_email is not None:
        if parsed_email.tzinfo is None:
            return parsed_email.replace(tzinfo=fallback_tz)
        return parsed_email
    return None


def _safe_inner_text(locator: Any) -> str:
    try:
        return locator.inner_text(timeout=2000).strip()
    except Exception:
        return ""


def _safe_attr(locator: Any, attr: str) -> str:
    try:
        value = locator.get_attribute(attr, timeout=2000)
    except Exception:
        return ""
    return (value or "").strip()


def _wait_for_gmail_surface(page: Any, *, verbose: bool, phase: str) -> None:
    # Gmail often keeps background requests alive, so `networkidle` is an unreliable
    # primary readiness signal. Prefer concrete UI surface checks and only fall back
    # to a short networkidle wait when selectors are still missing.
    ready_selector = ",".join(
        [
            "tr.zA",
            "h2.hP",
            'input[aria-label="Search mail"]',
            'input[name="q"]',
        ]
    )
    try:
        page.wait_for_selector(ready_selector, state="attached", timeout=6000)
        page.wait_for_timeout(400)
        return
    except Exception:
        _log(
            f"Gmail surface selectors not ready after navigation ({phase}); trying short networkidle fallback.",
            enabled=verbose,
        )

    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        _log(
            f"networkidle timeout after Gmail {phase}; continuing with selector retry.",
            enabled=verbose,
        )
    try:
        page.wait_for_selector(ready_selector, state="attached", timeout=3000)
    except Exception:
        _log(
            f"Gmail surface selectors still not ready after fallback ({phase}); continuing.",
            enabled=verbose,
        )
    page.wait_for_timeout(500)


def _gmail_view_url(
    *,
    mailbox: str,
    mode: str,
    query: str | None = None,
    folder: str | None = None,
) -> str:
    if mode == "search":
        if not query:
            raise RuntimeError("Missing query for search mode.")
        return f"https://mail.google.com/mail/u/{mailbox}/#search/{quote(query, safe='')}"
    if mode == "search_input":
        return f"https://mail.google.com/mail/u/{mailbox}/#inbox"
    if mode == "crawl":
        target_folder = (folder or "inbox").lower()
        if target_folder == "all":
            return f"https://mail.google.com/mail/u/{mailbox}/#all"
        return f"https://mail.google.com/mail/u/{mailbox}/#inbox"
    raise RuntimeError(f"Unsupported Gmail view mode: {mode}")


def _is_inbox_like_url(url: str) -> bool:
    return "#search/" not in url and "#inbox" in url


def _goto_mail_view(
    page: Any,
    *,
    mailbox: str,
    mode: str,
    query: str | None,
    folder: str | None,
    verbose: bool,
) -> dict[str, Any]:
    target_url = _gmail_view_url(mailbox=mailbox, mode=mode, query=query, folder=folder)
    page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
    _wait_for_gmail_surface(page, verbose=verbose, phase="navigation")

    current_url = page.url
    result: dict[str, Any] = {
        "target_url": target_url,
        "current_url": current_url,
    }
    if mode == "search_input":
        if not query:
            raise RuntimeError("Missing query for search_input mode.")
        search_input = page.locator('input[aria-label="Search mail"]').first
        if not search_input.count():
            search_input = page.locator('input[name="q"]').first
        if not search_input.count():
            raise RuntimeError("Could not find Gmail search input.")

        search_input.click(timeout=10000)
        search_input.fill(query, timeout=10000)
        search_input.press("Enter")
        _wait_for_gmail_surface(page, verbose=verbose, phase="input search")
        current_url = page.url
        result["current_url"] = current_url
        result["search_applied"] = "#search/" in current_url or "?q=" in current_url
        result["inbox_like"] = _is_inbox_like_url(current_url) and "?q=" not in current_url
    elif mode == "search":
        result["search_applied"] = "#search/" in current_url
        result["inbox_like"] = _is_inbox_like_url(current_url)
    return result


def _goto_older_page(page: Any, *, verbose: bool) -> bool:
    selectors = [
        'div[aria-label="Older"]',
        'div[role="button"][aria-label*="Older"]',
        'div[data-tooltip="Older"]',
    ]
    for selector in selectors:
        button = page.locator(selector).first
        if not button.count():
            continue

        aria_disabled = _safe_attr(button, "aria-disabled").lower()
        class_name = _safe_attr(button, "class")
        if aria_disabled == "true" or "aqj" in class_name:
            return False

        try:
            button.click(timeout=5000)
            page.wait_for_timeout(1300)
            return True
        except Exception:
            continue

    _log("No usable 'Older' button found; stopping pagination.", enabled=verbose)
    return False


def _return_to_list(page: Any, *, verbose: bool) -> None:
    try:
        page.keyboard.press("u")
        page.wait_for_timeout(900)
        return
    except Exception:
        pass

    try:
        page.go_back(wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(900)
    except Exception:
        _log("Failed to return to Gmail list view after opening thread.", enabled=verbose)


def _extract_message_candidate(
    *,
    page: Any,
    strategy_name: str,
    row_subject: str,
    row_sender: str,
    row_time_hint: str,
    target_dt: dt.datetime,
    sender: str | None,
    tz: dt.tzinfo,
    include_body: bool,
) -> dict[str, Any]:
    message_subject = _safe_inner_text(page.locator("h2.hP").first) or row_subject
    sender_chip = page.locator("span.gD").first
    sender_email = _safe_attr(sender_chip, "email")
    sender_name = _safe_attr(sender_chip, "name") or _safe_inner_text(sender_chip) or row_sender

    timestamps: list[str] = []
    ts_nodes = page.locator("span.g3")
    ts_count = min(ts_nodes.count(), 12)
    for ts_i in range(ts_count):
        node = ts_nodes.nth(ts_i)
        title_value = _safe_attr(node, "title")
        if title_value:
            timestamps.append(title_value)
        text_value = _safe_inner_text(node)
        if text_value:
            timestamps.append(text_value)

    parsed_datetimes: list[dt.datetime] = []
    for ts_value in timestamps:
        parsed = _parse_gmail_datetime(ts_value, tz)
        if parsed is not None:
            parsed_datetimes.append(parsed)
    if not parsed_datetimes and row_time_hint:
        row_parsed = _parse_gmail_datetime(row_time_hint, tz)
        if row_parsed is not None:
            parsed_datetimes.append(row_parsed)

    try:
        links = page.locator("div.a3s a[href]").evaluate_all(
            "nodes => nodes.map(n => n.href).filter(Boolean)"
        )
    except Exception:
        links = []

    body_text = None
    if include_body:
        body_text = _safe_inner_text(page.locator("div.a3s").first)

    minute_match = any(_same_minute(value, target_dt, tz) for value in parsed_datetimes)
    sender_match = _sender_matches(sender, sender_name, sender_email)
    candidate: dict[str, Any] = {
        "strategy": strategy_name,
        "subject": message_subject,
        "sender_name": sender_name,
        "sender_email": sender_email,
        "sender_match": sender_match,
        "row_time_hint": row_time_hint,
        "timestamps": timestamps,
        "timestamps_local": [x.astimezone(tz).isoformat() for x in parsed_datetimes],
        "minute_match": minute_match,
        "url": page.url,
        "links": links,
    }
    if include_body:
        candidate["body_text"] = body_text
    return candidate


def _scan_current_view(
    *,
    page: Any,
    strategy_name: str,
    subject: str,
    target_dt: dt.datetime,
    sender: str | None,
    tz: dt.tzinfo,
    max_rows: int,
    max_pages: int,
    include_body: bool,
    verbose: bool,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    pages_scanned = 0
    rows_scanned = 0

    for page_index in range(1, max_pages + 1):
        rows = page.locator("tr.zA")
        row_count = min(rows.count(), max_rows)
        pages_scanned += 1
        _log(
            f"[{strategy_name}] scanning page {page_index}/{max_pages}, rows={row_count}",
            enabled=verbose,
        )

        for row_index in range(row_count):
            row = page.locator("tr.zA").nth(row_index)
            row_subject = _safe_inner_text(row.locator("span.bog").first)
            row_sender = _safe_inner_text(row.locator("span.zF, span.yP").first)
            row_time_hint = _safe_attr(row.locator("td.xW span").first, "title")
            if not row_time_hint:
                row_time_hint = _safe_inner_text(row.locator("td.xW span").first)

            rows_scanned += 1
            if len(sample_rows) < 30:
                sample_rows.append(
                    {
                        "page": page_index,
                        "row": row_index + 1,
                        "subject": row_subject,
                        "sender": row_sender,
                        "row_time_hint": row_time_hint,
                    }
                )

            if not _subject_matches_requested(row_subject, subject):
                continue

            try:
                row.click(timeout=15000)
            except Exception:
                continue
            page.wait_for_timeout(1100)

            candidate = _extract_message_candidate(
                page=page,
                strategy_name=strategy_name,
                row_subject=row_subject,
                row_sender=row_sender,
                row_time_hint=row_time_hint,
                target_dt=target_dt,
                sender=sender,
                tz=tz,
                include_body=include_body,
            )
            candidates.append(candidate)
            match_hit = (
                _subject_matches_requested(str(candidate.get("subject") or ""), subject)
                and bool(candidate.get("minute_match"))
                and bool(candidate.get("sender_match"))
            )
            _return_to_list(page, verbose=verbose)

            if match_hit:
                return {
                    "found": True,
                    "match": candidate,
                    "candidates": candidates,
                    "pages_scanned": pages_scanned,
                    "rows_scanned": rows_scanned,
                    "sample_rows": sample_rows,
                    "final_url": page.url,
                }

        if page_index >= max_pages:
            break
        if not _goto_older_page(page, verbose=verbose):
            break

    return {
        "found": False,
        "match": None,
        "candidates": candidates,
        "pages_scanned": pages_scanned,
        "rows_scanned": rows_scanned,
        "sample_rows": sample_rows,
        "final_url": page.url,
    }


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in candidates:
        key = (
            str(item.get("strategy") or ""),
            str(item.get("subject") or ""),
            str(item.get("row_time_hint") or ""),
            str(item.get("url") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _playwright_lookup(
    *,
    subject: str,
    target_dt: dt.datetime,
    sender: str | None,
    tz: dt.tzinfo,
    mailbox: str,
    channel: str,
    headless: bool,
    storage_state: Path | None,
    inject_browser_cookies: bool,
    browser_name: str,
    cookie_file: Path | None,
    cookie_header: str | None,
    max_rows: int,
    max_pages: int,
    date_window_days: int,
    include_body: bool,
    verbose: bool,
) -> dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is required for session fallback. Install with: uv pip install playwright"
        ) from exc

    _log("Running Playwright session fallback.", enabled=verbose)
    strategies = _build_search_ladder(
        subject=subject,
        target_dt=target_dt,
        tz=tz,
        sender=sender,
        date_window_days=date_window_days,
    )
    attempts: list[dict[str, Any]] = []
    all_candidates: list[dict[str, Any]] = []

    with sync_playwright() as playwright:
        browser_instance = None
        try:
            try:
                browser_instance = playwright.chromium.launch(headless=headless, channel=channel)
            except Exception:
                browser_instance = playwright.chromium.launch(headless=headless)

            context_kwargs: dict[str, Any] = {}
            if storage_state is not None:
                context_kwargs["storage_state"] = str(storage_state)
            context = browser_instance.new_context(**context_kwargs)
            if inject_browser_cookies and storage_state is None:
                jar: http.cookiejar.CookieJar
                if cookie_header:
                    jar = _cookiejar_from_header(cookie_header)
                elif cookie_file:
                    jar = _load_cookie_file(cookie_file)
                else:
                    jar = _load_browser_cookie3(browser_name)
                cookies_payload: list[dict[str, Any]] = []
                for cookie in jar:
                    domain = cookie.domain or ""
                    if "google.com" not in domain and "gmail.com" not in domain:
                        continue
                    cookie_item: dict[str, Any] = {
                        "name": cookie.name,
                        "value": cookie.value,
                        "domain": domain,
                        "path": cookie.path or "/",
                        "secure": bool(cookie.secure),
                    }
                    if cookie.expires:
                        cookie_item["expires"] = float(cookie.expires)
                    if cookie.has_nonstandard_attr("HttpOnly"):
                        cookie_item["httpOnly"] = True
                    cookies_payload.append(cookie_item)
                if cookies_payload:
                    _log(
                        f"Injecting {len(cookies_payload)} browser cookies into Playwright context.",
                        enabled=verbose,
                    )
                    context.add_cookies(cookies_payload)
            page = context.new_page()

            inbox_url = f"https://mail.google.com/mail/u/{mailbox}/#inbox"
            page.goto(inbox_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(1200)
            if "accounts.google.com" in page.url:
                raise RuntimeError(
                    "Playwright session is not authenticated. "
                    "Use --storage-state with a logged-in Gmail session."
                )

            for strategy in strategies:
                attempt: dict[str, Any] = {
                    "name": strategy.get("name"),
                    "mode": strategy.get("mode"),
                    "query": strategy.get("query"),
                    "folder": strategy.get("folder"),
                    "target_url": None,
                    "initial_url": None,
                    "final_url": None,
                    "search_applied": None,
                    "inbox_like": None,
                    "pages_scanned": 0,
                    "rows_scanned": 0,
                    "sample_rows": [],
                    "candidate_count": 0,
                    "found": False,
                    "error": None,
                }
                attempts.append(attempt)

                try:
                    nav = _goto_mail_view(
                        page,
                        mailbox=mailbox,
                        mode=strategy.get("mode", ""),
                        query=strategy.get("query"),
                        folder=strategy.get("folder"),
                        verbose=verbose,
                    )
                    attempt["target_url"] = nav.get("target_url")
                    attempt["initial_url"] = nav.get("current_url")
                    if strategy.get("mode") in {"search", "search_input"}:
                        attempt["search_applied"] = bool(nav.get("search_applied"))
                        attempt["inbox_like"] = bool(nav.get("inbox_like"))
                        if not attempt["search_applied"] or attempt["inbox_like"]:
                            attempt["final_url"] = page.url
                            continue

                    scan = _scan_current_view(
                        page=page,
                        strategy_name=str(strategy.get("name") or ""),
                        subject=subject,
                        target_dt=target_dt,
                        sender=sender,
                        tz=tz,
                        max_rows=max_rows,
                        max_pages=max_pages,
                        include_body=include_body,
                        verbose=verbose,
                    )
                    attempt["pages_scanned"] = scan.get("pages_scanned", 0)
                    attempt["rows_scanned"] = scan.get("rows_scanned", 0)
                    attempt["sample_rows"] = scan.get("sample_rows", [])
                    attempt["candidate_count"] = len(scan.get("candidates", []))
                    attempt["final_url"] = scan.get("final_url")

                    all_candidates.extend(scan.get("candidates", []))
                    if scan.get("found"):
                        attempt["found"] = True
                        deduped = _dedupe_candidates(all_candidates)
                        return {
                            "found": True,
                            "match": scan.get("match"),
                            "candidates": deduped,
                            "attempts": attempts,
                            "strategy": strategy.get("name"),
                        }
                except Exception as exc:
                    attempt["error"] = str(exc)
                    attempt["final_url"] = page.url

            deduped = _dedupe_candidates(all_candidates)
            return {
                "found": False,
                "match": None,
                "candidates": deduped,
                "attempts": attempts,
                "strategy": None,
            }
        finally:
            if browser_instance is not None:
                try:
                    browser_instance.close()
                except Exception:
                    pass


def _write_json_output(payload: dict[str, Any], output_path: Path | None) -> None:
    serialized = json.dumps(payload, indent=2, sort_keys=True)
    print(serialized)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(serialized + "\n", encoding="utf-8")


def _is_cert_verify_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return "certificate_verify_failed" in lowered or "unable to get local issuer certificate" in lowered


def main() -> None:
    args = parse_args()
    tz = _timezone_from_args(args.timezone)
    target_dt = _parse_received_datetime(args.received_at, tz)
    ladder_preview = _build_search_ladder(
        subject=args.subject,
        target_dt=target_dt,
        tz=tz,
        sender=args.sender,
        date_window_days=args.date_window_days,
    )
    search_query = str(ladder_preview[0].get("query") or "")
    errors: list[str] = []
    warnings: list[str] = []

    payload: dict[str, Any] = {
        "found": False,
        "method": None,
        "strategy": None,
        "subject": args.subject,
        "sender_filter": args.sender or None,
        "target_received_at_local": target_dt.astimezone(tz).isoformat(),
        "search_query": search_query,
        "search_ladder": ladder_preview,
        "match": None,
        "candidates": [],
        "attempts": [],
        "warnings": warnings,
        "errors": errors,
    }

    if not args.skip_atom:
        jar: http.cookiejar.CookieJar | None = None
        try:
            if args.cookie_header:
                _log("Using --cookie-header for Atom lookup.", enabled=args.verbose)
                jar = _cookiejar_from_header(args.cookie_header)
            elif args.cookie_file:
                _log(f"Loading cookie file: {args.cookie_file}", enabled=args.verbose)
                jar = _load_cookie_file(args.cookie_file)
            else:
                _log(
                    f"Loading browser cookies via browser_cookie3 ({args.browser}).",
                    enabled=args.verbose,
                )
                jar = _load_browser_cookie3(args.browser)

            use_insecure_atom = bool(args.atom_insecure)
            try:
                atom_xml = _fetch_atom_feed(
                    jar,
                    args.mailbox,
                    args.atom_timeout_seconds,
                    args.verbose,
                    use_insecure_atom,
                )
            except Exception as first_exc:
                if _is_cert_verify_error(first_exc) and not use_insecure_atom:
                    warnings.append(
                        "Atom TLS verification failed; retrying Atom lookup with insecure TLS."
                    )
                    atom_xml = _fetch_atom_feed(
                        jar,
                        args.mailbox,
                        args.atom_timeout_seconds,
                        args.verbose,
                        True,
                    )
                    use_insecure_atom = True
                else:
                    raise

            entries = _parse_atom_entries(atom_xml)
            match, candidates = _select_atom_match(entries, args.subject, target_dt, tz, args.sender)
            payload["candidates"] = candidates
            payload["atom"] = {
                "attempted": True,
                "insecure": use_insecure_atom,
                "entries": len(entries),
                "subject_candidates": len(candidates),
            }
            if match is not None:
                payload["found"] = True
                payload["method"] = "atom_feed"
                payload["strategy"] = "atom_feed_exact_subject_minute"
                payload["match"] = match
                _write_json_output(payload, args.output)
                return
        except Exception as exc:  # pragma: no cover - runtime/network behavior
            error_text = f"Atom lookup failed: {exc}"
            if _is_cert_verify_error(exc):
                warnings.append(error_text)
            else:
                errors.append(error_text)
            payload["atom"] = {"attempted": True, "error": str(exc)}
            _log(error_text, enabled=True)
    else:
        payload["atom"] = {"attempted": False, "skipped": True}

    if args.session_fallback:
        try:
            fallback = _playwright_lookup(
                subject=args.subject,
                target_dt=target_dt,
                sender=args.sender,
                tz=tz,
                mailbox=args.mailbox,
                channel=args.playwright_channel,
                headless=args.headless,
                storage_state=args.storage_state,
                inject_browser_cookies=args.inject_browser_cookies,
                browser_name=args.browser,
                cookie_file=args.cookie_file,
                cookie_header=args.cookie_header,
                max_rows=args.max_rows,
                max_pages=args.max_pages,
                date_window_days=args.date_window_days,
                include_body=args.include_body,
                verbose=args.verbose,
            )
            atom_candidates = payload.get("candidates", [])
            fallback_candidates = fallback.get("candidates", [])
            payload["candidates"] = _dedupe_candidates(atom_candidates + fallback_candidates)
            payload["attempts"] = fallback.get("attempts", [])
            payload["strategy"] = fallback.get("strategy")
            if fallback.get("found"):
                payload["found"] = True
                payload["method"] = "playwright_session"
                payload["match"] = fallback.get("match")
            else:
                payload["method"] = payload["method"] or "playwright_session"
        except Exception as exc:
            error_text = f"Session fallback failed: {exc}"
            errors.append(error_text)
            _log(error_text, enabled=True)

    _write_json_output(payload, args.output)
    if args.exit_nonzero_on_miss and not payload["found"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
