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
from urllib.parse import parse_qs, quote, urlparse
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
    received_group = parser.add_mutually_exclusive_group(required=True)
    received_group.add_argument(
        "--received-at",
        help=(
            "Target received datetime. Examples: "
            "'2026-01-19 17:18', '2026-01-19T17:18:00-05:00', "
            "'Jan 19, 2026, 5:18 PM'."
        ),
    )
    received_group.add_argument(
        "--received-on",
        help=(
            "Target received date (date-only mode). Examples: '2026-02-08', "
            "'Feb 8, 2026'. Matches any time on that date."
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
        "--row-hydration-timeout-ms",
        type=int,
        default=7000,
        help="Max wait for Gmail list rows to hydrate before treating a 0-row view as real.",
    )
    parser.add_argument(
        "--zero-row-retries",
        type=int,
        default=2,
        help="Extra hydration retries when Gmail shell is present but list rows are 0.",
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


def _parse_received_date(raw: str) -> dt.date:
    value = raw.strip()
    fmt_candidates = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%a, %b %d, %Y",
        "%A, %b %d, %Y",
        "%a, %B %d, %Y",
        "%A, %B %d, %Y",
    ]
    for fmt in fmt_candidates:
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.date()
    except ValueError:
        pass
    _die("Could not parse --received-on. Use ISO date like '2026-02-08'.")
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


def _same_local_date(a: dt.datetime, target_date: dt.date, tz: dt.tzinfo) -> bool:
    return a.astimezone(tz).date() == target_date


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


def _subject_probe_phrase(subject: str) -> str:
    normalized = _normalize_subject_text(subject).lower()
    if ":" in normalized:
        head = normalized.split(":", 1)[0].strip()
        if head:
            return head
    return normalized


def _subject_probe_matches(observed: str | None, requested: str) -> tuple[bool, bool]:
    exact = _subject_matches_requested(observed, requested)
    observed_norm = _normalize_subject_text(observed).lower()
    probe = _subject_probe_phrase(requested)
    broad = bool(observed_norm and probe and probe in observed_norm)
    return exact, broad


def _normalize_link_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip().lower()


def _alert_management_link_reason(*, href: str | None, text: str | None) -> str | None:
    href_norm = (href or "").strip().lower()
    text_norm = _normalize_link_text(text)
    if not href_norm and not text_norm:
        return None

    href_patterns = [
        r"unsubscribe",
        r"removealert",
        r"manage[-_/]?alerts?",
        r"alert[-_/]?preferences?",
        r"email[-_/]?(notification[-_/]?)?preferences?",
        r"notification[-_/]?preferences?",
    ]
    if any(re.search(pattern, href_norm) for pattern in href_patterns):
        if re.search(r"unsubscribe|removealert", href_norm):
            return "alert_unsubscribe_link"
        return "alert_management_preferences_link"

    text_patterns = [
        r"\bunsubscribe\b",
        r"\bmanage\s+(my\s+)?alerts?\b",
        r"\b(alert|email|notification)\s+preferences?\b",
        r"\bmanage\s+preferences?\b",
    ]
    if any(re.search(pattern, text_norm) for pattern in text_patterns):
        if re.search(r"\bunsubscribe\b", text_norm):
            return "alert_unsubscribe_link"
        return "alert_management_preferences_link"
    return None


def _unsupported_link_scheme_reason(href: str | None) -> str | None:
    raw = (href or "").strip()
    if not raw:
        return None
    if raw.startswith("//"):
        return None
    matched = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*):", raw)
    if not matched:
        return None
    scheme = matched.group(1).lower()
    if scheme in {"http", "https"}:
        return None
    return "unsupported_url_scheme"


def _blocked_link_reason(*, href: str | None, text: str | None) -> str | None:
    if _is_gmail_full_message_webview_link(href):
        return "gmail_message_webview_link"
    return _alert_management_link_reason(href=href, text=text) or _unsupported_link_scheme_reason(href)


def _is_alert_management_link(*, href: str | None, text: str | None) -> bool:
    return _alert_management_link_reason(href=href, text=text) is not None


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
    target_date: dt.date,
    date_only_mode: bool,
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
            candidate["date_match"] = _same_local_date(issued_dt, target_date, tz)
        else:
            candidate["issued_local"] = None
            candidate["minute_match"] = False
            candidate["date_match"] = False
        candidate["sender_match"] = _sender_matches(
            sender,
            str(candidate.get("author_name") or ""),
            str(candidate.get("author_email") or ""),
        )
        candidates.append(candidate)
        if (candidate["date_match"] if date_only_mode else candidate["minute_match"]) and candidate[
            "sender_match"
        ]:
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


def _safe_link_details(page: Any) -> list[dict[str, str]]:
    details: list[dict[str, str]] = []
    try:
        links = page.locator("div.a3s a[href]")
        count = min(links.count(), 500)
    except Exception:
        return details
    for idx in range(count):
        node = links.nth(idx)
        href = _safe_attr(node, "href")
        if not href:
            continue
        details.append({"href": href, "text": _safe_inner_text(node)})
    return details


def _safe_link_details_any(page: Any, *, max_links: int = 800) -> list[dict[str, str]]:
    details: list[dict[str, str]] = []
    try:
        links = page.locator("a[href]")
        count = min(links.count(), max_links)
    except Exception:
        return details
    for idx in range(count):
        node = links.nth(idx)
        href = _safe_attr(node, "href")
        if not href:
            continue
        details.append({"href": href, "text": _safe_inner_text(node)})
    return details


def _normalize_link_href(value: str | None) -> str:
    href = (value or "").strip()
    if not href:
        return ""
    if href.startswith("//"):
        return f"https:{href}"
    return href


def _is_gmail_full_message_webview_link(href: str | None) -> bool:
    normalized = _normalize_link_href(href)
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    if "mail.google.com" not in host:
        return False
    query = parse_qs(parsed.query or "")
    view_value = ((query.get("view") or [""])[0] or "").lower()
    permmsgid_value = ((query.get("permmsgid") or [""])[0] or "").strip()
    return view_value == "lg" and bool(permmsgid_value)


def _merge_link_details(*groups: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for group in groups:
        for item in group:
            if not isinstance(item, dict):
                continue
            href = _normalize_link_href(item.get("href"))
            text = str(item.get("text") or "").strip()
            if not href:
                continue
            key = (href, text)
            if key in seen:
                continue
            seen.add(key)
            merged.append({"href": href, "text": text})
    return merged


def _expand_gmail_webview_message(
    page: Any,
    *,
    webview_url: str,
    include_body: bool,
    verbose: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "attempted": True,
        "expanded": False,
        "webview_url": webview_url,
        "final_url": "",
        "error": "",
        "added_link_count": 0,
        "all_link_count": 0,
    }
    popup = None
    try:
        popup = page.context.new_page()
        popup.goto(webview_url, wait_until="domcontentloaded", timeout=45000)
        popup.wait_for_timeout(800)
        result["final_url"] = str(getattr(popup, "url", "") or "")
        if "accounts.google.com" in result["final_url"]:
            result["error"] = "redirected_to_google_signin"
            return result
        try:
            popup.wait_for_selector("body", state="attached", timeout=5000)
        except Exception:
            _log("Gmail webview body selector wait timed out; continuing.", enabled=verbose)
        link_details = _safe_link_details_any(popup)
        result["all_link_count"] = len(link_details)
        result["link_details"] = link_details
        if include_body:
            result["body_text"] = _safe_inner_text(popup.locator("body").first)
        result["expanded"] = bool(link_details) or bool(result.get("body_text"))
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result
    finally:
        if popup is not None:
            try:
                popup.close()
            except Exception:
                pass


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


def _safe_count(locator: Any, limit: int | None = None) -> int:
    try:
        count = int(locator.count())
    except Exception:
        return 0
    if limit is not None and count > limit:
        return limit
    return max(0, count)


def _gmail_list_row_locators(page: Any) -> list[tuple[str, Any]]:
    return [
        ("tr.zA", page.locator("tr.zA")),
        ('tr[role="row"]:has(span.bog)', page.locator('tr[role="row"]:has(span.bog)')),
        ('[role="main"] tr:has(span.bog)', page.locator('[role="main"] tr:has(span.bog)')),
    ]


def _select_gmail_list_rows(page: Any) -> tuple[Any, dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    fallback_locator = page.locator("tr.zA")
    fallback_selector = "tr.zA"
    for selector, locator in _gmail_list_row_locators(page):
        count = _safe_count(locator)
        candidates.append({"selector": selector, "count": count})
        if count > 0:
            return locator, {"selector": selector, "row_count": count, "candidates": candidates}
        if selector == "tr.zA":
            fallback_locator = locator
            fallback_selector = selector
    return fallback_locator, {"selector": fallback_selector, "row_count": 0, "candidates": candidates}


def _probe_gmail_list_ui(page: Any) -> dict[str, Any]:
    spinner_selectors = [
        'div[role="progressbar"]',
        '[aria-label*="Loading"]',
        '[aria-label*="loading"]',
        ".v2",
        ".aAk",
    ]
    spinner_hits: list[dict[str, Any]] = []
    for selector in spinner_selectors:
        count = _safe_count(page.locator(selector))
        if count:
            spinner_hits.append({"selector": selector, "count": count})
    rows_locator, row_pick = _select_gmail_list_rows(page)
    probe = {
        "url": str(getattr(page, "url", "") or ""),
        "zA_rows": _safe_count(page.locator("tr.zA")),
        "role_rows": _safe_count(page.locator('tr[role="row"]')),
        "selected_row_selector": row_pick.get("selector"),
        "selected_row_count": int(row_pick.get("row_count") or 0),
        "row_candidates": row_pick.get("candidates") or [],
        "bog_nodes": _safe_count(page.locator("span.bog")),
        "search_inputs": _safe_count(
            page.locator('input[aria-label="Search mail"], input[name="q"]')
        ),
        "main_regions": _safe_count(page.locator('[role="main"]')),
        "message_headers": _safe_count(page.locator("h2.hP")),
        "spinners": spinner_hits,
    }
    # Keep a direct shell flag for downstream diagnostics/decision logic.
    probe["shell_present"] = bool(
        probe["search_inputs"] or probe["main_regions"] or probe["message_headers"]
    )
    return probe


def _gmail_zero_row_ui_is_ambiguous(ui_probe: dict[str, Any] | None) -> bool:
    probe = ui_probe or {}
    selected_row_count = int(probe.get("selected_row_count") or 0)
    bog_nodes = int(probe.get("bog_nodes") or 0)
    shell_present = bool(probe.get("shell_present"))
    if selected_row_count > 0 or bog_nodes > 0:
        return False
    return shell_present


def _wait_for_list_rows_hydration(
    page: Any,
    *,
    verbose: bool,
    strategy_name: str,
    page_index: int,
    timeout_ms: int,
    zero_row_retries: int,
) -> tuple[Any, dict[str, Any]]:
    timeout_ms = max(int(timeout_ms or 0), 0)
    zero_row_retries = max(int(zero_row_retries or 0), 0)
    poll_ms = 400
    attempts: list[dict[str, Any]] = []
    recovered = False
    retry_count = 0
    last_rows, last_pick = _select_gmail_list_rows(page)
    last_probe = _probe_gmail_list_ui(page)
    started_ts = dt.datetime.now().timestamp()

    while True:
        elapsed_ms = int((dt.datetime.now().timestamp() - started_ts) * 1000)
        selected_row_count = int(last_pick.get("row_count") or 0)
        ambiguous = _gmail_zero_row_ui_is_ambiguous(last_probe)
        probe_snapshot = dict(last_probe)
        attempts.append(
            {
                "probe": probe_snapshot,
                "selected_row_selector": last_pick.get("selector"),
                "selected_row_count": selected_row_count,
                "elapsed_ms": elapsed_ms,
                "ambiguous_zero_rows": ambiguous,
                "retry_index": retry_count,
            }
        )
        if selected_row_count > 0:
            break

        # If Gmail shell is absent and rows are zero, allow one short wait to avoid
        # racing DOM attach, but don't spin through all zero-row retries.
        if not ambiguous and elapsed_ms >= max(timeout_ms, poll_ms):
            break

        if elapsed_ms < timeout_ms:
            page.wait_for_timeout(poll_ms)
            last_rows, last_pick = _select_gmail_list_rows(page)
            last_probe = _probe_gmail_list_ui(page)
            continue

        if ambiguous and retry_count < zero_row_retries:
            retry_count += 1
            _log(
                f"[{strategy_name}] page {page_index}: Gmail shell is present but rows are 0; hydration retry {retry_count}/{zero_row_retries}.",
                enabled=verbose,
            )
            page.wait_for_timeout(900)
            last_rows, last_pick = _select_gmail_list_rows(page)
            last_probe = _probe_gmail_list_ui(page)
            if int(last_pick.get("row_count") or 0) > 0:
                recovered = True
            continue
        break

    final_ambiguous = _gmail_zero_row_ui_is_ambiguous(last_probe)
    info = {
        "strategy": strategy_name,
        "page": page_index,
        "selected_row_selector": last_pick.get("selector"),
        "selected_row_count": int(last_pick.get("row_count") or 0),
        "hydrated": bool(int(last_pick.get("row_count") or 0) > 0),
        "recovered_from_zero_rows": recovered,
        "zero_row_ambiguous": final_ambiguous,
        "retry_count": retry_count,
        "timeout_ms": timeout_ms,
        "attempts": attempts[:8],
        "ui_probe": last_probe,
        "refresh_attempted": False,
        "refresh_recovered": False,
    }
    return last_rows, info


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


def _first_list_row_signature(page: Any) -> str:
    try:
        rows, picked = _select_gmail_list_rows(page)
        if int(picked.get("row_count") or 0) <= 0:
            return ""
        row = rows.first
        subject = _safe_inner_text(row.locator("span.bog").first)
        sender = _safe_inner_text(row.locator("span.zF, span.yP").first)
        when = _safe_attr(row.locator("td.xW span").first, "title") or _safe_inner_text(
            row.locator("td.xW span").first
        )
        sig = " | ".join([subject.strip(), sender.strip(), when.strip()]).strip()
        return sig
    except Exception:
        return ""


def _wait_for_list_page_change(page: Any, *, before_signature: str, timeout_ms: int = 8000) -> bool:
    deadline = dt.datetime.now().timestamp() + (timeout_ms / 1000.0)
    while dt.datetime.now().timestamp() < deadline:
        page.wait_for_timeout(250)
        current = _first_list_row_signature(page)
        if current and current != before_signature:
            return True
    return False


def _search_results_content_looks_valid(
    *,
    strategy_name: str,
    page1_rows: int,
    page1_exact_hits: int,
    page1_broad_hits: int,
    page1_hydrated: bool = True,
    page1_zero_row_ambiguous: bool = False,
) -> bool:
    if not strategy_name.startswith("search_"):
        return True
    if page1_rows <= 0:
        if page1_zero_row_ambiguous or not page1_hydrated:
            return False
        return True

    exact_subject_strategies = {
        "search_strict_exact_subject",
        "search_exact_subject_only",
        "search_subject_window_no_sender",
    }
    if strategy_name in exact_subject_strategies:
        return page1_exact_hits > 0

    if strategy_name == "search_sender_broad_window":
        return page1_broad_hits > 0

    return True


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
    before_signature = _first_list_row_signature(page)
    selectors = [
        'button[aria-label="Older"]',
        'button[aria-label*="Older"]',
        '[role="button"][aria-label="Older"]',
        '[role="button"][aria-label*="Older"]',
        '[aria-label="Older"]',
        '[data-tooltip="Older"]',
        '[title="Older"]',
    ]
    for selector in selectors:
        locator = page.locator(selector)
        count = min(locator.count(), 4)
        if not count:
            continue
        for idx in range(count):
            button = locator.nth(idx)

            aria_disabled = _safe_attr(button, "aria-disabled").lower()
            disabled_attr = _safe_attr(button, "disabled").lower()
            class_name = _safe_attr(button, "class")
            if aria_disabled == "true" or disabled_attr in {"true", "disabled"} or "aqj" in class_name:
                continue

            try:
                button.scroll_into_view_if_needed(timeout=2000)
            except Exception:
                pass

            click_attempts = [
                lambda: button.click(timeout=5000),
                lambda: button.click(timeout=5000, force=True),
                lambda: button.evaluate(
                    """el => {
                        const target = el.closest('button,[role="button"],div') || el;
                        target.click();
                    }"""
                ),
            ]
            for click_action in click_attempts:
                try:
                    click_action()
                except Exception:
                    continue
                if _wait_for_list_page_change(page, before_signature=before_signature, timeout_ms=7000):
                    return True
                page.wait_for_timeout(500)

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
    target_date: dt.date,
    sender: str | None,
    tz: dt.tzinfo,
    include_body: bool,
    date_only_mode: bool,
    verbose: bool = False,
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

    thread_link_details = _safe_link_details(page)
    all_link_details = thread_link_details
    all_links = [
        _normalize_link_href(item.get("href")) for item in all_link_details if isinstance(item, dict)
    ] or []
    if not all_links and all_link_details:
        # Defensive fallback in case Playwright returns a list of href strings from older runtimes.
        all_links = [str(item).strip() for item in all_link_details if str(item).strip()]
        all_link_details = [{"href": href, "text": ""} for href in all_links]

    body_text = None
    if include_body:
        body_text = _safe_inner_text(page.locator("div.a3s").first)

    gmail_webview_expansion: dict[str, Any] | None = None
    webview_links = [
        _normalize_link_href(item.get("href"))
        for item in all_link_details
        if isinstance(item, dict) and _is_gmail_full_message_webview_link(item.get("href"))
    ]
    if webview_links:
        webview_url = webview_links[0]
        _log(
            f"Expanding clipped Gmail message via webview link: {webview_url}",
            enabled=verbose,
        )
        expansion = _expand_gmail_webview_message(
            page,
            webview_url=webview_url,
            include_body=include_body,
            verbose=verbose,
        )
        extra_link_details = [
            item for item in expansion.get("link_details") or [] if isinstance(item, dict)
        ]
        merged_link_details = _merge_link_details(all_link_details, extra_link_details)
        added_link_count = max(0, len(merged_link_details) - len(_merge_link_details(all_link_details)))
        all_link_details = merged_link_details
        all_links = [
            _normalize_link_href(item.get("href")) for item in all_link_details if isinstance(item, dict)
        ]
        gmail_webview_expansion = {
            "attempted": bool(expansion.get("attempted")),
            "expanded": bool(expansion.get("expanded")),
            "webview_url": webview_url,
            "final_url": str(expansion.get("final_url") or ""),
            "error": str(expansion.get("error") or ""),
            "base_link_count": len(thread_link_details),
            "webview_link_count": len(extra_link_details),
            "merged_link_count": len(all_link_details),
            "added_link_count": added_link_count,
        }
        if include_body:
            webview_body_text = str(expansion.get("body_text") or "")
            if webview_body_text:
                candidate_body_source = "gmail_thread"
                if len(webview_body_text.strip()) > len((body_text or "").strip()):
                    body_text = webview_body_text
                    candidate_body_source = "gmail_webview"
                else:
                    candidate_body_source = "gmail_thread"
                gmail_webview_expansion["body_text_length"] = len(webview_body_text)
                gmail_webview_expansion["body_text_used"] = candidate_body_source == "gmail_webview"

    blocked_link_details: list[dict[str, str]] = []
    safe_link_details: list[dict[str, str]] = []
    for item in all_link_details:
        if not isinstance(item, dict):
            continue
        href_value = _normalize_link_href(item.get("href"))
        text_value = str(item.get("text") or "").strip()
        normalized = {"href": href_value, "text": text_value}
        if not href_value:
            continue
        block_reason = _blocked_link_reason(href=href_value, text=text_value)
        if block_reason:
            blocked_link_details.append({**normalized, "reason": block_reason})
        else:
            safe_link_details.append(normalized)

    links = [item["href"] for item in safe_link_details]
    blocked_links = [item["href"] for item in blocked_link_details]

    minute_match = any(_same_minute(value, target_dt, tz) for value in parsed_datetimes)
    date_match = any(_same_local_date(value, target_date, tz) for value in parsed_datetimes)
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
        "date_match": date_match,
        "url": page.url,
        "links": links,
        "link_details": safe_link_details,
        "all_links": all_links,
        "all_link_details": all_link_details,
        "blocked_links": blocked_links,
        "blocked_link_details": blocked_link_details,
    }
    if include_body:
        candidate["body_text"] = body_text
        candidate["body_text_source"] = (
            "gmail_webview"
            if gmail_webview_expansion and gmail_webview_expansion.get("body_text_used")
            else "gmail_thread"
        )
    if gmail_webview_expansion is not None:
        candidate["gmail_webview_expansion"] = gmail_webview_expansion
    candidate["time_match_mode"] = "date" if date_only_mode else "minute"
    return candidate


def _scan_current_view(
    *,
    page: Any,
    strategy_name: str,
    subject: str,
    target_dt: dt.datetime,
    target_date: dt.date,
    sender: str | None,
    tz: dt.tzinfo,
    max_rows: int,
    max_pages: int,
    row_hydration_timeout_ms: int,
    zero_row_retries: int,
    include_body: bool,
    verbose: bool,
    date_only_mode: bool,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    pages_scanned = 0
    rows_scanned = 0
    page1_exact_subject_hits = 0
    page1_broad_subject_hits = 0
    page1_rows = 0
    page1_hydrated = True
    page1_zero_row_ambiguous = False
    page1_ui_probe: dict[str, Any] = {}
    page1_list_hydration: dict[str, Any] = {}

    for page_index in range(1, max_pages + 1):
        if progress_callback is not None:
            progress_callback("list_hydration_probe", {"page": page_index, "strategy": strategy_name})
        rows, hydration = _wait_for_list_rows_hydration(
            page,
            verbose=verbose,
            strategy_name=strategy_name,
            page_index=page_index,
            timeout_ms=row_hydration_timeout_ms,
            zero_row_retries=zero_row_retries,
        )
        row_count = min(int(hydration.get("selected_row_count") or 0), max_rows)
        pages_scanned += 1
        if page_index == 1:
            page1_rows = row_count
            page1_hydrated = bool(hydration.get("hydrated"))
            page1_zero_row_ambiguous = bool(hydration.get("zero_row_ambiguous"))
            page1_ui_probe = hydration.get("ui_probe") or {}
            page1_list_hydration = {
                k: v
                for k, v in hydration.items()
                if k != "ui_probe"
            }
        if progress_callback is not None:
            phase_name = "list_hydration_recovered" if hydration.get("recovered_from_zero_rows") else (
                "list_hydration_failed" if row_count <= 0 else "list_hydration_probe"
            )
            progress_callback(
                phase_name,
                {
                    "page": page_index,
                    "strategy": strategy_name,
                    "row_count": row_count,
                    "hydrated": bool(hydration.get("hydrated")),
                    "zero_row_ambiguous": bool(hydration.get("zero_row_ambiguous")),
                    "selected_row_selector": hydration.get("selected_row_selector"),
                    "retry_count": int(hydration.get("retry_count") or 0),
                    "ui_probe": hydration.get("ui_probe") or {},
                },
            )
        if row_count <= 0:
            if bool(hydration.get("zero_row_ambiguous")):
                warnings.append(f"{strategy_name}: ui_shell_present_but_rows_missing on page {page_index}")
            elif not bool(hydration.get("hydrated")):
                warnings.append(f"{strategy_name}: gmail_list_not_hydrated on page {page_index}")
            else:
                warnings.append(f"{strategy_name}: zero_rows_after_hydration on page {page_index}")
        _log(
            f"[{strategy_name}] scanning page {page_index}/{max_pages}, rows={row_count}",
            enabled=verbose,
        )

        for row_index in range(row_count):
            row = rows.nth(row_index)
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

            exact_probe_hit, broad_probe_hit = _subject_probe_matches(row_subject, subject)
            if page_index == 1:
                page1_exact_subject_hits += int(exact_probe_hit)
                page1_broad_subject_hits += int(broad_probe_hit)

            if not exact_probe_hit:
                continue

            row_ctx = {
                "page": page_index,
                "row": row_index + 1,
                "row_subject": row_subject,
                "row_sender": row_sender,
                "row_time_hint": row_time_hint,
            }
            if progress_callback is not None:
                progress_callback("candidate_row_match", row_ctx)

            opened = False
            open_error = ""
            for open_attempt in range(1, 3):
                try:
                    _log(
                        f"[{strategy_name}] opening candidate row p{page_index} r{row_index+1} (attempt {open_attempt}/2)",
                        enabled=verbose,
                    )
                    row.click(timeout=12000)
                    page.wait_for_timeout(700)
                    try:
                        page.wait_for_selector("h2.hP, div.a3s", state="attached", timeout=6000)
                    except Exception:
                        _wait_for_gmail_surface(page, verbose=verbose, phase="thread open")
                    opened = True
                    if progress_callback is not None:
                        progress_callback("candidate_opened", {**row_ctx, "open_attempt": open_attempt})
                    break
                except Exception as exc:
                    open_error = str(exc)
                    _log(
                        f"[{strategy_name}] failed to open candidate row p{page_index} r{row_index+1} (attempt {open_attempt}/2): {exc}",
                        enabled=verbose,
                    )
                    page.wait_for_timeout(500)
            if not opened:
                warnings.append(
                    f"{strategy_name}: could not open row p{page_index} r{row_index+1} ({open_error or 'unknown error'})."
                )
                continue

            _log(
                f"[{strategy_name}] extracting message details p{page_index} r{row_index+1}",
                enabled=verbose,
            )
            try:
                candidate = _extract_message_candidate(
                    page=page,
                    strategy_name=strategy_name,
                    row_subject=row_subject,
                    row_sender=row_sender,
                    row_time_hint=row_time_hint,
                    target_dt=target_dt,
                    target_date=target_date,
                    sender=sender,
                    tz=tz,
                    include_body=include_body,
                    date_only_mode=date_only_mode,
                    verbose=verbose,
                )
            except Exception as exc:
                warnings.append(
                    f"{strategy_name}: failed to extract message details for row p{page_index} r{row_index+1} ({exc})."
                )
                _log(
                    f"[{strategy_name}] extraction error p{page_index} r{row_index+1}: {exc}",
                    enabled=verbose,
                )
                _return_to_list(page, verbose=verbose)
                continue
            candidates.append(candidate)
            if progress_callback is not None:
                progress_callback(
                    "candidate_extracted",
                    {
                        **row_ctx,
                        "candidate_subject": candidate.get("subject"),
                        "minute_match": bool(candidate.get("minute_match")),
                        "date_match": bool(candidate.get("date_match")),
                        "sender_match": bool(candidate.get("sender_match")),
                    },
                )
            match_hit = (
                _subject_matches_requested(str(candidate.get("subject") or ""), subject)
                and bool(candidate.get("date_match") if date_only_mode else candidate.get("minute_match"))
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
                    "warnings": warnings,
                    "search_row_probe": {
                        "page1_rows": page1_rows,
                        "page1_exact_subject_hits": page1_exact_subject_hits,
                        "page1_broad_subject_hits": page1_broad_subject_hits,
                        "page1_hydrated": page1_hydrated,
                        "page1_zero_row_ambiguous": page1_zero_row_ambiguous,
                    },
                    "ui_probe": page1_ui_probe,
                    "list_hydration": {"page1": page1_list_hydration},
                    "final_url": page.url,
                }

        if page_index >= max_pages:
            break
        advanced = _goto_older_page(page, verbose=verbose)
        if not advanced:
            if page_index == 1 and row_count >= max_rows and max_rows > 0:
                warnings.append(
                    f"{strategy_name}: page 1 returned {row_count} rows but no usable Older control was found; results may be truncated just beyond page 1."
                )
            break

    return {
        "found": False,
        "match": None,
        "candidates": candidates,
        "pages_scanned": pages_scanned,
        "rows_scanned": rows_scanned,
        "sample_rows": sample_rows,
        "warnings": warnings,
        "search_row_probe": {
            "page1_rows": page1_rows,
            "page1_exact_subject_hits": page1_exact_subject_hits,
            "page1_broad_subject_hits": page1_broad_subject_hits,
            "page1_hydrated": page1_hydrated,
            "page1_zero_row_ambiguous": page1_zero_row_ambiguous,
        },
        "ui_probe": page1_ui_probe,
        "list_hydration": {"page1": page1_list_hydration},
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
    target_date: dt.date,
    date_only_mode: bool,
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
    row_hydration_timeout_ms: int,
    zero_row_retries: int,
    include_body: bool,
    verbose: bool,
    checkpoint_path: Path | None = None,
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
    fallback_warnings: list[str] = []

    def emit_checkpoint(phase: str, extra: dict[str, Any] | None = None) -> None:
        if checkpoint_path is None:
            return
        checkpoint_payload: dict[str, Any] = {
            "partial": True,
            "phase": phase,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "found": False,
            "subject": subject,
            "sender_filter": sender or None,
            "target_received_at_local": target_dt.astimezone(tz).isoformat(),
            "target_received_on_local": str(target_date),
            "time_match_mode": "date" if date_only_mode else "minute",
            "attempts": attempts,
            "candidate_count": len(all_candidates),
            "warnings": fallback_warnings,
        }
        if extra:
            checkpoint_payload.update(extra)
        try:
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint_path.write_text(
                json.dumps(checkpoint_payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except Exception:
            pass

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
                    "content_mismatch": False,
                    "pages_scanned": 0,
                    "rows_scanned": 0,
                    "sample_rows": [],
                    "candidate_count": 0,
                    "ui_probe": {},
                    "list_hydration": {},
                    "search_validation": None,
                    "found": False,
                    "error": None,
                }
                attempts.append(attempt)
                emit_checkpoint("attempt_start", {"current_attempt": attempt.get("name")})

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
                            emit_checkpoint(
                                "attempt_search_not_applied",
                                {"current_attempt": attempt.get("name"), "current_url": page.url},
                            )
                            continue

                    scan: dict[str, Any] | None = None
                    for scan_round in range(1, 3):
                        scan = _scan_current_view(
                            page=page,
                            strategy_name=str(strategy.get("name") or ""),
                            subject=subject,
                            target_dt=target_dt,
                            target_date=target_date,
                            sender=sender,
                            tz=tz,
                            max_rows=max_rows,
                            max_pages=max_pages,
                            row_hydration_timeout_ms=row_hydration_timeout_ms,
                            zero_row_retries=zero_row_retries,
                            include_body=include_body,
                            verbose=verbose,
                            date_only_mode=date_only_mode,
                            progress_callback=lambda phase, info, _attempt=attempt: emit_checkpoint(
                                phase,
                                {"current_attempt": _attempt.get("name"), "progress": info},
                            ),
                        )
                        page1_hydration = ((scan.get("list_hydration") or {}).get("page1") or {})
                        if int(page1_hydration.get("retry_count") or 0) > 0:
                            emit_checkpoint(
                                "list_hydration_retry",
                                {
                                    "current_attempt": attempt.get("name"),
                                    "retry_count": int(page1_hydration.get("retry_count") or 0),
                                    "page1": page1_hydration,
                                },
                            )

                        zero_row_ambiguous = bool(page1_hydration.get("zero_row_ambiguous"))
                        page1_rows_scanned = int(
                            ((scan.get("search_row_probe") or {}).get("page1_rows") or 0)
                        )
                        if (
                            scan_round == 1
                            and not bool(scan.get("found"))
                            and str(strategy.get("mode") or "") in {"search", "search_input", "crawl"}
                            and zero_row_ambiguous
                            and page1_rows_scanned <= 0
                        ):
                            page1_hydration["refresh_attempted"] = True
                            attempt.setdefault("list_hydration", {})
                            attempt["list_hydration"] = {"page1": page1_hydration}
                            refresh_warning = (
                                f"{strategy.get('name')}: page 1 had 0 rows with Gmail shell present; refreshing view once before downgrading the attempt."
                            )
                            if refresh_warning not in fallback_warnings:
                                fallback_warnings.append(refresh_warning)
                            emit_checkpoint(
                                "attempt_zero_row_refresh",
                                {"current_attempt": attempt.get("name"), "page1": page1_hydration},
                            )
                            nav = _goto_mail_view(
                                page,
                                mailbox=mailbox,
                                mode=str(strategy.get("mode", "")),
                                query=strategy.get("query"),
                                folder=strategy.get("folder"),
                                verbose=verbose,
                            )
                            attempt["final_url"] = nav.get("current_url") or page.url
                            if strategy.get("mode") in {"search", "search_input"}:
                                attempt["search_applied"] = bool(nav.get("search_applied"))
                                attempt["inbox_like"] = bool(nav.get("inbox_like"))
                                if not attempt["search_applied"] or attempt["inbox_like"]:
                                    emit_checkpoint(
                                        "attempt_search_not_applied",
                                        {"current_attempt": attempt.get("name"), "current_url": page.url},
                                    )
                                    scan = None
                                    break
                            continue
                        break

                    if scan is None:
                        continue
                    attempt["pages_scanned"] = scan.get("pages_scanned", 0)
                    attempt["rows_scanned"] = scan.get("rows_scanned", 0)
                    attempt["sample_rows"] = scan.get("sample_rows", [])
                    attempt["candidate_count"] = len(scan.get("candidates", []))
                    attempt["final_url"] = scan.get("final_url")
                    attempt["ui_probe"] = scan.get("ui_probe") or {}
                    attempt["list_hydration"] = scan.get("list_hydration") or {}
                    attempt_warnings = [str(x) for x in scan.get("warnings", []) if str(x)]
                    if attempt_warnings:
                        attempt["warnings"] = attempt_warnings
                        fallback_warnings.extend(
                            [w for w in attempt_warnings if w not in fallback_warnings]
                        )
                    probe = scan.get("search_row_probe") or {}
                    attempt["search_row_probe"] = probe
                    if strategy.get("mode") in {"search", "search_input"}:
                        page1_hydrated = bool(probe.get("page1_hydrated", True))
                        page1_zero_row_ambiguous = bool(probe.get("page1_zero_row_ambiguous", False))
                        if page1_zero_row_ambiguous and int(probe.get("page1_rows") or 0) <= 0:
                            attempt["search_applied"] = False
                            attempt["search_validation"] = {
                                "mode": "inconclusive_zero_row_ui",
                                "reason": "Gmail search view returned 0 rows while shell UI was present; row hydration may have failed.",
                                "probe": probe,
                            }
                            inconclusive_warning = (
                                f"{strategy.get('name')}: Gmail search validity was inconclusive because the UI shell loaded but row hydration stayed at 0 rows."
                            )
                            if inconclusive_warning not in fallback_warnings:
                                fallback_warnings.append(inconclusive_warning)
                            emit_checkpoint(
                                "attempt_search_inconclusive_zero_rows",
                                {"current_attempt": attempt.get("name"), "search_row_probe": probe},
                            )
                            continue
                        looks_valid = _search_results_content_looks_valid(
                            strategy_name=str(strategy.get("name") or ""),
                            page1_rows=int(probe.get("page1_rows") or 0),
                            page1_exact_hits=int(probe.get("page1_exact_subject_hits") or 0),
                            page1_broad_hits=int(probe.get("page1_broad_subject_hits") or 0),
                            page1_hydrated=page1_hydrated,
                            page1_zero_row_ambiguous=page1_zero_row_ambiguous,
                        )
                        if not looks_valid:
                            attempt["content_mismatch"] = True
                            attempt["search_applied"] = False
                            attempt["search_validation"] = {
                                "mode": "invalid_content_mismatch",
                                "reason": "Gmail URL looked like search mode, but first-page rows did not match the expected subject pattern.",
                                "probe": probe,
                            }
                            mismatch_warning = (
                                f"{strategy.get('name')}: Gmail URL looked like search mode, but first-page row content did not match the expected query subject pattern."
                            )
                            if mismatch_warning not in fallback_warnings:
                                fallback_warnings.append(mismatch_warning)
                            emit_checkpoint(
                                "attempt_content_mismatch",
                                {"current_attempt": attempt.get("name"), "search_row_probe": probe},
                            )
                            continue
                        attempt["search_validation"] = {
                            "mode": "valid",
                            "reason": "",
                            "probe": probe,
                        }

                    all_candidates.extend(scan.get("candidates", []))
                    emit_checkpoint(
                        "attempt_scan_complete",
                        {
                            "current_attempt": attempt.get("name"),
                            "scan_found": bool(scan.get("found")),
                            "candidate_count": len(all_candidates),
                        },
                    )
                    if scan.get("found"):
                        attempt["found"] = True
                        deduped = _dedupe_candidates(all_candidates)
                        emit_checkpoint(
                            "match_found",
                            {
                                "found": True,
                                "strategy": strategy.get("name"),
                                "match": scan.get("match"),
                                "candidate_count": len(deduped),
                            },
                        )
                        return {
                            "found": True,
                            "match": scan.get("match"),
                            "candidates": deduped,
                            "attempts": attempts,
                            "strategy": strategy.get("name"),
                            "warnings": fallback_warnings,
                        }
                except Exception as exc:
                    attempt["error"] = str(exc)
                    attempt["final_url"] = page.url
                    emit_checkpoint(
                        "attempt_error",
                        {"current_attempt": attempt.get("name"), "error": str(exc), "current_url": page.url},
                    )

            deduped = _dedupe_candidates(all_candidates)
            emit_checkpoint("playwright_complete", {"candidate_count": len(deduped)})
            return {
                "found": False,
                "match": None,
                "candidates": deduped,
                "attempts": attempts,
                "strategy": None,
                "warnings": fallback_warnings,
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
        partial_path = output_path.with_suffix(output_path.suffix + ".partial.json")
        try:
            if partial_path.exists():
                partial_path.unlink()
        except Exception:
            pass


def _is_cert_verify_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return "certificate_verify_failed" in lowered or "unable to get local issuer certificate" in lowered


def main() -> None:
    args = parse_args()
    tz = _timezone_from_args(args.timezone)
    date_only_mode = bool(args.received_on and not args.received_at)
    if date_only_mode:
        target_date = _parse_received_date(str(args.received_on))
        target_dt = dt.datetime.combine(target_date, dt.time(12, 0), tzinfo=tz)
    else:
        target_dt = _parse_received_datetime(str(args.received_at), tz)
        target_date = target_dt.astimezone(tz).date()
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
        "target_received_on_local": str(target_date),
        "time_match_mode": "date" if date_only_mode else "minute",
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
            match, candidates = _select_atom_match(
                entries,
                args.subject,
                target_dt,
                tz,
                args.sender,
                target_date,
                date_only_mode,
            )
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
                payload["strategy"] = (
                    "atom_feed_exact_subject_date" if date_only_mode else "atom_feed_exact_subject_minute"
                )
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
                target_date=target_date,
                date_only_mode=date_only_mode,
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
                row_hydration_timeout_ms=args.row_hydration_timeout_ms,
                zero_row_retries=args.zero_row_retries,
                include_body=args.include_body,
                verbose=args.verbose,
                checkpoint_path=(
                    args.output.with_suffix(args.output.suffix + ".partial.json") if args.output else None
                ),
            )
            atom_candidates = payload.get("candidates", [])
            fallback_candidates = fallback.get("candidates", [])
            payload["candidates"] = _dedupe_candidates(atom_candidates + fallback_candidates)
            payload["attempts"] = fallback.get("attempts", [])
            payload["strategy"] = fallback.get("strategy")
            for item in fallback.get("warnings", []) or []:
                text = str(item).strip()
                if text and text not in warnings:
                    warnings.append(text)
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
