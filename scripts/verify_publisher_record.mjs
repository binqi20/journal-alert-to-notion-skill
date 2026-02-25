#!/usr/bin/env node
/**
 * Verify paper metadata from publisher/DOI pages with domain-aware policy and
 * anti-bot challenge handling.
 *
 * Output JSON is designed to feed build_notion_payload.py.
 */

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { execFile } from "node:child_process";
import { pathToFileURL } from "node:url";

const DEFAULT_TIMEOUT_MS = 45_000;
const DEFAULT_RETRIES = 3;
const DEFAULT_CONCURRENCY = 2;
const DEFAULT_CHALLENGE_WAIT_MS = 45_000;
const DEFAULT_CHALLENGE_POLL_MS = 3_000;
const DEFAULT_TRACKING_TIMEOUT_MS = 20_000;
const DEFAULT_SCIENCEDIRECT_MODE = "auto";
const CURL_MAX_BUFFER_BYTES = 50 * 1024 * 1024;
const CURL_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const DOMAIN_POLICIES = [
  {
    name: "informs",
    hostPattern: /(^|\.)pubsonline\.informs\.org$/i,
    protected: true,
    abstractSelectors: [
      "section.abstract",
      "#abstract",
      "div.abstractSection",
      "div.hlFld-Abstract",
      "div.NLM_abstract",
    ],
  },
  {
    name: "sage",
    hostPattern: /(^|\.)journals\.sagepub\.com$/i,
    protected: true,
    abstractSelectors: [
      "section.abstract",
      "#abstract",
      "div.abstractSection",
      "div.articeBody_abstract",
    ],
  },
  {
    name: "wiley",
    hostPattern: /(^|\.)onlinelibrary\.wiley\.com$/i,
    protected: true,
    abstractSelectors: ["section.article-section__abstract", "section.abstract", "#abstract"],
  },
  {
    name: "springer_link",
    hostPattern: /(^|\.)link\.springer\.com$/i,
    protected: true,
    abstractSelectors: [
      "section#Abs1",
      "section.Abstract",
      ".Abstract",
      'section.c-article-section[id^="Abs"]',
      "#Abs1-content",
      "section.abstract",
      "#abstract",
    ],
  },
  {
    name: "sciencedirect",
    hostPattern: /(^|\.)sciencedirect\.com$/i,
    protected: true,
    abstractSelectors: ["#abstracts", ".Abstracts", "section.abstract"],
  },
  {
    name: "oup",
    hostPattern: /(^|\.)academic\.oup\.com$/i,
    protected: true,
    abstractSelectors: [
      "section.abstract p",
      "section.abstract",
      "div.abstract",
      'section[data-widgetname="ArticleFulltext"] section.abstract',
    ],
  },
  {
    name: "aom_atypon",
    hostPattern: /(^|\.)journals\.aom\.org$/i,
    protected: true,
    abstractSelectors: [
      "div.hlFld-Abstract",
      "div.abstractSection.abstractInFull",
      "section.abstract",
      "#abstract",
      "div.abstract",
    ],
  },
  {
    name: "doi",
    hostPattern: /(^|\.)doi\.org$/i,
    protected: false,
    abstractSelectors: ["section.abstract", "#abstract", "div.abstract", ".abstract"],
  },
  {
    name: "default",
    hostPattern: /.*/,
    protected: false,
    abstractSelectors: ["section.abstract", "#abstract", "div.abstract", ".abstract"],
  },
];

const CHALLENGE_TERMS = [
  "just a moment",
  "attention required",
  "access denied",
  "verify you are human",
  "cf-ray",
  "cloudflare",
  "/cdn-cgi/",
  "security check",
];

const TRACKING_HOST_PATTERNS = [
  /(^|\.)el\.aom\.org$/i,
  /(^|\.)el\.wiley\.com$/i,
  /(^|\.)click\.skem1\.com$/i,
  /(^|\.)lnk\.springernature\.com$/i,
  /(^|\.)links\.springernature\.com$/i,
  /(^|\.)link\.mail\.elsevier\.com$/i,
  /(^|\.)click\.notification\.elsevier\.com$/i,
];

const AOM_DOI_JOURNAL_BY_PREFIX = {
  amj: "Academy of Management Journal",
  amd: "Academy of Management Discoveries",
  amr: "Academy of Management Review",
  amp: "Academy of Management Perspectives",
  annals: "Academy of Management Annals",
  amle: "Academy of Management Learning & Education",
};

const INCLUDE_ARTICLE_TYPES = new Set(["research-article", "research-paper", "editorial"]);
const EXCLUDE_ARTICLE_TYPES = new Set([
  "book-review",
  "media-review",
  "discussion",
  "commentary",
  "corrigendum",
  "erratum",
  "retraction",
  "interview",
  "call-for-papers",
  "announcement",
  "news",
]);

const POLICY_ARTICLE_TYPE_OVERRIDES = {
  springer_link: [
    { pattern: /\boriginal\s*paper\b/i, normalized: "research-article" },
    { pattern: /\boriginal\s*article\b/i, normalized: "research-article" },
  ],
};

function usage() {
  const script = path.basename(process.argv[1] || "verify_publisher_record.mjs");
  console.error(`Usage:
  ${script} --url <url> [--url <url> ...] [options]
  ${script} --input <records.json> [options]

Options:
  --url <url>                  Single URL input (repeatable)
  --input <json-file>          JSON file with URLs/records
  --output <json-file>         Write JSON output to file
  --concurrency <n>            Concurrent workers (default: ${DEFAULT_CONCURRENCY})
  --max-retries <n>            Retries per URL (default: ${DEFAULT_RETRIES})
  --timeout-ms <n>             Timeout per page load (default: ${DEFAULT_TIMEOUT_MS})
  --challenge-wait-ms <n>      Max wait for anti-bot challenge to clear (default: ${DEFAULT_CHALLENGE_WAIT_MS})
  --challenge-poll-ms <n>      Poll interval while waiting for challenge clear (default: ${DEFAULT_CHALLENGE_POLL_MS})
  --tracking-timeout-ms <n>    Timeout for tracked-link resolution (default: ${DEFAULT_TRACKING_TIMEOUT_MS})
  --sciencedirect-mode <mode>  ScienceDirect strategy: auto|curl|browser (default: ${DEFAULT_SCIENCEDIRECT_MODE})
  --skip-tracking-resolution    Do not pre-resolve tracked links (e.g. click.skem1.com)
  --cdp-url <url>              Connect to existing Chrome via CDP
  --channel <name>             Launch channel when not using CDP (default: chrome)
  --headless                   Launch headless browser
  --verbose                    Verbose stderr logs
`);
}

function parseArgs(argv) {
  const args = {
    urls: [],
    input: null,
    output: null,
    concurrency: DEFAULT_CONCURRENCY,
    maxRetries: DEFAULT_RETRIES,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    challengeWaitMs: DEFAULT_CHALLENGE_WAIT_MS,
    challengePollMs: DEFAULT_CHALLENGE_POLL_MS,
    trackingTimeoutMs: DEFAULT_TRACKING_TIMEOUT_MS,
    sciencedirectMode: DEFAULT_SCIENCEDIRECT_MODE,
    skipTrackingResolution: false,
    cdpUrl: null,
    channel: "chrome",
    headless: false,
    verbose: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    switch (arg) {
      case "--url":
        if (!next) throw new Error("--url requires a value");
        args.urls.push(next);
        i += 1;
        break;
      case "--input":
        if (!next) throw new Error("--input requires a value");
        args.input = next;
        i += 1;
        break;
      case "--output":
        if (!next) throw new Error("--output requires a value");
        args.output = next;
        i += 1;
        break;
      case "--concurrency":
        if (!next) throw new Error("--concurrency requires a value");
        args.concurrency = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--max-retries":
        if (!next) throw new Error("--max-retries requires a value");
        args.maxRetries = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--timeout-ms":
        if (!next) throw new Error("--timeout-ms requires a value");
        args.timeoutMs = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--challenge-wait-ms":
        if (!next) throw new Error("--challenge-wait-ms requires a value");
        args.challengeWaitMs = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--challenge-poll-ms":
        if (!next) throw new Error("--challenge-poll-ms requires a value");
        args.challengePollMs = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--tracking-timeout-ms":
        if (!next) throw new Error("--tracking-timeout-ms requires a value");
        args.trackingTimeoutMs = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--sciencedirect-mode":
        if (!next) throw new Error("--sciencedirect-mode requires a value");
        args.sciencedirectMode = String(next).toLowerCase();
        i += 1;
        break;
      case "--skip-tracking-resolution":
        args.skipTrackingResolution = true;
        break;
      case "--cdp-url":
        if (!next) throw new Error("--cdp-url requires a value");
        args.cdpUrl = next;
        i += 1;
        break;
      case "--channel":
        if (!next) throw new Error("--channel requires a value");
        args.channel = next;
        i += 1;
        break;
      case "--headless":
        args.headless = true;
        break;
      case "--verbose":
        args.verbose = true;
        break;
      case "-h":
      case "--help":
        usage();
        process.exit(0);
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (!args.urls.length && !args.input) {
    throw new Error("Provide at least one --url or --input");
  }
  if (!Number.isFinite(args.concurrency) || args.concurrency < 1) {
    throw new Error("--concurrency must be >= 1");
  }
  if (!Number.isFinite(args.maxRetries) || args.maxRetries < 1) {
    throw new Error("--max-retries must be >= 1");
  }
  if (!Number.isFinite(args.timeoutMs) || args.timeoutMs < 1000) {
    throw new Error("--timeout-ms must be >= 1000");
  }
  if (!Number.isFinite(args.challengeWaitMs) || args.challengeWaitMs < 0) {
    throw new Error("--challenge-wait-ms must be >= 0");
  }
  if (!Number.isFinite(args.challengePollMs) || args.challengePollMs < 250) {
    throw new Error("--challenge-poll-ms must be >= 250");
  }
  if (!Number.isFinite(args.trackingTimeoutMs) || args.trackingTimeoutMs < 1000) {
    throw new Error("--tracking-timeout-ms must be >= 1000");
  }
  if (!["auto", "curl", "browser"].includes(args.sciencedirectMode)) {
    throw new Error("--sciencedirect-mode must be one of: auto, curl, browser");
  }
  return args;
}

function log(message, enabled) {
  if (enabled) {
    process.stderr.write(`[verify_publisher_record] ${message}\n`);
  }
}

function cleanText(value) {
  if (!value) return "";
  return String(value).replace(/\s+/g, " ").trim();
}

function pickFirst(...values) {
  for (const v of values) {
    const cleaned = cleanText(v);
    if (cleaned) return cleaned;
  }
  return "";
}

function isNotVerifiedValue(value) {
  const cleaned = cleanText(value);
  return !cleaned || cleaned.toLowerCase() === "[not verified]";
}

function normalizeDoi(doiOrUrl) {
  if (!doiOrUrl) return "";
  let raw = decodeURIComponent(String(doiOrUrl).trim());
  raw = raw.replace(/^https?:\/\/(?:dx\.)?doi\.org\//i, "");
  raw = raw.replace(/^doi:\s*/i, "");
  const match = raw.match(/10\.\d{4,9}\/[-._;()/:A-Z0-9]+/i);
  if (!match) return "";
  const doi = match[0].replace(/[)\],.;\s]+$/g, "");
  return `https://doi.org/${doi.toLowerCase()}`;
}

function extractDoiFromText(value) {
  if (!value) return "";
  const match = String(value).match(/10\.\d{4,9}\/[-._;()/:A-Z0-9]+/i);
  return match ? `https://doi.org/${match[0].replace(/[)\],.;\s]+$/g, "").toLowerCase()}` : "";
}

function policyForUrl(rawUrl) {
  let host = "";
  try {
    host = new URL(rawUrl).hostname;
  } catch {
    host = "";
  }
  return DOMAIN_POLICIES.find((item) => item.hostPattern.test(host)) || DOMAIN_POLICIES.at(-1);
}

function hostForUrl(rawUrl) {
  try {
    return new URL(rawUrl).hostname || "";
  } catch {
    return "";
  }
}

function isTrackingUrl(rawUrl) {
  const host = hostForUrl(rawUrl);
  return TRACKING_HOST_PATTERNS.some((pattern) => pattern.test(host));
}

function maybeCanonicalArticleUrl(rawUrl) {
  try {
    const u = new URL(rawUrl);
    if (u.pathname.includes("/advance-article/doi/") || u.pathname.includes("/article/doi/")) {
      return `${u.origin}${u.pathname}`;
    }
    return rawUrl;
  } catch {
    return rawUrl;
  }
}

function aomJournalFromDoi(doiUrl) {
  const doi = normalizeDoi(doiUrl);
  const match = doi.match(/10\.5465\/([a-z]+)\./i);
  if (!match) return "";
  return AOM_DOI_JOURNAL_BY_PREFIX[String(match[1]).toLowerCase()] || "";
}

function inferJournalFallback({ journal, pageTitle, finalUrl, doiUrl }) {
  const direct = cleanText(journal);
  if (direct) return direct;

  const fromDoi = aomJournalFromDoi(doiUrl);
  if (fromDoi) return fromDoi;

  const title = cleanText(pageTitle);
  if (title.includes("|")) {
    const maybeJournal = cleanText(title.split("|").slice(1).join("|")).replace(/\s+In-Press$/i, "");
    if (maybeJournal) return maybeJournal;
  }

  if (/(^|\.)journals\.aom\.org$/i.test(hostForUrl(finalUrl || ""))) {
    return "Academy of Management [Not verified]";
  }
  return "";
}

function classifyUnsafeAlertManagementLink(rawUrl, linkText = "") {
  const urlLower = String(rawUrl || "").toLowerCase();
  const textLower = cleanText(linkText).toLowerCase();
  if (!urlLower && !textLower) return null;

  const hrefMatches = (patterns) => patterns.some((pattern) => pattern.test(urlLower));
  const textMatches = (patterns) => patterns.some((pattern) => pattern.test(textLower));

  if (
    hrefMatches([/unsubscribe/i, /removealert/i]) ||
    textMatches([/\bunsubscribe\b/i])
  ) {
    return "alert_unsubscribe_link";
  }

  if (
    hrefMatches([
      /manage[-_/?=&%]*alerts?/i,
      /alert[-_/?=&%]*preferences?/i,
      /email[-_/?=&%]*(notification[-_/?=&%]*)?preferences?/i,
      /notification[-_/?=&%]*preferences?/i,
    ]) ||
    textMatches([
      /\bmanage\s+(my\s+)?alerts?\b/i,
      /\b(alert|email|notification)\s+preferences?\b/i,
      /\bmanage\s+preferences?\b/i,
    ])
  ) {
    return "alert_management_preferences_link";
  }

  return null;
}

function classifyUnsupportedUrlScheme(rawUrl) {
  const value = cleanText(rawUrl);
  if (!value) return null;
  if (/^\/\//.test(value)) return null;
  try {
    const u = new URL(value);
    const scheme = String(u.protocol || "").toLowerCase();
    if (!scheme || scheme === "http:" || scheme === "https:" || scheme === "file:") return null;
    return "unsupported_url_scheme";
  } catch {
    return null;
  }
}

function classifyUnsafePreNavigationLink(rawUrl, linkText = "") {
  return classifyUnsafeAlertManagementLink(rawUrl, linkText) || classifyUnsupportedUrlScheme(rawUrl);
}

function classifyKnownNonArticleLink(rawUrl) {
  try {
    const u = new URL(rawUrl);
    const host = u.hostname.toLowerCase();
    const pathName = u.pathname || "/";

    if (/(^|\.)journals\.aom\.org$/.test(host)) {
      if (/^\/doi\//i.test(pathName)) {
        return null;
      }
      if (/^\/action\/removealert/i.test(pathName)) {
        return "aom_alert_management_link";
      }
      if (/^\/action\/showlogin/i.test(pathName) || /^\/s\/login/i.test(pathName)) {
        return "aom_login_link";
      }
      if (/^\/action\//i.test(pathName)) {
        return "aom_non_article_action_link";
      }
    }

    if (/(^|\.)myaccount\.aom\.org$/.test(host)) return "aom_account_link";
    if (/(^|\.)aom\.org$/.test(host) && !/(^|\.)journals\.aom\.org$/.test(host)) {
      return "aom_marketing_or_policy_link";
    }
    if (/(^|\.)onlinelibrary\.wiley\.com$/.test(host)) {
      if (/^\/doi\//i.test(pathName)) {
        return null;
      }
      if (/^\/action\/removealert/i.test(pathName)) {
        return "wiley_alert_management_link";
      }
      if (/^\/action\//i.test(pathName)) {
        return "wiley_non_article_action_link";
      }
      if (/^\/toc\//i.test(pathName)) {
        return "wiley_toc_or_issue_link";
      }
      if (/^\/journal\//i.test(pathName)) {
        return "wiley_journal_home_link";
      }
      if (pathName === "/" || pathName === "") {
        return "wiley_marketing_or_home_link";
      }
    }
    if (/(^|\.)link\.springer\.com$/.test(host)) {
      if (/^\/article\//i.test(pathName)) {
        return null;
      }
      if (/^\/content\/pdf\//i.test(pathName) || /^\/content\/html\//i.test(pathName)) {
        return "springer_article_asset_link";
      }
      if (/^\/journal\//i.test(pathName)) {
        if (/\/volumes-and-issues(\/|$)/i.test(pathName)) {
          return "springer_toc_or_issue_link";
        }
        return "springer_journal_home_link";
      }
      if (/^\/search/i.test(pathName) || /^\/collections\//i.test(pathName)) {
        return "springer_navigation_or_collection_link";
      }
      if (/^\/account\//i.test(pathName) || /^\/myaccount\//i.test(pathName)) {
        return "springer_account_or_preferences_link";
      }
      if (u.searchParams.has("error") || u.searchParams.has("code")) {
        return "springer_email_webview_link";
      }
      if (pathName === "/" || pathName === "") {
        return "springer_marketing_or_home_link";
      }
    }
    if (/(^|\.)mail\.google\.com$/.test(host)) {
      if (u.searchParams.get("view") === "lg" && u.searchParams.has("permmsgid")) {
        return "gmail_message_webview_link";
      }
    }
    if (/(^|\.)atypon\.com$/.test(host)) return "technology_partner_link";
    return null;
  } catch {
    return null;
  }
}

function normalizeFinalUrlForRunDedupe(rawUrl) {
  try {
    const u = new URL(maybeCanonicalArticleUrl(rawUrl));
    u.hash = "";
    const dropParams = [];
    for (const [key] of u.searchParams.entries()) {
      if (
        /^utm_/i.test(key) ||
        /^mc_/i.test(key) ||
        /^campaign$/i.test(key) ||
        /^source$/i.test(key)
      ) {
        dropParams.push(key);
      }
    }
    for (const key of dropParams) {
      u.searchParams.delete(key);
    }
    const query = u.searchParams.toString();
    return `${u.origin}${u.pathname}${query ? `?${query}` : ""}`;
  } catch {
    return "";
  }
}

function shouldSkipDuplicateFinalUrl(seenFinalUrls, rawUrl, inputUrl) {
  if (!seenFinalUrls) return false;
  const key = normalizeFinalUrlForRunDedupe(rawUrl);
  if (!key) return false;
  if (isTrackingUrl(rawUrl)) return false;
  if (normalizeFinalUrlForRunDedupe(inputUrl) === key) return false;
  return seenFinalUrls.has(key);
}

function rememberFinalUrl(seenFinalUrls, rawUrl) {
  if (!seenFinalUrls) return;
  const key = normalizeFinalUrlForRunDedupe(rawUrl);
  if (!key) return;
  seenFinalUrls.add(key);
}

async function resolveTrackedUrl(inputUrl, args) {
  if (args.skipTrackingResolution || !isTrackingUrl(inputUrl)) {
    return {
      inputUrl,
      resolvedUrl: inputUrl,
      usedTrackingResolution: false,
      trackingResolutionError: "",
    };
  }

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), args.trackingTimeoutMs);
    const response = await fetch(inputUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      },
    });
    clearTimeout(timer);
    const resolvedUrl = maybeCanonicalArticleUrl(response.url || inputUrl);
    const resolvedHost = hostForUrl(resolvedUrl);
    let resolvedPath = "";
    try {
      resolvedPath = new URL(resolvedUrl).pathname || "";
    } catch {
      resolvedPath = "";
    }
    if (
      /(^|\.)el\.wiley\.com$/i.test(hostForUrl(inputUrl)) &&
      /(^|\.)onlinelibrary\.wiley\.com$/i.test(resolvedHost) &&
      /^\/action\/cookieAbsent$/i.test(resolvedPath)
    ) {
      return {
        inputUrl,
        resolvedUrl: inputUrl,
        usedTrackingResolution: true,
        trackingStatus: response.status,
        trackingResolutionError:
          "Tracker pre-resolution landed on Wiley cookieAbsent endpoint; ignoring and falling back to browser navigation.",
      };
    }
    return {
      inputUrl,
      resolvedUrl,
      usedTrackingResolution: true,
      trackingStatus: response.status,
      trackingResolutionError: "",
    };
  } catch (error) {
    return {
      inputUrl,
      resolvedUrl: inputUrl,
      usedTrackingResolution: true,
      trackingResolutionError: String(error?.message || error),
    };
  }
}

function parseYear(value) {
  const m = String(value || "").match(/\b(19|20)\d{2}\b/);
  return m ? m[0] : "";
}

function stripAcademicHonorifics(name) {
  return cleanText(name)
    .replace(/^(dr|professor|prof|mr|mrs|ms)\.?\s+/i, "")
    .replace(/\s*,?\s*(ph\.?\s*d\.?|m\.?\s*d\.?|dba|mba|jd)\s*$/i, "")
    .trim();
}

function formatApaAuthor(name) {
  const cleaned = stripAcademicHonorifics(name);
  if (!cleaned) return "";
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length === 1) return parts[0];
  const surname = parts.at(-1);
  const given = parts.slice(0, -1);
  const initials = given
    .map((part) => part.replace(/[^A-Za-z-]/g, ""))
    .filter(Boolean)
    .map((part) => {
      if (part.includes("-")) {
        return part
          .split("-")
          .filter(Boolean)
          .map((seg) => `${seg[0].toUpperCase()}.`)
          .join("-");
      }
      return `${part[0].toUpperCase()}.`;
    })
    .join(" ");
  return initials ? `${surname}, ${initials}` : surname;
}

function formatApaAuthors(authors) {
  const deduped = [];
  const seen = new Set();
  for (const name of authors || []) {
    const formatted = formatApaAuthor(name);
    if (!formatted) continue;
    const key = formatted.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(formatted);
  }
  if (!deduped.length) return "";
  if (deduped.length === 1) return deduped[0];
  if (deduped.length === 2) return `${deduped[0]}, & ${deduped[1]}`;
  return `${deduped.slice(0, -1).join(", ")}, & ${deduped.at(-1)}`;
}

function buildApaCitation(record) {
  const authors = formatApaAuthors(record.authors || []);
  const year = cleanText(record.year);
  const title = cleanText(record.title);
  const journal = cleanText(record.journal);
  const volume = cleanText(record.volume);
  const issue = cleanText(record.issue);
  const pageRange = cleanText(record.pageRange);
  const doiUrl = cleanText(record.doiUrl);

  if ([authors, year, title, journal, doiUrl].some((field) => isNotVerifiedValue(field))) {
    return "[Not verified]";
  }

  const hasVolume = !isNotVerifiedValue(volume);
  const hasIssue = !isNotVerifiedValue(issue);
  const hasPageRange = !isNotVerifiedValue(pageRange);
  const hasPublishedOnline = !isNotVerifiedValue(record.publishedOnline);
  if (!hasVolume && !hasIssue && !hasPageRange && hasPublishedOnline) {
    return `${authors} (${year}). ${title}. ${journal}. Advance online publication. ${doiUrl}`;
  }
  const volumeIssue = hasVolume ? (hasIssue ? `${volume}(${issue})` : volume) : "";
  const publicationTailParts = [];
  if (volumeIssue) publicationTailParts.push(volumeIssue);
  if (hasPageRange) publicationTailParts.push(pageRange);
  const publicationTail = publicationTailParts.join(", ");
  if (!publicationTail) {
    return "[Not verified]";
  }
  if (publicationTail) {
    return `${authors} (${year}). ${title}. ${journal}, ${publicationTail}. ${doiUrl}`;
  }
  return `${authors} (${year}). ${title}. ${journal}. ${doiUrl}`;
}

function ensureField(value) {
  const cleaned = cleanText(value);
  return cleaned || "[Not verified]";
}

function requiredMissing(record) {
  const required = ["title", "doiUrl", "journal", "year", "abstract", "citation"];
  return required.filter((key) => isNotVerifiedValue(record[key]));
}

function normalizeArticleTypeValue(value, { policyName = "", allowPolicyOverrides = false } = {}) {
  const text = cleanText(value).toLowerCase();
  if (!text) return { normalized: "", matchedHint: "" };

  if (allowPolicyOverrides) {
    const overrides = POLICY_ARTICLE_TYPE_OVERRIDES[String(policyName || "").toLowerCase()] || [];
    for (const override of overrides) {
      if (override.pattern.test(text)) {
        return {
          normalized: override.normalized,
          matchedHint: cleanText(value),
        };
      }
    }
  }

  if (/\beditorial\s*(board|data)\b/i.test(text)) return { normalized: "announcement", matchedHint: cleanText(value) };
  if (/(book\s*review|media\s*review)/i.test(text)) return { normalized: "book-review", matchedHint: cleanText(value) };
  if (/\b(editorial|from the editors?)\b/i.test(text)) return { normalized: "editorial", matchedHint: cleanText(value) };
  if (/\bdiscussion\b/i.test(text)) return { normalized: "discussion", matchedHint: cleanText(value) };
  if (/\b(commentary|perspective|opinion)\b/i.test(text)) return { normalized: "commentary", matchedHint: cleanText(value) };
  if (/\b(corrigendum|corrigenda)\b/i.test(text)) return { normalized: "corrigendum", matchedHint: cleanText(value) };
  if (/\b(erratum|errata)\b/i.test(text)) return { normalized: "erratum", matchedHint: cleanText(value) };
  if (/\b(retraction|withdrawal)\b/i.test(text)) return { normalized: "retraction", matchedHint: cleanText(value) };
  if (/\binterview\b/i.test(text)) return { normalized: "interview", matchedHint: cleanText(value) };
  if (/\bcall\s*for\s*papers?\b/i.test(text)) return { normalized: "call-for-papers", matchedHint: cleanText(value) };
  if (/\b(announcement|announcements)\b/i.test(text)) return { normalized: "announcement", matchedHint: cleanText(value) };
  if (/\bnews\b/i.test(text)) return { normalized: "news", matchedHint: cleanText(value) };
  if (/\b(research\s*paper|research\s*article|original\s*article)\b/i.test(text)) {
    return { normalized: "research-article", matchedHint: cleanText(value) };
  }
  if (/\b(scholarlyarticle|journalarticle)\b/i.test(text)) {
    return { normalized: "research-article", matchedHint: cleanText(value) };
  }
  if (/\barticle\b/i.test(text)) return { normalized: "research-article", matchedHint: cleanText(value) };
  return { normalized: "", matchedHint: "" };
}

function classifyArticleType({ policyName = "", rawTypeHints = [], semanticHints = [] }) {
  const seen = new Set();
  const rawHints = [];
  for (const hint of rawTypeHints || []) {
    if (!hint) continue;
    const value = cleanText(hint.value ?? hint);
    if (!value) continue;
    const sourceLabel = cleanText(hint.sourceLabel || "publisher_raw_type") || "publisher_raw_type";
    const key = `${sourceLabel}\u0000${value}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rawHints.push({ value, sourceLabel });
  }

  const semanticHintList = [];
  for (const hint of semanticHints || []) {
    if (!hint) continue;
    const value = cleanText(hint.value ?? hint);
    if (!value) continue;
    const sourceLabel = cleanText(hint.sourceLabel || "title_heuristic") || "title_heuristic";
    const key = `${sourceLabel}\u0000${value}`;
    if (seen.has(key)) continue;
    seen.add(key);
    semanticHintList.push({ value, sourceLabel });
  }

  const classifyOne = (hint, allowPolicyOverrides) => {
    const normalizedResult = normalizeArticleTypeValue(hint.value, {
      policyName,
      allowPolicyOverrides,
    });
    const normalized = normalizedResult.normalized;
    if (!normalized) return null;
    if (EXCLUDE_ARTICLE_TYPES.has(normalized)) {
      return {
        articleType: normalized,
        ingestDecision: "exclude",
        ingestReason: `excluded_by_type:${normalized}`,
        articleTypeClassificationSource: hint.sourceLabel,
        articleTypeMatchedHint: normalizedResult.matchedHint || hint.value,
      };
    }
    if (INCLUDE_ARTICLE_TYPES.has(normalized)) {
      return {
        articleType: normalized,
        ingestDecision: "include",
        ingestReason: `included_by_type:${normalized}`,
        articleTypeClassificationSource: hint.sourceLabel,
        articleTypeMatchedHint: normalizedResult.matchedHint || hint.value,
      };
    }
    return null;
  };

  for (const hint of rawHints) {
    const classified = classifyOne(hint, true);
    if (classified) return classified;
  }

  for (const hint of semanticHintList) {
    const classified = classifyOne(hint, false);
    if (classified) return classified;
  }

  return {
    articleType: "[Not verified]",
    ingestDecision: "not_verified",
    ingestReason: rawHints.length ? "article_type_unmapped_raw_hint" : "article_type_unclear",
    articleTypeClassificationSource: "none",
    articleTypeMatchedHint: rawHints[0]?.value || "",
  };
}

function logArticleTypeDecision(record, sourceUrl, verbose) {
  if (!verbose || !record) return;
  log(
    [
      "ArticleType",
      `url=${sourceUrl || record.sourceUrl || ""}`,
      `raw=${JSON.stringify(record.articleTypeRaw || "")}`,
      `source=${record.articleTypeClassificationSource || "none"}`,
      `matched=${JSON.stringify(record.articleTypeMatchedHint || "")}`,
      `articleType=${record.articleType || ""}`,
      `decision=${record.ingestDecision || ""}`,
      `reason=${record.ingestReason || ""}`,
    ].join(" | "),
    verbose
  );
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function uniqueStrings(values) {
  const out = [];
  const seen = new Set();
  for (const value of values) {
    const cleaned = cleanText(value);
    if (!cleaned) continue;
    if (seen.has(cleaned)) continue;
    seen.add(cleaned);
    out.push(cleaned);
  }
  return out;
}

function isScienceDirectLikeUrl(rawUrl) {
  const host = hostForUrl(rawUrl);
  if (/(^|\.)sciencedirect\.com$/i.test(host)) return true;
  return /10\.1016\//i.test(String(rawUrl || ""));
}

function scienceDirectPiiFromUrl(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    const fromPath = parsed.pathname.match(/\/science\/article\/pii\/([A-Za-z0-9]+)/i);
    if (fromPath?.[1]) {
      return fromPath[1];
    }
    const fromQuery = parsed.searchParams.get("_piikey") || parsed.searchParams.get("pii");
    if (fromQuery) {
      return fromQuery.replace(/[^A-Za-z0-9]/g, "");
    }
  } catch {
    // ignore invalid URL
  }
  return "";
}

function canonicalScienceDirectArticleUrl(rawUrl) {
  const pii = scienceDirectPiiFromUrl(rawUrl);
  if (!pii) return rawUrl;
  return `https://www.sciencedirect.com/science/article/pii/${pii}`;
}

function decodeHtmlEntities(value) {
  const input = String(value || "");
  return input
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      const code = Number.parseInt(hex, 16);
      return Number.isFinite(code) ? String.fromCodePoint(code) : _;
    })
    .replace(/&#(\d+);/g, (_, num) => {
      const code = Number.parseInt(num, 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : _;
    });
}

function stripHtmlTags(value) {
  return String(value || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ");
}

function parseHtmlAttribute(tag, attrName) {
  const pattern = new RegExp(
    `${attrName}\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s\"'>]+))`,
    "i"
  );
  const match = tag.match(pattern);
  if (!match) return "";
  return match[1] ?? match[2] ?? match[3] ?? "";
}

function extractMetaMapFromHtml(html) {
  const map = new Map();
  const metaTagRegex = /<meta\b[^>]*>/gi;
  let match;
  while ((match = metaTagRegex.exec(html)) !== null) {
    const tag = match[0];
    const key = cleanText(
      decodeHtmlEntities(parseHtmlAttribute(tag, "name") || parseHtmlAttribute(tag, "property"))
    ).toLowerCase();
    const content = cleanText(decodeHtmlEntities(parseHtmlAttribute(tag, "content")));
    if (!key || !content) continue;
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key).push(content);
  }
  return map;
}

function pickMetaValues(metaMap, names) {
  const out = [];
  for (const name of names) {
    const values = metaMap.get(String(name).toLowerCase());
    if (values?.length) {
      out.push(...values);
    }
  }
  return uniqueStrings(out);
}

function pickMetaFirst(metaMap, names) {
  const values = pickMetaValues(metaMap, names);
  return values[0] || "";
}

function extractTagText(html, tagName) {
  const regex = new RegExp(`<${tagName}\\b[^>]*>([\\s\\S]*?)<\\/${tagName}>`, "i");
  const match = html.match(regex);
  if (!match?.[1]) return "";
  return cleanText(decodeHtmlEntities(stripHtmlTags(match[1])));
}

function extractJsonObjectAfterMarker(source, marker) {
  const markerIndex = source.indexOf(marker);
  if (markerIndex < 0) return null;
  const start = source.indexOf("{", markerIndex + marker.length);
  if (start < 0) return null;

  let depth = 0;
  let inString = false;
  let quote = "";
  let escaped = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === "\\") {
        escaped = true;
        continue;
      }
      if (ch === quote) {
        inString = false;
        quote = "";
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      inString = true;
      quote = ch;
      continue;
    }
    if (ch === "{") {
      depth += 1;
      continue;
    }
    if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return source.slice(start, i + 1);
      }
    }
  }
  return null;
}

function extractJsonLdNodes(html) {
  const nodes = [];
  const regex = /<script\b[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match;
  while ((match = regex.exec(html)) !== null) {
    const raw = cleanText(match[1]);
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        nodes.push(...parsed);
      } else {
        nodes.push(parsed);
      }
    } catch {
      // Ignore malformed JSON-LD blocks.
    }
  }
  return nodes.filter((node) => node && typeof node === "object");
}

function collectStringCandidatesByKey(node, keyRegex, output, seen = new Set(), depth = 0) {
  if (!node || depth > 20) return;
  if (typeof node !== "object") return;
  if (seen.has(node)) return;
  seen.add(node);

  if (Array.isArray(node)) {
    for (const item of node) {
      collectStringCandidatesByKey(item, keyRegex, output, seen, depth + 1);
    }
    return;
  }

  for (const [key, value] of Object.entries(node)) {
    if (typeof value === "string" && keyRegex.test(key)) {
      const cleaned = cleanText(decodeHtmlEntities(stripHtmlTags(value)));
      if (cleaned) {
        output.push(cleaned);
      }
    }
    if (value && typeof value === "object") {
      collectStringCandidatesByKey(value, keyRegex, output, seen, depth + 1);
    }
  }
}

function walkJsonNode(node, visit, seen = new Set(), depth = 0) {
  if (node === null || node === undefined || depth > 30) return;
  if (typeof node !== "object") return;
  if (seen.has(node)) return;
  seen.add(node);

  visit(node);
  if (Array.isArray(node)) {
    for (const item of node) {
      walkJsonNode(item, visit, seen, depth + 1);
    }
    return;
  }
  for (const value of Object.values(node)) {
    walkJsonNode(value, visit, seen, depth + 1);
  }
}

function collectUnderscoreTextValues(node) {
  const values = [];
  walkJsonNode(node, (entry) => {
    if (entry && typeof entry === "object" && typeof entry._ === "string") {
      const cleaned = cleanText(decodeHtmlEntities(stripHtmlTags(entry._)));
      if (cleaned) {
        values.push(cleaned);
      }
    }
  });
  return uniqueStrings(values);
}

function extractScienceDirectAuthorsFromPreloadedState(state) {
  const names = [];
  const authorRoots = [];
  if (state?.authors?.content) {
    authorRoots.push(state.authors.content);
  }
  if (state?.article?.["last-author"]) {
    authorRoots.push(state.article["last-author"]);
  }

  for (const root of authorRoots) {
    walkJsonNode(root, (entry) => {
      if (entry?.["#name"] !== "author" || !Array.isArray(entry?.$$)) return;
      let given = "";
      let surname = "";
      for (const child of entry.$$) {
        if (child?.["#name"] === "given-name") {
          given = cleanText(child._);
        }
        if (child?.["#name"] === "surname") {
          surname = cleanText(child._);
        }
      }
      const combined = cleanText([given, surname].filter(Boolean).join(" "));
      if (combined) {
        names.push(combined);
      }
    });
  }

  return uniqueStrings(names);
}

function normalizeAbstractText(value) {
  const cleaned = cleanText(decodeHtmlEntities(stripHtmlTags(value)));
  if (!cleaned) return "";
  return cleaned.replace(/^abstract[:\s-]*/i, "").trim();
}

function chooseBestAbstract(candidates) {
  const normalized = uniqueStrings(candidates.map((item) => normalizeAbstractText(item)));
  const filtered = normalized.filter((text) => text.length >= 80);
  if (!filtered.length) return normalized[0] || "";
  const scored = filtered
    .filter((text) => !/(all rights reserved|cookie|javascript|privacy policy)/i.test(text))
    .sort((a, b) => b.length - a.length);
  return scored[0] || filtered[0] || "";
}

function detectChallengeInHtml(html, url = "") {
  const title = extractTagText(html, "title");
  const body = cleanText(stripHtmlTags(html)).slice(0, 4000);
  const haystack = `${title}\n${body}\n${url}`.toLowerCase();
  const hits = CHALLENGE_TERMS.filter((term) => haystack.includes(term));
  return {
    isChallenge: hits.length > 0,
    signals: hits,
    title,
    url,
  };
}

async function fetchHtmlWithCurl(url, args) {
  const maxTimeSeconds = Math.max(5, Math.ceil(args.timeoutMs / 1000));
  const writeOutMarker = "\n__CODEX_EFFECTIVE_URL__:%{url_effective}\n__CODEX_HTTP_CODE__:%{http_code}\n";
  const curlArgs = [
    "-L",
    "--compressed",
    "--silent",
    "--show-error",
    "--max-time",
    String(maxTimeSeconds),
    "-A",
    CURL_USER_AGENT,
    "--write-out",
    writeOutMarker,
    url,
  ];

  return await new Promise((resolve, reject) => {
    execFile(
      "curl",
      curlArgs,
      { maxBuffer: CURL_MAX_BUFFER_BYTES },
      (error, stdout = "", stderr = "") => {
        if (error) {
          reject(new Error(`curl failed: ${error.message}${stderr ? ` | ${stderr}` : ""}`));
          return;
        }
        const payload = String(stdout);
        const markerMatch = payload.match(
          /__CODEX_EFFECTIVE_URL__:(.*)\n__CODEX_HTTP_CODE__:(\d{3})\s*$/s
        );
        if (!markerMatch) {
          resolve({
            html: payload,
            effectiveUrl: url,
            statusCode: null,
            stderr: String(stderr || ""),
          });
          return;
        }
        const markerText = markerMatch[0];
        const html = payload.slice(0, payload.length - markerText.length);
        resolve({
          html,
          effectiveUrl: cleanText(markerMatch[1]) || url,
          statusCode: Number.parseInt(markerMatch[2], 10),
          stderr: String(stderr || ""),
        });
      }
    );
  });
}

function extractMetadataFromScienceDirectHtml(html, sourceUrl, finalUrl) {
  const metaMap = extractMetaMapFromHtml(html);
  const jsonLdNodes = extractJsonLdNodes(html);
  const pageTitle = extractTagText(html, "title");

  const jsonLdPrimary = jsonLdNodes.find((node) => node?.["@type"]) || jsonLdNodes[0] || {};
  const jsonLdAuthorNames = (() => {
    const authorNode = jsonLdPrimary?.author;
    if (!authorNode) return [];
    const list = Array.isArray(authorNode) ? authorNode : [authorNode];
    return uniqueStrings(
      list.map((item) => {
        if (typeof item === "string") return item;
        if (item && typeof item === "object") {
          return item.name || "";
        }
        return "";
      })
    );
  })();

  const preloadedState = (() => {
    const rawJson = extractJsonObjectAfterMarker(html, "window.__PRELOADED_STATE__");
    if (!rawJson) return null;
    try {
      return JSON.parse(rawJson);
    } catch {
      return null;
    }
  })();
  const preloadedArticle = preloadedState?.article && typeof preloadedState.article === "object"
    ? preloadedState.article
    : {};
  const preloadedTitle = cleanText(
    preloadedArticle?.title?.content?.find?.((item) => item?.["#name"] === "title")?._ ||
      preloadedArticle?.titleString ||
      ""
  );
  const preloadedAuthors = extractScienceDirectAuthorsFromPreloadedState(preloadedState);
  const preloadedAbstractTexts = collectUnderscoreTextValues(preloadedState?.abstracts?.content || []);
  const preloadedJournal = cleanText(preloadedArticle?.srctitle || "");
  const preloadedVolume = cleanText(preloadedArticle?.["vol-first"] || "");
  const preloadedIssue = cleanText(preloadedArticle?.["iss-first"] || "");
  const preloadedPage = cleanText(
    preloadedArticle?.["first-fp"] ||
      preloadedArticle?.["article-number"] ||
      preloadedArticle?.pages?.[0]?.["first-page"] ||
      ""
  );
  const preloadedDate = cleanText(
    preloadedArticle?.["cover-date-start"] ||
      preloadedState?.dates?.["Publication date"] ||
      preloadedState?.dates?.["Available online"] ||
      ""
  );
  const preloadedDoi = cleanText(preloadedArticle?.doi || "");
  const preloadedType = cleanText(
    preloadedState?.documentTypeLabel || preloadedArticle?.["documentTypeLabel"] || ""
  );

  const abstractCandidates = [];
  abstractCandidates.push(
    ...pickMetaValues(metaMap, [
      "citation_abstract",
      "dc.description",
      "description",
      "og:description",
    ])
  );
  if (jsonLdPrimary?.abstract) {
    abstractCandidates.push(jsonLdPrimary.abstract);
  }
  if (preloadedState) {
    collectStringCandidatesByKey(preloadedState, /abstract/i, abstractCandidates);
    abstractCandidates.push(...preloadedAbstractTexts);
  }

  const title =
    pickMetaFirst(metaMap, ["citation_title", "dc.title", "og:title", "twitter:title"]) ||
    preloadedTitle ||
    cleanText(jsonLdPrimary?.headline || jsonLdPrimary?.name) ||
    pageTitle;
  const authorsFromMeta = pickMetaValues(metaMap, ["citation_author", "dc.creator"]);
  const authors =
    authorsFromMeta.length > 0
      ? authorsFromMeta
      : preloadedAuthors.length > 0
        ? preloadedAuthors
        : jsonLdAuthorNames;
  const journal =
    pickMetaFirst(metaMap, ["citation_journal_title", "prism.publicationname", "dc.source"]) ||
    preloadedJournal ||
    cleanText(jsonLdPrimary?.isPartOf?.name || "");
  const publicationDate =
    pickMetaFirst(metaMap, [
      "citation_publication_date",
      "citation_online_date",
      "citation_date",
      "dc.date",
      "prism.publicationdate",
      "article:published_time",
    ]) ||
    preloadedDate ||
    cleanText(jsonLdPrimary?.datePublished || jsonLdPrimary?.dateCreated || "");
  const volume = pickMetaFirst(metaMap, ["citation_volume", "prism.volume"]) || preloadedVolume;
  const issue = pickMetaFirst(metaMap, ["citation_issue", "prism.number"]) || preloadedIssue;
  const firstPage = pickMetaFirst(metaMap, ["citation_firstpage", "prism.startingpage"]);
  const lastPage = pickMetaFirst(metaMap, ["citation_lastpage", "prism.endingpage"]);
  const pageRange = firstPage && lastPage ? `${firstPage}-${lastPage}` : firstPage || preloadedPage;
  const doiMeta =
    pickMetaFirst(metaMap, ["citation_doi", "prism.doi", "dc.identifier"]) ||
    preloadedDoi ||
    cleanText(jsonLdPrimary?.identifier || "");
  const articleTypeHint =
    pickMetaFirst(metaMap, [
      "citation_article_type",
      "dc.type",
      "prism.section",
      "prism.contenttype",
      "article:section",
      "og:type",
    ]) ||
    preloadedType ||
    cleanText(jsonLdPrimary?.["@type"] || "");
  const ogUrl = pickMetaFirst(metaMap, ["og:url"]);
  const abstractText = chooseBestAbstract(abstractCandidates);

  return {
    sourceUrl,
    finalUrl: ogUrl || finalUrl || sourceUrl,
    title,
    authors,
    journal,
    publicationDate,
    volume,
    issue,
    firstPage,
    lastPage,
    pageRange,
    doiMeta,
    abstractText,
    articleTypeHint,
    pageTitle,
  };
}

async function readInputUrls(inputPath) {
  const raw = await fs.readFile(inputPath, "utf-8");
  const parsed = JSON.parse(raw);

  const urls = [];
  const blockedByGuard = new Set();
  const pushUrl = (value) => {
    const url = cleanText(value);
    if (!url) return;
    const unsafeReason = classifyUnsafePreNavigationLink(url);
    if (unsafeReason) {
      blockedByGuard.add(`${url}::${unsafeReason}`);
      return;
    }
    urls.push(url);
  };
  const pushLinkDetail = (entry) => {
    if (!entry || typeof entry !== "object") return;
    const href = cleanText(entry.href || entry.url || "");
    const text = cleanText(entry.text || entry.anchorText || "");
    if (!href) return;
    const unsafeReason = classifyUnsafePreNavigationLink(href, text);
    if (unsafeReason) {
      blockedByGuard.add(`${href}::${unsafeReason}`);
      return;
    }
    urls.push(href);
  };

  const ingest = (node) => {
    if (!node) return;
    if (typeof node === "string") {
      pushUrl(node);
      return;
    }
    if (Array.isArray(node)) {
      for (const item of node) ingest(item);
      return;
    }
    if (typeof node === "object") {
      if (Array.isArray(node.candidates)) {
        // find_gmail_message.py writes links under candidates[*]; ingest them directly.
        ingest(node.candidates);
      }
      const hasEmbeddedLinks =
        Array.isArray(node.link_details) || Array.isArray(node.links) || Array.isArray(node.urls);
      let consumedLinkDetails = false;
      if (Array.isArray(node.link_details)) {
        consumedLinkDetails = true;
        for (const entry of node.link_details) pushLinkDetail(entry);
      }
      if (!hasEmbeddedLinks) {
        pushUrl(node.url);
      }
      pushUrl(node.href);
      pushUrl(node.sourceUrl);
      pushUrl(node.doiUrl);
      if (Array.isArray(node.urls)) {
        for (const u of node.urls) pushUrl(u);
      }
      if (!consumedLinkDetails && Array.isArray(node.links)) {
        for (const u of node.links) pushUrl(u);
      }
      if (Array.isArray(node.records)) ingest(node.records);
      if (Array.isArray(node.results)) ingest(node.results);
    }
  };

  ingest(parsed);
  return uniqueStrings(urls);
}

async function detectChallenge(page) {
  const probe = await page.evaluate(() => {
    const title = document.title || "";
    const body = (document.body?.innerText || "").slice(0, 4000);
    const url = window.location.href || "";
    return { title, body, url };
  });
  const haystack = `${probe.title}\n${probe.body}\n${probe.url}`.toLowerCase();
  const hits = CHALLENGE_TERMS.filter((term) => haystack.includes(term));
  return {
    isChallenge: hits.length > 0,
    signals: hits,
    title: probe.title,
    url: probe.url,
  };
}

async function waitForChallengeClear(page, args, verbose) {
  const started = Date.now();
  let probe = await detectChallenge(page);
  while (probe.isChallenge && Date.now() - started < args.challengeWaitMs) {
    await page.waitForTimeout(args.challengePollMs);
    probe = await detectChallenge(page);
    log(
      `Challenge still present (${Math.round((Date.now() - started) / 1000)}s): ${probe.signals.join(", ")}`,
      verbose
    );
  }
  return {
    ...probe,
    waitedMs: Date.now() - started,
  };
}

function buildRecordFromExtracted(extracted, sourceUrl, policyName = "") {
  const doiUrl =
    normalizeDoi(extracted?.doiMeta) ||
    normalizeDoi(extractDoiFromText(extracted?.finalUrl || "")) ||
    normalizeDoi(extractDoiFromText(sourceUrl));
  const normalizedTitle = cleanText(extracted?.title).replace(
    /\s+\|\s+Academy of Management .*$/i,
    ""
  );
  const inferredJournal = inferJournalFallback({
    journal: extracted?.journal,
    pageTitle: extracted?.pageTitle,
    finalUrl: extracted?.finalUrl,
    doiUrl,
  });
  const year = parseYear(extracted?.publicationDate || "");
  const normalized = {
    sourceUrl: sourceUrl,
    finalUrl: extracted?.finalUrl || sourceUrl,
    title: ensureField(normalizedTitle),
    authors: uniqueStrings(extracted?.authors || []),
    year: ensureField(year),
    journal: ensureField(inferredJournal || extracted?.journal),
    volume: ensureField(extracted?.volume),
    issue: ensureField(extracted?.issue),
    pageRange: ensureField(extracted?.pageRange),
    publishedOnline: ensureField(extracted?.publicationDate),
    doiUrl: ensureField(doiUrl),
    abstract: ensureField(extracted?.abstractText),
    citation: "[Not verified]",
    articleTypeRaw: ensureField(extracted?.articleTypeHint),
    articleType: "[Not verified]",
    ingestDecision: "not_verified",
    ingestReason: "article_type_unclear",
    articleTypeClassificationSource: "none",
    articleTypeMatchedHint: "",
  };
  const classification = classifyArticleType({
    policyName,
    rawTypeHints: [
      { value: extracted?.articleTypeMetaHint, sourceLabel: "publisher_raw_type" },
      { value: extracted?.articleTypeDomHint, sourceLabel: "publisher_dom_type" },
      { value: extracted?.articleTypeJsonLdHint, sourceLabel: "publisher_jsonld_type" },
    ],
    semanticHints: [
      { value: extracted?.title, sourceLabel: "title_heuristic" },
      { value: extracted?.pageTitle, sourceLabel: "page_title_heuristic" },
    ],
  });
  normalized.articleType = classification.articleType;
  normalized.ingestDecision = classification.ingestDecision;
  normalized.ingestReason = classification.ingestReason;
  normalized.articleTypeClassificationSource = classification.articleTypeClassificationSource || "none";
  normalized.articleTypeMatchedHint = classification.articleTypeMatchedHint || "";
  normalized.citation = buildApaCitation(normalized);
  normalized.missingFields = requiredMissing(normalized);
  normalized.status = normalized.missingFields.length ? "not_verified" : "verified";
  return normalized;
}

async function extractMetadata(page, sourceUrl, policy) {
  const selectors = policy.abstractSelectors || [];
  const extracted = await page.evaluate((abstractSelectors) => {
    const metaIndex = (() => {
      const index = new Map();
      const push = (key, value) => {
        const normalizedKey = String(key || "").trim().toLowerCase();
        const normalizedValue = String(value || "").replace(/\s+/g, " ").trim();
        if (!normalizedKey || !normalizedValue) return;
        const existing = index.get(normalizedKey) || [];
        existing.push(normalizedValue);
        index.set(normalizedKey, existing);
      };
      for (const node of document.querySelectorAll("meta[name], meta[property]")) {
        push(node.getAttribute("name"), node.getAttribute("content"));
        push(node.getAttribute("property"), node.getAttribute("content"));
      }
      return index;
    })();

    const readMetaValues = (names) => {
      const values = [];
      for (const name of names) {
        const fromIndex = metaIndex.get(String(name || "").toLowerCase()) || [];
        for (const value of fromIndex) {
          if (value) values.push(value);
        }
      }
      return values;
    };

    const pickText = (selectorList) => {
      for (const selector of selectorList) {
        const node = document.querySelector(selector);
        const text = (node?.textContent || "").replace(/\s+/g, " ").trim();
        if (text) return text;
      }
      return "";
    };

    const title = (() => {
      const metaTitle = readMetaValues([
        "citation_title",
        "dc.title",
        "og:title",
        "twitter:title",
      ])[0];
      if (metaTitle) return metaTitle;
      return (
        pickText(["h1.citation__title", "h1.article-title", "h1"]) ||
        (document.title || "").replace(/\s+/g, " ").trim()
      );
    })();

    const authors = (() => {
      const byMeta = readMetaValues(["citation_author", "dc.creator"]);
      if (byMeta.length) return byMeta;
      const byDom = Array.from(
        document.querySelectorAll(
          'a[rel="author"], .author-name, .loa__author-name, .article-header__authors li'
        )
      )
        .map((node) => (node.textContent || "").replace(/\s+/g, " ").trim())
        .filter(Boolean);
      return byDom;
    })();

    const journal = (() => {
      const fromMeta = readMetaValues([
        "citation_journal_title",
        "prism.publicationName",
        "dc.source",
      ])[0];
      if (fromMeta) return fromMeta;
      const fromAtyponBreadcrumb = Array.from(
        document.querySelectorAll(".article__breadcrumbs a, .article__breadcrumbs span")
      )
        .map((node) => (node.textContent || "").replace(/\s+/g, " ").trim())
        .map((value) => value.replace(/\s+In-Press$/i, ""))
        .find((value) => /academy of management/i.test(value));
      if (fromAtyponBreadcrumb) return fromAtyponBreadcrumb;
      const fromTitleSuffix = (document.title || "")
        .split("|")
        .slice(1)
        .join("|")
        .replace(/\s+/g, " ")
        .replace(/\s+In-Press$/i, "")
        .trim();
      if (fromTitleSuffix) return fromTitleSuffix;
      return pickText([
        ".publication-title",
        ".journal-title",
        '[data-test="journal-title"]',
      ]);
    })();

    let pubDate = readMetaValues([
      "citation_publication_date",
      "citation_online_date",
      "citation_date",
      "dc.date",
      "prism.publicationDate",
      "article:published_time",
    ])[0];
    if (!pubDate) {
      pubDate = pickText([".epub-section__date", ".publication-date", ".article-header__date"]);
    }
    let jsonLdType = "";
    const jsonLdNodes = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
    for (const node of jsonLdNodes) {
      const raw = (node.textContent || "").trim();
      if (!raw) continue;
      try {
        const parsed = JSON.parse(raw);
        const candidates = Array.isArray(parsed) ? parsed : [parsed];
        for (const candidate of candidates) {
          if (!candidate || typeof candidate !== "object") continue;
          const typeValue = String(candidate["@type"] || "").trim();
          if (!jsonLdType && typeValue) {
            jsonLdType = typeValue;
          }
          if (!pubDate) {
            const dateValue =
              candidate.datePublished || candidate.dateCreated || candidate.dateModified || "";
            const dateText = String(dateValue || "").trim();
            if (dateText) {
              pubDate = dateText;
            }
          }
        }
      } catch {
        // ignore malformed JSON-LD blocks
      }
      if (pubDate && jsonLdType) break;
    }

    const volume = readMetaValues(["citation_volume", "prism.volume"])[0] || "";
    const issue = readMetaValues(["citation_issue", "prism.number"])[0] || "";
    const firstPage = readMetaValues(["citation_firstpage", "prism.startingPage"])[0] || "";
    const lastPage = readMetaValues(["citation_lastpage", "prism.endingPage"])[0] || "";
    const doiMeta =
      readMetaValues(["citation_doi", "dc.identifier", "prism.doi"])[0] || "";

    const abstractMetaCandidates = readMetaValues([
      "citation_abstract",
      "dc.description",
      "description",
      "og:description",
    ]);
    const abstractFromDom = pickText(abstractSelectors);

    const chooseBestAbstractLocal = (candidates) => {
      const normalized = [];
      const seen = new Set();
      for (const raw of candidates) {
        const cleaned = String(raw || "")
          .replace(/\s+/g, " ")
          .replace(/^abstract[:\s-]*/i, "")
          .trim();
        if (!cleaned) continue;
        const key = cleaned.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        normalized.push(cleaned);
      }
      if (!normalized.length) return "";

      const filtered = normalized.filter(
        (text) => !/(all rights reserved|cookie|javascript|privacy policy)/i.test(text)
      );
      const pool = filtered.length ? filtered : normalized;
      const scored = pool
        .map((text) => {
          let score = text.length;
          if (/\.\.\.$/.test(text)) score -= 120; // common meta-description truncation
          if (/^click on the article title to read more\.?$/i.test(text)) score -= 200;
          if (/^click on the title to browse this issue\.?$/i.test(text)) score -= 200;
          return { text, score };
        })
        .sort((a, b) => b.score - a.score || b.text.length - a.text.length);
      return scored[0]?.text || "";
    };

    const articleTypeFromMeta = readMetaValues([
      "citation_article_type",
      "dc.type",
      "prism.section",
      "prism.contentType",
      "article:section",
      "og:type",
    ])[0];
    const articleTypeFromDom = pickText([
      ".article-header__article-type",
      ".article-header__category",
      ".issue-item__article-type",
      ".article__category",
      ".toc__section",
      ".article__tocHeading",
    ]);

    const abstractText = chooseBestAbstractLocal([abstractFromDom, ...abstractMetaCandidates]);

    const pageRange = (() => {
      if (firstPage && lastPage) return `${firstPage}-${lastPage}`;
      if (firstPage) return firstPage;
      return "";
    })();

    return {
      title,
      authors,
      journal,
      publicationDate: pubDate || "",
      volume,
      issue,
      firstPage,
      lastPage,
      pageRange,
      doiMeta,
      abstractText,
      articleTypeMetaHint: articleTypeFromMeta || "",
      articleTypeDomHint: articleTypeFromDom || "",
      articleTypeJsonLdHint: jsonLdType || "",
      articleTypeHint: articleTypeFromMeta || articleTypeFromDom || jsonLdType || "",
      finalUrl: window.location.href,
      pageTitle: document.title || "",
    };
  }, selectors);
  return buildRecordFromExtracted(extracted, sourceUrl, policy?.name || "");
}

function challengeError(message, details) {
  const error = new Error(message);
  error.name = "ChallengeError";
  error.details = details;
  return error;
}

function buildVerificationFailureRecord(inputUrl, policy, errors) {
  return {
    sourceUrl: inputUrl,
    finalUrl: inputUrl,
    title: "[Not verified]",
    authors: [],
    year: "[Not verified]",
    journal: "[Not verified]",
    volume: "[Not verified]",
    issue: "[Not verified]",
    pageRange: "[Not verified]",
    publishedOnline: "[Not verified]",
    doiUrl: ensureField(normalizeDoi(inputUrl)),
    abstract: "[Not verified]",
    citation: "[Not verified]",
    missingFields: ["title", "journal", "year", "abstract", "citation"],
    status: "not_verified",
    articleTypeRaw: "[Not verified]",
    articleType: "[Not verified]",
    ingestDecision: "not_verified",
    ingestReason: "verification_failed",
    articleTypeClassificationSource: "none",
    articleTypeMatchedHint: "",
    policy: {
      name: policy?.name || "default",
      protected: Boolean(policy?.protected),
    },
    verifiedAt: new Date().toISOString(),
    errors,
  };
}

function isChallengeFailureRecord(record) {
  if (!record || !Array.isArray(record.errors)) return false;
  return record.errors.some((err) => {
    const name = String(err?.errorName || "");
    const message = String(err?.message || "");
    return name === "ChallengeError" || /challenge page detected/i.test(message);
  });
}

function isLikelyWileyRecord(record) {
  const candidates = [record?.sourceUrl, record?.finalUrl, record?.doiUrl];
  return candidates.some((value) => {
    const text = String(value || "");
    return /onlinelibrary\.wiley\.com|wiley\.com|10\.1002\//i.test(text);
  });
}

function shouldRetryWithWileyFallback(record, args) {
  if (!args.headless || args.cdpUrl) return false;
  if (!record || record.status === "verified") return false;
  return isChallengeFailureRecord(record) && isLikelyWileyRecord(record);
}

async function detectLocalCdpUrl(verbose) {
  const endpoints = [
    "http://127.0.0.1:9222/json/version",
    "http://localhost:9222/json/version",
  ];
  for (const endpoint of endpoints) {
    try {
      const response = await fetch(endpoint);
      if (!response.ok) continue;
      const data = await response.json();
      const wsUrl = cleanText(data?.webSocketDebuggerUrl || "");
      if (wsUrl) {
        log(`Detected local CDP endpoint via ${endpoint}`, verbose);
        return wsUrl;
      }
    } catch {
      // Ignore and try next endpoint.
    }
  }
  return null;
}

async function runVerificationPass(urls, args, seenFinalUrls = new Set()) {
  let browserState = null;
  let browserInitPromise = null;
  const getContext = async () => {
    if (browserState) return browserState.context;
    if (!browserInitPromise) {
      browserInitPromise = initBrowser(args).then((state) => {
        browserState = state;
        return state;
      });
    }
    const initialized = await browserInitPromise;
    return initialized.context;
  };

  let results = [];
  try {
    results = await mapLimit(urls, args.concurrency, async (url) => {
      return verifySingleUrl(getContext, url, args, seenFinalUrls);
    });
  } finally {
    if (browserState) {
      await browserState.close();
    }
  }

  return {
    results,
    mode: browserState?.mode || "curl_only",
  };
}

function buildExcludedLinkRecord(inputUrl, finalUrl, policy, reason, resolvedTracking) {
  const maybeDoi = normalizeDoi(finalUrl) || normalizeDoi(inputUrl);
  return {
    sourceUrl: inputUrl,
    finalUrl: finalUrl || inputUrl,
    title: "[Not verified]",
    authors: [],
    year: "[Not verified]",
    journal: "[Not verified]",
    volume: "[Not verified]",
    issue: "[Not verified]",
    pageRange: "[Not verified]",
    publishedOnline: "[Not verified]",
    doiUrl: ensureField(maybeDoi),
    abstract: "[Not verified]",
    citation: "[Not verified]",
    missingFields: ["title", "journal", "year", "abstract", "citation"],
    status: "excluded",
    articleTypeRaw: "non-article-link",
    articleType: "announcement",
    ingestDecision: "exclude",
    ingestReason: reason || "non_article_link",
    articleTypeClassificationSource: "none",
    articleTypeMatchedHint: "",
    policy: {
      name: policy?.name || "default",
      protected: Boolean(policy?.protected),
    },
    resolvedUrl: finalUrl || inputUrl,
    tracking: {
      usedResolution: Boolean(resolvedTracking?.usedTrackingResolution),
      sourceUrl: resolvedTracking?.inputUrl || inputUrl,
      resolvedUrl: resolvedTracking?.resolvedUrl || finalUrl || inputUrl,
      statusCode: resolvedTracking?.trackingStatus || null,
      error: resolvedTracking?.trackingResolutionError || "",
    },
    verifiedAt: new Date().toISOString(),
  };
}

async function verifyScienceDirectViaCurl(sourceUrl, targetUrl, policy, resolvedTracking, args) {
  const fetchTargets = uniqueStrings([
    canonicalScienceDirectArticleUrl(targetUrl),
    targetUrl,
    canonicalScienceDirectArticleUrl(sourceUrl),
    sourceUrl,
  ]);
  const errors = [];

  for (const fetchTarget of fetchTargets) {
    try {
      log(`ScienceDirect curl fetch: ${fetchTarget}`, args.verbose);
      const fetched = await fetchHtmlWithCurl(fetchTarget, args);
      const challenge = detectChallengeInHtml(fetched.html, fetched.effectiveUrl || fetchTarget);
      if (challenge.isChallenge) {
        throw challengeError("ScienceDirect challenge page detected (curl path)", challenge);
      }
      const extracted = extractMetadataFromScienceDirectHtml(
        fetched.html,
        sourceUrl,
        fetched.effectiveUrl || fetchTarget
      );
      const record = buildRecordFromExtracted(extracted, sourceUrl, policy?.name || "");
      record.policy = {
        name: policy.name,
        protected: Boolean(policy.protected),
      };
      record.resolvedUrl = fetched.effectiveUrl || fetchTarget;
      record.tracking = {
        usedResolution: resolvedTracking.usedTrackingResolution,
        sourceUrl: resolvedTracking.inputUrl,
        resolvedUrl: resolvedTracking.resolvedUrl,
        statusCode: resolvedTracking.trackingStatus || null,
        error: resolvedTracking.trackingResolutionError || "",
      };
      record.retrieval = {
        mode: "curl_sciencedirect",
        fetchedUrl: fetchTarget,
        statusCode: Number.isFinite(fetched.statusCode) ? fetched.statusCode : null,
      };
      record.verifiedAt = new Date().toISOString();
      logArticleTypeDecision(record, sourceUrl, args.verbose);
      return { ok: true, record, errors };
    } catch (error) {
      errors.push({
        method: "curl_sciencedirect",
        targetUrl: fetchTarget,
        errorName: String(error?.name || "Error"),
        message: String(error?.message || error),
        challengeSignals: error?.details?.signals || [],
      });
    }
  }

  return { ok: false, errors };
}

async function verifySingleUrl(getContext, inputUrl, args, seenFinalUrls = null) {
  const canonical = normalizeDoi(inputUrl);
  const targets = uniqueStrings([inputUrl, canonical]);
  const backoffMs = [2000, 5000, 10000];
  const errors = [];
  let lastPolicy = policyForUrl(inputUrl);

  for (let attempt = 1; attempt <= args.maxRetries; attempt += 1) {
    for (const rawTargetUrl of targets) {
      const unsafeInputReason = classifyUnsafePreNavigationLink(rawTargetUrl);
      if (unsafeInputReason) {
        return {
          ok: true,
          record: buildExcludedLinkRecord(
            inputUrl,
            rawTargetUrl,
            policyForUrl(rawTargetUrl),
            unsafeInputReason,
            null
          ),
        };
      }

      const resolved = await resolveTrackedUrl(rawTargetUrl, args);
      const targetUrl = resolved.resolvedUrl;
      const policy = policyForUrl(targetUrl);
      lastPolicy = policy;

      const unsafeResolvedReason = classifyUnsafePreNavigationLink(targetUrl);
      if (unsafeResolvedReason) {
        rememberFinalUrl(seenFinalUrls, targetUrl);
        return {
          ok: true,
          record: buildExcludedLinkRecord(inputUrl, targetUrl, policy, unsafeResolvedReason, resolved),
        };
      }
      if (shouldSkipDuplicateFinalUrl(seenFinalUrls, targetUrl, inputUrl)) {
        return {
          ok: true,
          record: buildExcludedLinkRecord(
            inputUrl,
            targetUrl,
            policy,
            "duplicate_final_url_in_batch",
            resolved
          ),
        };
      }
      const nonArticleReason = classifyKnownNonArticleLink(targetUrl);
      if (nonArticleReason) {
        rememberFinalUrl(seenFinalUrls, targetUrl);
        return {
          ok: true,
          record: buildExcludedLinkRecord(inputUrl, targetUrl, policy, nonArticleReason, resolved),
        };
      }

      const shouldTryScienceDirectCurl =
        args.sciencedirectMode !== "browser" &&
        (policy.name === "sciencedirect" ||
          isScienceDirectLikeUrl(targetUrl) ||
          isScienceDirectLikeUrl(rawTargetUrl) ||
          isScienceDirectLikeUrl(inputUrl));

      if (shouldTryScienceDirectCurl) {
        const curlResult = await verifyScienceDirectViaCurl(
          inputUrl,
          targetUrl,
          policy,
          resolved,
          args
        );
        if (curlResult.ok) {
          if (
            curlResult.record.status === "verified" ||
            args.sciencedirectMode === "curl" ||
            curlResult.record.ingestDecision === "exclude"
          ) {
            if (
              shouldSkipDuplicateFinalUrl(
                seenFinalUrls,
                curlResult.record.resolvedUrl || curlResult.record.finalUrl || targetUrl,
                inputUrl
              )
            ) {
              return {
                ok: true,
                record: buildExcludedLinkRecord(
                  inputUrl,
                  curlResult.record.resolvedUrl || curlResult.record.finalUrl || targetUrl,
                  policyForUrl(curlResult.record.resolvedUrl || curlResult.record.finalUrl || targetUrl),
                  "duplicate_final_url_in_batch",
                  resolved
                ),
              };
            }
            rememberFinalUrl(
              seenFinalUrls,
              curlResult.record.resolvedUrl || curlResult.record.finalUrl || targetUrl
            );
            return { ok: true, record: curlResult.record };
          }
          errors.push({
            method: "curl_sciencedirect",
            attempt,
            rawTargetUrl,
            targetUrl,
            errorName: "IncompleteRecord",
            message:
              "ScienceDirect curl path returned partial metadata; falling back to browser extraction.",
            missingFields: curlResult.record.missingFields || [],
            trackingResolutionError: resolved.trackingResolutionError || "",
          });
        } else {
          for (const err of curlResult.errors) {
            errors.push({
              method: "curl_sciencedirect",
              attempt,
              rawTargetUrl,
              targetUrl,
              errorName: err.errorName || "Error",
              message: err.message || "ScienceDirect curl extraction failed",
              challengeSignals: err.challengeSignals || [],
              trackingResolutionError: resolved.trackingResolutionError || "",
            });
          }
        }

        if (args.sciencedirectMode === "curl") {
          continue;
        }
      }

      const context = await getContext();
      const page = await context.newPage();
      page.setDefaultTimeout(args.timeoutMs);
      try {
        log(
          `Attempt ${attempt}/${args.maxRetries} | policy=${policy.name} | target=${targetUrl}`,
          args.verbose
        );
        if (resolved.usedTrackingResolution) {
          log(
            `Tracking resolution: ${resolved.inputUrl} -> ${resolved.resolvedUrl}` +
              (resolved.trackingStatus ? ` (status ${resolved.trackingStatus})` : ""),
            args.verbose
          );
        }
        await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: args.timeoutMs });
        try {
          await page.waitForLoadState("networkidle", { timeout: 5000 });
        } catch {
          // Network idle can hang on analytics-heavy pages; continue.
        }
        const challenge = await waitForChallengeClear(page, args, args.verbose);
        if (challenge.isChallenge) {
          throw challengeError("Challenge page detected", challenge);
        }
        const navigatedUrl = maybeCanonicalArticleUrl(page.url() || targetUrl);
        const effectivePolicy = policyForUrl(navigatedUrl || targetUrl);
        lastPolicy = effectivePolicy;

        if (shouldSkipDuplicateFinalUrl(seenFinalUrls, navigatedUrl || targetUrl, inputUrl)) {
          const record = buildExcludedLinkRecord(
            inputUrl,
            navigatedUrl || targetUrl,
            effectivePolicy,
            "duplicate_final_url_in_batch",
            resolved
          );
          await page.close();
          return { ok: true, record };
        }
        const nonArticleAfterNav = classifyKnownNonArticleLink(navigatedUrl);
        if (nonArticleAfterNav) {
          rememberFinalUrl(seenFinalUrls, navigatedUrl || targetUrl);
          const record = buildExcludedLinkRecord(
            inputUrl,
            navigatedUrl || targetUrl,
            effectivePolicy,
            nonArticleAfterNav,
            resolved
          );
          await page.close();
          return { ok: true, record };
        }

        const record = await extractMetadata(page, inputUrl, effectivePolicy);
        record.policy = {
          name: effectivePolicy.name,
          protected: Boolean(effectivePolicy.protected),
        };
        record.resolvedUrl = navigatedUrl || targetUrl;
        record.tracking = {
          usedResolution: resolved.usedTrackingResolution,
          sourceUrl: resolved.inputUrl,
          resolvedUrl: resolved.resolvedUrl,
          statusCode: resolved.trackingStatus || null,
          error: resolved.trackingResolutionError || "",
        };
        record.verifiedAt = new Date().toISOString();
        record.navigationResolved = Boolean(
          navigatedUrl && normalizeFinalUrlForRunDedupe(navigatedUrl) !== normalizeFinalUrlForRunDedupe(targetUrl)
        );
        logArticleTypeDecision(record, inputUrl, args.verbose);
        rememberFinalUrl(seenFinalUrls, record.resolvedUrl || record.finalUrl || navigatedUrl || targetUrl);
        await page.close();
        return { ok: true, record };
      } catch (error) {
        const errText = String(error?.message || error);
        const errName = String(error?.name || "Error");
        const challengeSignals = error?.details?.signals || [];
        errors.push({
          attempt,
          rawTargetUrl,
          targetUrl,
          errorName: errName,
          message: errText,
          challengeSignals,
          trackingResolutionError: resolved.trackingResolutionError || "",
        });
        await page.close();
      }
    }
    const sleepMs = backoffMs[Math.min(attempt - 1, backoffMs.length - 1)];
    await sleep(sleepMs);
  }

  return {
    ok: false,
    record: buildVerificationFailureRecord(inputUrl, lastPolicy, errors),
  };
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let cursor = 0;

  async function runWorker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) return;
      results[index] = await worker(items[index], index);
    }
  }

  const runners = Array.from({ length: Math.min(limit, items.length) }, () => runWorker());
  await Promise.all(runners);
  return results;
}

async function initBrowser(args) {
  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    try {
      playwright = await import("playwright-core");
    } catch {
      throw new Error(
        "Missing dependency: playwright (or playwright-core) for Node. " +
          "Install with `npm i playwright`."
      );
    }
  }
  const { chromium } = playwright;
  if (args.cdpUrl) {
    const browser = await chromium.connectOverCDP(args.cdpUrl);
    const existingContext = browser.contexts()[0] || null;
    const context = existingContext || (await browser.newContext());
    const ownsContext = !existingContext;
    return {
      browser,
      context,
      mode: "cdp",
      close: async () => {
        if (ownsContext) {
          await context.close();
        }
        if (typeof browser.disconnect === "function") {
          await browser.disconnect();
        } else {
          await browser.close();
        }
      },
    };
  }
  const browser = await chromium.launch({
    headless: args.headless,
    channel: args.channel || "chrome",
  });
  const context = await browser.newContext();
  return {
    browser,
    context,
    mode: "launch",
    close: async () => {
      await context.close();
      await browser.close();
    },
  };
}

async function main() {
  let args;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (error) {
    usage();
    process.stderr.write(`\nError: ${error.message}\n`);
    process.exit(1);
  }

  const directUrls = uniqueStrings(args.urls);
  const fileUrls = args.input ? await readInputUrls(args.input) : [];
  const urls = uniqueStrings([...directUrls, ...fileUrls]);
  if (!urls.length) {
    throw new Error("No URLs found after parsing inputs.");
  }

  log(`Loaded ${urls.length} URL(s).`, args.verbose);

  const seenFinalUrls = new Set();
  const fallbackRuns = [];
  const initialPass = await runVerificationPass(urls, args, seenFinalUrls);
  let results = initialPass.results;
  let mode = initialPass.mode;

  const initialRecords = results.map((item) => item.record);
  const retrySourceUrls = uniqueStrings(
    initialRecords.filter((record) => shouldRetryWithWileyFallback(record, args)).map((record) => record.sourceUrl)
  );
  if (retrySourceUrls.length > 0) {
    let fallbackArgs = null;
    let fallbackKind = "";
    const autoCdpUrl = await detectLocalCdpUrl(args.verbose);
    if (autoCdpUrl) {
      fallbackArgs = {
        ...args,
        cdpUrl: autoCdpUrl,
        headless: false,
        concurrency: 1,
      };
      fallbackKind = "wiley_challenge_auto_cdp";
    } else {
      fallbackArgs = {
        ...args,
        headless: false,
        concurrency: 1,
        challengeWaitMs: Math.max(args.challengeWaitMs, 60_000),
      };
      fallbackKind = "wiley_challenge_headed_retry";
    }
    log(
      `Retrying ${retrySourceUrls.length} Wiley challenge-blocked URL(s) with fallback: ${fallbackKind}`,
      args.verbose
    );
    const fallbackPass = await runVerificationPass(retrySourceUrls, fallbackArgs, seenFinalUrls);
    fallbackRuns.push({
      type: fallbackKind,
      mode: fallbackPass.mode,
      urlCount: retrySourceUrls.length,
      urls: retrySourceUrls,
    });
    const replacementBySource = new Map(
      fallbackPass.results.map((item) => [String(item?.record?.sourceUrl || ""), item])
    );
    results = results.map((item) => {
      const key = String(item?.record?.sourceUrl || "");
      const replacement = replacementBySource.get(key);
      return replacement || item;
    });
    mode = `${mode}+${fallbackPass.mode}`;
  }

  const records = results.map((item) => item.record);
  const output = {
    generatedAt: new Date().toISOString(),
    mode,
    inputCount: urls.length,
    verifiedCount: records.filter((r) => r.status === "verified").length,
    notVerifiedCount: records.filter((r) => r.status !== "verified").length,
    includableCount: records.filter((r) => r.ingestDecision === "include").length,
    excludedCount: records.filter((r) => r.ingestDecision === "exclude").length,
    fallbackRuns,
    records,
  };

  const serialized = `${JSON.stringify(output, null, 2)}\n`;
  process.stdout.write(serialized);
  if (args.output) {
    await fs.mkdir(path.dirname(args.output), { recursive: true });
    await fs.writeFile(args.output, serialized, "utf-8");
  }
}

export { classifyArticleType, classifyKnownNonArticleLink, isTrackingUrl, normalizeArticleTypeValue };

const IS_MAIN = (() => {
  try {
    return Boolean(process.argv[1]) && import.meta.url === pathToFileURL(process.argv[1]).href;
  } catch {
    return false;
  }
})();

if (IS_MAIN) {
  main().catch((error) => {
    process.stderr.write(`Error: ${error.message || error}\n`);
    process.exit(1);
  });
}
