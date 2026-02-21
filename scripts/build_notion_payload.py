#!/usr/bin/env python3
"""Build duplicate-safe Notion create-pages payload from verified paper records.

This script normalizes DOI URLs, filters [Not verified] records, detects duplicates
against existing Notion rows, and outputs a deterministic write plan.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote


DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
DEFAULT_INCLUDE_TYPES = "research-article,research-paper,editorial"
DEFAULT_EXCLUDE_TYPES = (
    "book-review,media-review,discussion,commentary,corrigendum,erratum,retraction,"
    "interview,call-for-papers,announcement,news"
)


def _log(msg: str, *, enabled: bool) -> None:
    if enabled:
        print(f"[build_notion_payload] {msg}", file=sys.stderr)


def _die(message: str, code: int = 1) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(code)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--records",
        type=Path,
        required=True,
        help="JSON file containing verified records (array or object with records/results).",
    )
    parser.add_argument(
        "--existing",
        type=Path,
        help="Optional JSON file containing existing Notion rows (view query output).",
    )
    parser.add_argument(
        "--require-existing",
        action="store_true",
        help="Fail if --existing is not provided.",
    )
    parser.add_argument(
        "--data-source-id",
        help="Optional Notion data source id for create-pages parent object.",
    )
    parser.add_argument("--title-prop", default="Title", help="Notion title property name.")
    parser.add_argument("--citation-prop", default="Citation", help="Notion citation property name.")
    parser.add_argument("--abstract-prop", default="Abstract", help="Notion abstract property name.")
    parser.add_argument("--url-prop", default="userDefined:URL", help="Notion DOI URL property name.")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow records with missing non-DOI fields (still skips [Not verified]).",
    )
    parser.add_argument(
        "--include-types",
        default=DEFAULT_INCLUDE_TYPES,
        help=f"Comma-separated article types to include (default: {DEFAULT_INCLUDE_TYPES}).",
    )
    parser.add_argument(
        "--exclude-types",
        default=DEFAULT_EXCLUDE_TYPES,
        help=f"Comma-separated article types to exclude (default: {DEFAULT_EXCLUDE_TYPES}).",
    )
    parser.add_argument(
        "--allow-unknown-article-type",
        action="store_true",
        help="Allow ingestion when article type cannot be confidently classified.",
    )
    parser.add_argument("--output", type=Path, help="Optional output JSON file.")
    parser.add_argument("--verbose", action="store_true", help="Verbose stderr logs.")
    return parser.parse_args()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_not_verified(value: Any) -> bool:
    return _clean_text(value).lower() == "[not verified]"


def _parse_csv_set(raw_value: str) -> set[str]:
    return {
        item.strip().lower()
        for item in raw_value.split(",")
        if item is not None and item.strip()
    }


def _normalize_article_type(value: Any) -> str:
    text = _clean_text(value).lower()
    if not text or text == "[not verified]":
        return ""
    if re.search(r"\beditorial\s*(board|data)\b", text):
        return "announcement"
    if re.search(r"(book\s*review|media\s*review)", text):
        return "book-review"
    if re.search(r"\b(editorial|from the editors?)\b", text):
        return "editorial"
    if re.search(r"\bdiscussion\b", text):
        return "discussion"
    if re.search(r"\b(commentary|perspective|opinion)\b", text):
        return "commentary"
    if re.search(r"\b(corrigendum|corrigenda)\b", text):
        return "corrigendum"
    if re.search(r"\b(erratum|errata)\b", text):
        return "erratum"
    if re.search(r"\b(retraction|withdrawal)\b", text):
        return "retraction"
    if re.search(r"\binterview\b", text):
        return "interview"
    if re.search(r"\bcall\s*for\s*papers?\b", text):
        return "call-for-papers"
    if re.search(r"\b(announcement|announcements)\b", text):
        return "announcement"
    if re.search(r"\bnews\b", text):
        return "news"
    if re.search(r"\b(research\s*paper|research\s*article|original\s*article)\b", text):
        return "research-article"
    if re.search(r"\b(scholarlyarticle|journalarticle)\b", text):
        return "research-article"
    if re.search(r"\barticle\b", text):
        return "research-article"
    return text


def normalize_doi_url(raw_value: Any) -> str:
    text = _clean_text(raw_value)
    if not text:
        return ""
    decoded = unquote(text)
    normalized = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", decoded, flags=re.IGNORECASE)
    normalized = re.sub(r"^doi:\s*", "", normalized, flags=re.IGNORECASE)
    match = DOI_PATTERN.search(normalized)
    if not match:
        return ""
    doi = re.sub(r"[)\],.;\s]+$", "", match.group(0))
    return f"https://doi.org/{doi.lower()}"


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _die(f"Invalid JSON in {path}: {exc}")
    except FileNotFoundError:
        _die(f"File not found: {path}")
    raise AssertionError("unreachable")


def _unwrap_mcp_text_payload(node: Any) -> Any:
    """Handle tool wrapper shape: [{"type":"text","text":"{...json...}"}]."""
    if isinstance(node, list) and node and all(isinstance(x, dict) for x in node):
        if all(x.get("type") == "text" and isinstance(x.get("text"), str) for x in node):
            text_chunks: list[Any] = []
            for chunk in node:
                text = chunk.get("text", "")
                try:
                    text_chunks.append(json.loads(text))
                except json.JSONDecodeError:
                    continue
            if len(text_chunks) == 1:
                return text_chunks[0]
            if text_chunks:
                return text_chunks
    return node


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    node = _unwrap_mcp_text_payload(payload)
    if isinstance(node, dict):
        if isinstance(node.get("records"), list):
            return [x for x in node["records"] if isinstance(x, dict)]
        if isinstance(node.get("results"), list):
            return [x for x in node["results"] if isinstance(x, dict)]
        if isinstance(node.get("pages"), list):
            return [x for x in node["pages"] if isinstance(x, dict)]
    if isinstance(node, list):
        records = []
        for item in node:
            if isinstance(item, dict):
                if "records" in item and isinstance(item["records"], list):
                    records.extend(x for x in item["records"] if isinstance(x, dict))
                elif "results" in item and isinstance(item["results"], list):
                    records.extend(x for x in item["results"] if isinstance(x, dict))
                else:
                    records.append(item)
        return records
    return []


def _record_title(record: dict[str, Any]) -> str:
    return _clean_text(record.get("title") or record.get("Title"))


def _record_citation(record: dict[str, Any]) -> str:
    return _clean_text(record.get("citation") or record.get("Citation"))


def _record_abstract(record: dict[str, Any]) -> str:
    return _clean_text(record.get("abstract") or record.get("Abstract"))


def _record_source_url(record: dict[str, Any]) -> str:
    return _clean_text(record.get("sourceUrl") or record.get("url") or record.get("URL"))


def _record_doi_url(record: dict[str, Any]) -> str:
    doi_sources = [
        record.get("doiUrl"),
        record.get("doi"),
        record.get("DOI"),
        record.get("userDefined:URL"),
        record.get("URL"),
        record.get("url"),
        record.get("sourceUrl"),
    ]
    for source in doi_sources:
        normalized = normalize_doi_url(source)
        if normalized:
            return normalized
    return ""


def _record_article_type(record: dict[str, Any]) -> str:
    candidates = [
        record.get("articleType"),
        record.get("article_type"),
        record.get("type"),
        record.get("articleTypeRaw"),
    ]
    for value in candidates:
        normalized = _normalize_article_type(value)
        if normalized:
            return normalized
    return ""


def _record_ingest_decision(record: dict[str, Any]) -> str:
    return _clean_text(record.get("ingestDecision") or record.get("ingest_decision")).lower()


def _record_ingest_reason(record: dict[str, Any]) -> str:
    return _clean_text(record.get("ingestReason") or record.get("ingest_reason"))


def _extract_existing_doi_set(existing_payload: Any) -> tuple[set[str], dict[str, dict[str, Any]]]:
    rows = _extract_records(existing_payload)
    doi_set: set[str] = set()
    doi_row_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        normalized = _record_doi_url(row)
        if not normalized:
            continue
        doi_set.add(normalized)
        doi_row_map[normalized] = row
    return doi_set, doi_row_map


def build_plan(args: argparse.Namespace) -> dict[str, Any]:
    if args.require_existing and not args.existing:
        _die("--require-existing was set but --existing was not provided.")

    incoming_payload = _read_json(args.records)
    incoming_records = _extract_records(incoming_payload)
    _log(f"Loaded {len(incoming_records)} incoming records", enabled=args.verbose)

    existing_dois: set[str] = set()
    existing_map: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    if args.existing:
        existing_payload = _read_json(args.existing)
        existing_dois, existing_map = _extract_existing_doi_set(existing_payload)
        _log(f"Loaded {len(existing_dois)} existing DOI URLs", enabled=args.verbose)
    else:
        warning = (
            "No --existing input provided: duplicate checks only run within this batch. "
            "Provide --existing for full duplicate-safe writes."
        )
        warnings.append(warning)
        print(f"Warning: {warning}", file=sys.stderr)

    include_types = _parse_csv_set(args.include_types)
    exclude_types = _parse_csv_set(args.exclude_types)

    seen_in_batch: set[str] = set()
    to_create: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    skipped_not_verified: list[dict[str, Any]] = []
    skipped_non_research: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []

    for idx, record in enumerate(incoming_records):
        title = _record_title(record)
        citation = _record_citation(record)
        abstract = _record_abstract(record)
        doi_url = _record_doi_url(record)
        source_url = _record_source_url(record)
        article_type = _record_article_type(record)
        ingest_decision = _record_ingest_decision(record)
        ingest_reason = _record_ingest_reason(record)

        if ingest_decision == "exclude":
            skipped_non_research.append(
                {
                    "index": idx,
                    "title": title or "[Not verified]",
                    "doiUrl": doi_url or "[Not verified]",
                    "articleType": article_type or "[Not verified]",
                    "reason": ingest_reason or "excluded_by_verifier",
                    "sourceUrl": source_url,
                }
            )
            continue

        missing_fields: list[str] = []
        if not doi_url:
            missing_fields.append("doiUrl")
        if not args.allow_partial:
            if not title:
                missing_fields.append("title")
            if not citation:
                missing_fields.append("citation")
            if not abstract:
                missing_fields.append("abstract")

        not_verified_fields = []
        for field_name, field_value in (
            ("title", title),
            ("citation", citation),
            ("abstract", abstract),
            ("doiUrl", doi_url),
        ):
            if _is_not_verified(field_value):
                not_verified_fields.append(field_name)

        if missing_fields:
            invalid_records.append(
                {
                    "index": idx,
                    "title": title or "[Not verified]",
                    "doiUrl": doi_url or "[Not verified]",
                    "reason": "missing_required_fields",
                    "missingFields": missing_fields,
                    "sourceUrl": source_url,
                }
            )
            continue

        if not_verified_fields:
            skipped_not_verified.append(
                {
                    "index": idx,
                    "title": title or "[Not verified]",
                    "doiUrl": doi_url or "[Not verified]",
                    "reason": "[Not verified]",
                    "fields": not_verified_fields,
                    "sourceUrl": source_url,
                }
            )
            continue

        if ingest_decision == "not_verified" and not args.allow_unknown_article_type:
            skipped_not_verified.append(
                {
                    "index": idx,
                    "title": title or "[Not verified]",
                    "doiUrl": doi_url or "[Not verified]",
                    "reason": ingest_reason or "article_type_not_verified",
                    "fields": ["articleType"],
                    "sourceUrl": source_url,
                }
            )
            continue
        if ingest_decision not in {"include", "exclude", "not_verified"}:
            if article_type in exclude_types:
                skipped_non_research.append(
                    {
                        "index": idx,
                        "title": title or "[Not verified]",
                        "doiUrl": doi_url or "[Not verified]",
                        "articleType": article_type or "[Not verified]",
                        "reason": f"excluded_by_type:{article_type}",
                        "sourceUrl": source_url,
                    }
                )
                continue
            if article_type not in include_types and not args.allow_unknown_article_type:
                skipped_not_verified.append(
                    {
                        "index": idx,
                        "title": title or "[Not verified]",
                        "doiUrl": doi_url or "[Not verified]",
                        "reason": "article_type_unclear",
                        "fields": ["articleType"],
                        "sourceUrl": source_url,
                    }
                )
                continue

        if doi_url in existing_dois:
            existing_row = existing_map.get(doi_url, {})
            duplicates.append(
                {
                    "index": idx,
                    "title": title,
                    "doiUrl": doi_url,
                    "reason": "existing_database_duplicate",
                    "existingTitle": _record_title(existing_row),
                }
            )
            continue

        if doi_url in seen_in_batch:
            duplicates.append(
                {
                    "index": idx,
                    "title": title,
                    "doiUrl": doi_url,
                    "reason": "duplicate_within_batch",
                }
            )
            continue

        seen_in_batch.add(doi_url)
        properties = {
            args.title_prop: title,
            args.citation_prop: citation,
            args.abstract_prop: abstract,
            args.url_prop: doi_url,
        }
        to_create.append(
            {
                "index": idx,
                "title": title,
                "doiUrl": doi_url,
                "sourceUrl": source_url or None,
                "articleType": article_type or None,
                "properties": properties,
            }
        )

    create_pages_payload: dict[str, Any] = {"pages": [{"properties": x["properties"]} for x in to_create]}
    if args.data_source_id:
        create_pages_payload["parent"] = {"data_source_id": args.data_source_id}

    return {
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dataSourceId": args.data_source_id or None,
        "stats": {
            "incomingRecords": len(incoming_records),
            "existingDois": len(existing_dois),
            "toCreate": len(to_create),
            "duplicates": len(duplicates),
            "skippedNotVerified": len(skipped_not_verified),
            "skippedNonResearch": len(skipped_non_research),
            "invalid": len(invalid_records),
        },
        "warnings": warnings,
        "includeTypes": sorted(include_types),
        "excludeTypes": sorted(exclude_types),
        "toCreate": to_create,
        "duplicates": duplicates,
        "skippedNotVerified": skipped_not_verified,
        "skippedNonResearch": skipped_non_research,
        "invalid": invalid_records,
        "createPagesParameters": create_pages_payload,
    }


def main() -> None:
    args = parse_args()
    plan = build_plan(args)
    serialized = json.dumps(plan, indent=2, sort_keys=True)
    print(serialized)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
