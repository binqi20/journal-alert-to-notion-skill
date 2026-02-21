# Journal Alert to Notion

`journal-alert-to-notion` is a Codex skill for processing journal alert emails from Gmail, verifying paper metadata on official publisher pages, and creating duplicate-safe entries in Notion.

## What It Does

- Locate a target Gmail alert email by subject + timestamp.
- Extract candidate article links from the email body.
- Verify metadata on publisher pages (including protected domains).
- Build normalized citations and DOI records.
- Create idempotent Notion page payloads and ingest non-duplicates.

## Skill Spec

See [`SKILL.md`](./SKILL.md) for full workflow, inclusion rules, and command examples.

## Contact

- Email: `tangbinqi@gmail.com`
- Preferred support channel: open a GitHub Issue in this repository.
