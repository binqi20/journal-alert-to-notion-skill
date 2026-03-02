import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { promises as fs } from "node:fs";

import {
  classifyLikelyTrackedNonArticleInput,
  readInputUrls,
} from "../scripts/verify_publisher_record.mjs";

test("Wiley forwarded TOC tracker is excluded before browser verification", () => {
  const reason = classifyLikelyTrackedNonArticleInput(
    "http://el.wiley.com/ls/click?upn=abc",
    "Early View",
    "toc_like"
  );
  assert.equal(reason, "wiley_toc_or_issue_link");
});

test("Wiley forwarded journal-home tracker is excluded before browser verification", () => {
  const reason = classifyLikelyTrackedNonArticleInput(
    "http://el.wiley.com/ls/click?upn=abc",
    "Journal home",
    "journal_home"
  );
  assert.equal(reason, "wiley_journal_home_link");
});

test("Wiley forwarded article tracker is not excluded before browser verification", () => {
  const reason = classifyLikelyTrackedNonArticleInput(
    "http://el.wiley.com/ls/click?upn=abc",
    "Regulated versus unregulated competition: How drug shortages boost illegal pharmacy sales",
    "article_like"
  );
  assert.equal(reason, null);
});

test("full Gmail match JSON does not ingest the Gmail thread URL when link_details exist", async () => {
  const tmpPath = path.join(os.tmpdir(), `gmail-forwarded-match-${Date.now()}.json`);
  await fs.writeFile(
    tmpPath,
    JSON.stringify({
      candidates: [
        {
          url: "https://mail.google.com/mail/u/0/#inbox/thread-id",
          link_details: [
            {
              href: "http://el.wiley.com/ls/click?upn=article",
              text: "Regulated versus unregulated competition: How drug shortages boost illegal pharmacy sales",
              candidateKind: "article_like",
              sourceContext: "forwarded_body",
              candidateScore: 105,
            },
          ],
        },
      ],
    }),
    "utf8"
  );
  try {
    const entries = await readInputUrls(tmpPath);
    assert.equal(entries.length, 1);
    assert.equal(entries[0].url, "http://el.wiley.com/ls/click?upn=article");
  } finally {
    await fs.rm(tmpPath, { force: true });
  }
});
