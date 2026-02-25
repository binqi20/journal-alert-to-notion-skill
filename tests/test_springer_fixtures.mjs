import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

import {
  classifyArticleType,
  classifyKnownNonArticleLink,
  isTrackingUrl,
} from "../scripts/verify_publisher_record.mjs";

const execFileAsync = promisify(execFile);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const skillRoot = path.resolve(__dirname, "..");
const verifierScript = path.join(skillRoot, "scripts", "verify_publisher_record.mjs");
const fixturesDir = path.join(__dirname, "fixtures");

async function runVerifierOnFixture(filename) {
  const fixturePath = path.join(fixturesDir, filename);
  const fixtureUrl = pathToFileURL(fixturePath).href;
  const { stdout } = await execFileAsync(
    "node",
    [
      verifierScript,
      "--url",
      fixtureUrl,
      "--headless",
      "--concurrency",
      "1",
      "--max-retries",
      "1",
      "--timeout-ms",
      "15000",
      "--challenge-wait-ms",
      "0",
    ],
    {
      cwd: skillRoot,
      maxBuffer: 10 * 1024 * 1024,
    }
  );
  const parsed = JSON.parse(stdout);
  assert.equal(parsed.records.length, 1, "expected one record from verifier");
  return parsed.records[0];
}

test("Springer fixture parser extracts raw article type and DOI", async () => {
  const record = await runVerifierOnFixture("springer_jbe_original_paper_with_perspective_title.html");
  assert.equal(record.status, "verified");
  assert.equal(record.title, "Social Credit and Trade Credit: A Coevolutionary Perspective");
  assert.equal(record.articleTypeRaw, "Original Paper");
  assert.equal(record.doiUrl, "https://doi.org/10.1007/s10551-025-05968-0");
  assert.ok(record.abstract && record.abstract.length > 80);
});

test("Springer Original Paper raw type outranks title heuristic containing Perspective", () => {
  const classified = classifyArticleType({
    policyName: "springer_link",
    rawTypeHints: [{ value: "Original Paper", sourceLabel: "publisher_raw_type" }],
    semanticHints: [{ value: "Social Credit and Trade Credit: A Coevolutionary Perspective", sourceLabel: "title_heuristic" }],
  });
  assert.equal(classified.articleType, "research-article");
  assert.equal(classified.ingestDecision, "include");
  assert.equal(classified.articleTypeClassificationSource, "publisher_raw_type");
  assert.equal(classified.articleTypeMatchedHint, "Original Paper");
});

test("Springer tracker host variant links.springernature.com is recognized as tracked", () => {
  assert.equal(
    isTrackingUrl("https://links.springernature.com/f/a/example~~/AABE5hA~/abc"),
    true
  );
});

test("Springer TOC and Gmail webview links are explicitly excluded", async () => {
  const html = await fs.readFile(
    path.join(fixturesDir, "springer_jbe_toc_or_webview_non_article.html"),
    "utf-8"
  );
  const hrefs = Array.from(html.matchAll(/href=\"([^\"]+)\"/g)).map((m) => m[1]);
  assert.equal(hrefs.length, 2);
  assert.equal(classifyKnownNonArticleLink(hrefs[0]), "springer_toc_or_issue_link");
  assert.equal(classifyKnownNonArticleLink(hrefs[1]), "gmail_message_webview_link");
});
