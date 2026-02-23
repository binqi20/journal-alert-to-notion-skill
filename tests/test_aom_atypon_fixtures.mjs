import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

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

test("AOM AMJ fixture parses via DOI-prefix journal fallback", async () => {
  const record = await runVerifierOnFixture("aom_amj_doi_prefix_fallback.html");
  assert.equal(record.status, "verified");
  assert.equal(record.ingestDecision, "include");
  assert.equal(record.articleType, "research-article");
  assert.equal(record.journal, "Academy of Management Journal");
  assert.equal(record.year, "2026");
  assert.equal(record.doiUrl, "https://doi.org/10.5465/amj.2023.0515");
  assert.match(
    record.citation,
    /Academy of Management Journal\. Advance online publication\. https:\/\/doi\.org\/10\.5465\/amj\.2023\.0515$/
  );
});

test("AOM AMD fixture strips honorifics in APA citation formatting", async () => {
  const record = await runVerifierOnFixture("aom_amd_title_suffix_honorifics.html");
  assert.equal(record.status, "verified");
  assert.equal(record.ingestDecision, "include");
  assert.equal(record.articleType, "research-article");
  assert.equal(record.journal, "Academy of Management Discoveries");
  assert.equal(record.year, "2026");
  assert.equal(record.doiUrl, "https://doi.org/10.5465/amd.2025.0014");
  assert.doesNotMatch(record.citation, /\bDr\.\b|\bProfessor\b/);
  assert.match(record.citation, /^Hoffman, F\., Tumasjan, A\., Nyberg, A\. J\., & Welpe, I\. M\./);
});

test("Wiley SMJ fixture prefers full DOM abstract over truncated meta teaser", async () => {
  const record = await runVerifierOnFixture("wiley_smj_abstract_prefers_dom_over_meta.html");
  assert.equal(record.status, "verified");
  assert.equal(record.ingestDecision, "include");
  assert.equal(record.articleType, "research-article");
  assert.equal(record.journal, "Strategic Management Journal");
  assert.equal(record.doiUrl, "https://doi.org/10.1002/smj.79999");
  assert.ok(record.abstract && record.abstract.length > 250, "expected full DOM abstract text");
  assert.match(record.abstract, /full abstract text rendered in the Wiley article DOM/i);
  assert.match(record.abstract, /Managerial Summary/i);
  assert.doesNotMatch(record.abstract, /available\.\.\.$/i);
  assert.doesNotMatch(record.abstract, /truncated Wiley-style teaser abstract/i);
});
