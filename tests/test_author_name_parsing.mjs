import test from "node:test";
import assert from "node:assert/strict";

import {
  parseAuthorName,
  parseAuthorList,
  formatApaAuthorFromStructured,
} from "../scripts/verify_publisher_record.mjs";

test("parseAuthorName handles comma surname-first format", () => {
  const { author, warnings } = parseAuthorName("Guo, Chun", { source: "meta" });
  assert.equal(warnings.length, 0);
  assert.equal(author.family, "Guo");
  assert.deepEqual(author.given, ["Chun"]);
  assert.equal(author.parseMode, "comma_surname_first");
  assert.equal(author.formattedApa, "Guo, C.");
});

test("parseAuthorName preserves family particles in space-separated names", () => {
  const { author } = parseAuthorName("Ludwig van Beethoven", { source: "dom" });
  assert.equal(author.family, "van Beethoven");
  assert.deepEqual(author.given, ["Ludwig"]);
  assert.equal(author.formattedApa, "van Beethoven, L.");
});

test("parseAuthorName keeps hyphenated given-name initials", () => {
  const { author } = parseAuthorName("Doe, Jean-Luc");
  assert.equal(author.formattedApa, "Doe, J.-L.");
});

test("parseAuthorName passes through corporate authors", () => {
  const { author } = parseAuthorName("World Health Organization");
  assert.equal(author.parseMode, "corporate");
  assert.equal(author.formattedApa, "World Health Organization");
});

test("parseAuthorList deduplicates and formats structured authors", () => {
  const parsed = parseAuthorList(["Guo, Chun", "Guo, Chun", "Luo, Jingbo"]);
  assert.equal(parsed.authors.length, 2);
  assert.equal(parsed.authors[0].formattedApa, "Guo, C.");
  assert.equal(parsed.authors[1].formattedApa, "Luo, J.");
});

test("parseAuthorList preserves JBE Springer comma-format surname order", () => {
  const parsed = parseAuthorList([
    "Wang, Ziqiao",
    "Zhang, Wei",
    "He, Feng",
    "Huang, Xin",
  ]);
  assert.deepEqual(
    parsed.authors.map((a) => a.formattedApa),
    ["Wang, Z.", "Zhang, W.", "He, F.", "Huang, X."]
  );
});

test("parseAuthorName preserves comma-format family particles", () => {
  const { author } = parseAuthorName("Ben Khaled, Wafa");
  assert.equal(author.family, "Ben Khaled");
  assert.equal(author.formattedApa, "Ben Khaled, W.");
});

test("formatApaAuthorFromStructured supports suffix", () => {
  const formatted = formatApaAuthorFromStructured({
    raw: "Smith, John Jr.",
    family: "Smith",
    given: ["John"],
    suffix: "Jr",
    parseMode: "comma_surname_first",
    source: "meta",
    confidence: "high",
  });
  assert.equal(formatted, "Smith, J., Jr");
});
