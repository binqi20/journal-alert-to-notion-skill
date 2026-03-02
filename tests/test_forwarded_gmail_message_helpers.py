import importlib.util
import pathlib
import unittest
from zoneinfo import ZoneInfo


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "find_gmail_message.py"
SPEC = importlib.util.spec_from_file_location("find_gmail_message", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class ForwardedSubjectTests(unittest.TestCase):
    def test_unwraps_forward_prefixes(self) -> None:
        info = MODULE._unwrap_subject_prefixes("Fw: Re: Early View Alert: Strategic Management Journal")
        self.assertTrue(info["is_forwarded"])
        self.assertEqual(info["normalized"], "Early View Alert: Strategic Management Journal")
        self.assertEqual(info["prefixes"], ["fw", "re"])


class ForwardedMetadataExtractionTests(unittest.TestCase):
    def test_extracts_forwarded_headers_from_body(self) -> None:
        body = (
            "From: Strategic Management Journal <WileyOnlineLibrary@wiley.com>\n"
            "Date: Mon, Mar 2, 2026, 4:10 PM\n"
            "Subject: Early View Alert: Strategic Management Journal\n"
            "To: tangbinqi@gmail.com\n\n"
            "Regulated versus unregulated competition: How drug shortages boost illegal pharmacy sales"
        )
        meta = MODULE._extract_forwarded_metadata(body, ZoneInfo("Asia/Shanghai"))
        self.assertTrue(meta["forwarded_body_extracted"])
        self.assertEqual(meta["original_subject"], "Early View Alert: Strategic Management Journal")
        self.assertEqual(meta["original_sender_email"], "WileyOnlineLibrary@wiley.com")
        self.assertIn("2026-03-02T16:10:00", meta["original_date_local"])


class ForwardedLinkEnrichmentTests(unittest.TestCase):
    def test_enriches_forwarded_article_links(self) -> None:
        enriched = MODULE._enrich_link_details(
            [
                {
                    "href": "http://el.wiley.com/ls/click?upn=abc",
                    "text": "Regulated versus unregulated competition: How drug shortages boost illegal pharmacy sales",
                }
            ],
            message_kind="forwarded",
        )
        self.assertEqual(enriched[0]["candidateKind"], "article_like")
        self.assertEqual(enriched[0]["sourceContext"], "forwarded_body")
        self.assertGreaterEqual(enriched[0]["candidateScore"], 100)

    def test_marks_forwarded_unsubscribe_footer_links(self) -> None:
        enriched = MODULE._enrich_link_details(
            [{"href": "http://el.wiley.com/ls/click?upn=unsubscribe", "text": "unsubscribe here"}],
            message_kind="forwarded",
        )
        self.assertEqual(enriched[0]["candidateKind"], "blocked")
        self.assertEqual(enriched[0]["sourceContext"], "footer")

    def test_marks_mailto_links_as_wrapper_header(self) -> None:
        enriched = MODULE._enrich_link_details(
            [{"href": "mailto:WileyOnlineLibrary@wiley.com", "text": "WileyOnlineLibrary@wiley.com"}],
            message_kind="forwarded",
        )
        self.assertEqual(enriched[0]["candidateKind"], "blocked")
        self.assertEqual(enriched[0]["sourceContext"], "wrapper_header")


if __name__ == "__main__":
    unittest.main()
