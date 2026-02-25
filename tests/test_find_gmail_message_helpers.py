import importlib.util
import pathlib
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "find_gmail_message.py"
SPEC = importlib.util.spec_from_file_location("find_gmail_message", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class GmailListHydrationHelperTests(unittest.TestCase):
    def test_zero_rows_with_shell_present_is_ambiguous(self) -> None:
        probe = {
            "selected_row_count": 0,
            "bog_nodes": 0,
            "shell_present": True,
            "search_inputs": 1,
            "main_regions": 1,
        }
        self.assertTrue(MODULE._gmail_zero_row_ui_is_ambiguous(probe))

    def test_zero_rows_without_shell_present_is_not_ambiguous(self) -> None:
        probe = {
            "selected_row_count": 0,
            "bog_nodes": 0,
            "shell_present": False,
            "search_inputs": 0,
            "main_regions": 0,
        }
        self.assertFalse(MODULE._gmail_zero_row_ui_is_ambiguous(probe))

    def test_nonzero_rows_not_ambiguous(self) -> None:
        probe = {
            "selected_row_count": 4,
            "bog_nodes": 4,
            "shell_present": True,
        }
        self.assertFalse(MODULE._gmail_zero_row_ui_is_ambiguous(probe))


class SearchValidationTests(unittest.TestCase):
    def test_search_with_zero_row_ambiguous_ui_is_not_trusted(self) -> None:
        self.assertFalse(
            MODULE._search_results_content_looks_valid(
                strategy_name="search_exact_subject_only",
                page1_rows=0,
                page1_exact_hits=0,
                page1_broad_hits=0,
                page1_hydrated=False,
                page1_zero_row_ambiguous=True,
            )
        )

    def test_search_content_mismatch_is_invalid_when_rows_present(self) -> None:
        self.assertFalse(
            MODULE._search_results_content_looks_valid(
                strategy_name="search_strict_exact_subject",
                page1_rows=25,
                page1_exact_hits=0,
                page1_broad_hits=0,
                page1_hydrated=True,
                page1_zero_row_ambiguous=False,
            )
        )

    def test_search_content_match_is_valid_when_rows_present(self) -> None:
        self.assertTrue(
            MODULE._search_results_content_looks_valid(
                strategy_name="search_strict_exact_subject",
                page1_rows=25,
                page1_exact_hits=1,
                page1_broad_hits=3,
                page1_hydrated=True,
                page1_zero_row_ambiguous=False,
            )
        )


class GmailClippedMessageExpansionTests(unittest.TestCase):
    def test_detects_gmail_full_message_webview_link(self) -> None:
        href = (
            "https://mail.google.com/mail/u/0/?ui=2&ik=abc123&view=lg"
            "&permmsgid=msg-f:1857993378466304732"
        )
        self.assertTrue(MODULE._is_gmail_full_message_webview_link(href))

    def test_blocks_gmail_full_message_webview_from_verification_candidates(self) -> None:
        href = (
            "https://mail.google.com/mail/u/0/?ui=2&ik=abc123&view=lg"
            "&permmsgid=msg-f:1857993378466304732"
        )
        self.assertEqual(
            MODULE._blocked_link_reason(href=href, text="View entire message"),
            "gmail_message_webview_link",
        )


if __name__ == "__main__":
    unittest.main()
