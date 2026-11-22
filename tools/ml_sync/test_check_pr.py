"""Verify the PR convention gate independently of external services."""

import copy
import unittest
from pathlib import Path

from check_pr import check


class PrConventionTests(unittest.TestCase):
    def setUp(self):
        self.pr = {"head": {"ref": "feat/SCRUM-38"}, "base": {"ref": "develop"},
                   "title": "SCRUM-38 sync", "body": "What I Did\n개발 메모\nTo Reviewers"}

    def test_valid(self):
        self.assertEqual(check(self.pr), [])

    def test_actual_team_template(self):
        template = Path(__file__).resolve().parents[2] / ".github" / "pull_request_template.md"
        self.pr["body"] = template.read_text(encoding="utf-8").replace(
            "[DAJ-00](https://)",
            "SCRUM-38",
        )
        self.pr["title"] = "sync"
        self.assertEqual(check(self.pr), [])

    def test_team_heading_still_requires_review_section(self):
        self.pr["body"] = "Key Changes\nSCRUM-38"
        self.assertTrue(check(self.pr))

    def test_wrong_branch_and_base(self):
        for part, ref in [("head", "feat/DAJ-38"), ("base", "main")]:
            pr = copy.deepcopy(self.pr)
            pr[part]["ref"] = ref
            self.assertTrue(check(pr))

    def test_missing_sections(self):
        for body in [None, "What I Did", "To Reviewers", "SCRUM-38"]:
            pr = copy.deepcopy(self.pr)
            pr["body"] = body
            self.assertTrue(check(pr))

    def test_missing_or_nonmatching_key(self):
        for title in ("sync", "SCRUM-39", "SCRUM-380", "SCRUM-38x", "xSCRUM-38", "SCRUM-38_a", "PRE-SCRUM-38"):
            with self.subTest(title=title):
                self.pr["title"] = title
                self.assertTrue(check(self.pr))

    def test_key_in_body_with_punctuation(self):
        self.pr["title"] = "sync"
        self.pr["body"] = "Key Changes\n(SCRUM-38)\nTo Reviewers"
        self.assertEqual(check(self.pr), [])

    def test_shell_text_is_only_data(self):
        self.pr["title"] += " $(echo unsafe) `echo unsafe`"
        self.assertEqual(check(self.pr), [])


if __name__ == "__main__":
    unittest.main()
