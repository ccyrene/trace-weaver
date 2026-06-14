import unittest

from traceweaver import confidence
from traceweaver.scanners.sql_scanner import build_lineage


class TestConfidenceRules(unittest.TestCase):
    def test_sql_confidence_matrix(self):
        self.assertEqual(
            confidence.sql_confidence(high_confidence=True, paired=True), "high"
        )
        self.assertEqual(
            confidence.sql_confidence(high_confidence=True, paired=False), "medium"
        )
        self.assertEqual(
            confidence.sql_confidence(high_confidence=False, paired=True), "medium"
        )
        self.assertEqual(
            confidence.sql_confidence(high_confidence=False, paired=False), "low"
        )

    def test_fixed_levels(self):
        self.assertEqual(confidence.TASK_DEPENDENCY, "high")
        self.assertEqual(confidence.CONNECTION, "low")
        self.assertEqual(confidence.PATTERN, "low")
        self.assertEqual(confidence.LEVELS, ("high", "medium", "low"))

    def test_build_lineage_uses_the_rules(self):
        # Regex backend: paired -> medium, single-sided half-edge -> low.
        _, paired = build_lineage({"a"}, {"b"}, "d", "t", method="sql_regex")
        self.assertEqual(paired[0].confidence, "medium")
        _, half = build_lineage({"a"}, set(), "d", "t", method="sql_regex")
        self.assertEqual(half[0].confidence, "low")
        # sqlglot backend: paired -> high.
        _, hi = build_lineage(
            {"a"}, {"b"}, "d", "t", method="sqlglot", high_confidence=True
        )
        self.assertEqual(hi[0].confidence, "high")


if __name__ == "__main__":
    unittest.main()
