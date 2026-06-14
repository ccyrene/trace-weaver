import unittest

from traceweaver.scanners import sql_lineage
from traceweaver.scanners.sql_lineage import (
    dialect_for_operator,
    extract_sql_lineage,
    sqlglot_available,
)

INSERT_SELECT = """
INSERT INTO analytics.orders_clean
SELECT o.id, c.name
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
"""

CTAS_WITH_CTE = """
CREATE OR REPLACE TABLE mart.daily AS
WITH base AS (SELECT * FROM staging.events)
SELECT * FROM base JOIN dim.users u ON base.uid = u.id
"""

MERGE = """
MERGE INTO analytics.dim_customer AS t
USING staging.customer_updates AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.name = s.name
"""


def _pairs(edges):
    return {(e.source_dataset, e.target_dataset) for e in edges}


class TestSqlLineageSqlglot(unittest.TestCase):
    @unittest.skipUnless(sqlglot_available(), "sqlglot not installed")
    def test_insert_select(self):
        d, e, backend = extract_sql_lineage(
            INSERT_SELECT, "dag", "task", dialect="postgres"
        )
        self.assertEqual(backend, "sqlglot")
        pairs = _pairs(e)
        self.assertIn(("raw.orders", "analytics.orders_clean"), pairs)
        self.assertIn(("raw.customers", "analytics.orders_clean"), pairs)
        self.assertTrue(all(edge.confidence == "high" for edge in e))

    @unittest.skipUnless(sqlglot_available(), "sqlglot not installed")
    def test_ctas_excludes_cte(self):
        d, e, backend = extract_sql_lineage(
            CTAS_WITH_CTE, "dag", "task", dialect="snowflake"
        )
        names = {ds.name for ds in d}
        self.assertIn("mart.daily", names)
        self.assertIn("staging.events", names)
        self.assertIn("dim.users", names)
        self.assertNotIn("base", names)  # CTE alias must not appear as a dataset

    @unittest.skipUnless(sqlglot_available(), "sqlglot not installed")
    def test_merge_target(self):
        d, e, backend = extract_sql_lineage(MERGE, "dag", "task", dialect="snowflake")
        self.assertIn(("staging.customer_updates", "analytics.dim_customer"), _pairs(e))

    @unittest.skipUnless(sqlglot_available(), "sqlglot not installed")
    def test_multi_statement_pairs_per_statement(self):
        sql = "INSERT INTO sink.a SELECT * FROM src.a; INSERT INTO sink.b SELECT * FROM src.b"
        d, e, backend = extract_sql_lineage(sql, "dag", "task", dialect="postgres")
        pairs = _pairs(e)
        self.assertEqual(pairs, {("src.a", "sink.a"), ("src.b", "sink.b")})
        # No cross-statement edges.
        self.assertNotIn(("src.a", "sink.b"), pairs)
        self.assertNotIn(("src.b", "sink.a"), pairs)


class TestSqlLineageRegexFallback(unittest.TestCase):
    def setUp(self):
        self._had = sql_lineage._HAS_SQLGLOT
        sql_lineage._HAS_SQLGLOT = False

    def tearDown(self):
        sql_lineage._HAS_SQLGLOT = self._had

    def test_regex_fallback(self):
        d, e, backend = extract_sql_lineage(
            INSERT_SELECT, "dag", "task", dialect="postgres"
        )
        self.assertEqual(backend, "sql_regex")
        pairs = _pairs(e)
        self.assertIn(("raw.orders", "analytics.orders_clean"), pairs)
        self.assertIn(("raw.customers", "analytics.orders_clean"), pairs)
        self.assertTrue(all(edge.confidence == "medium" for edge in e))


class TestDialectMapping(unittest.TestCase):
    def test_dialects(self):
        self.assertEqual(dialect_for_operator("PostgresOperator"), "postgres")
        self.assertEqual(dialect_for_operator("SnowflakeOperator"), "snowflake")
        self.assertEqual(dialect_for_operator("BigQueryInsertJobOperator"), "bigquery")
        self.assertIsNone(dialect_for_operator(None))
        self.assertIsNone(dialect_for_operator("PythonOperator"))


if __name__ == "__main__":
    unittest.main()
