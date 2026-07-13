"""Runtime-behaviour tests for the ``@lineage`` authoring decorator.

The decorator is a static-lineage marker: at runtime it must be a no-op that
returns the original function and only attaches ``__traceweaver_lineage__``.
These tests lock that contract (they do NOT test the static scanner, which is
the Rust side and reads source without importing anything).
"""

import unittest

from trace_weaver import LINEAGE_ATTR, lineage


class LineageDecoratorRuntimeTest(unittest.TestCase):
    def test_returns_original_function_object_unchanged(self):
        def f(x, y):
            return x + y

        decorated = lineage(inputs=["s3://b/k"], outputs=["iceberg://w.db.t"])(f)
        # SAME object — no wrapping.
        self.assertIs(decorated, f)
        self.assertEqual(decorated(2, 3), 5)
        self.assertEqual(decorated.__name__, "f")

    def test_attribute_contents(self):
        @lineage(
            inputs=["s3://raw/sales/{date}.parquet"],
            outputs=["iceberg://warehouse.sales.bronze"],
            name="build_bronze",
            description="ingest raw sales",
        )
        def build_bronze():
            return None

        meta = getattr(build_bronze, LINEAGE_ATTR)
        self.assertEqual(meta["inputs"], ["s3://raw/sales/{date}.parquet"])
        self.assertEqual(meta["outputs"], ["iceberg://warehouse.sales.bronze"])
        self.assertEqual(meta["name"], "build_bronze")
        self.assertEqual(meta["description"], "ingest raw sales")

    def test_bare_form_marks_with_empty_datasets(self):
        @lineage
        def f():
            return 42

        self.assertEqual(f(), 42)
        meta = getattr(f, LINEAGE_ATTR)
        self.assertEqual(meta, {"inputs": [], "outputs": [], "name": None, "description": None})

    def test_empty_call_form(self):
        @lineage()
        def f():
            return 1

        self.assertEqual(f(), 1)
        self.assertEqual(getattr(f, LINEAGE_ATTR)["inputs"], [])

    def test_lenient_normalization_never_raises(self):
        # A bare string becomes a 1-element list; non-str scalars are coerced;
        # non-str name/description become None; None entries are dropped.
        @lineage(inputs="s3://b/only", outputs=[None, 123], name=7, description=object())
        def f():
            return None

        meta = getattr(f, LINEAGE_ATTR)
        self.assertEqual(meta["inputs"], ["s3://b/only"])
        self.assertEqual(meta["outputs"], ["123"])
        self.assertIsNone(meta["name"])
        self.assertIsNone(meta["description"])

    def test_stacking_with_fake_task_any_order(self):
        # Simulate Airflow's @task (returns the function unchanged). The lineage
        # attribute must survive regardless of decorator order.
        def fake_task(func):
            return func

        @fake_task
        @lineage(inputs=["s3://b/in"], outputs=["s3://b/out"])
        def below():
            return "b"

        @lineage(inputs=["s3://b/in"], outputs=["s3://b/out"])
        @fake_task
        def above():
            return "a"

        self.assertEqual(below(), "b")
        self.assertEqual(above(), "a")
        self.assertEqual(getattr(below, LINEAGE_ATTR)["outputs"], ["s3://b/out"])
        self.assertEqual(getattr(above, LINEAGE_ATTR)["inputs"], ["s3://b/in"])

    def test_each_function_gets_an_independent_dict(self):
        deco = lineage(inputs=["s3://b/x"], outputs=["s3://b/y"])

        @deco
        def f():
            return None

        @deco
        def g():
            return None

        fm = getattr(f, LINEAGE_ATTR)
        fm["inputs"].append("mutated")
        # g must not see f's mutation.
        self.assertEqual(getattr(g, LINEAGE_ATTR)["inputs"], ["s3://b/x"])


if __name__ == "__main__":
    unittest.main()
