import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from support import scan_files
import traceweaver.exporters.mermaid_exporter as me
from traceweaver.exporters.mermaid_exporter import MermaidExporter

GRAPH_DAG = """
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


def helper():
    return 1


def my_callable():
    helper()


with DAG(dag_id="g") as dag:
    a = PostgresOperator(
        task_id="a",
        python_callable=my_callable,
        postgres_conn_id="wh",
        sql="INSERT INTO out.t SELECT * FROM in.t",
    )
    t = PostgresOperator(
        task_id="t",
        sql="INSERT INTO final.t SELECT * FROM out.t",
    )
    a >> t
"""


class TestMermaidExporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = scan_files(g=GRAPH_DAG)
        cls.graph = MermaidExporter(Path(".")).render(cls.result)

    def test_header_is_left_to_right(self):
        # init directive then the LR flowchart declaration.
        self.assertIn("flowchart LR", self.graph)
        self.assertIn("direction LR", self.graph)

    def test_labels_dag_and_tasks(self):
        self.assertIn('"DAG: g"', self.graph)  # DAG name in subgraph title
        # Task names appear as plain text (no <b>/<i>, which break wherever
        # Mermaid runs with htmlLabels off — e.g. GitHub, VS Code).
        self.assertIn("a<br/>calls:", self.graph)  # task 'a' card + its calls
        self.assertIn('["t"]', self.graph)  # task 't' (no calls) as a plain node
        self.assertNotIn("<b>", self.graph)
        self.assertNotIn("<i>", self.graph)

    def test_node_shapes_and_classes(self):
        self.assertIn('[("in.t")]', self.graph)  # table -> cylinder
        self.assertIn('[("out.t")]', self.graph)
        self.assertIn('{{"wh"}}', self.graph)  # connection -> hexagon
        self.assertIn(":::task", self.graph)
        self.assertIn(":::dataset", self.graph)

    def test_calls_listed_inside_card_not_as_edges(self):
        # Functions are listed inside the task card, not drawn as separate nodes.
        self.assertIn("calls:", self.graph)
        self.assertIn("1. helper", self.graph)
        self.assertNotIn(" -. calls .-> ", self.graph)
        self.assertNotIn('(["helper"])', self.graph)

    def test_edge_styles(self):
        self.assertIn(" ==> ", self.graph)  # task dependency (thick)
        self.assertIn(" --> ", self.graph)  # lineage (solid)
        self.assertIn(" -.-> ", self.graph)  # low-confidence (dotted, e.g. conn)

    def test_edges_are_deduped(self):
        edges = [
            ln
            for ln in self.graph.splitlines()
            if "==>" in ln or "-->" in ln or "-.->" in ln
        ]
        self.assertEqual(len(edges), len(set(edges)))

    def test_writes_only_mmd(self):
        with tempfile.TemporaryDirectory() as tmp:
            notes = MermaidExporter(Path(tmp)).export(self.result)
            self.assertTrue((Path(tmp) / "lineage.mmd").exists())
            self.assertFalse((Path(tmp) / "lineage.md").exists())  # .mmd only
            self.assertEqual(notes, [])  # no image rendering requested

    def test_image_render_warns_gracefully_when_mmdc_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(me.shutil, "which", return_value=None):
                notes = MermaidExporter(Path(tmp), image_formats=("svg", "png")).export(
                    self.result
                )
            # Text artifacts are still written; a warning explains the skip.
            self.assertTrue((Path(tmp) / "lineage.mmd").exists())
            self.assertEqual(len(notes), 1)
            self.assertIn("mmdc", notes[0])


if __name__ == "__main__":
    unittest.main()
