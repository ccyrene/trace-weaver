import unittest

from traceweaver.scanners.datasets import connection_dataset, datasets_from_text


def _by_name(datasets):
    return {d.name: d for d in datasets}


class TestDatasetExtraction(unittest.TestCase):
    def test_object_store_uris(self):
        found = _by_name(
            datasets_from_text("read s3://bucket/in.parquet and gs://lake/out/")
        )
        self.assertEqual(found["s3://bucket/in.parquet"].dataset_type, "s3")
        self.assertEqual(found["gs://lake/out"].dataset_type, "gcs")

    def test_uri_not_double_counted_as_file(self):
        datasets = datasets_from_text("s3://lake/raw/orders.csv")
        # Reported once, as an s3 dataset (not also a bare 'file').
        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0].dataset_type, "s3")

    def test_bare_file_path(self):
        datasets = datasets_from_text("load /data/raw/input.csv now")
        names = [d.name for d in datasets]
        self.assertIn("/data/raw/input.csv", names)

    def test_https_url_is_ignored(self):
        # We do not claim http(s) support; such URLs must not become datasets.
        self.assertEqual(datasets_from_text("https://example.com/data.csv"), [])

    def test_connection_dataset(self):
        ds = connection_dataset("warehouse")
        self.assertEqual(ds.dataset_type, "connection")
        self.assertEqual(ds.namespace, "airflow_connection")
        self.assertEqual(ds.name, "warehouse")


if __name__ == "__main__":
    unittest.main()
