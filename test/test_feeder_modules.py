import unittest
import tempfile
import os
import sys
import json
from unittest.mock import MagicMock

# Insert lib into sys.path
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(TEST_DIR)
sys.path.insert(0, PROJECT_DIR)

from lib.scanner import scan_drop_zone, sort_batches_fifo, verify_payload_stability, parse_manifest_filename
from lib.schema_guard import extract_csv_columns, generate_staging_ddl, load_entity_mappings, match_target_table_for_file
from lib.manifest_factory import compute_file_md5, create_standardized_manifest, vault_raw_manifest

class TestScannerAndFIFO(unittest.TestCase):
    def test_parse_manifest_filename_varieties(self):
        # Format A: MANIFEST_ORACLE_FUSION_20261012_120000_1003.MF
        meta_a = parse_manifest_filename("MANIFEST_ORACLE_FUSION_20261012_120000_1003.MF")
        self.assertEqual(meta_a["source"], "ORACLE_FUSION")
        self.assertEqual(meta_a["batch_id"], "1003")
        self.assertEqual(meta_a["extract_timestamp"], "20261012_120000")

        # Format B: MANIFEST-batch401-.MF
        meta_b = parse_manifest_filename("MANIFEST-batch401-.MF")
        self.assertEqual(meta_b["source"], "ORACLE_FUSION")
        self.assertEqual(meta_b["batch_id"], "401")

    def test_fifo_sorting(self):
        batches = [
            {"batch_id": "3", "sort_key": (1700000200.0, "3")},
            {"batch_id": "1", "sort_key": (1700000000.0, "1")},
            {"batch_id": "2", "sort_key": (1700000100.0, "2")}
        ]
        sorted_b = sort_batches_fifo(batches)
        self.assertEqual([b["batch_id"] for b in sorted_b], ["1", "2", "3"])

    def test_payload_stability_checks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # File 1 exists with data
            f1 = os.path.join(tmpdir, "file1.csv")
            with open(f1, "w") as f:
                f.write("A,B\n1,2\n")

            # Missing file test
            ok, msg = verify_payload_stability(tmpdir, ["file1.csv", "missing.csv"], settling_window_sec=0)
            self.assertFalse(ok)
            self.assertIn("Missing", msg)

            # Zero-byte file test
            f_zero = os.path.join(tmpdir, "zero.csv")
            with open(f_zero, "w") as f:
                pass
            ok, msg = verify_payload_stability(tmpdir, ["file1.csv", "zero.csv"], settling_window_sec=0)
            self.assertFalse(ok)
            self.assertIn("empty", msg)

            # Success test
            ok, msg = verify_payload_stability(tmpdir, ["file1.csv"], settling_window_sec=0)
            self.assertTrue(ok)

class TestSchemaGuard(unittest.TestCase):
    def test_extract_csv_columns_sanitization(self):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write('"Invoice Id", "Amount ($)", Order# ,DATE\n1,100,2,2026-01-01\n')
            tmp_path = tmp.name

        try:
            cols = extract_csv_columns(tmp_path)
            self.assertEqual(cols, ["INVOICE_ID", "AMOUNT", "ORDER", "DATE"])
        finally:
            os.remove(tmp_path)

    def test_generate_staging_ddl(self):
        ddl = generate_staging_ddl("test_stg", "ERP_TEST", ["COL_A", "COL_B"])
        self.assertIn("CREATE MULTISET TABLE test_stg.ERP_TEST", ddl)
        self.assertIn("COL_A VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC", ddl)
        self.assertIn("COL_B VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC", ddl)
        self.assertIn("INFA_LAST_REPLICATED TIMESTAMP(0)", ddl)
        self.assertIn("INFA_BATCH_ID VARCHAR(100)", ddl)
        self.assertIn("NO PRIMARY INDEX", ddl)

    def test_entity_mapping_matching(self):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write("SOURCE,TARGET_TABLE,FILE_SIGNATURE,EXTENSION\n")
            tmp.write("ORACLE_FUSION,ERP_USER,userpvo,.csv\n")
            tmp.write("ORACLE_FUSION,ERP_PO,poheader,.csv\n")
            map_path = tmp.name

        try:
            rules = load_entity_mappings(map_path, filter_source="ORACLE_FUSION")
            self.assertEqual(len(rules), 2)
            self.assertEqual(match_target_table_for_file("file_userpvo-batch1001-.csv", rules), "ERP_USER")
            self.assertEqual(match_target_table_for_file("file_poheader-1.csv", rules), "ERP_PO")
            self.assertIsNone(match_target_table_for_file("unknown_file.csv", rules))
        finally:
            os.remove(map_path)

class TestManifestFactory(unittest.TestCase):
    def test_md5_and_standardized_manifest_and_vaulting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test data file
            df = os.path.join(tmpdir, "data_test.csv")
            with open(df, "w") as f:
                f.write("ID,VAL\n1,Alpha\n")

            md5_val = compute_file_md5(df)
            self.assertTrue(len(md5_val) == 32)

            # Create standardized manifest
            out_mf = create_standardized_manifest(
                output_dir=tmpdir,
                source="ORACLE_FUSION",
                extract_timestamp="20261012_150000",
                batch_id="9999",
                file_checksum_pairs=[("data_test.csv", md5_val)]
            )
            self.assertTrue(os.path.exists(out_mf))
            with open(out_mf, "r") as f:
                content = f.read()
            self.assertIn("data_test.csv," + md5_val, content)

            # Raw manifest vaulting
            raw_mf = os.path.join(tmpdir, "MANIFEST-batch9999-.MF")
            with open(raw_mf, "w") as f:
                f.write("data_test.csv\n")

            vault_dir = vault_raw_manifest(
                raw_manifest_path=raw_mf,
                archive_base_dir=os.path.join(tmpdir, "archive"),
                source="ORACLE_FUSION",
                extract_timestamp="20261012_150000",
                batch_id="9999",
                receipt_payload=[{"file": "data_test.csv", "md5": md5_val}]
            )

            self.assertTrue(os.path.exists(os.path.join(vault_dir, "MANIFEST-batch9999-.MF")))
            receipt_json = os.path.join(vault_dir, "intake_receipt.json")
            self.assertTrue(os.path.exists(receipt_json))
            with open(receipt_json, "r") as f:
                r_data = json.load(f)
            self.assertEqual(r_data["batch_id"], "9999")
            self.assertEqual(r_data["source"], "ORACLE_FUSION")

if __name__ == "__main__":
    unittest.main()
