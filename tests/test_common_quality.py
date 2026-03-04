from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMON_PATH = ROOT / "notebooks" / "databricks" / "_common.py"

SPEC = importlib.util.spec_from_file_location("db_common", COMMON_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load module from: {COMMON_PATH}")
db_common = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(db_common)


class CommonQualityTests(unittest.TestCase):
    def test_build_quality_rules_defaults(self) -> None:
        rules = db_common.build_quality_rules(None)
        self.assertTrue(rules["enforce_exact_input_count"])
        self.assertEqual(rules["min_silver_rows"], 1)
        self.assertEqual(rules["min_gold_rows"], 1)
        self.assertTrue(rules["fail_on_quality_violation"])

    def test_build_quality_rules_invalid_min_raises(self) -> None:
        with self.assertRaises(ValueError):
            db_common.build_quality_rules({"min_silver_rows": 0})

    def test_build_quality_report_passed(self) -> None:
        rules = db_common.build_quality_rules(
            {
                "enforce_exact_input_count": True,
                "min_silver_rows": 1,
                "min_gold_rows": 1,
            }
        )
        report = db_common.build_quality_report(
            quality_rules=rules,
            expected_raw_records=3,
            raw_records=3,
            bronze_records=3,
            silver_rows=3,
            gold_rows=3,
        )
        self.assertTrue(report["passed"])
        self.assertEqual(report["failed_checks"], [])

    def test_build_quality_report_failed(self) -> None:
        rules = db_common.build_quality_rules(
            {
                "enforce_exact_input_count": True,
                "min_silver_rows": 2,
                "min_gold_rows": 2,
            }
        )
        report = db_common.build_quality_report(
            quality_rules=rules,
            expected_raw_records=3,
            raw_records=2,
            bronze_records=2,
            silver_rows=1,
            gold_rows=1,
        )
        self.assertFalse(report["passed"])
        self.assertIn("raw_equals_expected_input", report["failed_checks"])
        self.assertIn("silver_min_rows", report["failed_checks"])
        self.assertIn("gold_min_rows", report["failed_checks"])

    def test_build_sla_rules_defaults(self) -> None:
        rules = db_common.build_sla_rules(None)
        self.assertEqual(rules["max_total_seconds"], 900)
        self.assertEqual(rules["min_raw_records"], 1)
        self.assertTrue(rules["require_quality_passed"])
        self.assertTrue(rules["fail_on_sla_violation"])

    def test_build_sla_report_failed(self) -> None:
        rules = db_common.build_sla_rules(
            {
                "max_total_seconds": 10,
                "max_extract_seconds": 5,
                "max_silver_seconds": 2,
                "max_gold_seconds": 2,
                "min_raw_records": 3,
                "require_quality_passed": True,
            }
        )
        report = db_common.build_sla_report(
            sla_rules=rules,
            timings={
                "extract_seconds": 6,
                "silver_seconds": 3,
                "gold_seconds": 3,
                "total_seconds": 12,
            },
            raw_records=1,
            quality_report={"passed": False},
        )
        self.assertFalse(report["passed"])
        self.assertIn("quality_gate_passed", report["failed_checks"])
        self.assertIn("total_seconds_sla", report["failed_checks"])

    def test_build_delta_config_defaults(self) -> None:
        config = db_common.build_delta_config(None)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["schema"], "weather_prd")
        self.assertTrue(config["merge_schema"])

    def test_build_delta_config_enabled_requires_schema(self) -> None:
        with self.assertRaises(ValueError):
            db_common.build_delta_config(
                {
                    "enabled": True,
                    "schema": "",
                    "silver_table": "silver_t",
                    "gold_table": "gold_t",
                    "checkpoint_table": "ck_t",
                }
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
