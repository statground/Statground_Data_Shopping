import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_shopping_raw_outbox.sh")


class ShoppingRawOutboxGateTest(unittest.TestCase):
    def test_count_and_query_failures_are_reported_separately(self):
        cases = (
            ("0\n", "0", 0, "verified: 0 pending"),
            ("2\n", "0", 1, "holds 2 Gmarket or Kurly batches"),
            ("1\n2\n", "0", 1, "invalid count"),
            ("", "22", 1, "did not return a count"),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            curl = root / "curl"
            curl.write_text('#!/bin/sh\nprintf "%s" "$MOCK_BODY"\nexit "$MOCK_STATUS"\n')
            curl.chmod(0o700)
            summary = root / "summary"
            for body, status, expected_exit, message in cases:
                with self.subTest(body=body, status=status):
                    summary.write_text("")
                    completed = subprocess.run(
                        ["bash", str(SCRIPT), "http://unused.invalid/"],
                        env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}",
                             "CLICKHOUSE_USER": "test", "CLICKHOUSE_PASSWORD": "test",
                             "GITHUB_STEP_SUMMARY": str(summary), "MOCK_BODY": body, "MOCK_STATUS": status},
                        capture_output=True, text=True, timeout=5, check=False,
                    )
                    self.assertEqual(completed.returncode, expected_exit)
                    self.assertIn(message, completed.stdout)
                    self.assertEqual(summary.read_text(), completed.stdout)
