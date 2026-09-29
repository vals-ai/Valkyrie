"""The TLS helper must never turn missing or mismatched identity into a tunnel."""

import subprocess
import sys
import unittest
from pathlib import Path

from outbound_proxy.sni_acl import authorize


class TlsIdentityTest(unittest.TestCase):
    def test_accepts_exact_dns_identity_only(self) -> None:
        for line in (
            "valsmith.vals.ai:443 valsmith.vals.ai",
            "valsmith.vals.ai%3A443 valsmith.vals.ai",
            "VALSMITH.vals.ai:443 valsmith.vals.ai",
        ):
            with self.subTest(line=line):
                self.assertTrue(authorize(line + " -"))

    def test_rejects_missing_or_conflicting_identity(self) -> None:
        for line in (
            "valsmith.vals.ai:443 -",
            "valsmith.vals.ai:443 model-gateway.vals.ai",
            "valsmith.vals.ai:443 valsmith.vals.ai.attacker.example",
            "valsmith.vals.ai:443 valsmith.vals.ai.",
            "1.2.3.4:443 1.2.3.4",
            "[::1]:443 ::1",
            "valsmith.vals.ai:80 valsmith.vals.ai",
            "valsmith.vals.ai:0443 valsmith.vals.ai",
            "valsmith.vals.ai valsmith.vals.ai",
            "https://valsmith.vals.ai:443 valsmith.vals.ai",
            "secret@valsmith.vals.ai:443 valsmith.vals.ai",
            "valsmith.vals.ai:443/path valsmith.vals.ai",
            "valsmith.vals.ai:443 valsmith.vals.ai%00",
            "valsmith.vals.ai%253A443 valsmith.vals.ai",
            "valsmith.vals.ai:443 valsmith.vals.ai injected",
            "valsmith.vals.ai:443",
            "",
        ):
            with self.subTest(line=line):
                self.assertFalse(authorize(line + " -"))

    def test_process_recovers_from_invalid_input_without_echoing_it(self) -> None:
        helper = Path(__file__).parents[1] / "outbound_proxy" / "sni_acl.py"
        result = subprocess.run(
            [sys.executable, str(helper)],
            input="secret-canary\nvalsmith.vals.ai:443 valsmith.vals.ai -\n",
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertEqual(result.stdout, "ERR\nOK\n")
        self.assertEqual(result.stderr, "")
