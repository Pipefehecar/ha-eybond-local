"""Recovery evidence must distinguish absence/corruption without repairing either."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from custom_components.eybond_local.support.shadow_learning.session import (
    active_shadow_learning_state_path,
    inspect_shadow_learning_recovery,
)


class RecoveryDiagnosticsTests(unittest.TestCase):
    def test_disk_states_are_distinct_and_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = active_shadow_learning_state_path(root)
            path.parent.mkdir(parents=True)
            def inspect():
                return inspect_shadow_learning_recovery(root, entry_id="entry", collector_pn="PN")
            self.assertEqual(inspect(), {"shadow_learning_recovery_disk_status": "missing"})
            cases = [
                (b"{", "invalid_json"), (b"[]", "invalid_record"),
                (b"x" * 65537, "oversized"),
                (b'{"entry_id":"other", "collector_pn":"secret"}', "foreign_entry"),
                (b'{"entry_id":"entry", "collector_pn":"other"}', "collector_mismatch"),
                (b'{"entry_id":"entry", "collector_pn":""}', "identity_incomplete"),
            ]
            for contents, expected in cases:
                with self.subTest(expected=expected):
                    path.write_bytes(contents)
                    self.assertEqual(inspect(), {"shadow_learning_recovery_disk_status": expected})
                    self.assertEqual(path.read_bytes(), contents)
            with patch.object(Path, "open", side_effect=PermissionError("private/path")):
                self.assertEqual(inspect(), {"shadow_learning_recovery_disk_status": "unreadable"})

    def test_current_record_exports_only_safe_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = active_shadow_learning_state_path(root)
            path.parent.mkdir(parents=True)
            payload = {
                "entry_id": "entry", "collector_pn": "PN", "status": "restore_failed",
                "restore_required": True, "restore_attempt_count": 2,
                "original_endpoint": "private.example,18899,TCP",
                "last_restore_error": "secret credentials and routes",
                "trace_path": "/private/file", "unexpected_secret": "do not export",
            }
            path.write_text(json.dumps(payload))
            before = path.read_bytes()
            result = inspect_shadow_learning_recovery(root, entry_id="entry", collector_pn="PN")
            self.assertEqual(result, {
                "shadow_learning_recovery_disk_status": "current_entry",
                "shadow_learning_recovery_record_status": "restore_failed",
                "shadow_learning_recovery_pending": True,
                "shadow_learning_recovery_restore_required": True,
                "shadow_learning_recovery_original_endpoint_present": True,
                "shadow_learning_recovery_last_restore_error_present": True,
                "shadow_learning_recovery_attempt_count": 2,
            })
            self.assertEqual(path.read_bytes(), before)
            payload["status"] = "private error string"
            payload["restore_attempt_count"] = "private string"
            path.write_text(json.dumps(payload))
            result = inspect_shadow_learning_recovery(root, entry_id="entry", collector_pn="PN")
            self.assertEqual(result["shadow_learning_recovery_record_status"], "unknown")
            self.assertNotIn("shadow_learning_recovery_attempt_count", result)


if __name__ == "__main__":
    unittest.main()
