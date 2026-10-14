"""Synthetic auxiliary archives only: no production state or credentials."""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from zerotrader.archive import ResearchArchive
from zerotrader.cloud_auxiliary import (
    AuxiliaryError, FILE_KIND, MANIFEST_KIND, allowed_path, capture_auxiliary,
    restore_auxiliary,
)


class CloudAuxiliaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="zoh-synthetic-aux-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        app = self.root / "app"
        (app / "zerotrader").mkdir(parents=True)
        (app / "zerotrader" / "synthetic.py").write_text("SYNTHETIC = True\n")
        source = patch("zerotrader.archive.ROOT", app)
        source.start()
        self.addCleanup(source.stop)
        self.runtime = self.root / "source"
        self.archive = ResearchArchive(self.runtime)
        self.run = self.archive.start_run("synthetic", {}, {})
        self.target = self.root / "restored"

    def put(self, name, value):
        path = self.runtime / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value if isinstance(value, bytes) else json.dumps(value).encode())

    def capture(self):
        return capture_auxiliary(self.archive, self.run, self.runtime)

    def test_full_manifest_restores_exact_bytes_and_only_allowlisted_files(self):
        files = {
            "fill-archive-cursor.json": b'{"last_successful_poll":"2026-10-14T20:00:00Z"}\n',
            "archive-backfill.json": b'{"synthetic":true}',
            "research-history/20261012.json": b'{"result":"rejected"}',
            "reviews/2026-10-09.review.txt": "有限审核\n".encode(),
            "reviews/method.py": b"SYNTHETIC = True\n",
            "monitor-state.json": b'{"daily_reviews":{}}',
            "daily.json": b'{"SYN":[]}',
            "cloud-cutover.json": b'{"version":1}',
        }
        for name, raw in files.items():
            self.put(name, raw)
        # A trap file verifies no attempt to open unallowlisted state/credentials.
        self.put("state.json", b"invalid and not allowed")
        self.put("exports/raw.csv", b"not allowed")
        self.put("worker.json", b"not allowed")
        self.put("credential-fixture.json", b"not allowed")
        digest = self.capture()
        result = restore_auxiliary(self.archive.path, self.target)
        self.assertEqual(result["manifest_sha256"], digest)
        self.assertEqual(result["files"], len(files))
        for name, raw in files.items():
            self.assertEqual((self.target / name).read_bytes(), raw)
        self.assertFalse((self.target / "state.json").exists())
        self.assertFalse((self.target / "worker.json").exists())

    def test_unchanged_files_are_not_duplicated_but_a_b_a_manifest_is_latest(self):
        self.put("research.json", {"version": "A"})
        first = self.capture()
        before = self.archive.stats()
        self.assertEqual(self.capture(), first)
        self.assertEqual(self.archive.stats()["events"], before["events"])
        self.put("research.json", {"version": "B"})
        self.capture()
        self.put("research.json", {"version": "A"})
        self.assertEqual(self.capture(), first)
        restore_auxiliary(self.archive.path, self.target)
        self.assertEqual(json.loads((self.target / "research.json").read_text()), {"version": "A"})
        with self.archive._connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind=?", (FILE_KIND,)).fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind=?", (MANIFEST_KIND,)).fetchone()[0], 3)

    def test_deleted_current_file_stays_in_history_but_not_current_manifest(self):
        self.put("research.json", {"v": 1})
        self.capture()
        (self.runtime / "research.json").unlink()
        self.capture()
        restore_auxiliary(self.archive.path, self.target)
        self.assertFalse((self.target / "research.json").exists())
        with self.archive._connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind=?", (FILE_KIND,)).fetchone()[0], 1)

    def test_path_traversal_and_unreviewed_paths_rejected(self):
        for name in ("../latest.json", "/latest.json", "reviews/../latest.json",
                     "reviews//a.txt", "reviews\\a.txt", "C:/latest.json",
                     "reviews/.hidden.txt", "research-history/nested/a.json", "worker.json",
                     "reviews/__init__.py", "reviews/_helpers.py",
                     "credentials.json", "exports/a.csv", "reviews/a.exe", "latest.json:stream"):
            with self.subTest(name=name):
                self.assertFalse(allowed_path(name))

    def test_missing_manifest_never_creates_runtime(self):
        with self.assertRaises(AuxiliaryError):
            restore_auxiliary(self.archive.path, self.target)
        self.assertFalse(self.target.exists())

    def test_existing_destination_is_not_overwritten(self):
        self.put("research.json", {"v": 1})
        self.capture()
        self.target.mkdir()
        (self.target / "research.json").write_bytes(b"existing")
        with self.assertRaises(AuxiliaryError):
            restore_auxiliary(self.archive.path, self.target)
        self.assertEqual((self.target / "research.json").read_bytes(), b"existing")

    def test_credential_fields_in_allowlisted_json_are_rejected_without_output(self):
        self.put("research.json", {"api_key": "SYNTHETIC_NOT_A_REAL_SECRET"})
        with self.assertRaises(AuxiliaryError) as caught:
            self.capture()
        self.assertNotIn("SYNTHETIC_NOT_A_REAL_SECRET", str(caught.exception))
        self.assertEqual(self.archive.stats()["events"], 1)

    def test_corrupt_manifest_or_raw_hash_rejected_before_writing(self):
        raw = b"{}"
        payload = {"version": 1, "path": "research.json", "sha256": "0" * 64,
                   "bytes": len(raw), "base64": base64.b64encode(raw).decode()}
        blob = self.archive.put_blob(payload, FILE_KIND)
        self.archive.event(self.run, FILE_KIND, payload)
        manifest = {"version": 1, "total_bytes": len(raw), "excluded": [], "files": [
            {"path": payload["path"], "sha256": payload["sha256"], "bytes": len(raw), "payload_hash": blob}]}
        self.archive.event(self.run, MANIFEST_KIND, manifest)
        with self.assertRaises(AuxiliaryError):
            restore_auxiliary(self.archive.path, self.target)
        self.assertFalse(self.target.exists())

    def test_file_blob_without_file_event_is_not_accepted(self):
        raw = b"{}"
        payload = {"version": 1, "path": "research.json", "sha256": hashlib.sha256(raw).hexdigest(),
                   "bytes": 2, "base64": base64.b64encode(raw).decode()}
        blob = self.archive.put_blob(payload, FILE_KIND)
        self.archive.event(self.run, MANIFEST_KIND, {"version": 1, "total_bytes": 2, "excluded": [], "files": [
            {"path": "research.json", "sha256": payload["sha256"], "bytes": 2, "payload_hash": blob}]})
        with self.assertRaises(AuxiliaryError):
            restore_auxiliary(self.archive.path, self.target)

    def test_capacity_failure_keeps_prior_manifest(self):
        self.put("research.json", {"v": 1})
        self.capture()
        self.put("latest.json", {"v": "longer than capacity"})
        with patch("zerotrader.cloud_auxiliary.MAX_FILE_BYTES", 1):
            with self.assertRaises(AuxiliaryError):
                self.capture()
        restore_auxiliary(self.archive.path, self.target)
        self.assertFalse((self.target / "latest.json").exists())

    def test_reviewed_attachment_exclusions_are_disclosed_without_reading_bytes(self):
        names = ("reviews/peer-code-synthetic/vendor/example.py",
                 "reviews/peer-code-synthetic/package/__init__.py",
                 "reviews/peer-code-synthetic/_package/_helpers.py",
                 "reviews/claude-small-account-review-20261002-164726.json")
        for name in names:
            self.put(name, b"not valid JSON; must not be read")
        self.put("reviews/kept.txt", b"synthetic report")
        excluded_paths = {self.runtime / name for name in names}
        original_read_bytes = Path.read_bytes

        def guarded_read_bytes(path):
            if path in excluded_paths:
                raise AssertionError("Excluded attachment contents must not be read")
            return original_read_bytes(path)

        with patch.object(Path, "read_bytes", guarded_read_bytes):
            self.capture()
            result = restore_auxiliary(self.archive.path, self.target)
        self.assertEqual({item["path"] for item in result["excluded"]}, set(names))
        self.assertEqual(result["files"], 1)
        for name in names:
            self.assertFalse(allowed_path(name))
            self.assertFalse((self.target / name).exists())
        # A cold runtime lacks the excluded files, but the next manifest keeps
        # their disclosure from the verified archive history.
        for name in names:
            (self.runtime / name).unlink()
        self.capture()
        again = restore_auxiliary(self.archive.path, self.root / "second-restored")
        self.assertEqual(again["excluded"], result["excluded"])

    def test_invalid_or_unrecognized_exclusion_metadata_fails_before_restore(self):
        reason = "historical_peer_source_attachment_retained_locally"
        items = [{"path": name, "reason": reason} for name in (
            "reviews/peer-code-synthetic/../outside.py",
            "/reviews/peer-code-synthetic/__init__.py",
            "C:/reviews/peer-code-synthetic/__init__.py",
            "reviews/peer-code-synthetic//__init__.py",
            "reviews\\peer-code-synthetic\\__init__.py",
            "reviews/peer-code-synthetic/__init__.py:stream",
            "reviews/not-reviewed/__init__.py",
            "reviews/peer-code-synthetic/.hidden.py",
        )]
        items.append({"path": "reviews/peer-code-synthetic/__init__.py", "reason": "unknown"})
        for item in items:
            with self.subTest(item=item):
                self.archive.event(self.run, MANIFEST_KIND, {
                    "version": 1, "files": [], "total_bytes": 0, "excluded": [item]})
                with self.assertRaises(AuxiliaryError):
                    restore_auxiliary(self.archive.path, self.target)
                self.assertFalse(self.target.exists())


if __name__ == "__main__":
    unittest.main()
