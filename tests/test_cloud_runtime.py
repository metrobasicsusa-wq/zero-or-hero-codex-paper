import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zerotrader.storage import data_dir, named_lock


class CloudRuntimeTests(unittest.TestCase):
    def test_missing_or_relative_cloud_directory_is_rejected(self):
        with patch.dict(os.environ, {'ZOH_PRIVATE_RUNTIME': 'relative-runtime'}):
            with self.assertRaises(RuntimeError):
                data_dir()

    def test_explicit_runtime_does_not_create_empty_state(self):
        with tempfile.TemporaryDirectory() as root:
            target = str(Path(root) / 'missing')
            with patch.dict(os.environ, {'ZOH_PRIVATE_RUNTIME': target}):
                with self.assertRaises(RuntimeError):
                    data_dir()
            self.assertFalse(Path(target).exists())

    def test_existing_private_runtime_and_singleton(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {'ZOH_PRIVATE_RUNTIME': root}):
                self.assertEqual(data_dir(), Path(root))
            with named_lock(root, 'worker.lock'):
                with self.assertRaises(RuntimeError):
                    with named_lock(root, 'worker.lock'):
                        pass
            with named_lock(root, 'worker.lock'):
                pass
