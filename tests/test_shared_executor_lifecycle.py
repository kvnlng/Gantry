import unittest
import concurrent.futures
from unittest.mock import MagicMock, patch
from isocenter.session import DicomSession
from isocenter.parallel import run_parallel
import time
import os

class TestSharedExecutorLifecycle(unittest.TestCase):
    def setUp(self):
        self.db_path = ":memory:"
        self.session = DicomSession(self.db_path)

    def tearDown(self):
        if hasattr(self, 'session'):
            self.session.close()

    def test_executor_initialized(self):
        """Verify that the executor is initialized in the constructor."""
        self.assertIsNotNone(self.session._executor)
        self.assertIsInstance(self.session._executor, concurrent.futures.ProcessPoolExecutor)
        # Verify it's running (not shutdown) - submitting a simple task
        future = self.session._executor.submit(sum, [1, 2])
        self.assertEqual(future.result(), 3)

    def test_executor_shutdown(self):
        """Verify that close() shuts down the executor."""
        executor = self.session._executor
        self.session.close()

        # Verify shutdown
        with self.assertRaises(RuntimeError):
            executor.submit(sum, [1, 2])

    @patch('isocenter.io_handlers.run_parallel')
    @patch('isocenter.session.DicomSession.save')
    def test_export_uses_fresh_recycled_pool_not_shared_executor(self, mock_save, mock_run_parallel):
        """Verify that export builds its own recycling pool instead of reusing the shared executor.

        ProcessPoolExecutor (the shared self._executor used by ingest) doesn't support
        worker recycling, so long export batches would leak memory across workers if they
        reused it. Export must always pass its own maxtasksperchild-bounded pool to
        run_parallel rather than the shared executor.
        """
        mock_run_parallel.return_value = []

        # Construct Object Graph to make total_instances > 0
        p = MagicMock()
        p.patient_id = "P1"
        st = MagicMock()
        se = MagicMock()
        inst = MagicMock()
        inst.instance_number = 1
        inst.sop_instance_uid = "1.2.3.4.5"

        # Link them
        p.studies = [st]
        st.series = [se]
        se.instances = [inst]

        # Add to store
        self.session.store.patients.append(p)

        # Act
        self.session.export("out_folder")

        # Assert
        self.assertTrue(mock_run_parallel.called)
        args, kwargs = mock_run_parallel.call_args

        # Export must request a recycled pool (bounds memory growth over long batches).
        self.assertIn('maxtasksperchild', kwargs)
        self.assertEqual(kwargs['maxtasksperchild'], 25)

        # ...and that pool must NOT be the shared, non-recycling self._executor.
        passed_executor = kwargs.get('executor')
        self.assertNotEqual(passed_executor, self.session._executor)

    @patch('isocenter.io_handlers.run_parallel')
    @patch('os.path.isfile')
    @patch('os.path.isdir')
    def test_ingest_uses_executor(self, mock_isdir, mock_isfile, mock_run_parallel):
        """Verify that ingest passes the executor to run_parallel."""
        # Setup mock behavior
        mock_isfile.return_value = True # Pretend it's a file
        mock_isdir.return_value = False

        mock_run_parallel.return_value = []

        # Act
        self.session.ingest("dummy_file.dcm")

        # Assert
        # Check that run_parallel was called with executor=self.session._executor
        # We need to ensure new_files was populated. Reference io_handlers.py
        # DicomStore.get_ingested_paths returns set(). Defaults are fine.

        self.assertTrue(mock_run_parallel.called)
        args, kwargs = mock_run_parallel.call_args
        self.assertIn('executor', kwargs)
        self.assertEqual(kwargs['executor'], self.session._executor)

    @patch('isocenter.io_handlers.run_parallel')
    @patch('os.path.isfile')
    def test_consistency_across_calls(self, mock_isfile, mock_run):
        """Verify that the same executor is reused across multiple calls."""
        mock_isfile.return_value = True
        mock_run.return_value = []

        self.session.ingest("file1")
        exec1 = mock_run.call_args[1].get('executor')

        self.session.ingest("file2")
        exec2 = mock_run.call_args[1].get('executor')

        self.assertEqual(exec1, exec2)
        self.assertEqual(exec1, self.session._executor)


def test_force_threads_does_not_reach_ingest(tmp_path, monkeypatch):
    """`ISOCENTER_FORCE_THREADS` is read and ignored by `ingest()` (#390, #363).

    Characterization: green on the code it was written against, and the
    `ISOCENTER_FORCE_THREADS` row in `docs/environment.md` is written
    from it (the #333 convention). The lever resolves to threads --
    `_use_threads` says so under the variable -- and `ingest()` hands
    `run_parallel()` the session's own `ProcessPoolExecutor` as
    `executor=`, which `_run_on_shared_executor` uses as given without
    consulting the strategy. Nothing warns. Whether ingest *should*
    honour the lever is a design question the spec rules out of scope
    (the shared executor exists so ingest does not pay a pool start-up
    per call); the row says what is.

    The spy replaces `isocenter.io_handlers`' binding of `run_parallel`
    -- that is the one `DicomImporter.import_files` calls; the session's
    own binding never sees an ingest -- and returns no results, so the
    junk file is never read. Killing mutation: `executor=self._executor`
    deleted from `ingest()`'s `import_files` call -- the spy then records
    `executor=None`.
    """
    import concurrent.futures

    from isocenter import parallel

    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.delenv("ISOCENTER_FORCE_PROCESSES", raising=False)
    monkeypatch.delenv("ISOCENTER_MAX_TASKS_PER_CHILD", raising=False)
    (tmp_path / "one.dcm").write_bytes(b"not a dicom file; never read")

    recorded = {}

    def spy(func, items, **kwargs):
        recorded.update(kwargs)
        return []

    monkeypatch.setattr("isocenter.io_handlers.run_parallel", spy)

    with DicomSession(str(tmp_path / "s.db")) as session:
        session.ingest(str(tmp_path / "one.dcm"))

        assert parallel._use_threads(False, None) is True, (
            "the lever is set; the strategy must resolve to threads for "
            "this to be a characterization of *ignoring* it")
        assert recorded.get("executor") is session._executor, (
            "ingest() must hand run_parallel the session's own executor; "
            f"it passed {recorded.get('executor')!r}")
        assert isinstance(session._executor,
                          concurrent.futures.ProcessPoolExecutor)


if __name__ == '__main__':
    unittest.main()
