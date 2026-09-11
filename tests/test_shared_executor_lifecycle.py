import unittest
import concurrent.futures
from unittest.mock import MagicMock, patch
from isocenter.session import DicomSession
from isocenter.parallel import run_parallel
import time
import os

import pytest

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


_LEVERS = ("ISOCENTER_FORCE_THREADS", "ISOCENTER_FORCE_PROCESSES",
           "ISOCENTER_MAX_TASKS_PER_CHILD")


def _clear_levers(monkeypatch):
    for name in _LEVERS:
        monkeypatch.delenv(name, raising=False)


def _lever_records(caplog):
    """Every record that names an `ISOCENTER_` variable.

    Read from `caplog`, which hangs on the root logger: `configure_logger()`
    resets the `isocenter` logger's own handlers when a session opens, so
    a handler attached there before the session would count nothing.
    """
    return [r for r in caplog.records if "ISOCENTER_" in r.getMessage()]


def _spy_on_dispatch(monkeypatch, caplog, recorded):
    """Replace `io_handlers`' binding of `run_parallel` with a recorder.

    That is the binding `DicomImporter.import_files` calls; the session's
    own never sees an ingest. It returns no results, so no file is read,
    and it notes how many lever records existed when it was called, so a
    test can say the warning came *before* the dispatch.
    """
    def spy(func, items, **kwargs):
        recorded.update(kwargs)
        recorded["items"] = list(items)
        recorded["lever_records_at_dispatch"] = len(_lever_records(caplog))
        return []

    monkeypatch.setattr("isocenter.io_handlers.run_parallel", spy)


def test_force_threads_does_not_reach_ingest(tmp_path, monkeypatch, caplog):
    """`ISOCENTER_FORCE_THREADS` does not reach `ingest()`, which says so (#390, #393).

    The lever resolves to threads -- `_resolve_execution_choice` says so
    under the variable -- and `ingest()` hands `run_parallel()` the
    session's own `ProcessPoolExecutor` as `executor=`, which
    `_run_on_shared_executor` uses as given without consulting the
    strategy. Whether ingest *should* honour the lever is a design
    question the spec rules out of scope (the shared executor exists so
    ingest does not pay a pool start-up per call). Until #393 nothing
    said so; the ruling there was to warn, once per `ingest()` call that
    dispatches, in #400's shape: it names the variable, says the result
    is unaffected, and says where the variable does apply. The
    `ISOCENTER_FORCE_THREADS` row in `docs/environment.md` is written
    from this test (the #333 convention).

    Killing mutations: `executor=self._executor` deleted from `ingest()`'s
    `import_files` call (the spy records `executor=None`); the warning
    deleted (no record); the warning moved after the dispatch (the spy
    sees none when it is called).
    """
    import concurrent.futures

    from isocenter import parallel

    _clear_levers(monkeypatch)
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    (tmp_path / "one.dcm").write_bytes(b"not a dicom file; never read")

    recorded = {}
    _spy_on_dispatch(monkeypatch, caplog, recorded)

    with DicomSession(str(tmp_path / "s.db")) as session:
        caplog.clear()
        session.ingest(str(tmp_path / "one.dcm"))
        records = _lever_records(caplog)

        assert parallel._resolve_execution_choice(
                False, None, None).use_threads is True, (
            "the lever is set; the strategy must resolve to threads for "
            "this to be a characterization of *ignoring* it")
        assert recorded.get("executor") is session._executor, (
            "ingest() must hand run_parallel the session's own executor; "
            f"it passed {recorded.get('executor')!r}")
        assert isinstance(session._executor,
                          concurrent.futures.ProcessPoolExecutor)

    assert len(records) == 1, [r.getMessage() for r in records]
    (record,) = records
    message = record.getMessage()
    assert record.levelname == "WARNING", record.levelname
    assert "ISOCENTER_FORCE_THREADS" in message, message
    assert "had no effect on this ingest()" in message, message
    assert "result is unaffected" in message, message
    assert recorded["lever_records_at_dispatch"] == 1, (
        "the warning must be said before the work is dispatched, so a "
        "dispatch that raises cannot swallow it")


def test_ingest_with_no_lever_is_silent_where_threads_are_the_default(
        tmp_path, monkeypatch, caplog):
    """A free-threaded build resolves to threads, and nobody asked (#393).

    `use_threads` is True here with no lever set, so a warning
    conditioned on `use_threads` alone would fire on every free-threaded
    `ingest()` -- the gate's 3.14t leg -- about a variable the operator
    never touched.
    """
    import sys

    from isocenter import parallel

    _clear_levers(monkeypatch)
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    assert parallel._resolve_execution_choice(
        False, None, None).use_threads is True, (
        "the free-threaded simulation must resolve to threads, or this "
        "test proves nothing about the default")
    (tmp_path / "one.dcm").write_bytes(b"not a dicom file; never read")

    recorded = {}
    _spy_on_dispatch(monkeypatch, caplog, recorded)

    with DicomSession(str(tmp_path / "s.db")) as session:
        caplog.clear()
        session.ingest(str(tmp_path / "one.dcm"))
        records = _lever_records(caplog)

    assert "items" in recorded, "the ingest never dispatched"
    assert records == [], [r.getMessage() for r in records]


def test_force_threads_with_recycling_warns_exactly_once_at_ingest(
        tmp_path, monkeypatch, caplog):
    """With recycling also set, #185's line is the one line (#393).

    Recycling beats the threads request, so `use_threads` is False and
    `threads_requested_by` is None, and #185's warning -- emitted inside
    the real `run_parallel`, and already naming `ingest()` -- is what an
    operator reads. A second line from `ingest()` would say the same
    thing twice. The real `run_parallel` runs, so the one junk file is
    read by the session's pool and comes back as a failure.
    """
    _clear_levers(monkeypatch)
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    monkeypatch.setenv("ISOCENTER_MAX_TASKS_PER_CHILD", "2")
    (tmp_path / "one.dcm").write_bytes(b"not a dicom file")

    with DicomSession(str(tmp_path / "s.db")) as session:
        caplog.clear()
        summary = session.ingest(str(tmp_path / "one.dcm"))
        records = _lever_records(caplog)

    assert len(summary.failures) == 1, summary
    assert len(records) == 1, [r.getMessage() for r in records]
    assert "worker recycling" in records[0].getMessage(), (
        records[0].getMessage())


def test_an_ingest_with_nothing_new_is_silent(tmp_path, monkeypatch, caplog):
    """No files to read, no dispatch, nothing to warn about (#393).

    Re-ingesting a folder already held lands here too, and an operator
    running the same ingest in a loop should not read the line each time
    about work that never started.
    """
    _clear_levers(monkeypatch)
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    empty = tmp_path / "empty"
    empty.mkdir()

    recorded = {}
    _spy_on_dispatch(monkeypatch, caplog, recorded)

    with DicomSession(str(tmp_path / "s.db")) as session:
        caplog.clear()
        session.ingest(str(empty))
        records = _lever_records(caplog)

    assert "items" not in recorded, "nothing new, so nothing to dispatch"
    assert records == [], [r.getMessage() for r in records]


@pytest.mark.parametrize("pool", ["none", "threads"])
def test_a_direct_import_whose_executor_can_honour_threads_is_silent(
        tmp_path, monkeypatch, caplog, pool):
    """The warning belongs to a process pool that ignores the lever (#393).

    A direct `import_files` call with no executor gets `run_parallel`'s
    own pool, which does honour `ISOCENTER_FORCE_THREADS`; one handed a
    `ThreadPoolExecutor` is already in threads. Neither had its lever
    ignored, so neither is told it was.
    """
    import concurrent.futures

    from isocenter.io_handlers import DicomImporter
    from isocenter.store import DicomStore

    _clear_levers(monkeypatch)
    monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")
    junk = tmp_path / "one.dcm"
    junk.write_bytes(b"not a dicom file; never read")

    recorded = {}
    _spy_on_dispatch(monkeypatch, caplog, recorded)

    if pool == "threads":
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            caplog.clear()
            DicomImporter.import_files([str(junk)], DicomStore(),
                                       executor=executor)
    else:
        caplog.clear()
        DicomImporter.import_files([str(junk)], DicomStore(), executor=None)

    assert "items" in recorded, "the import never dispatched"
    records = _lever_records(caplog)
    assert records == [], [r.getMessage() for r in records]


if __name__ == '__main__':
    unittest.main()
