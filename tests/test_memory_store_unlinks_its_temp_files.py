"""A `:memory:` store's temp sidecar and lock files go away with it (#376).

`SqliteStore(":memory:")` backs its sidecar with a
`NamedTemporaryFile(delete=False)`, and until #376 nothing unlinked it:
measured, the file was still on disk after `close()`. With the sidecar
gate and the pass-lock (#368) that is three files per `:memory:`
session -- `<tmp>_pixels.bin`, `<tmp>_pixels.bin.lock`,
`<tmp>_pixels.bin.pass.lock` -- leaked into the system temp directory
for every short-lived session.

`stop()` now unlinks all three, **only** when the store owns them. Two
populations must not:

- A **pickled clone**. `SqliteStore` is pickled whole into spawned
  workers, and `tests/test_save_redact_race.py` calls `clone.stop()` on
  such clones. A clone that unlinked the parent's sidecar mid-run would
  take every frame the parent still references with it. So ownership is
  a flag `__getstate__` drops and `__setstate__` sets `False`.
- A **file-backed store**. Its sidecar is data, and its lock files are
  stable paths other processes of this session may be polling.
  Zero files unlinked for file-backed; three for `:memory:`.
"""
import os
import pickle

from isocenter.persistence import SqliteStore


def _touch(path):
    with open(path, "a", encoding="utf-8"):
        pass


def _three_paths(store):
    return (store.sidecar_path, store._gate_path(), store._pass_lock_path())


def test_a_memory_store_unlinks_its_sidecar_and_lock_files_on_stop():
    store = SqliteStore(":memory:")
    paths = _three_paths(store)
    # The gate and pass-lock files are created lazily by the first
    # acquisition; a store that never wrote a frame has none. Stand
    # them up so the unlink has something to remove for each path.
    for path in paths[1:]:
        _touch(path)
    assert all(os.path.exists(p) for p in paths)

    store.stop()

    leaked = [p for p in paths if os.path.exists(p)]
    assert not leaked, (
        f"stop() left {leaked} behind; a :memory: store's temp sidecar "
        "and lock files leak into the system temp directory once per "
        "session (#376)")


def test_a_pickled_clone_does_not_unlink_the_parents_files():
    store = SqliteStore(":memory:")
    try:
        paths = _three_paths(store)
        for path in paths[1:]:
            _touch(path)

        clone = pickle.loads(pickle.dumps(store))
        assert clone.sidecar_path == store.sidecar_path, (
            "the clone must point at the same sidecar, or the test is not "
            "exercising a shared file")
        clone.stop()

        missing = [p for p in paths if not os.path.exists(p)]
        assert not missing, (
            f"a pickled clone's stop() unlinked the parent's {missing}: "
            "a spawned worker's teardown would take every frame the "
            "parent still references with it (#376)")
    finally:
        store.stop()


def test_a_file_backed_store_leaves_its_sidecar_and_lock_files(tmp_path):
    store = SqliteStore(str(tmp_path / "keep.db"))
    paths = _three_paths(store)
    for path in paths[1:]:
        _touch(path)

    store.stop()

    missing = [p for p in paths if not os.path.exists(p)]
    assert not missing, (
        f"stop() on a file-backed store unlinked {missing}; the sidecar "
        "is data and the lock files are stable paths other processes of "
        "this session may be polling (#376)")
