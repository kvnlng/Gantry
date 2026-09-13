"""Fixed project secrets for tests that need to know which one a store holds.

In `tests/support/`, not at the top of `tests/`: a top-level module that
is not `test_*.py` or `conftest.py` is one pytest never collects, and
`test_packaging_contract` refuses it (#347). No `__init__.py`, so the
directory is a namespace package and adds no second `__init__.py`
basename for the source-citation index to disambiguate.

Not a conftest autouse, and not a patch of the generator. A test that
decides a store's secret says so explicitly, by writing one of these to a
file and loading it into that store. Patching the generator instead would
make every store in the process share one secret, and "two stores that
share a secret agree" would then pass by accident -- the property the
cross-store tests exist to check.

The file is written here by hand rather than through
`SqliteStore.write_project_secret`, so a change to the production writer
cannot move both sides of a round trip together. No `isocenter` import,
so the mutation probe charges coverage to the test that uses this, not
to this helper.
"""
import os

#: Two distinct fixed secrets. Every literal pinned under one of them in
#: the suite was computed once from the implementation and pasted.
FIXED_A = bytes(range(32))
FIXED_B = bytes(range(1, 33))


def write_secret_file(directory, secret=FIXED_A, name=None):
    """Write `secret` in the v1 file format; return the path."""
    path = os.path.join(str(directory),
                        name or f"secret-{secret[:2].hex()}.txt")
    with open(path, "w", encoding="ascii") as handle:
        handle.write(f"isocenter-project-secret-v1:{secret.hex()}\n")
    return path


def load_fixed_secret(session, directory, secret=FIXED_A):
    """Give `session`'s store `secret` before its first audit()."""
    path = write_secret_file(directory, secret,
                             name=f"load-{secret[:2].hex()}-{id(session)}.txt")
    session.store_backend.load_project_secret(path)
    return path
