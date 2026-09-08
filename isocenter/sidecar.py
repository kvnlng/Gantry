# Module scope, deliberately. `fcntl` is POSIX-only and `setup.py`
# says `Operating System :: POSIX` because of it (#376). At module
# scope a Windows install fails at `import isocenter` -- `sidecar` is
# imported by `persistence`, which is imported by `session` -- where the
# packaging claim is checked; inside `write_frame`, where this used to
# be, the same install succeeded and failed at the first sidecar write.
# `tests/test_packaging_contract.py` walks the AST for this import at
# module scope, and a `try:` guard would make the classifier true and
# that walk's answer false.
import fcntl
import os
import zlib
from typing import Tuple


class SidecarManager:
    """
    Manages appending and reading from a binary sidecar file.

    Thread-safe for writes (append-only) using file locking (`fcntl`).
    Format: Raw concatenated blobs (optionally compressed). Offsets/lengths are
    managed by the caller (Instance object).
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            # Create empty file
            with open(self.filepath, 'wb'):
                pass

    def write_frame(self, data: bytes, compression: str = 'zlib') -> Tuple[int, int]:
        """
        Appends data to the sidecar file.

        Args:
            data (bytes): The binary data to store.
            compression (str): 'zlib' or 'raw'.

        Returns:
            Tuple[int, int]: (offset, length) of the written blob.
        """
        if compression == 'zlib':
            blob = zlib.compress(data)
        elif compression == 'raw':
            blob = data
        else:
            raise ValueError(f"Unsupported compression: {compression}")

        length = len(blob)

        # Process-Safe Locking using fcntl (POSIX). This flock is on the
        # sidecar's own inode and is a leaf: it serialises appends
        # against each other and nothing else. It does NOT serialise
        # writers against `compact_sidecar`, whose `os.replace` gives
        # the path a new inode -- a writer blocked here wakes and
        # appends into the unlinked one. That is the gate's job
        # (`SqliteStore._hold_sidecar_gate`, on a stable path beside
        # the sidecar), which is held by every caller of this method
        # and never taken inside it (#368).
        #
        # We assume strict append mode
        # Use r+b and explicit seek to ensure tell() is accurate and writes are
        # contiguous, avoiding 'ab' mode ambiguity in some environments.
        with open(self.filepath, 'r+b') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0, 2)  # Force Seek to End
                offset = f.tell()
                f.write(blob)
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        return offset, length

    def read_frame(self, offset: int, length: int, compression: str = 'zlib') -> bytes:
        """
        Reads a frame from the sidecar at the specified offset.

        Args:
            offset (int): File offset in bytes.
            length (int): Length of the blob to read.
            compression (str): Compression method used ('zlib' or 'raw').

        Returns:
            bytes: The decompressed/raw data.

        Raises:
            IOError: If read is incomplete.
            ValueError: If compression is unsupported.
        """

        with open(self.filepath, 'rb') as f:
            # print(f"  -> Sidecar: Seek {offset}", flush=True)
            f.seek(offset)
            # print(f"  -> Sidecar: Read {length}", flush=True)
            blob = f.read(length)

        if len(blob) != length:
            raise IOError(f"Incomplete read from sidecar. Expected {length}, got {len(blob)}.")

        if compression == 'zlib':

            try:
                dobj = zlib.decompressobj()
                chunks = []
                chunk_size = 1024 * 1024  # 1MB chunks
                total_in = len(blob)

                for i in range(0, total_in, chunk_size):
                    # print(f"    dchunk {i}/{total_in}", flush=True)
                    chunk_data = blob[i:i + chunk_size]
                    chunks.append(dobj.decompress(chunk_data))

                chunks.append(dobj.flush())
                res = b"".join(chunks)

                return res
            except Exception as e:
                # print(f"[Worker {os.getpid()}] Sidecar: DECOMPRESS ERROR: {e}", flush=True)
                raise e
        elif compression == 'raw':
            return blob
        else:
            raise ValueError(f"Unsupported compression: {compression}")

    size = property(lambda self: os.path.getsize(self.filepath))

    # No `__getstate__`/`__setstate__`. They existed to drop and rebuild a
    # `threading.Lock` that nothing ever acquired (#366); with it gone the
    # only attribute is `filepath`, a string, and default pickling carries
    # it. This class deliberately holds **no mutable state**, which is why
    # `compact_sidecar` could rebind `self.sidecar` for years without
    # consequence -- a fresh manager is indistinguishable from the one it
    # replaced. Keep it that way: adding mutable state here would make
    # every pickled copy in a spawned worker a separate answer, and would
    # resurrect the rebind as a real bug.
