
import pytest
import numpy as np
import zlib
import hashlib

@pytest.mark.parametrize("dtype, shape", [
    (np.uint8, (100, 100)),
    (np.uint16, (100, 100)),
    (np.uint8, (100, 100, 3)), # RGB
])
def test_zlib_compression_hash_consistency(dtype, shape):
    """
    Verifies that the SHA256 hash of the raw bytes of a numpy array
    matches the hash of the decompressed bytes after a zlib round-trip.

    This ensures that our persistence integrity checks (which hash data before write)
    match the validation checks (which hash data after read/decompression).
    """
    # 1. Create array
    if dtype == np.uint16:
        arr = np.ones(shape, dtype=dtype) * 60000
    else:
        arr = np.ones(shape, dtype=dtype) * 255

    # 2. Modify to ensure non-uniformity (simulate real data/redaction)
    if len(shape) == 2:
        arr[0:10, 0:10] = 0
    else:
        arr[0:10, 0:10, :] = 0

    # 3. Hash via tobytes() (Persistence Logic reference)
    hash_tobytes = hashlib.sha256(arr.tobytes()).hexdigest()

    # 4. Hash via Compress/Decompress cycle (Sidecar verification logic)
    compressed = zlib.compress(arr)
    decompressed = zlib.decompress(compressed)
    hash_cycle = hashlib.sha256(decompressed).hexdigest()

    assert hash_tobytes == hash_cycle, (
        f"Hash mismatch for {dtype} {shape}:\n"
        f"  Original: {hash_tobytes}\n"
        f"  Cycle:    {hash_cycle}"
    )
