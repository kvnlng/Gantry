"""Step D1 (§11.13.5): is `jpeg2k_encode` lossless for the supported dtypes
on the imagecodecs release under test?

Runs standalone -- no isocenter import -- so it can be pointed at a scratch
venv holding only `imagecodecs` and `numpy`, which is how the declared floor
was measured:

    python -m venv /tmp/floorvenv
    /tmp/floorvenv/bin/pip install "imagecodecs==<candidate>" numpy
    /tmp/floorvenv/bin/python probe_404_floor.py

`2023.9.18`, the floor `setup.py` declared before #404, publishes no cp312
wheel and does not build from source here, so it cannot be installed on this
project's own Python floor at all. `2024.6.1` is the next release, installs
from a wheel, and returns the same verdicts as 2026.8.16 on every cell below.

Note what this probe does **not** measure: whether the DICOM file built from
an exact codestream can be read back. `probe_404_matrix.py` measures that end
to end, and the two answers differ -- a 16-bit multi-sample frame encodes
exactly here and no pydicom decoding plugin will open the file. That gap is
why `_J2K_ENCODABLE_FRAMES` is keyed on `(itemsize, samples > 1)` rather than
on the codec's own verdict.
"""
import numpy as np
import imagecodecs

print("imagecodecs", imagecodecs.__version__)

rng = np.random.default_rng(404)
CASES = {
    "uint8": rng.integers(0, 256, (64, 64)).astype("uint8"),
    "int8": rng.integers(-128, 128, (64, 64)).astype("int8"),
    "uint16": rng.integers(0, 65536, (64, 64)).astype("uint16"),
    # Full range, so a lossy or a sign-losing encode cannot pass by accident.
    "int16": rng.integers(-32768, 32768, (64, 64)).astype("int16"),
    "uint8_rgb": rng.integers(0, 256, (16, 16, 3)).astype("uint8"),
    "int8_rgb": rng.integers(-128, 128, (16, 16, 3)).astype("int8"),
    # Exact at the codec and unreadable as a DICOM file: see the note above.
    "uint16_rgb": rng.integers(0, 65536, (16, 16, 3)).astype("uint16"),
    "int16_rgb": rng.integers(-32768, 32768, (16, 16, 3)).astype("int16"),
}

for name, arr in CASES.items():
    try:
        blob = imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")
        back = imagecodecs.jpeg2k_decode(blob)
        exact = np.array_equal(back, arr)
        print(f"  {name:10s} encoded {len(blob):6d}b  decoded dtype "
              f"{back.dtype!s:8s} exact={exact}  head={blob[:4].hex()}")
    except Exception as exc:  # noqa: BLE001
        print(f"  {name:10s} RAISED {type(exc).__name__}: {exc}")

print("refused population (must raise or be inexact):")
for name in ("uint32", "int32", "uint64", "int64", "float32", "bool"):
    arr = np.zeros((16, 16), dtype=name)
    if name in ("uint32", "int32"):
        arr = (rng.integers(0, 2 ** 31, (16, 16))).astype(name)
    try:
        blob = imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")
        back = imagecodecs.jpeg2k_decode(blob)
        print(f"  {name:8s} encoded, exact={np.array_equal(back, arr)}")
    except Exception as exc:  # noqa: BLE001
        print(f"  {name:8s} RAISED {type(exc).__name__}: {exc}")
