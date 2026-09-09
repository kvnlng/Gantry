"""#404 detail: container format, the int32 edge, RGB, and the pydicom error."""
import io
import numpy as np


def magic(blob):
    if blob[:4] == b"\x00\x00\x00\x0c" and blob[4:8] == b"jP  ":
        return "JP2 box  " + blob[:8].hex()
    if blob[:2] == b"\xff\x4f":
        return "J2K codestream " + blob[:4].hex()
    return "unknown " + blob[:8].hex()


def main():
    from PIL import Image
    import imagecodecs

    print("=== 1. container format written by each encoder (uint16) ===")
    a = np.arange(256, dtype=np.uint16).reshape(16, 16)
    bio = io.BytesIO()
    Image.fromarray(a).save(bio, format="JPEG2000", compression="lossless")
    print("  Pillow, as io_handlers calls it today :", magic(bio.getvalue()))
    bio2 = io.BytesIO()
    Image.fromarray(a).save(bio2, format="JPEG2000", compression="lossless",
                            no_jp2=True)
    print("  Pillow with no_jp2=True              :", magic(bio2.getvalue()))
    print("  imagecodecs codecformat='J2K'        :",
          magic(imagecodecs.jpeg2k_encode(a, level=0, codecformat="J2K")))
    print("  imagecodecs default                  :",
          magic(imagecodecs.jpeg2k_encode(a, level=0)))

    print()
    print("=== 2. int32: where does imagecodecs stop being exact? ===")
    for bits in (8, 12, 16, 20, 24, 26, 30, 31, 32):
        hi = 2 ** (bits - 1) - 1
        lo = -(2 ** (bits - 1))
        arr = np.linspace(lo, hi, 256).astype(np.int32).reshape(16, 16)
        arr[0, 0], arr[0, 1] = lo, hi
        try:
            blob = imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")
            back = imagecodecs.jpeg2k_decode(blob)
            ok = np.array_equal(back, arr) and back.dtype == arr.dtype
            print("  int32 %2d-bit range [%d, %d]: %s (dtype %s)"
                  % (bits, lo, hi, "EXACT" if ok else "MISMATCH", back.dtype))
        except Exception as exc:
            print("  int32 %2d-bit: %s: %s" % (bits, type(exc).__name__, exc))

    print()
    print("  same sweep, unsigned:")
    for bits in (8, 16, 24, 30, 31, 32):
        hi = 2 ** bits - 1
        arr = np.linspace(0, hi, 256).astype(np.uint32).reshape(16, 16)
        arr[0, 0], arr[0, 1] = 0, hi
        try:
            blob = imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")
            back = imagecodecs.jpeg2k_decode(blob)
            ok = np.array_equal(back, arr) and back.dtype == arr.dtype
            print("  uint32 %2d-bit range [0, %d]: %s"
                  % (bits, hi, "EXACT" if ok else "MISMATCH"))
        except Exception as exc:
            print("  uint32 %2d-bit: %s: %s" % (bits, type(exc).__name__, exc))

    print()
    print("=== 3. RGB and multi-sample, both encoders (no colour regression) ===")
    rgb8 = np.random.default_rng(0).integers(0, 256, (16, 16, 3)).astype(np.uint8)
    for label, fn in (
            ("Pillow", lambda x: np.asarray(Image.open(io.BytesIO(
                _pillow_blob(x))))),
            ("imagecodecs", lambda x: imagecodecs.jpeg2k_decode(
                imagecodecs.jpeg2k_encode(x, level=0, codecformat="J2K")))):
        try:
            back = fn(rgb8)
            print("  RGB uint8 %-12s: %s shape=%s dtype=%s"
                  % (label, "EXACT" if np.array_equal(back, rgb8) else "MISMATCH",
                     back.shape, back.dtype))
        except Exception as exc:
            print("  RGB uint8 %-12s: %s: %s" % (label, type(exc).__name__, exc))

    rgb16 = np.random.default_rng(1).integers(0, 4096, (16, 16, 3)).astype(np.int16)
    try:
        back = imagecodecs.jpeg2k_decode(
            imagecodecs.jpeg2k_encode(rgb16, level=0, codecformat="J2K"))
        print("  RGB int16 imagecodecs : %s dtype=%s"
              % ("EXACT" if np.array_equal(back, rgb16) else "MISMATCH", back.dtype))
    except Exception as exc:
        print("  RGB int16 imagecodecs : %s: %s" % (type(exc).__name__, exc))

    print()
    print("=== 4. the pydicom encoding-plugin error, in full ===")
    try:
        from pydicom.dataset import Dataset, FileMetaDataset
        from pydicom.uid import JPEG2000Lossless, ExplicitVRLittleEndian
        ds = Dataset()
        ds.file_meta = FileMetaDataset()
        ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        arr = np.arange(256, dtype=np.int16).reshape(16, 16)
        ds.Rows, ds.Columns = arr.shape
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 1
        ds.compress(JPEG2000Lossless, arr)
    except Exception as exc:
        print("  %s: %s" % (type(exc).__name__, exc))


def _pillow_blob(x):
    from PIL import Image
    bio = io.BytesIO()
    Image.fromarray(x).save(bio, format="JPEG2000", compression="lossless")
    return bio.getvalue()


if __name__ == "__main__":
    main()
