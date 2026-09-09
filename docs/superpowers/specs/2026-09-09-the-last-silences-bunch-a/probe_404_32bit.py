"""#404: can 32-bit be made exact, and is level=0 truly reversible?"""
import numpy as np
import imagecodecs


def arr32(bits, signed=True):
    if signed:
        lo = -pow(2, bits - 1)
        hi = pow(2, bits - 1) - 1
        dt = np.int32
    else:
        lo = 0
        hi = pow(2, bits) - 1
        dt = np.uint32
    a = np.linspace(lo, hi, 256).astype(dt).reshape(16, 16)
    a[0, 0] = lo
    a[0, 1] = hi
    return a


def trial(a, level=None, bps=None, reversible=None):
    try:
        blob = imagecodecs.jpeg2k_encode(
            a, level=level, codecformat="J2K",
            bitspersample=bps, reversible=reversible)
        back = imagecodecs.jpeg2k_decode(blob)
        good = np.array_equal(back, a) and back.dtype == a.dtype
        return "EXACT" if good else "MISMATCH(%s)" % back.dtype
    except Exception as exc:
        return "%s: %s" % (type(exc).__name__, str(exc)[:70])


def main():
    print("=== exact boundary for int32, level=0 ===")
    for bits in (24, 25, 26, 27):
        print("  %2d-bit signed  : %s" % (bits, trial(arr32(bits), level=0)))

    print("")
    print("=== does bitspersample rescue 31-bit int32? ===")
    a = arr32(31)
    print("  level=0                        : %s" % trial(a, level=0))
    print("  level=0, bitspersample=31      : %s" % trial(a, level=0, bps=31))
    print("  level=0, bitspersample=32      : %s" % trial(a, level=0, bps=32))
    print("  reversible=True                : %s" % trial(a, reversible=True))
    print("  reversible=True, bps=31        : %s"
          % trial(a, reversible=True, bps=31))

    print("")
    print("=== is level=0 really reversible for int16? ===")
    rng = np.random.default_rng(7)
    noisy = rng.integers(-32768, 32767, (64, 64)).astype(np.int16)
    print("  level=0          : %s" % trial(noisy, level=0))
    print("  reversible=True  : %s" % trial(noisy, reversible=True))
    print("  no options       : %s" % trial(noisy))

    print("")
    print("=== 16-bit full range and a DICOM-typical CT window ===")
    cases = [
        ("int16 full range",
         np.linspace(-32768, 32767, 4096).astype(np.int16).reshape(64, 64)),
        ("int16 CT [-1024,3071]",
         np.linspace(-1024, 3071, 4096).astype(np.int16).reshape(64, 64)),
        ("uint16 full range",
         np.linspace(0, 65535, 4096).astype(np.uint16).reshape(64, 64)),
        ("int8 full range",
         np.linspace(-128, 127, 4096).astype(np.int8).reshape(64, 64)),
    ]
    for name, a in cases:
        print("  %-24s : %s" % (name, trial(a, level=0)))


if __name__ == "__main__":
    main()
