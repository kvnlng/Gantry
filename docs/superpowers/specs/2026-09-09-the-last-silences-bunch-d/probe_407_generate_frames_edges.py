"""#407: is `generate_frames(..., number_of_frames=1)` a safe replacement
for the single-frame `b"".join(generate_fragments(...))`?

Four shapes the fix has to survive:
  1. one fragment, populated BOT (what `encapsulate()` writes)
  2. one fragment, EMPTY BOT (what many vendors write)
  3. TWO fragments for ONE frame, empty BOT (legal per PS3.5 A.4)
  4. RLE Lossless, one fragment
"""
import os
import sys

import pydicom
from pydicom.encaps import (encapsulate, generate_fragments,
                            generate_frames)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))


def item(payload):
    return (b"\xfe\xff\x00\xe0"
            + len(payload).to_bytes(4, "little")
            + payload)


def show(label, buf, nframes=1):
    print(f"\n=== {label}")
    frags = [f for f in generate_fragments(buf)]
    print("  generate_fragments:", len(frags), [len(f) for f in frags],
          [f[:4].hex() for f in frags])
    print("  joined head:", b"".join(frags)[:8].hex())
    try:
        frames = list(generate_frames(buf, number_of_frames=nframes))
        print("  generate_frames:", len(frames), [len(f) for f in frames],
              [f[:4].hex() for f in frames])
    except Exception as e:
        print("  generate_frames RAISED:", type(e).__name__, str(e)[:160])


def main():
    print("pydicom:", pydicom.__version__)
    body = b"\xff\x4f\xff\x51" + b"A" * 60

    show("1. encapsulate() one frame (populated BOT)",
         encapsulate([body], has_bot=True))
    show("2. one fragment, EMPTY BOT",
         item(b"") + item(body))
    show("3. two fragments, ONE frame, empty BOT",
         item(b"") + item(body[:32]) + item(body[32:]))
    show("4. encapsulate() two frames", encapsulate([body, body]), nframes=2)
    show("5. two fragments one frame, but number_of_frames=1, populated BOT",
         encapsulate([body], has_bot=True))


if __name__ == "__main__":
    main()
