"""A pixel decoder built on `imagecodecs`, and the frame-count check (#418).

`offset_table_frame_count` compares the frame count an encapsulated
`PixelData`'s offset table names with the one `NumberOfFrames` declares.
It is shared by this module's `get_pixel_data`, by
`Instance.get_pixel_data`'s file arm, by `ingest_worker` for the top
level, by `_decode_nested_pixels` for an icon (#433) and by
`_decode_pixels`' imagecodecs fallback (#416), so none of them can
disagree about what a mismatch is.

Two decoders share `_decode_frame`: `get_pixel_data`, the read path's
fallback, which refuses a mismatch itself; and `decode_declared_frames`,
ingest's fallback, which decodes exactly the frames its caller asks for
because that caller has already counted the table and decided (#418's
truncation, which a refusal here would turn into a rejected file).

**Its limit, stated.** An *empty* Basic Offset Table with no Extended
Offset Table is legal (PS3.5 A.4) and names no frames, and the fragments
alone do not say where one frame ends and the next begins -- one frame
may legally span several fragments. So a multi-fragment file with an
empty table cannot be checked, and it is decoded as it always was: the
single-frame arm yields frame 0. That is a known silence, not a closed
one.
"""
import struct
import sys
from itertools import islice
from typing import Optional, Tuple, Union

import numpy as np
from pydicom.uid import UID
from pydicom.encaps import generate_frames, parse_basic_offsets
IMPORT_ERROR = None
try:
    import imagecodecs
except ImportError as e:
    imagecodecs = None
    IMPORT_ERROR = e


def is_available():
    """
    Checks if `imagecodecs` library is installed and importable.

    Returns:
        bool: True if available, False otherwise.
    """
    if imagecodecs is None:
        # Log to stderr so it appears in logs even if pydicom swallows the handler check
        print(
            f"[isocenter_imagecodecs_handler] NOT AVAILABLE. Import Error: {IMPORT_ERROR}",
            file=sys.stderr)
        return False
    return True


# UID Constants
JPEGLossless = UID("1.2.840.10008.1.2.4.57")
JPEGLosslessSV1 = UID("1.2.840.10008.1.2.4.70")
JPEG2000Lossless = UID("1.2.840.10008.1.2.4.90")
JPEG2000 = UID("1.2.840.10008.1.2.4.91")
JPEGBaseline = UID("1.2.840.10008.1.2.4.50")
JPEGExtended = UID("1.2.840.10008.1.2.4.51")
JPEGLSLossless = UID("1.2.840.10008.1.2.4.80")
JPEGLSLossy = UID("1.2.840.10008.1.2.4.81")
RLELossless = UID("1.2.840.10008.1.2.5")

HANDLER_NAME = "isocenter_imagecodecs_handler"

DEPENDENCIES = {
    "imagecodecs": ("http://www.lfd.uci.edu/~gohlke/pythonlibs/#imagecodecs", "imagecodecs"),
}

SUPPORTED_TRANSFER_SYNTAXES = [
    JPEGLossless,
    JPEGLosslessSV1,
    JPEG2000Lossless,
    JPEG2000,
    JPEGBaseline,
    JPEGExtended,
    JPEGLSLossless,
    JPEGLSLossy,
    RLELossless
]


def supports_transfer_syntax(transfer_syntax):
    """
    Checks if the transfer syntax is supported by this handler.

    Args:
        transfer_syntax (UID): The Transfer Syntax UID.

    Returns:
        bool: True if supported.
    """
    return transfer_syntax in SUPPORTED_TRANSFER_SYNTAXES


#: What `offset_table_frame_count` returns: ``(table_frames,
#: declared_frames, declared_raw, table_name)``. ``declared_raw`` is
#: NumberOfFrames as the file states it -- an int, ``""`` when the element
#: is present and empty, None when it is absent.
FrameCount = Tuple[int, int, Optional[Union[int, str]], str]


def offset_table_frame_count(ds) -> Optional[FrameCount]:
    """The frames the offset table names, beside the frames declared (#418).

    Args:
        ds (pydicom.Dataset): A dataset carrying `PixelData`.

    Returns:
        ``(table_frames, declared_frames, declared_raw, table_name)``,
        or None when there is nothing to compare: the transfer syntax is
        not encapsulated (or cannot be read at all -- a `force=True` read
        of a header-less file has an empty `file_meta`, #281), there is no
        `PixelData`, the offset table is empty with no Extended Offset
        Table beside it (the documented limit in the module docstring), or
        the table does not parse. In every None case the caller decodes as
        it did before this check existed; None never means "consistent".

        ``declared_frames`` is ``NumberOfFrames`` as the decoder reads it,
        which is 1 when the element is absent or 0. Measured on pydicom
        3.0.2: `as_array(ds, allow_excess_frames=False)` on a two-offset
        table with no NumberOfFrames returns frame 0 alone. The decoder
        reads nothing else as 1: it *refuses* a negative value ("must be
        greater than or equal to 1") and an empty one ("invalid literal
        for int()"). For those two, ``declared_frames`` is 1 only so the
        table has a number to be compared with; it is not a reading.

        ``declared_raw`` is the value the file states -- an int, ``""``
        when the element is present and empty, None when it is absent --
        so a message says "absent (read as 1)", "is 0 (read as 1)", "is -1
        (invalid)" or "is empty" rather than put a number in the dataset's
        mouth. Presence is asked of the dataset, not read off the value:
        an empty element assigned in memory is ``""`` but written and read
        back is None, and it is present either way.
    """
    # `ValueError` too: pydicom raises `ValueError("UID is not a transfer
    # syntax.")` for a UID it cannot classify -- a private syntax such as
    # GE's 1.2.840.113619.5.2, a SOP Class UID in the TS slot, an empty
    # UID. That is the decoder's refusal to make, in its own words, which
    # name the UID; this check runs outside `ingest_worker`'s decode `try`,
    # so raising here replaced that reason with one that did not.
    try:
        if not ds.file_meta.TransferSyntaxUID.is_encapsulated:
            return None
    except (AttributeError, ValueError):
        return None
    if "PixelData" not in ds:
        return None

    if "NumberOfFrames" not in ds:
        declared_raw = None
    elif ds.NumberOfFrames in (None, ""):
        declared_raw = ""
    else:
        try:
            declared_raw = int(ds.NumberOfFrames)
        except (TypeError, ValueError):
            return None
    declared = (declared_raw
                if isinstance(declared_raw, int) and declared_raw > 0
                else 1)

    # The EOT first: when it is present the BOT is required to be empty
    # (PS3.5 A.4), so a BOT-only count would see nothing. Eight bytes per
    # frame, one 64-bit offset each. `ds.get` hands back the raw bytes
    # here rather than a DataElement (measured, pydicom 3.0.2); the
    # `getattr` takes either, so neither shape reads as "no table".
    eot = ds.get("ExtendedOffsetTable")
    if eot:
        eot_bytes = getattr(eot, "value", eot)
        return (len(eot_bytes) // 8, declared, declared_raw,
                "Extended Offset Table")

    try:
        offsets = parse_basic_offsets(ds.PixelData)
    except (ValueError, struct.error, TypeError, AttributeError):
        # Unparsable -- including a `PixelData` of None, which
        # `parse_basic_offsets` meets as `AttributeError: 'NoneType' object
        # has no attribute 'read'`. Not this check's question: the decoder
        # that runs next refuses such a buffer on its own terms.
        return None
    if not offsets:
        return None
    return (len(offsets), declared, declared_raw, "Basic Offset Table")


def frame_count_mismatch(ds) -> Optional[str]:
    """The refusal message when the offset table and NumberOfFrames disagree.

    None when they agree or cannot be compared (see
    `offset_table_frame_count`). The wording is load-bearing for
    `Instance.get_pixel_data`: its file arm turns a message containing
    "no pixel data" into ``return None`` and one containing "decompress"
    or "missing dependencies" into the codecs-missing message. "names N
    frames; NumberOfFrames declares M" contains none of the three; keep it
    that way.
    """
    counted = offset_table_frame_count(ds)
    if counted is None or counted[0] == counted[1]:
        return None
    return frame_count_mismatch_words(counted)


def frame_count_mismatch_words(counted: FrameCount) -> str:
    """One spelling of the mismatch, for every refusal and loss row (#418).

    Args:
        counted: What `offset_table_frame_count` returned.
    """
    table_frames, declared, declared_raw, table_name = counted
    # "(read as 1)" only where the decoder does read 1 -- absent and 0.
    # Saying "declares 1" for either would put a number in the file's mouth
    # that it never wrote; saying "read as 1" for an empty or negative
    # value would describe a reading the decoder refuses to make.
    if declared_raw is None:
        declared_words = f"NumberOfFrames is absent (read as {declared})"
    elif declared_raw == "":
        declared_words = "NumberOfFrames is empty"
    elif declared_raw == 0:
        declared_words = f"NumberOfFrames is 0 (read as {declared})"
    elif declared_raw < 0:
        declared_words = f"NumberOfFrames is {declared_raw} (invalid)"
    else:
        declared_words = f"NumberOfFrames declares {declared}"
    return f"{table_name} names {table_frames} frames; {declared_words}"


def needs_to_convert_to_RGB(ds):
    """
    Determines if the dataset needs RGB conversion.
    Currently returns False as we preserve original photometric interpretation where possible.
    """
    return False


def should_change_PhotometricInterpretation_to_RGB(ds):
    """
    Checks if Photometric Interpretation should be changed to RGB.
    Currently returns False.
    """
    return False


def _decode_frame(transfer_syntax, bitstream, ds):
    """One frame's codestream to an array, by the codec its syntax names."""
    if transfer_syntax in [JPEGLossless, JPEGLosslessSV1]:
        return imagecodecs.ljpeg_decode(bitstream)
    if transfer_syntax in [JPEGBaseline, JPEGExtended]:
        return imagecodecs.jpeg_decode(bitstream)
    if transfer_syntax in [JPEG2000Lossless, JPEG2000]:
        return imagecodecs.jpeg2k_decode(bitstream)
    if transfer_syntax in [JPEGLSLossless, JPEGLSLossy]:
        return imagecodecs.jpegls_decode(bitstream)
    if transfer_syntax == RLELossless:
        return imagecodecs.rle_decode(bitstream, shape=(ds.Rows, ds.Columns))
    raise RuntimeError(f"Unsupported syntax: {transfer_syntax}")


def decode_declared_frames(ds, number_of_frames):
    """Decode the first `number_of_frames` frames, and ask nothing else.

    For `io_handlers._decode_pixels`' fallback (#416). **It does not
    compare the offset table with NumberOfFrames**, and that is the point:
    its caller has already asked `offset_table_frame_count` and decided --
    refuse fewer, drop an excess only when told to -- and a second check
    here would refuse the excess #418 truncates, rejecting a file ingest
    means to keep. `get_pixel_data` below is the one that refuses a
    mismatch; this is not a second spelling of it.

    `islice`, because `generate_frames(buf, number_of_frames=1)` yields
    every frame a populated Basic Offset Table names, not one (measured,
    pydicom 3.0.2): without it an excess would be decoded whole.

    Returns:
        np.ndarray: the frame for one, the frames stacked for more. The
        caller checks dtype and size against the header.
    """
    if not is_available():
        raise RuntimeError("imagecodecs is not available")
    transfer_syntax = ds.file_meta.TransferSyntaxUID
    frames = [_decode_frame(transfer_syntax, bitstream, ds)
              for bitstream in islice(
                  generate_frames(ds.PixelData,
                                  number_of_frames=number_of_frames),
                  number_of_frames)]
    return frames[0] if number_of_frames == 1 else np.stack(frames)


def get_pixel_data(ds):
    """
    Decodes pixel data from an encapsulated dataset using `imagecodecs`.

    Handles multiple transfer syntaxes (JPEG, JPEG2000, JPEG-LS, RLE) and
    encapsulated bitstreams (fragments).

    Args:
        ds (pydicom.Dataset): The dataset containing PixelData.

    Returns:
        np.ndarray: The decoded pixel array.

    Raises:
        RuntimeError: If imagecodecs is missing or decoding fails, or if
            the offset table names a different number of frames from
            NumberOfFrames (#418) -- "<table> names N frames;
            NumberOfFrames declares M".
    """
    if not is_available():
        raise RuntimeError("imagecodecs is not available")

    transfer_syntax = ds.file_meta.TransferSyntaxUID
    pixel_bytes = ds.PixelData

    # Before either arm, and outside the `try` below, so the refusal
    # reaches the caller in its own words rather than prefixed with
    # "imagecodecs failed to decode". Both arms trust NumberOfFrames:
    # the single-frame arm asks for one frame and so returned frame 0 of
    # a two-frame table, and the multi-frame arm returned whatever the
    # table held -- a silent short read when it named fewer (#418).
    mismatch = frame_count_mismatch(ds)
    if mismatch is not None:
        raise RuntimeError(mismatch)

    # Handle encapsulated data (fragments)

    try:
        num_frames = getattr(ds, 'NumberOfFrames', 1)

        # Multi-Frame Handling
        if num_frames > 1 and ds.file_meta.TransferSyntaxUID.is_encapsulated:

            # generate_frames handles BOT and fragments logic
            frames = []
            for frame_bitstream in generate_frames(ds.PixelData, number_of_frames=num_frames):
                decoded = _decode_frame(transfer_syntax, frame_bitstream, ds)
                frames.append(decoded)

            return np.array(frames)

        # Single-Frame Handling
        else:
            if ds.file_meta.TransferSyntaxUID.is_encapsulated:
                # `generate_fragments` yields EVERY item of the
                # encapsulated pixel data, and the first item is the
                # Basic Offset Table (PS3.5 A.4). Joining them therefore
                # prefixed the codestream with the BOT's own bytes -- four
                # zeros ahead of `ff4f ff51` for a single-frame file --
                # and `imagecodecs` refused the result with `not a J2K or
                # JP2 data stream`, so this arm had never decoded
                # anything. It failed identically on the JP2 container
                # this project wrote before #404, so it is not that
                # container's fault and predates it (#407).
                #
                # Only a *populated* offset table breaks the join, which
                # is why nothing noticed: with an empty table the join is
                # accidentally correct. `pydicom.encaps.encapsulate`
                # writes a populated one by default and `_compress_j2k`
                # calls it that way, so every file this project
                # compresses hit it -- and a hand-built
                # `item(b"") + item(codestream)` fixture would pass
                # without this fix.
                #
                # `generate_frames` is what the multi-frame arm above
                # already uses, and it is the right answer here for a
                # second reason as well as the BOT: one frame may legally
                # be split across several fragments, so "take the last
                # fragment" would decode the tail of such a frame.
                frames = list(generate_frames(pixel_bytes,
                                              number_of_frames=1))
                if not frames:
                    # Unreachable against pydicom 3.x, and kept anyway as
                    # a pin on its contract rather than on a log line
                    # anyone will read. Measured: `generate_frames(buf,
                    # number_of_frames=1)` yields at least one frame for
                    # every buffer that parses at all -- an empty offset
                    # table alone, an empty table plus an empty fragment,
                    # and a populated table alone all come back as
                    # `[b""]` -- and a buffer too short to parse raises
                    # `struct.error` above this line instead. So `frames`
                    # is never `[]` today. What this costs is one branch;
                    # what it buys is that if that contract ever changes,
                    # the log says which dataset had no frame instead of
                    # `IndexError: list index out of range`. Do not
                    # write a test for it: there is no input that reaches
                    # it (#407).
                    raise RuntimeError(
                        "encapsulated PixelData holds no frame")
                codestream = frames[0]
            else:
                codestream = pixel_bytes

            return _decode_frame(transfer_syntax, codestream, ds)

    except Exception as e:
        print(
            f"[isocenter_imagecodecs_handler] Decode error for {transfer_syntax}: {e}",
            file=sys.stderr)
        raise RuntimeError(f"imagecodecs failed to decode {transfer_syntax}: {e}") from e
