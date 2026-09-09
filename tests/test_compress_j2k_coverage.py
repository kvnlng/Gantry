"""`_compress_j2k`'s arms, exercised against its collaborator directly.

Rewritten in #404, when the JPEG 2000 encoder changed from Pillow's
`Image.fromarray(...).save(..., format="JPEG2000")` to
`imagecodecs.jpeg2k_encode(frame, level=0, codecformat="J2K")`. Every test
here patched `isocenter.io_handlers.Image`, a name the module no longer
has, so each one was about a collaborator that does not exist rather than
about the function.

Two of them asserted the **reconstruct-from-bytes** branch --
`_compress_j2k(ds, pixel_array=None)` rebuilding the array from
`ds.PixelData` -- which #404 deleted as unreachable rather than corrected.
The replacement is `test_the_no_array_call_encodes_nothing` below, which
pins the deletion; the end-to-end characterization lives in
`tests/test_signed_pixels_survive_a_compressed_export.py::
test_compress_j2k_without_an_array_writes_nothing_and_raises_nothing`.

The `ImportError` test went with them. `imagecodecs` is imported unguarded
at module scope now, so an `ImportError` inside this function is not a
reachable state and the `except ImportError` arm that turned it into
"Pillow or pydicom not installed" is gone.
"""
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian, JPEG2000Lossless

from isocenter.io_handlers import _compress_j2k


@pytest.fixture
def mock_dataset_compress():
    ds = MagicMock(spec=Dataset)
    ds.file_meta = MagicMock()
    ds.Rows = 10
    ds.Columns = 10
    ds.SamplesPerPixel = 1
    ds.BitsAllocated = 8
    ds.PixelRepresentation = 0
    ds.NumberOfFrames = 1
    ds.PixelData = b'\x00' * 100
    return ds


def test_compress_j2k_with_array(mock_dataset_compress):
    """The array handed in is the array encoded, and the result is stored."""
    arr = np.zeros((10, 10), dtype=np.uint8)

    with patch('isocenter.io_handlers.jpeg2k_encode',
               return_value=b"codestream") as encode:
        with patch('isocenter.io_handlers.encapsulate',
                   return_value=b"compressed_data"):
            _compress_j2k(mock_dataset_compress, pixel_array=arr)

    encode.assert_called_once()
    assert mock_dataset_compress.PixelData == b"compressed_data"


def test_compress_j2k_asks_for_a_lossless_bare_codestream(mock_dataset_compress):
    """`level=0` and `codecformat="J2K"` are both load-bearing.

    `level=0` is lossless; anything else silently degrades the pixels.
    `codecformat="J2K"` emits a bare codestream, which is what transfer
    syntax 1.2.840.10008.1.2.4.90 names -- the default wraps it in a JP2
    box, which is what every release before #404 wrote.
    """
    arr = np.zeros((10, 10), dtype=np.uint8)

    with patch('isocenter.io_handlers.jpeg2k_encode',
               return_value=b"codestream") as encode:
        with patch('isocenter.io_handlers.encapsulate',
                   return_value=b"encapsulated_frames"):
            _compress_j2k(mock_dataset_compress, pixel_array=arr)

    _args, kwargs = encode.call_args
    assert kwargs["level"] == 0
    assert kwargs["codecformat"] == "J2K"
    assert mock_dataset_compress.PixelData == b"encapsulated_frames"
    assert mock_dataset_compress.file_meta.TransferSyntaxUID == JPEG2000Lossless


def test_compress_j2k_encodes_each_frame_separately(mock_dataset_compress):
    """Encapsulated multi-frame pixel data is one fragment per frame."""
    mock_dataset_compress.NumberOfFrames = 2
    arr = np.zeros((2, 10, 10), dtype=np.uint8)

    with patch('isocenter.io_handlers.jpeg2k_encode',
               return_value=b"codestream") as encode:
        with patch('isocenter.io_handlers.encapsulate',
                   return_value=b"encapsulated"):
            _compress_j2k(mock_dataset_compress, pixel_array=arr)

    assert encode.call_count == 2
    for call in encode.call_args_list:
        assert call.args[0].shape == (10, 10)


def test_the_no_array_call_encodes_nothing():
    """The deleted branch, pinned at the unit level.

    `pixel_array is None` means "nothing to compress" and nothing else. It
    used to mean "rebuild the array from `ds.PixelData`", reading those
    bytes as `uint16` regardless of `PixelRepresentation` -- a second
    decoder that could disagree with `SidecarPixelLoader`, and one no
    caller could reach. *Red when:* the reconstruct-from-bytes arm is
    restored.
    """
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
    ds.Rows = ds.Columns = 10
    ds.SamplesPerPixel = 1
    ds.NumberOfFrames = 1
    ds.BitsAllocated = 16
    ds.PixelRepresentation = 1
    ds.PixelData = b'\x00' * 200

    with patch('isocenter.io_handlers.jpeg2k_encode') as encode:
        _compress_j2k(ds, pixel_array=None)

    encode.assert_not_called()
    assert ds.file_meta.TransferSyntaxUID == ImplicitVRLittleEndian
    assert ds.PixelData == b'\x00' * 200


def test_compress_j2k_generic_exception(mock_dataset_compress):
    """A codec failure still reaches the caller as `Compression failed`."""
    with patch('isocenter.io_handlers.jpeg2k_encode',
               side_effect=ValueError("Bad Data")):
        with pytest.raises(RuntimeError, match="Compression failed"):
            _compress_j2k(mock_dataset_compress,
                          pixel_array=np.zeros((10, 10), dtype=np.uint8))


def test_the_frame_refusal_is_not_rewrapped_by_the_generic_handler(
        mock_dataset_compress):
    """`Compression failed: Compression failed: ...` is the shape to avoid.

    The refusal is raised inside the same `try` the codec runs in, so
    without its own `except ... raise` clause the outer handler would
    stringify it into its own message. The sentence the user reads has to
    be ours, unwrapped.
    """
    mock_dataset_compress.BitsAllocated = 32
    with patch('isocenter.io_handlers.jpeg2k_encode') as encode:
        with pytest.raises(RuntimeError) as excinfo:
            _compress_j2k(mock_dataset_compress,
                          pixel_array=np.zeros((10, 10), dtype=np.int32))

    encode.assert_not_called()
    message = str(excinfo.value)
    assert message.count("Compression failed") == 1, message
    assert "int32" in message
    assert "use_compression=False" in message
