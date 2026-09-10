
import pytest
from unittest.mock import MagicMock, patch
import numpy as np
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import UID
from isocenter import imagecodecs_handler

# Define UIDs for convenience (matching those in imagecodecs_handler)
JPEGLossless = UID("1.2.840.10008.1.2.4.57")
RLELossless = UID("1.2.840.10008.1.2.5")
UnsupportedUID = UID("1.2.840.10008.1.2.4.100")  # hypothetical unsupported

@pytest.fixture
def mock_dataset():
    ds = MagicMock(spec=Dataset)
    ds.file_meta = MagicMock()
    ds.Rows = 10
    ds.Columns = 10
    ds.PixelData = b"fake_pixel_data"
    ds.NumberOfFrames = 1
    return ds

def test_imagecodecs_not_available(mock_dataset):
    """Test behavior when imagecodecs is reported as not available."""
    with patch('isocenter.imagecodecs_handler.is_available', return_value=False):
        with pytest.raises(RuntimeError, match="imagecodecs is not available"):
            imagecodecs_handler.get_pixel_data(mock_dataset)

def test_unsupported_transfer_syntax(mock_dataset):
    """Test behavior when an unsupported transfer syntax is encountered."""
    mock_dataset.file_meta.TransferSyntaxUID = UnsupportedUID
    with patch('isocenter.imagecodecs_handler.is_available', return_value=True):
        with pytest.raises(RuntimeError, match="imagecodecs failed to decode"):
            imagecodecs_handler.get_pixel_data(mock_dataset)


# Even-length on purpose: `encapsulate` pads an odd-length fragment with a
# trailing null (items must be even, PS3.5 7.5), and a padded payload would
# make the "handed exactly this" assertions below read `b"chunk\x00"`.
CHUNK = b"ljpeg_chunk!"
RLE_CHUNK = b"rle_chunk!"


def test_decode_error_handling(mock_dataset):
    """A codec exception is wrapped as a RuntimeError, on real bytes.

    The encapsulated `PixelData` is built with `encapsulate()` rather than
    mocked, because the version of this test that patched
    `generate_fragments` proved nothing about the arm it names: the real
    call also raised on `b"fake_pixel_data"`, so it passed whether or not
    the codec was ever reached (#407).
    """
    mock_dataset.file_meta.TransferSyntaxUID = JPEGLossless
    mock_dataset.PixelData = encapsulate([CHUNK])

    # Unconditionally patch the local reference to imagecodecs in the handler
    with patch('isocenter.imagecodecs_handler.imagecodecs') as mock_ic:
        mock_ic.ljpeg_decode.side_effect = ValueError("Bad data")
        with pytest.raises(RuntimeError, match="imagecodecs failed to decode"):
            imagecodecs_handler.get_pixel_data(mock_dataset)
        # The codec saw the fragment alone: had the Basic Offset Table been
        # joined in front of it, this would be four zero bytes longer.
        assert mock_ic.ljpeg_decode.call_args[0][0] == CHUNK

def test_rle_lossless_handling(mock_dataset):
    """Test RLE Lossless specific path."""
    mock_dataset.file_meta.TransferSyntaxUID = RLELossless
    mock_dataset.PixelData = encapsulate([RLE_CHUNK])
    expected_output = np.zeros((10, 10), dtype=np.uint8)

    with patch('isocenter.imagecodecs_handler.imagecodecs') as mock_ic:
        mock_ic.rle_decode.return_value = expected_output
        result = imagecodecs_handler.get_pixel_data(mock_dataset)
        mock_ic.rle_decode.assert_called_once()
        # #407: the codec is handed the fragment, not the Basic Offset
        # Table and the fragment joined together.
        assert mock_ic.rle_decode.call_args[0][0] == RLE_CHUNK
        assert result is expected_output

def test_multi_frame_handling(mock_dataset):
    """Test multi-frame image decoding logic."""
    mock_dataset.NumberOfFrames = 2

    mock_uid = MagicMock()
    mock_uid.is_encapsulated = True
    mock_uid.__eq__.side_effect = lambda x: x == JPEGLossless
    mock_dataset.file_meta.TransferSyntaxUID = mock_uid

    frame1 = np.zeros((10, 10), dtype=np.uint8)
    frame2 = np.ones((10, 10), dtype=np.uint8)

    # Mock generate_frames to return two frames
    with patch('isocenter.imagecodecs_handler.generate_frames', return_value=[b"f1", b"f2"]):
        with patch('isocenter.imagecodecs_handler.imagecodecs') as mock_ic:
            mock_ic.ljpeg_decode.side_effect = [frame1, frame2]
            result = imagecodecs_handler.get_pixel_data(mock_dataset)
            assert result.shape == (2, 10, 10)
            np.testing.assert_array_equal(result[0], frame1)
            np.testing.assert_array_equal(result[1], frame2)

def test_is_available_import_error():
    """Test is_available returns False when import fails."""
    # We can't easily unload the module if it's already loaded, but we can simulate the state
    # where imagecodecs is None.
    with patch('isocenter.imagecodecs_handler.imagecodecs', None):
        assert imagecodecs_handler.is_available() is False

def test_is_available_success():
    """Test is_available returns True when module is present."""
    with patch('isocenter.imagecodecs_handler.imagecodecs', MagicMock()):
        assert imagecodecs_handler.is_available() is True
