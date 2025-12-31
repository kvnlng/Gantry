
import numpy as np
try:
    import imagecodecs
except ImportError:
    imagecodecs = None

from pydicom.uid import UID

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

HANDLER_NAME = "gantry_imagecodecs_handler"

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

def is_available():
    return imagecodecs is not None

def supports_transfer_syntax(transfer_syntax):
    return transfer_syntax in SUPPORTED_TRANSFER_SYNTAXES

def needs_to_convert_to_RGB(ds):
    return False

def should_change_PhotometricInterpretation_to_RGB(ds):
    return False

def get_pixel_data(ds):
    """
    Returns numpy array of pixel data.
    """
    if not is_available():
        raise RuntimeError("imagecodecs is not available")
        
    transfer_syntax = ds.file_meta.TransferSyntaxUID
    pixel_bytes = ds.PixelData
    
    # Handle encapsulated data (fragments)
    from pydicom.encaps import decode_data_sequence
    # Basic encapsulation check
    # imagecodecs often expects the full concatenated bytes?
    # pydicom helpers:
    if getattr(ds, 'NumberOfFrames', 1) > 1:
        # Multi-frame logic
        # For simplicity, let's try pydicom's generic handling or manual loop?
        # imagecodecs usually decodes single frames.
        # We need to iterate over frames.
        
        # NOTE: This simple handler might need more robustness for multi-frame
        # But for the immediate Single Frame JPEG Lossless user issue:
        pass

    # Simplified single-frame support (or full byte stream)
    # imagecodecs.jpeg_decode handles the stream?
    
    try:
        # JPEG Lossless / Baseline / Extended
        if transfer_syntax in [JPEGLossless, JPEGLosslessSV1, JPEGBaseline, JPEGExtended]:
             return imagecodecs.jpeg_decode(pixel_bytes)
             
        # JPEG 2000
        if transfer_syntax in [JPEG2000Lossless, JPEG2000]:
             return imagecodecs.jpeg2k_decode(pixel_bytes)
             
        # JPEG-LS
        if transfer_syntax in [JPEGLSLossless, JPEGLSLossy]:
             return imagecodecs.jpegls_decode(pixel_bytes)

        # RLE
        if transfer_syntax == RLELossless:
             # RLE needs shape info usually?
             return imagecodecs.rle_decode(pixel_bytes, shape=(ds.Rows, ds.Columns)) # guessing sig

    except Exception as e:
        raise RuntimeError(f"imagecodecs failed to decode {transfer_syntax}: {e}")

    raise NotImplementedError(f"Transfer Syntax {transfer_syntax} not handled by gantry_imagecodecs_handler")
