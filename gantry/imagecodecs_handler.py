import sys
IMPORT_ERROR = None
try:
    import imagecodecs
except ImportError as e:
    imagecodecs = None
    IMPORT_ERROR = e

from pydicom.uid import UID

# ... (UID Constants omitted) ...

def is_available():
    if imagecodecs is None:
        # Log to stderr so it appears in logs even if pydicom swallows the handler check
        print(f"[gantry_imagecodecs_handler] NOT AVAILABLE. Import Error: {IMPORT_ERROR}", file=sys.stderr)
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
    import sys

    # Flatten fragments to get the raw bitstream
    # This works for Single Frame. For Multi-Frame, this creates one large stream.
    # imagecodecs often handles single concatenated stream or we might need to loop.
    # For robust Single Frame fix:
    try:
        # decode_data_sequence returns iterator of bytes (fragments)
        # join them to form the contiguous codestream
        has_frames = getattr(ds, 'NumberOfFrames', 1) > 1
        
        # De-encapsulate
        if ds.file_meta.TransferSyntaxUID.is_encapsulated:
            codestream = b"".join(decode_data_sequence(pixel_bytes))
        else:
            codestream = pixel_bytes
            
    except Exception as e:
        print(f"[gantry_imagecodecs_handler] De-encapsulation error: {e}", file=sys.stderr)
        raise e
    
    try:
        # JPEG Lossless (Process 14) and SV1 (.70)
        # Standard libjpeg often fails here; use specific ljpeg codec
        if transfer_syntax in [JPEGLossless, JPEGLosslessSV1]:
             return imagecodecs.ljpeg_decode(codestream)

        # JPEG Baseline / Extended (Process 1, 2, 4)
        if transfer_syntax in [JPEGBaseline, JPEGExtended]:
             return imagecodecs.jpeg_decode(codestream)
             
        # JPEG 2000
        if transfer_syntax in [JPEG2000Lossless, JPEG2000]:
             return imagecodecs.jpeg2k_decode(codestream)
             
        # JPEG-LS
        if transfer_syntax in [JPEGLSLossless, JPEGLSLossy]:
             # imagecodecs JPEGLS might expect bytes
             return imagecodecs.jpegls_decode(codestream)

        # RLE
        if transfer_syntax == RLELossless:
             # RLE needs shape info usually?
             return imagecodecs.rle_decode(codestream, shape=(ds.Rows, ds.Columns)) # guessing sig

    except Exception as e:
        print(f"[gantry_imagecodecs_handler] Decode error for {transfer_syntax}: {e}", file=sys.stderr)
        raise RuntimeError(f"imagecodecs failed to decode {transfer_syntax}: {e}")

    raise NotImplementedError(f"Transfer Syntax {transfer_syntax} not handled by gantry_imagecodecs_handler")
