import sys
IMPORT_ERROR = None
try:
    import imagecodecs
except ImportError as e:
    imagecodecs = None
    IMPORT_ERROR = e

from pydicom.uid import UID

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

    try:
        num_frames = getattr(ds, 'NumberOfFrames', 1)
        
        # Helper to decode a single bitstream
        def decode_frame(bitstream):
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

        # Multi-Frame Handling
        if num_frames > 1 and ds.file_meta.TransferSyntaxUID.is_encapsulated:
            from pydicom.encaps import generate_pixel_data_frame
            import numpy as np
            
            # generate_pixel_data_frame handles BOT and fragments logic
            frames = []
            for frame_bitstream in generate_pixel_data_frame(ds.PixelData, num_frames):
                decoded = decode_frame(frame_bitstream)
                frames.append(decoded)
            
            return np.array(frames)
            
        # Single-Frame Handling
        else:
            if ds.file_meta.TransferSyntaxUID.is_encapsulated:
                codestream = b"".join(decode_data_sequence(pixel_bytes))
            else:
                codestream = pixel_bytes
            
            return decode_frame(codestream)

    except Exception as e:
        print(f"[gantry_imagecodecs_handler] Decode error for {transfer_syntax}: {e}", file=sys.stderr)
        raise RuntimeError(f"imagecodecs failed to decode {transfer_syntax}: {e}")
