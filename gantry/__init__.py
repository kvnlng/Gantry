import warnings
# Suppress all pydicom warnings (e.g. strict UID validation)
warnings.filterwarnings("ignore", module="pydicom.*")

from .session import DicomSession as Session

# Expose the Builder for power users
from .builders import DicomBuilder as Builder

# Expose Equipment for type hinting
# Expose Equipment for type hinting
from .entities import Equipment

# Configure pydicom handlers
# We prioritize pylibjpeg (if installed) and pillow.
# GDCM is often problematic to install via pip, so pylibjpeg is preferred for JPEG/JPEG-LS/RLE.
try:
    from pydicom import config as pydicom_config
    # These are import strings for the handlers
    # We explicitly define the priority list to ensure maximum compatibility.
    pydicom_config.pixel_data_handlers = [
        "pydicom.pixel_data_handlers.gdcm_handler",      # Attempt GDCM first (if installed by user)
        "pydicom.pixel_data_handlers.pylibjpeg_handler", # Preferred pure-python decoder
        "pydicom.pixel_data_handlers.jpeg_ls_handler",   # CharPyLS
        "pydicom.pixel_data_handlers.pillow_handler",    # Fallback for standard JPEGs
        "pydicom.pixel_data_handlers.numpy_handler",     # For uncompressed data
        "pydicom.pixel_data_handlers.rle_handler"        # Native Numpy RLE
    ]
except ImportError:
    pass

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # Backport for older Pythons if needed, though Gantry requires 3.9+ where this is standard
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("gantry")
except PackageNotFoundError:
    # Package is not installed
    __version__ = "0.0.0"
__all__ = ["Session", "Builder", "Equipment"]