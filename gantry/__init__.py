import warnings
# Suppress strict pydicom validation warnings for UIDs (common in test data)
warnings.filterwarnings("ignore", message="Invalid value for VR UI", category=UserWarning)

from .session import DicomSession as Session

# Expose the Builder for power users
from .builders import DicomBuilder as Builder

# Expose Equipment for type hinting
from .entities import Equipment

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