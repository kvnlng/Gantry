import warnings
# Suppress strict pydicom validation warnings for UIDs (common in test data)
warnings.filterwarnings("ignore", message="Invalid value for VR UI", category=UserWarning)

from .session import DicomSession as Session

# Expose the Builder for power users
from .builders import DicomBuilder as Builder

# Expose Equipment for type hinting
from .entities import Equipment

__version__ = "0.2.0"
__all__ = ["Session", "Builder", "Equipment"]