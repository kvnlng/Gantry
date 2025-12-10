from .session import DicomSession as Session

# Expose the Builder for power users
from .builders import DicomBuilder as Builder

# Expose Equipment for type hinting
from .entities import Equipment

# Expose config validation for custom usage
from .config_manager import ConfigValidationError, ROIValidator, ConfigSchemaValidator

__version__ = "0.1.0"
__all__ = [
    "Session",
    "Builder",
    "Equipment",
    "ConfigValidationError",
    "ROIValidator",
    "ConfigSchemaValidator",
]