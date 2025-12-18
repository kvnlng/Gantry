import os
import numpy as np
import pydicom
from pydicom.uid import generate_uid
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


# --- Base Classes ---

@dataclass(slots=True)
class DicomSequence:
    """
    Represents a DICOM Sequence (SQ) containing multiple DicomItems.
    """
    tag: str
    items: List['DicomItem'] = field(default_factory=list)


@dataclass(slots=True)
class DicomItem:
    """
    Base class for any entity that holds DICOM attributes and sequences.
    """
    # init=False to avoid constructor conflicts during inheritance
    attributes: Dict[str, Any] = field(init=False)
    sequences: Dict[str, DicomSequence] = field(init=False)

    def __post_init__(self):
        self.attributes = {}
        self.sequences = {}

    def set_attr(self, tag: str, value: Any):
        """Sets a generic attribute by its hex tag (e.g., '0010,0010')."""
        self.attributes[tag] = value

    def add_sequence_item(self, tag: str, item: 'DicomItem'):
        """Appends a new item to a sequence, creating the sequence if needed."""
        if tag not in self.sequences:
            self.sequences[tag] = DicomSequence(tag=tag)
        self.sequences[tag].items.append(item)


@dataclass(frozen=True, slots=True)
class Equipment:
    """
    Immutable Equipment definition.
    Frozen=True allows hashing, enabling unique set generation.
    """
    manufacturer: str
    model_name: str
    device_serial_number: str = ""


# --- Core Hierarchy ---

@dataclass(slots=True)
class Instance(DicomItem):
    """
    Represents a single DICOM image (SOP Instance).
    Manages lazy loading of pixel data.
    """
    sop_instance_uid: str = ""
    sop_class_uid: str = ""
    instance_number: int = 0

    # Persistence: Link to original file for lazy loading
    file_path: Optional[str] = None

    # Transient: Actual pixel data (NOT persisted to pickle)
    pixel_array: Optional[np.ndarray] = field(default=None, repr=False)
    
    # Transient: Track if dates have been shifted in memory
    date_shifted: bool = field(default=False, init=False)

    def __post_init__(self):
        super().__post_init__()
        self.set_attr("0008,0018", self.sop_instance_uid)
        self.set_attr("0008,0016", self.sop_class_uid)
        self.set_attr("0020,0013", self.instance_number)



    def regenerate_uid(self):
        """
        Generates a new, globally unique SOP Instance UID.
        Call this whenever pixel data is modified.
        """
        # 1. Generate new UID using pydicom's generator (or your org root)
        new_uid = generate_uid()
        
        # 2. Update the Object Property
        self.sop_instance_uid = new_uid
        
        # 3. Update the DICOM Attribute Dictionary
        self.set_attr("0008,0018", new_uid)
        
        # 4. Detach from physical file
        # Since this object is now a "new" instance in memory, 
        # it no longer matches the file on disk.
        self.file_path = None 
        
        print(f"  -> Identity regenerated: {new_uid}")

    def get_pixel_data(self) -> Optional[np.ndarray]:
        """
        Returns pixel_array. Loads from disk if not in memory.
        Returns None if no pixel data is present.
        """
        if self.pixel_array is not None:
            return self.pixel_array

        if self.file_path and os.path.exists(self.file_path):
            try:
                # Read pixel data on demand
                ds = pydicom.dcmread(self.file_path)
                try:
                    self.set_pixel_data(ds.pixel_array)  # Cache it in memory
                    return self.pixel_array
                except (AttributeError, TypeError):
                    # No pixel data element
                    return None
                except Exception as e:
                    if "no pixel data" in str(e).lower():
                        return None
                    raise e
                    
            except Exception as e:
                if "missing dependencies" in str(e) or "decompress" in str(e):
                    raise RuntimeError(
                        f"Failed to decompress pixel data for {os.path.basename(self.file_path)}. "
                        "Missing image codecs. Please install them with: pip install \"gantry[images]\""
                    ) from e
                
                # If we just caught the re-raised "no pixel data" exception, it would be handled above, 
                # but if dcmread fails completely or something else happens:
                raise RuntimeError(f"Lazy load failed for {self.file_path}: {e}")

        raise FileNotFoundError(f"Pixels missing and file not found: {self.file_path}")

    def set_pixel_data(self, array: np.ndarray):
        """
        Sets the pixel array and automatically updates metadata tags
        (rows, cols, samples, frames, etc.) based on array shape.
        Handles unpacking of 2D/3D/4D arrays.
        """
        self.pixel_array = array
        shape = array.shape
        ndim = len(shape)

        # Defaults
        samples = 1
        frames = 1

        if ndim == 2:
            rows, cols = shape
        elif ndim == 3:
            if shape[-1] in [3, 4]:
                rows, cols, samples = shape
            else:
                frames, rows, cols = shape
        elif ndim == 4:
            frames, rows, cols, samples = shape
        else:
            raise ValueError(f"Unknown shape: {shape}")

        self.set_attr("0028,0010", rows)
        self.set_attr("0028,0011", cols)
        self.set_attr("0028,0002", samples)
        if frames > 1: self.set_attr("0028,0008", str(frames))
        if samples >= 3: self.set_attr("0028,0004", "RGB")


@dataclass(slots=True)
class Series:
    """
    Groups Instances by Series Instance UID.
    Typically represents a single scan or reconstruction.
    """
    series_instance_uid: str
    modality: str
    series_number: int
    equipment: Optional[Equipment] = None
    instances: List[Instance] = field(default_factory=list)


@dataclass(slots=True)
class Study:
    """
    Groups Series by Study Instance UID.
    Represents a single patient visit or examination.
    """
    study_instance_uid: str
    study_date: Any
    series: List[Series] = field(default_factory=list)
    date_shifted: bool = False


@dataclass(slots=True)
class Patient:
    """
    Root of the object hierarchy. Groups Studies by Patient ID.
    """
    patient_id: str
    patient_name: str
    studies: List[Study] = field(default_factory=list)