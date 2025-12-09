import os
import numpy as np
import pydicom
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


# --- Base Classes ---

@dataclass
class DicomSequence:
    tag: str
    items: List['DicomItem'] = field(default_factory=list)


@dataclass
class DicomItem:
    # init=False to avoid constructor conflicts during inheritance
    attributes: Dict[str, Any] = field(init=False)
    sequences: Dict[str, DicomSequence] = field(init=False)

    def __post_init__(self):
        self.attributes = {}
        self.sequences = {}

    def set_attr(self, tag: str, value: Any):
        self.attributes[tag] = value

    def add_sequence_item(self, tag: str, item: 'DicomItem'):
        if tag not in self.sequences:
            self.sequences[tag] = DicomSequence(tag=tag)
        self.sequences[tag].items.append(item)


@dataclass(frozen=True)
class Equipment:
    """
    Immutable Equipment definition.
    Frozen=True allows hashing, enabling unique set generation.
    """
    manufacturer: str
    model_name: str
    device_serial_number: str = ""


# --- Core Hierarchy ---

@dataclass
class Instance(DicomItem):
    sop_instance_uid: str = ""
    sop_class_uid: str = ""
    instance_number: int = 0

    # Persistence: Link to original file for lazy loading
    file_path: Optional[str] = None

    # Transient: Actual pixel data (NOT persisted to pickle)
    pixel_array: Optional[np.ndarray] = field(default=None, repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.set_attr("0008,0018", self.sop_instance_uid)
        self.set_attr("0008,0016", self.sop_class_uid)
        self.set_attr("0020,0013", self.instance_number)

    def __getstate__(self):
        """Called when saving to disk. Drop the heavy pixel array."""
        state = self.__dict__.copy()
        state['pixel_array'] = None
        return state

    def __setstate__(self, state):
        """Called when loading from disk. Restore metadata."""
        self.__dict__.update(state)

    def get_pixel_data(self) -> np.ndarray:
        """
        Returns pixel_array. Loads from disk if not in memory.
        """
        if self.pixel_array is not None:
            return self.pixel_array

        if self.file_path and os.path.exists(self.file_path):
            try:
                # Read pixel data on demand
                ds = pydicom.dcmread(self.file_path)
                self.set_pixel_data(ds.pixel_array)  # Cache it in memory
                return self.pixel_array
            except Exception as e:
                raise RuntimeError(f"Lazy load failed for {self.file_path}: {e}")

        raise FileNotFoundError(f"Pixels missing and file not found: {self.file_path}")

    def set_pixel_data(self, array: np.ndarray):
        """ unpacking of 2D/3D/4D arrays."""
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


@dataclass
class Series:
    series_instance_uid: str
    modality: str
    series_number: int
    equipment: Optional[Equipment] = None
    instances: List[Instance] = field(default_factory=list)


@dataclass
class Study:
    study_instance_uid: str
    study_date: Any
    series: List[Series] = field(default_factory=list)


@dataclass
class Patient:
    patient_id: str
    patient_name: str
    studies: List[Study] = field(default_factory=list)