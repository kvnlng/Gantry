import os
import numpy as np
import pydicom
from pydicom.uid import generate_uid
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable


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
    # init=False to avoid constructor conflicts during inheritance
    attributes: Dict[str, Any] = field(init=False)
    sequences: Dict[str, DicomSequence] = field(init=False)
    
    # Versioning for robust persistence
    _mod_count: int = field(init=False, default=0)
    _saved_mod_count: int = field(init=False, default=-1)

    def __post_init__(self):
        self.attributes = {}
        self.sequences = {}
        # Initial state is dirty (1 > 0)
        self._mod_count = 1
        self._saved_mod_count = 0

    @property
    def _dirty(self) -> bool:
        return self._mod_count > self._saved_mod_count
        
    @_dirty.setter
    def _dirty(self, value: bool):
        # Legacy support: setting True increments version
        if value:
            self._mod_count += 1
        else:
            # Unsafe clear: assumes current state is saved
            self._saved_mod_count = self._mod_count

    def mark_saved(self, version_saved: int):
        """Marks specific version as saved. Robust against concurrent edits."""
        if version_saved > self._saved_mod_count:
            self._saved_mod_count = version_saved

    def set_attr(self, tag: str, value: Any):
        """Sets a generic attribute by its hex tag (e.g., '0010,0010')."""
        self.attributes[tag] = value
        self._mod_count += 1

    def add_sequence_item(self, tag: str, item: 'DicomItem'):
        """Appends a new item to a sequence, creating the sequence if needed."""
        if tag not in self.sequences:
            self.sequences[tag] = DicomSequence(tag=tag)
        self.sequences[tag].items.append(item)
        self._mod_count += 1

    def mark_clean(self):
        # Legacy: force clean
        self._saved_mod_count = self._mod_count
        for seq in self.sequences.values():
            for item in seq.items:
                item.mark_clean()


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
    
    # Transient: Lazy Loader (Callable that returns np.ndarray)
    # Used for Sidecar or deferred logic
    _pixel_loader: Optional[Callable[[], np.ndarray]] = field(default=None, repr=False)

    # Transient: Track if dates have been shifted in memory
    date_shifted: bool = field(default=False, init=False)

    def __post_init__(self):
        # Inlined from DicomItem to avoid super() mismatch issues with slots/reloads
        self.attributes = {}
        self.sequences = {}
        
        # Versioning
        self._mod_count = 1
        self._saved_mod_count = 0
        
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
        
        from .logger import get_logger
        get_logger().debug(f"  -> Identity regenerated: {new_uid}")

    def unload_pixel_data(self):
        """
        Clears the cached pixel_array from memory to free resources.
        Only performs the clear if the data can be re-loaded (file_path or _pixel_loader exists).
        Returns True if unloaded, False if unsafe to unload.
        """
        if self.pixel_array is None:
            return True
            
        if self.file_path or self._pixel_loader:
            self.pixel_array = None
            return True
        else:
            # Data is in memory only (e.g. modified but not saved)
            return False

    def get_pixel_data(self) -> Optional[np.ndarray]:
        """
        Returns pixel_array. Loads from disk if not in memory.
        Returns None if no pixel data is present.
        """
        if self.pixel_array is not None:
            return self.pixel_array

        if self._pixel_loader:
             try:
                 # Invoke callback (e.g. sidecar read)
                 arr = self._pixel_loader()
                 # Use set_pixel_data to ensure attributes (rows, cols) are synced 
                 # This is critical if the loader returns a raw array but attributes were not yet set/restored
                 self.set_pixel_data(arr)
                 return self.pixel_array
             except Exception as e:
                 raise RuntimeError(f"Pixel Loader failed for {self.sop_instance_uid}: {e}")

        if self.file_path and os.path.exists(self.file_path):
            try:
                # Read pixel data on demand
                ds = None
                try:
                    ds = pydicom.dcmread(self.file_path)
                    
                    self.set_pixel_data(ds.pixel_array)  # Cache it in memory
                    return self.pixel_array
                except (AttributeError, TypeError):
                    # No pixel data element
                    return None
                except Exception as e:
                    if "no pixel data" in str(e).lower():
                        return None
                    # Re-raise to be handled by outer except
                    raise e
                    
            except Exception as e:
                # Try explicit fallback to gantry.imagecodecs_handler
                # Pydicom sometimes fails to iterate handlers correctly or swallows errors.
                try:
                    import gantry.imagecodecs_handler as h
                    if ds is not None and h.is_available() and h.supports_transfer_syntax(ds.file_meta.TransferSyntaxUID):
                        arr = h.get_pixel_data(ds)
                        self.set_pixel_data(arr)
                        return self.pixel_array
                except Exception as fallback_e:
                    # Fallback failed, proceed to raise original error
                    pass

                # Try to get Transfer Syntax UID for better debugging
                ts_uid = "Unknown"
                if ds is not None and hasattr(ds, "file_meta"):
                     ts_uid = getattr(ds.file_meta, "TransferSyntaxUID", "Unknown")
                
                if "missing dependencies" in str(e) or "decompress" in str(e):
                    # Enhanced debug output
                    handlers = []
                    try:
                        # pydicom is already imported globally
                        handlers = [str(h) for h in pydicom.config.pixel_data_handlers]
                    except: pass

                    raise RuntimeError(
                        f"Failed to decompress pixel data for {os.path.basename(self.file_path)} "
                        f"(Transfer Syntax: {ts_uid}).\n"
                        f"Underlying Error: {e}\n"
                        f"Active pydicom handlers: {handlers}\n"
                        "Missing image codecs. Please ensure 'pillow', 'pylibjpeg', or 'gdcm' are installed."
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
        
        # Ensure BitsAllocated matches array data type
        # SidecarPixelLoader relies on this to determine uint8 vs uint16
        bits = array.itemsize * 8
        self.set_attr("0028,0100", bits)
        
        self._mod_count += 1


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
    _dirty: bool = field(default=True, init=False)

    def __post_init__(self):
        self._dirty = True

    def mark_clean(self):
        self._dirty = False
        for i in self.instances:
            i.mark_clean()


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
    _dirty: bool = field(default=True, init=False)

    def __post_init__(self):
        self._dirty = True

    def mark_clean(self):
        self._dirty = False
        for s in self.series:
            s.mark_clean()


@dataclass(slots=True)
class Patient:
    """
    Root of the object hierarchy. Groups Studies by Patient ID.
    """
    patient_id: str
    patient_name: str
    studies: List[Study] = field(default_factory=list)
    _dirty: bool = field(default=True, init=False)

    def __post_init__(self):
        self._dirty = True

    def mark_clean(self):
        self._dirty = False
        for s in self.studies:
            s.mark_clean()