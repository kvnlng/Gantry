from typing import Dict, List
from .entities import Instance, Patient, DicomItem, DicomSequence
from .io_handlers import DicomStore
from .logger import get_logger


# Define standard codes for the Sequence
CODE_BASIC_PROFILE = {"0008,0100": "113100", "0008,0102": "DCM", "0008,0104": "Basic Application Confidentiality Profile"}
CODE_CLEAN_PIXEL =   {"0008,0100": "113101", "0008,0102": "DCM", "0008,0104": "Clean Pixel Data Option"}


class MachinePixelIndex:
    """
    Inverted index allowing O(1) retrieval of Instances by Device Serial Number.
    """
    def __init__(self):
        self._index: Dict[str, List[Instance]] = {}

    def index_store(self, store: DicomStore):
        """Indexes all instances in the given store."""
        self._index.clear()
        for p in store.patients:
            for st in p.studies:
                for se in st.series:
                    if se.equipment and se.equipment.device_serial_number:
                        sn = se.equipment.device_serial_number
                        if sn not in self._index: self._index[sn] = []
                        self._index[sn].extend(se.instances)

    def get_by_machine(self, sn):
        return self._index.get(sn, [])


class RedactionService:
    """
    Applies pixel data redaction based on rules.
    """
    def __init__(self, store: DicomStore):
        self.index = MachinePixelIndex()
        self.index.index_store(store)
        self.logger = get_logger()

    def process_machine_rules(self, machine_rules: dict):
        """
        Applies all zones defined in a single machine config object.
        """
        serial = machine_rules.get("serial_number")
        zones = machine_rules.get("redaction_zones", [])

        if not serial:
            self.logger.warning("Skipping rule with missing serial number.")
            return

        # Check if we even have this machine in our store
        # (Optimization: Don't load pixels if machine isn't in the dataset)
        targets = self.index.get_by_machine(serial)
        if not targets:
            self.logger.warning(f"Config rule exists for {serial}, but no matching images found in Session.")
            return

        self.logger.info(f"Applying config rules for Machine: {serial} ({len(targets)} images)...")

        for zone in zones:
            roi = zone.get("roi")  # Expected [r1, r2, c1, c2]
            if roi and len(roi) == 4:
                self.redact_machine_region(serial, tuple(roi))
            else:
                self.logger.warning(f"Invalid ROI format in config: {roi}")

    def redact_machine_region(self, machine_sn: str, roi: tuple):
        """
        Zeroes out a rectangular region for all images from the specified machine.
        roi: (row_start, row_end, col_start, col_end)
        Includes safety checks for image bounds.
        """
        targets = self.index.get_by_machine(machine_sn)
        self.logger.info(f"Redacting {len(targets)} images for {machine_sn}...")

        for inst in targets:
            try:
                # Triggers Lazy Load from disk
                arr = inst.get_pixel_data()
                # Apply redaction in memory
                r1, r2, c1, c2 = roi
                
                rows, cols = arr.shape[-2], arr.shape[-1]
                
                # Safety Checks
                if r1 >= rows or c1 >= cols:
                     self.logger.warning(f"ROI {roi} is completely outside image dimensions ({rows}x{cols}). Skipping.")
                     continue
                
                # Clipping
                r2_clamped = min(r2, rows)
                c2_clamped = min(c2, cols)
                
                if r2_clamped != r2 or c2_clamped != c2:
                    self.logger.warning(f"ROI {roi} extends beyond image ({rows}x{cols}). Clipping to image bounds.")

                arr[r1:r2_clamped, c1:c2_clamped] = 0
                self._apply_redaction_flags(inst)

                inst.regenerate_uid()

                self.logger.debug(f"  Modified {inst.sop_instance_uid}")
            except Exception as e:
                self.logger.error(f"  Failed {inst.sop_instance_uid}: {e}")

    def _apply_redaction_flags(self, inst: Instance):
        """
        Sets DICOM tags indicating Pixel Data modification (Derivation)
        WITHOUT claiming full patient de-identification.
        """
        
        # 1. Image Type (0008,0008)
        # We need to preserve existing values but ensure 'DERIVED' is first.
        # Note: In a robust implementation, we'd read the old value first. 
        # Here we force a standard Derived type.
        current_type = inst.attributes.get("0008,0008", [])
        if isinstance(current_type, str):
            current_type = [current_type]
        
        # Ensure 'DERIVED' is the first value (Value 1)
        new_type = ["DERIVED"] + [x for x in current_type if x != "ORIGINAL" and x != "DERIVED"]
        # Ensure we have at least 'PRIMARY' or 'SECONDARY' as Value 2
        if len(new_type) < 2:
            new_type.append("SECONDARY")
            
        inst.set_attr("0008,0008", new_type)

        # 2. Burned In Annotation (0028,0301) -> NO
        inst.set_attr("0028,0301", "NO")
        
        # 3. Derivation Description (0008,2111)
        inst.set_attr("0008,2111", "Gantry Pixel Redaction: Burned-in PHI removed")
        
        # 4. Derivation Code Sequence (0008,9215)
        # Code 113062: Pixel Data modification
        seq = DicomSequence(tag="0008,9215")
        item = DicomItem()
        item.set_attr("0008,0100", "113062")
        item.set_attr("0008,0102", "DCM")
        item.set_attr("0008,0104", "Pixel Data modification")
        seq.items.append(item)
        
        inst.sequences["0008,9215"] = seq