from typing import Dict, List
from .entities import Instance, Patient, DicomItem, DicomSequence
from .io_handlers import DicomStore
from .logger import get_logger
from tqdm import tqdm


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
    def __init__(self, store: DicomStore, store_backend=None):
        self.store = store
        self.index = MachinePixelIndex()
        self.index.index_store(store)
        self.logger = get_logger()
        self.store_backend = store_backend

    def scan_burned_in_annotations(self):
        """
        Scans all instances for 'Burned In Annotation' (0028,0301) == 'YES'.
        Logs warnings for any found that have NOT been remediated (Image Type not DERIVED).
        User requested we 'Must treat them somehow'.
        """
        self.logger.info("Scanning for untreated Burned In Annotations...")
        count = 0
        untreated = 0
        
        for p in self.store.patients:
            for st in p.studies:
                for se in st.series:
                    for inst in se.instances:
                         # Check Tag (case insensitive)
                         val = inst.attributes.get("0028,0301", "NO")
                         if isinstance(val, str) and "YES" in val.upper():
                             count += 1
                             # Check if we remediated it
                             img_type = inst.attributes.get("0008,0008", [])
                             if isinstance(img_type, str): img_type = [img_type]
                             
                             is_treated = any("DERIVED" in str(x).upper() for x in img_type)
                             
                             if not is_treated:
                                 untreated += 1
                                 if untreated <= 5:
                                     self.logger.error(f"High Risk: Untreated Burned In Annotation in {inst.sop_instance_uid}")
                                 elif untreated == 6:
                                      self.logger.error("... (Suppressing further individual errors for Burned In Annotations) ...")
                                 
                                 if self.store_backend:
                                     self.store_backend.log_audit(
                                         action_type="RISK",
                                         entity_uid=inst.sop_instance_uid,
                                         details="Burned In Annotation (0028,0301) present but not remediated."
                                     )
        
        if untreated > 0:
            self.logger.warning(f"Found {untreated} instances with potential Burned In Annotations that were NOT remediated.")
            print(f"WARNING: {untreated} instances flagged with 'Burned In Annotation' were not targeted by any rule.")
            print("Action Required: Review audit logs or add rules for these instances.")
        elif count > 0:
            self.logger.info(f"Verified {count} Burned In Annotations were remediated.")

        targets = self.index.get_by_machine(serial)
        if not targets:
            if show_progress: # Reusing show_progress as proxy for verbose logging during rule planning? No, let's stick to explicit args.
                 # Wait, user asked to remove warnings unless "user asks for them".
                 # Using the new signature requires update in session.py too.
                 pass
            self.logger.warning(f"Config rule exists for {serial}, but no matching images found in Session.")
            return

    def process_machine_rules(self, machine_rules: dict, show_progress: bool = True, verbose: bool = False):
        """
        Applies all zones defined in a single machine config object.
        """
        serial = machine_rules.get("serial_number")
        zones = machine_rules.get("redaction_zones", [])

        if not serial:
            if verbose: self.logger.warning("Skipping rule with missing serial number.")
            return

        if not zones:
            if verbose: self.logger.info(f"Machine {serial} has no redaction zones configured. Skipping.")
            return

        # Check if we even have this machine in our store
        # (Optimization: Don't load pixels if machine isn't in the dataset)
        targets = self.index.get_by_machine(serial)
        if not targets:
            if verbose: self.logger.warning(f"Config rule exists for {serial}, but no matching images found in Session.")
            return

        if verbose: self.logger.info(f"Applying config rules for Machine: {serial} ({len(targets)} images)...")

        valid_rois = []
        for zone in zones:
            if isinstance(zone, list):
                # Legacy/Simplified format: zone IS the ROI
                roi = zone
            else:
                 roi = zone.get("roi")  # Expected [r1, r2, c1, c2]
            
            if roi and len(roi) == 4:
                valid_rois.append(tuple(roi))
            else:
                self.logger.warning(f"Invalid ROI format in config: {roi}")
        
        if valid_rois:
            self.redact_machine_instances(serial, valid_rois, show_progress=show_progress, verbose=verbose)

    def redact_machine_instances(self, machine_sn: str, rois: List[tuple], show_progress: bool = True, verbose: bool = False):
        """
        Applies a LIST of ROIs to all images from the specified machine.
        Optimized to iterate images ONCE.
        """
        targets = self.index.get_by_machine(machine_sn)
        self.logger.info(f"Redacting {len(targets)} images for {machine_sn} ({len(rois)} zones)...")
        
        if self.store_backend and targets:
             self.store_backend.log_audit(
                action_type="REDACTION",
                entity_uid=machine_sn,
                details=f"Redacting {len(targets)} images with {len(rois)} zones"
            )

        for inst in tqdm(targets, desc=f"Redacting {machine_sn}", unit="img", disable=not show_progress):
            try:
                # Triggers Lazy Load from disk
                arr = inst.get_pixel_data()
                
                if arr is None:
                    if verbose: self.logger.warning(f"  Skipping {inst.sop_instance_uid}: No pixel data found (or file missing).")
                    continue

                modified = False
                for roi in rois:
                    if self._apply_roi_to_instance(inst, arr, roi):
                        modified = True
                
                if modified:
                    self._apply_redaction_flags(inst)
                    inst.regenerate_uid()
                    self.logger.debug(f"  Modified {inst.sop_instance_uid}")

            except Exception as e:
                self.logger.error(f"  Failed {inst.sop_instance_uid}: {e}")

    def _apply_roi_to_instance(self, inst: Instance, arr, roi: tuple) -> bool:
        """
        Applies a single ROI to the pixel array in place.
        Returns True if successful/applied.
        """
        try:
            r1, r2, c1, c2 = roi
            
            # Identify Dimensions & Indices
            ndim = len(arr.shape)
            
            # Default to last two dimensions (standard Grayscale/Planar)
            row_dim = ndim - 2
            col_dim = ndim - 1
            
            if ndim >= 3 and arr.shape[-1] in [3, 4]:
                 # RGB/RGBA Interleaved: (..., Rows, Cols, Channels)
                 row_dim = ndim - 3
                 col_dim = ndim - 2
            
            rows = arr.shape[row_dim]
            cols = arr.shape[col_dim]
            
            # Safety Checks
            if r1 >= rows or c1 >= cols:
                 # self.logger.warning(f"ROI {roi} is completely outside image dimensions ({rows}x{cols}). Skipping.")
                 return False
            
            # Clipping
            r2_clamped = min(r2, rows)
            c2_clamped = min(c2, cols)
            
            # Construct Slices dynamically
            slices = [slice(None)] * ndim
            slices[row_dim] = slice(r1, r2_clamped)
            slices[col_dim] = slice(c1, c2_clamped)
            
            # Apply Redaction
            arr[tuple(slices)] = 0
            return True
        except:
            return False



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