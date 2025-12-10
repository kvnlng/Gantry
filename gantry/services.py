from typing import Dict, List
from .entities import Instance, Patient
from .io_handlers import DicomStore


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

    def process_machine_rules(self, machine_rules: dict):
        """
        Applies all zones defined in a single machine config object.
        """
        serial = machine_rules.get("serial_number")
        zones = machine_rules.get("redaction_zones", [])

        if not serial:
            print("⚠️ Skipping rule with missing serial number.")
            return

        # Check if we even have this machine in our store
        # (Optimization: Don't load pixels if machine isn't in the dataset)
        targets = self.index.get_by_machine(serial)
        if not targets:
            print(f"Config rule exists for {serial}, but no matching images found in Session.")
            return

        print(f"Applying config rules for Machine: {serial} ({len(targets)} images)...")

        for zone in zones:
            roi = zone.get("roi")  # Expected [r1, r2, c1, c2]
            if roi and len(roi) == 4:
                self.redact_machine_region(serial, tuple(roi))
            else:
                print(f"Invalid ROI format in config: {roi}")

    def redact_machine_region(self, machine_sn: str, roi: tuple):
        """
        Zeroes out a rectangular region for all images from the specified machine.
        roi: (row_start, row_end, col_start, col_end)
        Includes safety checks for image bounds.
        """
        targets = self.index.get_by_machine(machine_sn)
        print(f"Redacting {len(targets)} images for {machine_sn}...")

        for inst in targets:
            try:
                # Triggers Lazy Load from disk
                arr = inst.get_pixel_data()
                # Apply redaction in memory
                r1, r2, c1, c2 = roi
                
                rows, cols = arr.shape[-2], arr.shape[-1]
                
                # Safety Checks
                if r1 >= rows or c1 >= cols:
                     print(f"⚠️ ROI {roi} is completely outside image dimensions ({rows}x{cols}). Skipping.")
                     continue
                
                # Clipping
                r2_clamped = min(r2, rows)
                c2_clamped = min(c2, cols)
                
                if r2_clamped != r2 or c2_clamped != c2:
                    print(f"⚠️ ROI {roi} extends beyond image ({rows}x{cols}). Clipping to image bounds.")

                arr[r1:r2_clamped, c1:c2_clamped] = 0
                print(f"  Modified {inst.sop_instance_uid}")
            except Exception as e:
                print(f"  Failed {inst.sop_instance_uid}: {e}")