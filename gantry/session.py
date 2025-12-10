import glob
from .io_handlers import DicomStore, DicomImporter, DicomExporter
from .services import RedactionService
from .config_manager import ConfigLoader

class DicomSession:
    """
    The Main Facade for the Gantry library.
    Manages the lifecycle of the DicomStore (Load/Import/Redact/Export/Save).
    """
    def __init__(self, persistence_file="dicom_session.pkl"):
        self.persistence_file = persistence_file
        self.store = DicomStore.load_state(persistence_file)
        self.active_rules: List[Dict[str, Any]] = []

        print(f"Session started. {len(self.store.patients)} patients loaded.")

    def import_folder(self, folder_path):
        """Scans a folder for .dcm files and imports them into the session."""
        files = glob.glob(f"{folder_path}/*.dcm")
        DicomImporter.import_files(files, self.store)
        self._save()

    def inventory(self):
        """Prints a summary of devices (Manufacturers/Models) found in the current session."""
        eqs = self.store.get_unique_equipment()
        print(f"\nInventory: {len(eqs)} Devices")
        for e in eqs:
            print(f" - {e.manufacturer} {e.model_name} (S/N: {e.device_serial_number})")

    def redact_by_machine(self, serial_number, roi):
        """
        Manually triggers redaction for a machine.
        roi: (r1, r2, c1, c2)
        """
        svc = RedactionService(self.store)
        svc.redact_machine_region(serial_number, roi)

    def export(self, folder):
        """Exports the current state of all patients to a folder."""
        print("Exporting...")
        for p in self.store.patients:
            DicomExporter.save_patient(p, folder)
        print("Done.")

    def load_config(self, config_file: str):
        """
        User Action: 'Load these rules into memory, but DO NOT run them yet.'
        Useful for validation or previewing what will happen.
        """
        try:
            print(f"Loading configuration from {config_file}...")
            self.active_rules = ConfigLoader.load_rules(config_file)
            print(f"Loaded {len(self.active_rules)} machine rule definitions.")
            print("Tip: Run .preview_config() to see matches, or .execute_config() to apply.")
        except Exception as e:
            print(f"Load failed: {e}")
            self.active_rules = []

    def preview_config(self):
        """
        User Action: 'Tell me what WOULD happen if I ran these rules.'
        checks the loaded rules against the current Store inventory.
        """
        if not self.active_rules:
            print("No configuration loaded. Use .load_config() first.")
            return

        print("\n--- Dry Run / Configuration Preview ---")

        # We need the index to check matches
        # We instantiate the service just to query the index, not to modify
        service = RedactionService(self.store)

        match_count = 0

        for rule in self.active_rules:
            serial = rule.get("serial_number", "UNKNOWN")
            model = rule.get("model_name", "Unknown Model")
            zones = rule.get("redaction_zones", [])

            # check matches in store
            targets = service.index.get_by_machine(serial)

            if targets:
                count = len(targets)
                match_count += count
                print(f"MATCH: '{serial}' ({model})")
                print(f"    - Found {count} images in current session.")
                print(f"    - Actions: Will apply {len(zones)} redaction zones.")
            else:
                print(f"NO MATCH: '{serial}'. Rule loaded, but no images found.")

        print(f"\nSummary: Execution will modify approximately {match_count} images.")
        print("---------------------------------------")

    def execute_config(self):
        """
        User Action: 'Apply the currently loaded rules to the pixel data.'
        """
        if not self.active_rules:
            print("No configuration loaded. Use .load_config() first.")
            return

        print(f"\nExecuting {len(self.active_rules)} rules...")
        service = RedactionService(self.store)

        try:
            for rule in self.active_rules:
                service.process_machine_rules(rule)

            # Save state after modification
            self._save()
            print("Execution Complete. Session saved.")

            # Clear rules after execution?
            # Optional: Keep them if user wants to run again on new imports.
            # self.active_rules = []

        except Exception as e:
            print(f"Execution interrupted: {e}")

    def _save(self):
        self.store.save_state(self.persistence_file)

