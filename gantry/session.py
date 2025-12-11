import glob
from typing import List, Dict, Any
from .io_handlers import DicomStore, DicomImporter, DicomExporter
from .services import RedactionService
from .config_manager import ConfigLoader
from .privacy import PhiInspector, PhiFinding
from .remediation import RemediationService

from .logger import configure_logger, get_logger

from .persistence import SqliteStore

class DicomSession:
    """
    The Main Facade for the Gantry library.
    Manages the lifecycle of the DicomStore (Load/Import/Redact/Export/Save).
    """
    def __init__(self, persistence_file="gantry.db"):
        configure_logger()
        self.persistence_file = persistence_file
        self.store_backend = SqliteStore(persistence_file)
        
        # Hydrate memory from DB
        self.store = DicomStore() # Keep the wrapper for now, but populate it
        self.store.patients = self.store_backend.load_all()
        
        self.active_rules: List[Dict[str, Any]] = []

        get_logger().info(f"Session started. {len(self.store.patients)} patients loaded.")

    def import_folder(self, folder_path):
        """
        Scans a folder for .dcm files (recursively) and imports them into the session.
        """
        DicomImporter.import_files([folder_path], self.store)
        self._save()

    def inventory(self):
        get_logger().info("Generating inventory report.")
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
        svc = RedactionService(self.store, self.store_backend)
        svc.redact_machine_region(serial_number, roi)

    def apply_remediation(self, findings: List[PhiFinding]):
        """
        Applies remediation to the current session based on findings.
        Auto-logs to Audit Trail.
        """
        svc = RemediationService(self.store_backend)
        svc.apply_remediation(findings)

    def scan_for_phi(self, config_path: str = None) -> List[PhiFinding]:
        """
        Scans all patients in the session for potential PHI.
        """
        inspector = PhiInspector(config_path)
        if not inspector.phi_tags:
            get_logger().warning("PHI Scan Warning: No PHI tags defined. Scan will find nothing. Check your config.")
            print("⚠️ PHI Scan Warning: No PHI tags defined. Scan will find nothing. Check your config.")
        
        all_findings = []
        
        print("\nScanning for PHI...")
        for patient in self.store.patients:
            findings = inspector.scan_patient(patient)
            all_findings.extend(findings)
            
        get_logger().info(f"PHI Scan Complete. Found {len(all_findings)} issues.")
        print(f"Scan Complete. Found {len(all_findings)} potential PHI issues.")
        for f in all_findings:
            print(f" - [{f.entity_type}] {f.field_name}: {f.value} ({f.reason})")
            
        return all_findings

    def export(self, folder):
        """Exports the current state of all patients to a folder."""
        get_logger().info(f"Exporting session to {folder}...")
        print("Exporting...")
        for p in self.store.patients:
            DicomExporter.save_patient(p, folder)
        get_logger().info("Export complete.")
        print("Done.")

    def load_config(self, config_file: str):
        """
        User Action: 'Load these rules into memory, but DO NOT run them yet.'
        Useful for validation or previewing what will happen.
        """
        try:
            get_logger().info(f"Loading configuration from {config_file}...")
            print(f"Loading configuration from {config_file}...")
            self.active_rules = ConfigLoader.load_redaction_rules(config_file)
            get_logger().info(f"Loaded {len(self.active_rules)} machine rule definitions.")
            print(f"Loaded {len(self.active_rules)} machine rule definitions.")
            print("Tip: Run .preview_config() to see matches, or .execute_config() to apply.")
        except Exception as e:
            get_logger().error(f"Load failed: {e}")
            print(f"Load failed: {e}")
            self.active_rules = []

    def preview_config(self):
        """
        User Action: 'Tell me what WOULD happen if I ran these rules.'
        checks the loaded rules against the current Store inventory.
        """
        if not self.active_rules:
            get_logger().warning("No configuration loaded. Use .load_config() first.")
            print("No configuration loaded. Use .load_config() first.")
            return

        print("\n--- Dry Run / Configuration Preview ---")

        # We need the index to check matches
        # We instantiate the service just to query the index, not to modify
        service = RedactionService(self.store, self.store_backend)

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
            get_logger().warning("No configuration loaded. Use .load_config() first.")
            print("No configuration loaded. Use .load_config() first.")
            return

        print(f"\nExecuting {len(self.active_rules)} rules...")
        service = RedactionService(self.store, self.store_backend)

        try:
            for rule in self.active_rules:
                service.process_machine_rules(rule)

            # Save state after modification
            self._save()
            get_logger().info("Execution Complete. Session saved.")
            print("Execution Complete. Session saved.")

            # Clear rules after execution?
            # Optional: Keep them if user wants to run again on new imports.
            # self.active_rules = []

        except Exception as e:
            get_logger().error(f"Execution interrupted: {e}")
            print(f"Execution interrupted: {e}")

    def scaffold_config(self, output_path: str):
        """
        Generates a skeleton configuration file for machines found in the inventory
        that are NOT covered by the currently loaded rules.
        """
        import json
        
        # 1. Identify what we have
        all_equipment = self.store.get_unique_equipment()
        
        # 2. Identify what is already configured
        configured_serials = {rule.get("serial_number") for rule in self.active_rules}
        
        # 3. Find the gap
        missing_configs = []
        for eq in all_equipment:
            if eq.device_serial_number and eq.device_serial_number not in configured_serials:
                missing_configs.append({
                    "serial_number": eq.device_serial_number,
                    "model_name": eq.model_name,
                    "manufacturer": eq.manufacturer,
                    "comment": "Auto-detected. Please define redaction zones.",
                    "redaction_zones": []
                })
        
        if not missing_configs:
            get_logger().info("All detected machines are already configured. Nothing to scaffold.")
            print("All detected machines are already configured. Nothing to scaffold.")
            return

        # 4. Write to disk
        data = {
            "version": "1.0",
            "machines": missing_configs
        }
        
        try:
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=4)
            get_logger().info(f"Scaffolded configuration for {len(missing_configs)} new machines to {output_path}")
            print(f"Scaffolded configuration for {len(missing_configs)} new machines to {output_path}")
        except Exception as e:
            get_logger().error(f"Failed to write scaffold: {e}")
            print(f"Failed to write scaffold: {e}")

    def _save(self):
        self.store_backend.save_all(self.store.patients)

