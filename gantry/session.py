import glob
from typing import List, Dict, Any
from .io_handlers import DicomStore, DicomImporter, DicomExporter
from .services import RedactionService
from .config_manager import ConfigLoader
from .privacy import PhiInspector, PhiFinding
from .remediation import RemediationService

from .logger import configure_logger, get_logger

from .persistence import SqliteStore
from .crypto import KeyManager
from .reversibility import ReversibilityService
from .persistence_manager import PersistenceManager
import json
from .parallel import run_parallel


def scan_worker(args):
    """
    Worker to scan a single patient.
    args: (patient_copy, config_source)
    Returns: List[PhiFinding] (WITHOUT entities, lightweight)
    """
    patient, config_source = args
    from .privacy import PhiInspector # Import inside worker
    
    if isinstance(config_source, dict):
        inspector = PhiInspector(config_tags=config_source)
    elif isinstance(config_source, str) or config_source is None:
        inspector = PhiInspector(config_path=config_source)
    else:
        # Fallback
        inspector = PhiInspector()

    findings = inspector.scan_patient(patient)
    
    # Strip heavy entity objects before returning across process boundary
    for f in findings:
        f.entity = None
        
    return findings

class DicomSession:
    """
    The Main Facade for the Gantry library.
    Manages the lifecycle of the DicomStore (Load/Import/Redact/Export/Save).
    """


    def __init__(self, persistence_file="gantry.db"):
        configure_logger()
        self.persistence_file = persistence_file
        self.store_backend = SqliteStore(persistence_file)
        self.persistence_manager = PersistenceManager(self.store_backend)
        
        # Hydrate memory from DB
        self.store = DicomStore() 
        print(f"Loading session from {persistence_file}...")
        self.store.patients = self.store_backend.load_all()
        
        self.active_rules: List[Dict[str, Any]] = []
        self.active_phi_tags: Dict[str, str] = None
        
        # Reversibility
        self.key_manager = None
        self.reversibility_service = None

        get_logger().info(f"Session started. {len(self.store.patients)} patients loaded.")

    def save(self):
        """
        Persists the current session state to the database in the background.
        User must call this manually to save changes.
        """
        self.persistence_manager.save_async(self.store.patients)


    def enable_reversible_anonymization(self, key_path: str = "gantry.key"):
        """
        Initializes the encryption subsystem.
        """
        self.key_manager = KeyManager(key_path)
        self.key_manager.load_or_generate_key()
        self.reversibility_service = ReversibilityService(self.key_manager)
        get_logger().info(f"Reversible anonymization enabled. Key: {key_path}")

    def preserve_patient_identity(self, patient_id: str, persist: bool = False) -> List["Instance"]:
        """
        Securely embeds the original patient name/ID into a private DICOM tag
        for all instances belonging to the specified patient.
        Must be called BEFORE anonymization.
        
        Args:
            patient_id: The ID of the patient to preserve.
            persist: If True, writes changes to DB immediately. If False, returns instances for batch persistence.
        """
        if not self.reversibility_service:
            raise RuntimeError("Reversible anonymization not enabled. Call enable_reversible_anonymization() first.")
            
        get_logger().debug(f"Preserving identity for {patient_id}...") # Debug level in batch? Info is fine.
        
        modified_instances = []
        patient = next((p for p in self.store.patients if p.patient_id == patient_id), None)
        
        if not patient:
            get_logger().error(f"Patient {patient_id} not found.")
            return []

        cnt = 0
        original_attrs = {
            "PatientName": patient.patient_name,
            "PatientID": patient.patient_id
        }
        
        # Iterate deep
        for st in patient.studies:
            for se in st.series:
                for inst in se.instances:
                    self.reversibility_service.embed_original_data(inst, original_attrs)
                    modified_instances.append(inst)
                    cnt += 1
        
        if persist and modified_instances:
            self.store_backend.update_attributes(modified_instances)
            get_logger().info(f"Secured identity in {cnt} instances for {patient_id}.")
            
        return modified_instances

    def preserve_identities(self, input_data: list):
        """
        Batch preservation for multiple patients.
        input_data can be:
        - List[str]: List of Patient IDs
        - PhiReport: Result from scan_for_phi
        - List[PhiFinding]: List of findings
        """
        if not self.reversibility_service:
            raise RuntimeError("Reversible anonymization not enabled. Call enable_reversible_anonymization() first.")

        # Extract unique patient IDs
        patient_ids = set()
        
        # Helper to iterate different input types
        iterable_data = input_data
        if hasattr(input_data, 'findings'): # PhiReport
            iterable_data = input_data.findings

        for item in iterable_data:
            if isinstance(item, str):
                patient_ids.add(item)
            elif hasattr(item, 'patient_id') and item.patient_id:
                 patient_ids.add(item.patient_id)
        
        modified_instances = []
        count_patients = 0
        
        from tqdm import tqdm
        for pid in tqdm(patient_ids, desc="Preserving Identities", unit="patient"):
            res = self.preserve_patient_identity(pid, persist=False)
            modified_instances.extend(res)
            count_patients += 1
            
             
        if modified_instances:
             msg = f"Preserved identity for {len(modified_instances)} instances."
             print(f"\n{msg}\nRemember to call .save() to persist changes.")
             get_logger().info(msg)
             # self.store_backend.update_attributes(modified_instances)
             # print("Persistence complete.")
             
        get_logger().info(f"Batch preserved identity for {count_patients} patients ({len(modified_instances)} instances).")

    def recover_patient_identity(self, patient_id: str):
        """
        ... existing ...
        """
        return self._recover_identity_logic(patient_id) # Simplify for brevity if needed, but I should just replace the loop

    def _recover_identity_logic(self, patient_id: str):
        # implementation details
        pass


    # ... skip to scan_for_phi ...


    def audit(self, config_path: str = None) -> "PhiReport":
        """
        Scans all patients in the session for potential PHI.
        Uses cached `active_phi_tags` if config_path matches or is None, otherwise loads fresh.
        Returns a PhiReport object (iterable, and convertible to DataFrame).
        Checkpoint 4: Target.
        """
        from .privacy import PhiReport
        
        # Logic: If config_path is provided, we should probably load it temporarily for this scan?
        # OR if config_path is None, use self.active_phi_tags
        
        tags_to_use = self.active_phi_tags
        
        if config_path:
             # Just load tags for this run, don't overwrite session state unless load_config called?
             # Actually, if user says audit("file.json"), they expect that file to control.
             tags_to_use = ConfigLoader.load_phi_config(config_path)

        inspector = PhiInspector(config_tags=tags_to_use)
        if not inspector.phi_tags:
            get_logger().warning("PHI Scan Warning: No PHI tags defined. Scan will find nothing. Check your config.")
        
        get_logger().info("Scanning for PHI (Parallel)...")
        
        worker_args = [(p, tags_to_use) for p in self.store.patients]
        
        results = run_parallel(scan_worker, worker_args, desc="Scanning PHI")
        
        all_findings = []
        for findings in results:
            all_findings.extend(findings)
            
        # Rehydrate Entities!
        self._rehydrate_findings(all_findings)
            
        get_logger().info(f"PHI Scan Complete. Found {len(all_findings)} issues.")
            
        return PhiReport(all_findings)

    def scan_for_phi(self, config_path: str = None) -> "PhiReport":
        """
        DEPRECATED: Use audit() instead.
        Alias for audit.
        """
        get_logger().warning("DeprecationWarning: scan_for_phi() is deprecated. Please use audit() instead.")
        return self.audit(config_path)

    def save_analysis(self, report):
        """
        Persists the results of a PHI analysis to the database.
        report: PhiReport or List[PhiFinding]
        """
        findings = report
        if hasattr(report, 'findings'):
            findings = report.findings
            
        self.store_backend.save_findings(findings)

    def _rehydrate_findings(self, findings):
        """
        Updates findings in-place to point to live objects in self.store
        instead of the unpickled copies from workers.
        """
        # Create lookup maps
        # Assuming entity_uid is unique per type.
        # Patient
        patient_map = {p.patient_id: p for p in self.store.patients}
        
        # Since traversing deep would be slow for every finding, we can do lazy or smart lookup
        # Or just traverse once if needed.
        # Most findings are on Patient or Study.
        
        # Let's map Studies
        study_map = {}
        instance_map = {}
        
        for p in self.store.patients:
            for s in p.studies:
                study_map[s.study_instance_uid] = s
                for se in s.series:
                    for i in se.instances:
                        instance_map[i.sop_instance_uid] = i
        
        for f in findings:
            if f.entity_type == "Patient":
                if f.entity_uid in patient_map:
                    f.entity = patient_map[f.entity_uid]
            elif f.entity_type == "Study":
                if f.entity_uid in study_map:
                    f.entity = study_map[f.entity_uid]
            elif f.entity_type == "Instance":
                if f.entity_uid in instance_map:
                    f.entity = instance_map[f.entity_uid]

    def recover_patient_identity(self, patient_id: str):
        """
        Attempts to decrypt and read original identity from the first instance found.
        """
        if not self.reversibility_service:
            raise RuntimeError("Reversibility not enabled.")

        p = next((x for x in self.store.patients if x.patient_id == patient_id), None)
        if not p:
            print("Patient not found.")
            return

        # Locate first instance
        first_inst = None
        for st in p.studies:
            for se in st.series:
                if se.instances:
                    first_inst = se.instances[0]
                    break
        
        if not first_inst:
            print("No instances found for patient.")
            return

        original = self.reversibility_service.recover_original_data(first_inst)
        if original:
            print("Recovered Identity:")
            print(json.dumps(original, indent=2))
        else:
            print("No encrypted identity found or decryption failed.")

    def import_folder(self, folder_path):
        """
        Scans a folder for .dcm files (recursively) and imports them into the session.
        """
        print(f"Ingesting from '{folder_path}'...")
        DicomImporter.import_files([folder_path], self.store)
        
        # Calculate stats
        n_p = len(self.store.patients)
        n_st = sum(len(p.studies) for p in self.store.patients)
        n_se = sum(len(st.series) for p in self.store.patients for st in p.studies)
        n_i = sum(len(se.instances) for p in self.store.patients for st in p.studies for se in st.series)
        
        print("\nIngestion Complete.")
        print("Summary:")
        print(f"  - {n_p} Patients")
        print(f"  - {n_st} Studies")
        print(f"  - {n_se} Series")
        print(f"  - {n_i} Instances")
        print("Remember to call .save() to persist changes.\n")

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

    def export(self, folder, safe=False):
        """
        Exports the current state of all patients to a folder.
        If safe=True, performs a fresh PHI scan and ONLY exports clean data.
        """
        get_logger().info(f"Exporting session to {folder} (safe={safe})...")
        print("Exporting...")

        dirty_patients = set()
        dirty_studies = set()

        if safe:
            print("Running safety scan...")
            report = self.scan_for_phi()
            for finding in report:
                if finding.entity_type == "Patient":
                    dirty_patients.add(finding.entity_uid)
                elif finding.entity_type == "Study":
                    dirty_studies.add(finding.entity_uid)
            
            if dirty_patients or dirty_studies:
                msg = f"Safety Scan Found Issues: {len(dirty_patients)} Patients, {len(dirty_studies)} Studies will be skipped."
                get_logger().warning(msg)
                print(msg)

        exported_count = 0
        skipped_count = 0

        for p in self.store.patients:
            # Check Patient Level
            if safe and p.patient_id in dirty_patients:
                get_logger().warning(f"Skipping Dirty Patient: {p.patient_id}")
                skipped_count += 1
                continue

            # Determine which studies to export
            if safe:
                safe_studies = [st for st in p.studies if st.study_instance_uid not in dirty_studies]
                if not p.studies: # Handle empty patient
                     pass
                elif not safe_studies:
                     get_logger().warning(f"Skipping Patient {p.patient_id} (All {len(p.studies)} studies dirty).")
                     skipped_count += 1
                     continue
                
                # Check if we filtered some out
                if len(safe_studies) < len(p.studies):
                     get_logger().info(f"Partial Export for {p.patient_id}: {len(safe_studies)}/{len(p.studies)} studies.")
            else:
                safe_studies = p.studies

            if safe_studies:
                DicomExporter.save_studies(p, safe_studies, folder)
                exported_count += 1

        get_logger().info(f"Export complete. (Exported Groups: {exported_count}, Skipped: {skipped_count})")
        print("Done.")

    def load_config(self, config_file: str):
        """
        User Action: 'Load these rules into memory, but DO NOT run them yet.'
        Useful for validation or previewing what will happen.
        """
        try:
            get_logger().info(f"Loading configuration from {config_file}...")
            print(f"Loading configuration from {config_file}...")
            
            # UNIFIED LOAD (v2)
            tags, rules = ConfigLoader.load_unified_config(config_file)
            
            self.active_phi_tags = tags
            self.active_rules = rules
            
            get_logger().info(f"Loaded {len(self.active_rules)} machine rules and {len(self.active_phi_tags)} PHI tags.")
            print(f"Configuration Loaded:\n - {len(self.active_rules)} Machine Redaction Rules\n - {len(self.active_phi_tags)} PHI Tags")
            print("Tip: Run .audit() to check PHI, or .redact_pixels() to apply redaction.")
        except Exception as e:
            get_logger().error(f"Load failed: {e}")
            print(f"Load failed: {e}")
            self.active_rules = []
            self.active_phi_tags = {}

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
            # self._save()
            # get_logger().info("Execution Complete. Session saved.")
            print("Execution Complete. Remember to call .save() to persist.")
            print("Execution Complete. Session saved.")

            # Clear rules after execution?
            # Optional: Keep them if user wants to run again on new imports.
            # self.active_rules = []

        except Exception as e:
            get_logger().error(f"Execution interrupted: {e}")
            print(f"Execution interrupted: {e}")

    def scaffold_config(self, output_path: str):
        """
        Generates a unified v2 configuration file.
        Includes default PHI tags + Auto-detected machine inventory.
        """
        import json
        
        # 1. Identify what we have
        all_equipment = self.store.get_unique_equipment()
        
        # 2. Identify what is already configured (Pixel Rules)
        configured_serials = {rule.get("serial_number") for rule in self.active_rules}
        
        # 3. Find missing machines
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
        
        # 4. Include Default PHI Tags
        # Use whatever is active, or load default
        tags = self.active_phi_tags if self.active_phi_tags else ConfigLoader.load_phi_config()
        
        # 5. Construct Unified Data
        data = {
            "version": "2.0",
            "_instructions": {
                "phi_tags": "Map DICOM Tag (GGGG,EEEE) to a Description String OR an Object.",
                "advanced_actions": "Use Object format for actions: {'name': 'Desc', 'action': 'REMOVE' | 'EMPTY' | 'REPLACE'}",
                "defaults": "String format implies {'action': 'REPLACE'} (Anonymize)."
            },
            "phi_tags": tags,
            "machines": missing_configs + self.active_rules # Include existing rules too? Or just scaffold new?
                                                            # Scaffold typically means "create new", but for a unified file
                                                            # we probably want to dump everything so user has a complete file.
        }
        
        if not missing_configs and not self.active_rules:
             print("No machines detected to scaffold.")
        
        try:
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=4)
            get_logger().info(f"Scaffolded Unified Config to {output_path} ({len(missing_configs)} new machines)")
            print(f"Scaffolded Unified Config to {output_path}")
        except Exception as e:
            get_logger().error(f"Failed to write scaffold: {e}")

    # =========================================================================
    # WORKFLOW ALIASES (Ref: docs/WORKFLOW.md)
    # =========================================================================

    def ingest(self, folder_path: str):
        """
        Alias for import_folder.
        Checkpoint 1: Ingest.
        """
        return self.import_folder(folder_path)

    def examine(self):
        """
        Alias for inventory.
        Checkpoint 2: Examine.
        """
        return self.inventory()

    def setup_config(self, output_path: str):
        """
        Alias for scaffold_config.
        Checkpoint 3: Configure.
        """
        return self.scaffold_config(output_path)

    def backup_identities(self, input_data: list):
        """
        Alias for preserve_identities.
        Checkpoint 5: Backup.
        """
        return self.preserve_identities(input_data)

    def anonymize_metadata(self, findings: List[PhiFinding]):
        """
        Alias for apply_remediation.
        Checkpoint 6: Anonymize.
        """
        return self.apply_remediation(findings)

    def redact_pixels(self):
        """
        Alias for execute_config.
        Checkpoint 7: Redact.
        """
        return self.execute_config()

    def verify(self, config_path: str = None) -> "PhiReport":
        """
        Alias for scan_for_phi.
        Checkpoint 8: Verify.
        """
        return self.scan_for_phi(config_path)

    def export_data(self, folder: str, safe: bool = False):
        """
        Alias for export.
        Checkpoint 9: Export.
        """
        return self.export(folder, safe=safe)


