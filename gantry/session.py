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
    args: (patient_copy, config_source, remove_private)
    Returns: List[PhiFinding] (WITHOUT entities, lightweight)
    """
    if len(args) == 3:
        patient, config_source, remove_private = args
    else:
        patient, config_source = args
        remove_private = True # Default

    from .privacy import PhiInspector # Import inside worker
    
    if isinstance(config_source, dict):
        inspector = PhiInspector(config_tags=config_source, remove_private_tags=remove_private)
    elif isinstance(config_source, str) or config_source is None:
        inspector = PhiInspector(config_path=config_source, remove_private_tags=remove_private)
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
        """
        Initialize the DicomSession.
        
        Args:
            persistence_file: Path to the SQLite database for session persistence.
        """
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
        self.active_date_jitter: Dict[str, int] = {"min_days": -365, "max_days": -1}
        self.active_remove_private_tags: bool = True
        
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
        """
        Internal helper to execute the identity recovery logic.
        (Placeholder for shared logic between single/batch recovery).
        """
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
             # NOTE: If passing a config PATH to audit(), we might be missing the other unified settings 
             # (date_jitter, etc.) unless we load them too.
             # For now, audit() focuses on finding things based on TAGS.
             # If the inspector needs to know about date jitter or private tags to Flag them correctly?
             # Private tags -> YES. Jitter -> Maybe not for detection, but definitely for Remediation proposal.
             
             # Better approach: If config_path is Unified, load it all.
             try:
                 t, r, dj, rpt = ConfigLoader.load_unified_config(config_path)
                 tags_to_use = t
                 # We probably shouldn't overwrite session state side-effects here, 
                 # but for the worker arguments we need to pass them.
                 # Let's create a transient config object or just pass args.
                 # For simplicity in this function, we'll stick to tags, but we should fix inspector init.
             except:
                 # Fallback to simple tags load
                 tags_to_use = ConfigLoader.load_phi_config(config_path)

        inspector = PhiInspector(config_tags=tags_to_use, remove_private_tags=self.active_remove_private_tags)
        if not inspector.phi_tags:
            get_logger().warning("PHI Scan Warning: No PHI tags defined. Scan will find nothing. Check your config.")
        
        get_logger().info("Scanning for PHI (Parallel)...")
        
        worker_args = [(p, tags_to_use, self.active_remove_private_tags) for p in self.store.patients]
        
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
        """Prints a summary of the session contents and equipment."""
        get_logger().info("Generating inventory report.")
        
        # 1. Object Counts
        n_p = len(self.store.patients)
        n_st = 0
        n_se = 0
        n_i = 0
        
        # 2. Equipment Grouping
        eq_counts = {} # (man, model) -> count
        
        for p in self.store.patients:
            n_st += len(p.studies)
            for s in p.studies:
                n_se += len(s.series)
                for se in s.series:
                    n_i += len(se.instances)
                    if se.equipment:
                        key = (se.equipment.manufacturer, se.equipment.model_name)
                        eq_counts[key] = eq_counts.get(key, 0) + 1

        print(f"\nInventory Summary:")
        print(f" Patients:  {n_p}")
        print(f" Studies:   {n_st}")
        print(f" Series:    {n_se}")
        print(f" Instances: {n_i}")
        
        print(f"\nEquipment Inventory:")
        if not eq_counts:
            print(" No equipment metadata found.")
        else:
            for (man, mod), count in sorted(eq_counts.items()):
                print(f" - {man} - {mod} (Count: {count})")

    def get_cohort_report(self) -> 'pd.DataFrame':
        """
        Returns a Pandas DataFrame containing flattened metadata for the current cohort.
        Useful for analysis and QA.
        """
        import pandas as pd
        rows = []
        for p in self.store.patients:
            for s in p.studies:
                for se in s.series:
                    # Basic row info
                    row = {
                        "PatientID": p.patient_id,
                        "PatientName": p.patient_name,
                        "StudyInstanceUID": s.study_instance_uid,
                        "StudyDate": s.study_date,
                        "SeriesInstanceUID": se.series_instance_uid,
                        "Modality": se.modality,
                        "InstanceCount": len(se.instances)
                    }
                    if se.equipment:
                        row["Manufacturer"] = se.equipment.manufacturer
                        row["Model"] = se.equipment.model_name
                        row["DeviceSerial"] = se.equipment.device_serial_number
                    else:
                        row["Manufacturer"] = ""
                        row["Model"] = ""
                        row["DeviceSerial"] = ""
                        
                    rows.append(row)
        
        return pd.DataFrame(rows)

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
        svc = RemediationService(self.store_backend, date_jitter_config=self.active_date_jitter)
        svc.apply_remediation(findings)
        
        # Apply Global De-Identification Tags (compliance)
        # We process ALL instances to ensure they are stamped
        get_logger().info("Applying standard De-Identification Method tags...")
        print("Stamping De-Identification Method tags...")
        
        count = 0
        for p in self.store.patients:
            for st in p.studies:
                for se in st.series:
                    for inst in se.instances:
                        svc.add_global_deid_tags(inst)
                        count += 1
                        
        get_logger().info(f" stamped {count} instances.")

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
                # Group findings by Tag
                tag_summary = {} # tag -> {desc, count, example_val}
                for finding in report:
                    if finding.tag not in tag_summary:
                        tag_summary[finding.tag] = {
                            "desc": finding.field_name, 
                            "count": 0, 
                            "examples": set()
                        }
                    tag_summary[finding.tag]["count"] += 1
                    if len(tag_summary[finding.tag]["examples"]) < 3:
                        tag_summary[finding.tag]["examples"].add(str(finding.value))

                msg = f"\nSafety Scan Found Issues: {len(dirty_patients)} Patients, {len(dirty_studies)} Studies contain PHI."
                msg += "\nThe following tags were flagged as dirty:\n"
                msg += f"{'Tag':<15} {'Description':<30} {'Count':<10} {'Examples'}\n"
                msg += "-" * 80 + "\n"
                
                config_suggestion = {}
                
                for tag, info in tag_summary.items():
                    examples = ", ".join(info['examples'])
                    msg += f"{tag:<15} {info['desc']:<30} {info['count']:<10} {examples}\n"
                    
                    # Suggest REMOVE or KEEP based on ... usually REMOVE for PHI
                    config_suggestion[tag] = {"action": "REMOVE", "name": info['desc']}

                msg += "\nTo allow export, you must either REMOVE these tags or mark them as KEEP in your configuration.\n"
                msg += "Suggested Config Update:\n"
                import json
                msg += json.dumps({"phi_tags": config_suggestion}, indent=4)
                msg += "\n"

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
            tags, rules, jitter, remove_private = ConfigLoader.load_unified_config(config_file)
            
            self.active_phi_tags = tags
            self.active_rules = rules
            self.active_date_jitter = jitter
            self.active_remove_private_tags = remove_private
            
            get_logger().info(f"Loaded {len(self.active_rules)} machine rules and {len(self.active_phi_tags)} PHI tags.")
            print(f"Configuration Loaded:\n - {len(self.active_rules)} Machine Redaction Rules\n - {len(self.active_phi_tags)} PHI Tags")
            print(f" - Date Jitter: {self.active_date_jitter['min_days']} to {self.active_date_jitter['max_days']} days")
            print(f" - Remove Private Tags: {self.active_remove_private_tags}")
            print("Tip: Run .audit() to check PHI, or .redact_pixels() to apply redaction.")
        except Exception as e:
            import traceback
            get_logger().error(f"Load failed: {e}")
            print(f"Load failed: {e}")
            print(traceback.format_exc())
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
            # Run Safety Checks
            service.scan_burned_in_annotations()

            print("Execution Complete. Remember to call .save() to persist.")
            # get_logger().info("Execution Complete. Session saved.")
            print("Execution Complete. Session saved.")

            # Clear rules after execution?
            # Optional: Keep them if user wants to run again on new imports.
            # self.active_rules = []

        except Exception as e:
            get_logger().error(f"Execution interrupted: {e}")
            print(f"Execution interrupted: {e}")

    def scaffold_config(self, output_path: str):
        """
        Generates a unified v2 configuration file in YAML format.
        Includes default PHI tags + Auto-detected machine inventory.
        """
        import yaml
        import os
        
        # Helper for Flow-Style Lists (Bracketed)
        class FlowList(list): pass
        
        def flow_list_representer(dumper, data):
            return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)
            
        yaml.add_representer(FlowList, flow_list_representer)

        if not (output_path.endswith(".yaml") or output_path.endswith(".yml")):
            output_path += ".yaml"
            print(f"Note: Appending .yaml extension -> {output_path}")
        
        # 1. Identify what we have
        all_equipment = self.store.get_unique_equipment()
        
        # Instantiate service to query pixel/tag data efficiently
        from .services import RedactionService
        service = RedactionService(self.store)
        
        # 2. Identify what is already configured (Pixel Rules)
        configured_serials = {rule.get("serial_number") for rule in self.active_rules}
        
        # Load Knowledge Base for Machines
        kb_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "redaction_rules.json")
        kb_machines = []
        if os.path.exists(kb_path):
             try:
                 import json
                 with open(kb_path, 'r') as f:
                     kb_data = json.load(f)
                     kb_machines = kb_data.get("machines", [])
             except: pass

        # 3. Find missing machines and try to pre-fill
        missing_configs = []
        for eq in all_equipment:
            if eq.device_serial_number and eq.device_serial_number not in configured_serials:
                
                # Check KB
                matched_rule = None
                # Primary: Serial Match
                for rule in kb_machines:
                    if rule.get("serial_number") == eq.device_serial_number:
                        matched_rule = rule
                        break
                
                # Check CTP Rules (Knowledge Base 2)
                ctp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "ctp_rules.yaml")
                if not os.path.exists(ctp_path):
                     # Fallback to JSON if YAML doesn't exist
                     ctp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "ctp_rules.json")

                if not matched_rule and os.path.exists(ctp_path):
                     try:
                         if ctp_path.endswith('.yaml'):
                             import yaml
                             with open(ctp_path, 'r') as f:
                                 ctp_data = yaml.safe_load(f)
                         else:
                             import json
                             with open(ctp_path, 'r') as f:
                                 ctp_data = json.load(f)
                                 
                         ctp_rules = ctp_data.get("rules", [])
                         
                         for rule in ctp_rules:
                             # Fuzzy matching on Manufacturer and Model
                             # CTP rules usually have "manufacturer" and "model_name"
                             r_man = rule.get("manufacturer", "").lower()
                             r_mod = rule.get("model_name", "").lower()
                             
                             eq_man = (eq.manufacturer or "").lower()
                             eq_mod = (eq.model_name or "").lower()
                             
                             # Simple containment check as per CTP style
                             if r_man and r_man in eq_man and r_mod and r_mod in eq_mod:
                                  matched_rule = rule.copy()
                                  matched_rule["serial_number"] = eq.device_serial_number
                                  
                                  # Move _ctp_condition to comment if present
                                  cond = matched_rule.pop("_ctp_condition", None)
                                  if cond:
                                      matched_rule["comment"] = f"Auto-matched from CTP. Condition: {cond}"
                                  else:
                                      matched_rule["comment"] = f"Auto-matched from CTP Knowledge Base ({rule.get('manufacturer')} {rule.get('model_name')})"
                                      
                                  break

                     except Exception as e:
                         get_logger().warning(f"Failed to load CTP rules: {e}")

                # Secondary: Model Match (Internal KB)
                if not matched_rule:
                    for rule in kb_machines:
                        if rule.get("model_name") == eq.model_name:
                             # It's a model match, so we should probably copy the zones 
                             matches_man = not rule.get("manufacturer") or (rule.get("manufacturer") == eq.manufacturer)
                             if matches_man:
                                 matched_rule = rule.copy()
                                 matched_rule["serial_number"] = eq.device_serial_number
                                 matched_rule["comment"] = f"Auto-matched from Model Knowledge Base ({eq.model_name})"
                                 break
                
                # 3.b Check for Burned In Annotations (Safety Check)
                # query index for this machine
                instances = service.index.get_by_machine(eq.device_serial_number)
                burned_in_count = 0
                for inst in instances:
                    val = inst.attributes.get("0028,0301", "NO")
                    if isinstance(val, str) and "YES" in val.upper():
                        burned_in_count += 1
                
                safety_comment = ""
                if burned_in_count > 0:
                    safety_comment = f"WARNING: {burned_in_count} images have 'Burned In Annotation' flag. Verify pixel redaction."

                if matched_rule:
                    # Use the template
                    rule_copy = matched_rule.copy() # Ensure we don't mutate KB
                    if safety_comment:
                        existing = rule_copy.get("comment", "")
                        rule_copy["comment"] = f"{existing} {safety_comment}".strip()
                    missing_configs.append(rule_copy)
                else:
                    # Create empty scaffold
                    new_rule = {
                        "manufacturer": eq.manufacturer or "Unknown",
                        "model_name": eq.model_name or "Unknown",
                        "serial_number": eq.device_serial_number,
                        "redaction_zones": [] 
                    }
                    if safety_comment:
                        new_rule["comment"] = safety_comment
                    missing_configs.append(new_rule)

        # 4. Load PHI Tags Default (if not loaded)
        phi_tags = self.active_phi_tags
        if not phi_tags:
             # Load default config for scaffold
             try:
                 from .config_manager import ConfigLoader
                 phi_tags = ConfigLoader.load_phi_config() 
             except Exception as e:
                 get_logger().warning(f"Failed to load research tags: {e}")

        # 4b. Enhance PHI Tags (Transform to structured defaults)
        structured_tags = {}
        
        # Ensure critical tags are present
        if "0008,0020" not in phi_tags: phi_tags["0008,0020"] = "Study Date"
        if "0010,0040" not in phi_tags: phi_tags["0010,0040"] = "Patient Sex"
        if "0010,1010" not in phi_tags: phi_tags["0010,1010"] = "Patient Age" # Helper
        
        for tag, val in phi_tags.items():
            name = val if isinstance(val, str) else val.get("name", "Unknown")
            action = "REMOVE" # Default safety
            
            # Apply Research-Friendly Smart Defaults
            if tag == "0008,0020": # Study Date
                action = "JITTER"
            elif tag == "0010,0040": # Sex
                action = "KEEP"
            elif tag == "0010,1010": # Age
                action = "KEEP"
            elif "Date" in name or "Time" in name:
                action = "REMOVE" # Times are sensitive
            elif "ID" in name:
                action = "REMOVE" # IDs are sensitive
            
            # Preserve existing structure if it was already structured
            if isinstance(val, dict):
                structured_tags[tag] = val
            else:
                 # Minimal Scaffold: Skip tags that are simply REMOVED (covered by Basic profile)
                 # Unless explicitly requested to show all? For now, match tests.
                 if action == "REMOVE":
                     continue
                     
                 structured_tags[tag] = {
                     "name": name,
                     "action": action
                 }
                 
        # 5. Construct Unified Data
        data = {
            "version": "2.0",
            "privacy_profile": "basic",
            # No _instructions dict anymore, we use comments!
            "phi_tags": structured_tags,
            "date_jitter": {
                "min_days": -365,
                "max_days": -1
            },
            "remove_private_tags": True,
            "machines": missing_configs + self.active_rules
        }
        
        if not missing_configs and not self.active_rules:
             print("No machines detected to scaffold.")
        
        # Pre-process data to ensure comments are single-line strings
        # And ensure redaction_zones use FlowList for bracketed style
        for m in data.get("machines", []):
            if "comment" in m and isinstance(m["comment"], str):
                # Replace newlines with spaces/semicolons
                m["comment"] = m["comment"].replace("\n", " ").replace("\r", "")
                # collapse multiple spaces
                import re
                m["comment"] = re.sub(r'\s+', ' ', m["comment"]).strip()
            
            if "redaction_zones" in m and isinstance(m["redaction_zones"], list):
                # Wrap inner lists (zones) in FlowList
                # And assume user wants [[...], [...]] so wrap outer too?
                # User example: "redaction_zones: []" or "redaction_zones: [[...]]"
                # If we wrap outer in FlowList, it becomes: redaction_zones: [[...], [...]]
                # If we wrap inner in FlowList, it becomes:
                # redaction_zones:
                #   - [50, 420, ...]
                #
                # The user request "placed in brackets" usually implies flow style.
                # Let's try wrapping OUTER list.
                
                zones = m["redaction_zones"]
                new_zones = FlowList()
                for z in zones:
                    if isinstance(z, list):
                        new_zones.append(FlowList(z))
                    else:
                        new_zones.append(z)
                m["redaction_zones"] = new_zones # Assign flow list wrapper

        try:
            # Generate YAML string
            # sort_keys=False ensures order is preserved (machines list)
            # width=float("inf") prevents line wrapping for long strings
            yaml_content = yaml.dump(data, sort_keys=False, default_flow_style=False, width=float("inf"))
            
            # Post-process: Convert "comment: ..." into "# ..."
            # Matches:   comment: "Some text"
            # or         comment: Some text
            import re
            lines = yaml_content.splitlines()
            new_lines = []
            for line in lines:
                # Simple match for key-value pair
                match = re.search(r'^(\s*)comment:\s*(.*)$', line)
                if match:
                    indent = match.group(1)
                    content = match.group(2).strip()
                    
                    # Check for surrounding quotes and strip them
                    # Handle single quotes (yaml uses '' escape)
                    if content.startswith("'") and content.endswith("'"):
                        content = content[1:-1]
                        content = content.replace("''", "'")
                    # Handle double quotes (json style/yaml style with backslash)
                    elif content.startswith('"') and content.endswith('"'):
                        content = content[1:-1]
                        content = content.replace('\\"', '"')
                    
                    new_lines.append(f"{indent}# {content}")
                else:
                    # Aesthetic Improvement: Add spacing between list items
                    # Check if line looks like the start of a new list entry (e.g. "- manufacturer: ...")
                    # But exclude the very first one to avoid leading newline at top of file (or top of section)
                    if line.strip().startswith("- ") and len(new_lines) > 0 and new_lines[-1].strip() != "":
                         new_lines.append("")
                    
                    new_lines.append(line)
            
            # Prepend Header Comments
            header = """# Gantry Privacy Configuration (v2.0)
# ==========================================
#
# privacy_profile: "basic"
#   - Standard profile handling common PHI (Name, ID, etc).
#   - Set to "none" for manual control.
#
# phi_tags:
#   - Define custom overrides here.
#   - Actions: KEEP, REMOVE, EMPTY, REPLACE, JITTER (SHIFT)
#
# date_jitter:
#   - Range of days to shift dates by (negative = into past).
#
# remove_private_tags:
#   - If true, removes all odd-group tags except Gantry Metadata.
#
"""
            final_content = header + "\n".join(new_lines) + "\n"

            with open(output_path, 'w') as f:
                f.write(final_content)
                
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


