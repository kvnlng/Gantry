import sqlite3
import sqlite3
import os
import tempfile
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from .entities import Patient, Study, Series, Instance, Equipment
from .sidecar import SidecarManager
import numpy as np
from .logger import get_logger
from .privacy import PhiFinding, PhiRemediation

class SqliteStore:
    """
    Handles persistence of the Object Graph to a SQLite database.
    Also manages the Audit Log.
    """
    
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS patients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id TEXT NOT NULL,
        patient_name TEXT,
        UNIQUE(patient_id)
    );

    CREATE TABLE IF NOT EXISTS studies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id_fk INTEGER,
        study_instance_uid TEXT NOT NULL,
        study_date TEXT,
        FOREIGN KEY(patient_id_fk) REFERENCES patients(id)
    );

    CREATE TABLE IF NOT EXISTS series (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        study_id_fk INTEGER,
        series_instance_uid TEXT NOT NULL,
        modality TEXT,
        series_number INTEGER,
        manufacturer TEXT,
        model_name TEXT,
        device_serial_number TEXT,
        FOREIGN KEY(study_id_fk) REFERENCES studies(id)
    );

    CREATE TABLE IF NOT EXISTS instances (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        series_id_fk INTEGER,
        sop_instance_uid TEXT NOT NULL,
        sop_class_uid TEXT,
        instance_number INTEGER,
        file_path TEXT,
        pixel_file_id INTEGER DEFAULT 0,
        pixel_offset INTEGER,
        pixel_length INTEGER,
        compress_alg TEXT,
        attributes_json TEXT, -- Store extra attributes as JSON for now
        FOREIGN KEY(series_id_fk) REFERENCES series(id)
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        action_type TEXT,
        entity_uid TEXT,
        details TEXT
    );
    CREATE TABLE IF NOT EXISTS phi_findings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        entity_uid TEXT,
        entity_type TEXT,
        field_name TEXT,
        value TEXT,
        reason TEXT,
        patient_id TEXT,
        remediation_action TEXT,
        remediation_value TEXT,
        details_json TEXT
    );
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = get_logger()
        if db_path == ":memory:":
            # Use a temporary file for sidecar if DB is in-memory
            # SidecarManager currently requires a file path (append-only logic)
            # Create a temp file that persists until process exit (or manual cleanup)
            # We use NamedTemporaryFile but close it so SidecarManager can open/lock it.
            tf = tempfile.NamedTemporaryFile(suffix="_pixels.bin", delete=False)
            self.sidecar_path = tf.name
            tf.close()
            # Note: This temp file won't be auto-deleted by Gantry efficiently in all cases
            # but avoids pollution. In stricter envs, we might want weakref cleanup.
        else:
            self.sidecar_path = os.path.splitext(db_path)[0] + "_pixels.bin"
            
        self.sidecar = SidecarManager(self.sidecar_path)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path, timeout=300.0) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.executescript(self.SCHEMA)

    def log_audit(self, action_type: str, entity_uid: str, details: str):
        """Records an action in the audit log."""
        self.log_audit_batch([(action_type, entity_uid, details)])

    def log_audit_batch(self, entries: List[tuple]):
        """
        Batch inserts audit logs. 
        entries: List of (action_type, entity_uid, details)
        """
        if not entries: return
        
        timestamp = datetime.now().isoformat()
        # Prepare data with timestamp: (timestamp, action, uid, details)
        data = [(timestamp, e[0], e[1], e[2]) for e in entries]
        
        try:
            with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                conn.executemany(
                    "INSERT INTO audit_log (timestamp, action_type, entity_uid, details) VALUES (?, ?, ?, ?)",
                    data
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to batch log audit: {e}")

    def load_all(self) -> List[Patient]:
        """
        Reconstructs the entire object graph from the database.
        Returns a list of Patient objects.
        """
        patients = []
        if not os.path.exists(self.db_path):
            return patients

        try:
            with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()

                # Optimized: We could do joins, but for clarity/mapping let's do hierarchical fetch.
                # Or fetch all and Stitch. Stitching in memory is faster for SQLite than N+1 queries.
                
                # 1. Fetch AlL
                p_rows = cur.execute("SELECT * FROM patients").fetchall()
                st_rows = cur.execute("SELECT * FROM studies").fetchall()
                se_rows = cur.execute("SELECT * FROM series").fetchall()
                i_rows = cur.execute("SELECT * FROM instances").fetchall()

                # 2. Build Maps
                p_map = {}
                for r in p_rows:
                    p = Patient(r['patient_id'], r['patient_name'])
                    p_map[r['id']] = p
                    patients.append(p)

                st_map = {}
                for r in st_rows:
                    st = Study(r['study_instance_uid'], r['study_date'])
                    st_map[r['id']] = st
                    if r['patient_id_fk'] in p_map:
                        p_map[r['patient_id_fk']].studies.append(st)

                se_map = {}
                for r in se_rows:
                    se = Series(r['series_instance_uid'], r['modality'], r['series_number'])
                    if r['manufacturer'] or r['model_name']:
                        se.equipment = Equipment(r['manufacturer'], r['model_name'], r['device_serial_number'])
                    se_map[r['id']] = se
                    if r['study_id_fk'] in st_map:
                        st_map[r['study_id_fk']].series.append(se)

                for r in i_rows:
                    inst = Instance(
                        r['sop_instance_uid'], 
                        r['sop_class_uid'], 
                        r['instance_number'], 
                        file_path=r['file_path']
                    )
                    
                    # Wire up Sidecar Loader if present
                    if r['pixel_offset'] is not None and r['pixel_length'] is not None:
                         # Capture closure vars
                         offset = r['pixel_offset']
                         length = r['pixel_length']
                         alg = r['compress_alg']
                         
                         # We need to reshape after loading. The dimensions are in attributes.
                         # We can do this inside the lambda wrapper or a helper method.
                         # But Instance.attributes aren't populated yet! 
                         # Wait, we populate attributes right after this.
                         # So the lambda calls self.instance methods? No, lambda binds early.
                         
                         def make_loader(mgr, o, l, c, i_ref):
                             def loader():
                                 raw = mgr.read_frame(o, l, c)
                                 arr = np.frombuffer(raw, dtype=np.uint8) # Assumption: uint8?
                                 # Uh oh, we need dtype and shape.
                                 # Generally standard DICOM is uint8 or uint16.
                                 # We should probably store dtype in DB? Or infer from bits stored?
                                 
                                 # For now, let's assume we can reconstruct or simple reshape?
                                 # Wait, tobytes() is raw bytes. np.frombuffer needs dtype.
                                 # We can infer dtype from BitsAllocated in attributes later.
                                 
                                 # We need access to the instance's attributes AT CALL TIME.
                                 bits = i_ref.attributes.get("0028,0100", 8)
                                 # Only 8 or 16 supported typically
                                 dt = np.uint16 if bits > 8 else np.uint8
                                 
                                 arr = np.frombuffer(raw, dtype=dt)
                                 
                                 # Reshape
                                 rows = i_ref.attributes.get("0028,0010", 0)
                                 cols = i_ref.attributes.get("0028,0011", 0)
                                 samples = i_ref.attributes.get("0028,0002", 1)
                                 frames = int(i_ref.attributes.get("0028,0008", 0) or 0)
                                 
                                 # Logic to reconstruct shape
                                 # (Keep consistent with set_pixel_data logic)
                                 if frames > 1:
                                     target_shape = (frames, rows, cols, samples)
                                     if samples == 1: target_shape = (frames, rows, cols)
                                 elif samples > 1:
                                     target_shape = (rows, cols, samples)
                                 else:
                                     target_shape = (rows, cols)
                                 
                                 try:
                                     return arr.reshape(target_shape)
                                 except:
                                     # Fallback if shape calc is wrong
                                     return arr
                                     
                             return loader

                         inst._pixel_loader = make_loader(self.sidecar, offset, length, alg, inst)

                    # Restore extra attributes
                    if r['attributes_json']:
                        try:
                            attrs = json.loads(r['attributes_json'], object_hook=gantry_json_object_hook)
                            self._deserialize_into(inst, attrs)
                        except: 
                            pass # JSON error
                    
                    if r['series_id_fk'] in se_map:
                        se_map[r['series_id_fk']].instances.append(inst)

            self.logger.info(f"Loaded {len(patients)} patients from {self.db_path}")
            return patients

        except sqlite3.Error as e:
            self.logger.error(f"Failed to load PDF from DB: {e}")
            return []

    def load_patient(self, patient_uid: str) -> Optional[Patient]:
        """Loads a single patient and their graph from the DB by PatientID."""
        if not os.path.exists(self.db_path):
            return None
            
        try:
             with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                
                # Fetch Patient
                p_row = cur.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_uid,)).fetchone()
                if not p_row: return None
                
                p = Patient(p_row['patient_id'], p_row['patient_name'])
                p_pk = p_row['id']
                
                # Fetch Studies
                st_rows = cur.execute("SELECT * FROM studies WHERE patient_id_fk = ?", (p_pk,)).fetchall()
                for st_r in st_rows:
                    st = Study(st_r['study_instance_uid'], st_r['study_date'])
                    st_pk = st_r['id']
                    
                    # Fetch Series
                    se_rows = cur.execute("SELECT * FROM series WHERE study_id_fk = ?", (st_pk,)).fetchall()
                    for se_r in se_rows:
                        se = Series(se_r['series_instance_uid'], se_r['modality'], se_r['series_number'])
                        if se_r['manufacturer'] or se_r['model_name']:
                            se.equipment = Equipment(se_r['manufacturer'], se_r['model_name'], se_r['device_serial_number'])
                        se_pk = se_r['id']
                        
                        # Fetch Instances
                        i_rows = cur.execute("SELECT * FROM instances WHERE series_id_fk = ?", (se_pk,)).fetchall()
                        for r in i_rows:
                            inst = Instance(
                                r['sop_instance_uid'], 
                                r['sop_class_uid'], 
                                r['instance_number'], 
                                file_path=r['file_path']
                            )
                            # Wire up Sidecar (Copy-Paste logic from load_all, keep generic?)
                            # ideally refactor _hydrate_instance but inline is fine for now
                            if r['pixel_offset'] is not None and r['pixel_length'] is not None:
                                offset, length, alg = r['pixel_offset'], r['pixel_length'], r['compress_alg']
                                def make_loader(mgr, o, l, c, i_ref):
                                    def loader():
                                        raw = mgr.read_frame(o, l, c)
                                        arr = np.frombuffer(raw, dtype=np.uint8) 
                                        bits = i_ref.attributes.get("0028,0100", 8)
                                        dt = np.uint16 if bits > 8 else np.uint8
                                        arr = np.frombuffer(raw, dtype=dt)
                                        rows = i_ref.attributes.get("0028,0010", 0)
                                        cols = i_ref.attributes.get("0028,0011", 0)
                                        samples = i_ref.attributes.get("0028,0002", 1)
                                        frames = int(i_ref.attributes.get("0028,0008", 0) or 0)
                                        if frames > 1:
                                            target_shape = (frames, rows, cols, samples)
                                            if samples == 1: target_shape = (frames, rows, cols)
                                        elif samples > 1:
                                            target_shape = (rows, cols, samples)
                                        else:
                                            target_shape = (rows, cols)
                                        try: return arr.reshape(target_shape)
                                        except: return arr
                                    return loader
                                inst._pixel_loader = make_loader(self.sidecar, offset, length, alg, inst)

                            if r['attributes_json']:
                                try:
                                    attrs = json.loads(r['attributes_json'], object_hook=gantry_json_object_hook)
                                    self._deserialize_into(inst, attrs)
                                except: pass
                                
                            se.instances.append(inst)
                        
                        st.series.append(se)
                    p.studies.append(st)
                    
                return p
        except sqlite3.Error as e:
            self.logger.error(f"Failed to load patient: {e}")
            return None

    def _serialize_item(self, item: Instance) -> Dict[str, Any]:
        """
        Serializes a DicomItem (or Instance) to a dictionary, including attributes and sequences.
        """
        data = item.attributes.copy()
        if item.sequences:
            seq_data = {}
            for tag, seq in item.sequences.items():
                items_list = []
                for seq_item in seq.items:
                    # Recursive call for sequence items (which are DicomItems)
                    # We can reuse logic but need to handle DicomItem vs Instance
                    # Instance specific fields are handled by caller for the root, 
                    # but for seq items they are just DicomItems.
                    items_list.append(self._serialize_dicom_item(seq_item))
                seq_data[tag] = items_list
            data['__sequences__'] = seq_data
        return data

    def _serialize_dicom_item(self, item) -> Dict[str, Any]:
        """Helper for recursive serialization of generic DicomItems."""
        data = item.attributes.copy()
        if item.sequences:
            seq_data = {}
            for tag, seq in item.sequences.items():
                items_list = [self._serialize_dicom_item(i) for i in seq.items]
                seq_data[tag] = items_list
            data['__sequences__'] = seq_data
        return data

    def _deserialize_into(self, target_item, data: Dict[str, Any]):
        """
        Populates target_item with attributes and sequences from data dict.
        """
        sequences_data = data.pop('__sequences__', None)
        
        # 1. Attributes
        target_item.attributes.update(data)
        
        # 2. Sequences
        if sequences_data:
            from .entities import DicomItem
            for tag, items_list in sequences_data.items():
                for item_data in items_list:
                    new_item = DicomItem()
                    self._deserialize_into(new_item, item_data)
                    target_item.add_sequence_item(tag, new_item)

    def save_all(self, patients: List[Patient]):
        """
        Persists the current state.
        Strategy: TRUNCATE and RE-INSERT (except Audit Log).
        """
        self.logger.info(f"Saving {len(patients)} patients to {self.db_path}...")
        
        pixel_bytes_written = 0
        pixel_frames_written = 0
        sidecar_manager = self.sidecar
        
        try:
            with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                cur = conn.cursor()
                
                # 1. Clear Data Tables (Leave Audit Log)
                cur.execute("DELETE FROM instances")
                cur.execute("DELETE FROM series")
                cur.execute("DELETE FROM studies")
                cur.execute("DELETE FROM patients")
                
                # 2. Re-insert
                for p in patients:
                    cur.execute("INSERT INTO patients (patient_id, patient_name) VALUES (?, ?)", 
                                (p.patient_id, p.patient_name))
                    p_pk = cur.lastrowid
                    
                    for st in p.studies:
                        cur.execute("INSERT INTO studies (patient_id_fk, study_instance_uid, study_date) VALUES (?, ?, ?)",
                                    (p_pk, st.study_instance_uid, st.study_date))
                        st_pk = cur.lastrowid
                        
                        for se in st.series:
                            man = se.equipment.manufacturer if se.equipment else ""
                            mod = se.equipment.model_name if se.equipment else ""
                            sn = se.equipment.device_serial_number if se.equipment else ""
                            
                            cur.execute("""
                                INSERT INTO series (study_id_fk, series_instance_uid, modality, series_number, manufacturer, model_name, device_serial_number)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (st_pk, se.series_instance_uid, se.modality, se.series_number, man, mod, sn))
                            se_pk = cur.lastrowid
                            
                            for inst in se.instances:
                                # Serialize attributes AND sequences
                                full_data = self._serialize_item(inst)
                                attrs_json = json.dumps(full_data, cls=GantryJSONEncoder)
                                
                                full_data = self._serialize_item(inst)
                                attrs_json = json.dumps(full_data, cls=GantryJSONEncoder)
                                
                                # Pixel Persistence Sidecar Logic
                                p_offset = None
                                p_length = None
                                p_alg = None
                                
                                # If pixels are dirty (in memory), write them.
                                # If they came from sidecar (inst._pixel_loader matches our sidecar logic), 
                                # we might theoretically optimize and not rewrite if unchanged?
                                # But append-only is safest for simple "rewrite all" logic.
                                # For now, if pixel_array is present, we write newly.
                                
                                if inst.pixel_array is not None:
                                     b_data = inst.pixel_array.tobytes()
                                     # Simple compression
                                     c_alg = 'zlib'
                                     off, leng = sidecar_manager.write_frame(b_data, c_alg)
                                     p_offset = off
                                     p_length = leng
                                     p_alg = c_alg
                                     
                                     pixel_bytes_written += leng
                                     pixel_frames_written += 1
                                
                                cur.execute("""
                                    INSERT INTO instances (series_id_fk, sop_instance_uid, sop_class_uid, instance_number, file_path, 
                                                           pixel_offset, pixel_length, compress_alg, attributes_json)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (se_pk, inst.sop_instance_uid, inst.sop_class_uid, inst.instance_number, inst.file_path, 
                                      p_offset, p_length, p_alg, attrs_json))
                
                conn.commit()
                
                msg = f"Save complete. DB: {len(patients)} patients."
                if pixel_frames_written > 0:
                    mb = pixel_bytes_written / (1024*1024)
                    msg += f" Sidecar: {pixel_frames_written} frames ({mb:.2f} MB)."
                self.logger.info(msg)

        except sqlite3.Error as e:
            self.logger.error(f"Failed to save to DB: {e}")

    def update_attributes(self, instances: List[Patient]):
        """
        Efficiently updates the attributes_json for a list of instances.
        """
        if not instances:
            return

        self.logger.info(f"Updating attributes for {len(instances)} instances...")
        try:
            with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                cur = conn.cursor()
                
                # Pre-calculate data for executemany
                data = []
                for inst in instances:
                    # Serialize attributes AND sequences
                    full_data = self._serialize_item(inst)
                    attrs_json = json.dumps(full_data, cls=GantryJSONEncoder)
                    data.append((attrs_json, inst.sop_instance_uid))
                
                cur.executemany("""
                    UPDATE instances 
                    SET attributes_json = ? 
                    WHERE sop_instance_uid = ?
                """, data)
                
                conn.commit()
                self.logger.info("Update complete.")
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to update attributes: {e}")

    def save_findings(self, findings: List[PhiFinding]):
        """Persists PHI findings to the database."""
        timestamp = datetime.now().isoformat()
        
        if not findings:
            return

        self.logger.info(f"Saving {len(findings)} PHI findings...")
        
        try:
            with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                cur = conn.cursor()
                
                # Insert
                for f in findings:
                    rem_action = None
                    rem_value = None
                    if f.remediation_proposal:
                        rem_action = f.remediation_proposal.action_type
                        rem_value = str(f.remediation_proposal.new_value)
                        
                    cur.execute("""
                        INSERT INTO phi_findings 
                        (timestamp, entity_uid, entity_type, field_name, value, reason, patient_id, remediation_action, remediation_value, details_json) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (timestamp, f.entity_uid, f.entity_type, f.field_name, str(f.value), f.reason, f.patient_id, rem_action, rem_value, "{}"))
                
                conn.commit()
                self.logger.info("Findings saved.")
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to save findings: {e}")

    def load_findings(self) -> List[PhiFinding]:
        """Loads all findings from the database."""
        findings = []
        if not os.path.exists(self.db_path):
            return findings

        try:
             with sqlite3.connect(self.db_path, timeout=300.0) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                # Check if table exists (backward compatibility for old DBs if init didnt run on them)
                # But _init_db runs on __init__, so schema should be there.
                
                rows = cur.execute("SELECT * FROM phi_findings ORDER BY id").fetchall()
                
                for r in rows:
                    if r['remediation_action']:
                        prop = PhiRemediation(r['remediation_action'], r['field_name'], r['remediation_value'], None) 
                    else: 
                        prop = None
                        
                    f = PhiFinding(
                        entity_uid=r['entity_uid'],
                        entity_type=r['entity_type'],
                        field_name=r['field_name'],
                        value=r['value'],
                        reason=r['reason'],
                        patient_id=r['patient_id'],
                        remediation_proposal=prop
                    )
                    findings.append(f)
                    
        except sqlite3.Error as e:
            self.logger.error(f"Failed to load findings: {e}")
            
        return findings

class GantryJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            import base64
            # Keep consistent with current implementation or standard?
            # Existing was: return {"__type__": "bytes", "data": base64.b64encode(obj).decode('ascii')}
            return {"__type__": "bytes", "data": base64.b64encode(obj).decode('ascii')}
        
        from pydicom.multival import MultiValue
        if isinstance(obj, MultiValue):
            return list(obj)
            
        return super().default(obj)

def gantry_json_object_hook(d):
    if "__type__" in d and d["__type__"] == "bytes":
        import base64
        return base64.b64decode(d["data"])
    return d
