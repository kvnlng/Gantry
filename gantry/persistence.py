import sqlite3
import os
import json
from typing import List, Optional
from datetime import datetime
from .entities import Patient, Study, Series, Instance, Equipment
from .logger import get_logger

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
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = get_logger()
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(self.SCHEMA)

    def log_audit(self, action_type: str, entity_uid: str, details: str):
        """Records an action in the audit log."""
        timestamp = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO audit_log (timestamp, action_type, entity_uid, details) VALUES (?, ?, ?, ?)",
                (timestamp, action_type, entity_uid, details)
            )

    def load_all(self) -> List[Patient]:
        """
        Reconstructs the entire object graph from the database.
        Returns a list of Patient objects.
        """
        patients = []
        if not os.path.exists(self.db_path):
            return patients

        try:
            with sqlite3.connect(self.db_path) as conn:
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
                    # Restore extra attributes
                    if r['attributes_json']:
                        try:
                            attrs = json.loads(r['attributes_json'], object_hook=gantry_json_object_hook)
                            inst.attributes.update(attrs)
                        except: 
                            pass # JSON error
                    
                    if r['series_id_fk'] in se_map:
                        se_map[r['series_id_fk']].instances.append(inst)

            self.logger.info(f"Loaded {len(patients)} patients from {self.db_path}")
            return patients

        except sqlite3.Error as e:
            self.logger.error(f"Failed to load PDF from DB: {e}")
            return []

    def save_all(self, patients: List[Patient]):
        """
        Persists the current state.
        Strategy: TRUNCATE and RE-INSERT (except Audit Log).
        """
        self.logger.info(f"Saving {len(patients)} patients to {self.db_path}...")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
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
                                # Serialize non-standard attributes if needed?
                                # For now, let's just save what we have. 
                                # Note: 'attributes' dict might be huge. We might only want to save what's NOT standard?
                                # Or just save nothing extra for now as lazy loading re-reads from disk?
                                # CRITICAL: If we modified the object (redaction/anonymization), the file on disk is WRONG.
                                # So we MUST persist changes.
                                # However, our current architecture writes redacted files to 'export'.
                                # So 'file_path' usually points to ORIGINAL.
                                # If we modify metadata in memory, we need to save it. 
                                # Let's save the 'attributes' dict as JSON.
                                
                                attrs_json = json.dumps(inst.attributes, cls=GantryJSONEncoder)
                                cur.execute("""
                                    INSERT INTO instances (series_id_fk, sop_instance_uid, sop_class_uid, instance_number, file_path, attributes_json)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (se_pk, inst.sop_instance_uid, inst.sop_class_uid, inst.instance_number, inst.file_path, attrs_json))
                
                conn.commit()
                self.logger.info("Save complete.")

        except sqlite3.Error as e:
            self.logger.error(f"Failed to save to DB: {e}")

    def update_attributes(self, instances: List[Patient]):
        """
        Efficiently updates the attributes_json for a list of instances.
        Avoids full delete/insert cycle.
        Assumes instances are already tracked (have valid identities).
        """
        if not instances:
            return

        self.logger.info(f"Updating attributes for {len(instances)} instances...")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                
                # Pre-calculate data for executemany
                data = []
                for inst in instances:
                    attrs_json = json.dumps(inst.attributes, cls=GantryJSONEncoder)
                    # We rely on SOP Instance UID as the key
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

class GantryJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            import base64
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
