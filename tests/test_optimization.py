import pytest
import os
import sqlite3
import json
from gantry.session import DicomSession
from gantry.entities import Patient, Study, Series, Instance
from datetime import date

def test_optimization_preservation(tmp_path):
    db_path = str(tmp_path / "opt.db")
    key_path = str(tmp_path / "opt.key")
    
    session = DicomSession(db_path)
    session.enable_reversible_anonymization(key_path)
    
    # 1. Create Patient with Instances
    pid = "OPT_001"
    p = Patient(pid, "Optimization Test")
    st = Study("ST_1", date(2023,1,1))
    se = Series("SE_1", "CT", 1)
    
    instances = []
    for i in range(5):
        inst = Instance(f"SOP_{i}", "1.2.3", i)
        inst.file_path = None
        instances.append(inst)
        se.instances.append(inst)
        
    st.series.append(se)
    p.studies.append(st)
    session.store.patients.append(p)
    session.save()
    session.persistence_manager.flush()
    
    # 2. Monitor SQL to verify update_attributes is used
    # We will hook into sqlite3 to check calls or just verify data is updated without full re-insert
    # But since we mocked/implmented update_attributes, let's verify functional correctness first.
    
    # Preserve Identity
    session.lock_identities(pid)
    session.save()
    session.persistence_manager.flush()
    
    # Verify data persistence
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Check if attributes_json has the private tag
    # Check if attributes_json has the persisted sequence
    cur.execute("SELECT attributes_json FROM instances WHERE sop_instance_uid = 'SOP_0'")
    row = cur.fetchone()
    attrs = json.loads(row[0])
    
    assert "__sequences__" in attrs
    assert "0400,0500" in attrs["__sequences__"]
    
    conn.close()

def test_batch_reversibility(tmp_path):
    db_path = str(tmp_path / "batch.db")
    key_path = str(tmp_path / "batch.key")
    session = DicomSession(db_path)
    session.enable_reversible_anonymization(key_path)
    
    # Create multiple patients
    ids = ["P1", "P2", "P3"]
    for pid in ids:
        p = Patient(pid, f"Name {pid}")
        st = Study(f"ST_{pid}", date(2023,1,1))
        se = Series(f"SE_{pid}", "CT", 1)
        inst = Instance(f"SOP_{pid}", "1.2.3", 1)
        inst.file_path = None
        se.instances.append(inst)
        st.series.append(se)
        p.studies.append(st)
        session.store.patients.append(p)
        
    session.save()
    session.persistence_manager.flush()
    
    # Batch Preserve
    session.lock_identities_batch(ids)
    session.save()
    session.persistence_manager.flush()
    
    # Check persistence
    # Checking for the tag 0400,0500 in the JSON blob
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM instances WHERE attributes_json LIKE '%0400,0500%'")
    count = cur.fetchone()[0]
    assert count == 3
    conn.close()
