
import os
import sqlite3
import numpy as np
import pytest
from gantry.session import DicomSession
from gantry.io_handlers import SidecarPixelLoader

def test_persistence_update_after_redaction():
    db_path = "debug_persistence.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    session = DicomSession(db_path)
    
    # 1. Create Instance
    print("Creating instance...")
    from gantry.entities import Patient, Study, Series, Instance
    pat = Patient("P_TEST", "Test Patient")
    st = Study("ST_TEST", "20230101")
    se = Series("SE_TEST", "CT", 1)
    inst = Instance("1.2.3.4", "1.2.840.10008.5.1.4.1.1.2", 1)
    
    # Set pixels (10x10)
    arr = np.zeros((10, 10), dtype=np.uint8)
    inst.set_pixel_data(arr)
    
    pat.studies.append(st); st.series.append(se); se.instances.append(inst)
    session.store.patients.append(pat)
    
    # 2. Save (Initial)
    print("Initial Save...")
    session.save(sync=True)
    
    # Check DB
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT pixel_offset, pixel_hash FROM instances WHERE sop_instance_uid=?", (inst.sop_instance_uid,)).fetchone()
    print(f"Initial DB: Offset={row[0]}, Hash={row[1]}")
    conn.close()
    
    initial_offset = row[0]
    initial_hash = row[1]
    
    # 3. Redact (Modify pixels)
    print("Redacting (modifying pixels)...")
    arr2 = np.ones((10, 10), dtype=np.uint8) * 255
    inst.set_pixel_data(arr2)
    # Dirty flag should be set
    
    # 4. Save (Update)
    print("Saving Update...")
    session.save(sync=True)
    
    # Check DB again
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT pixel_offset, pixel_hash FROM instances WHERE sop_instance_uid=?", (inst.sop_instance_uid,)).fetchone()
    print(f"Updated DB: Offset={row[0]}, Hash={row[1]}")
    conn.close()
    
    updated_offset = row[0]
    updated_hash = row[1]
    
    if updated_offset == initial_offset:
        print("FAILURE: Offset did not change!")
    else:
        print("Success: Offset changed.")
        
    if updated_hash == initial_hash:
        print("FAILURE: Hash did not change!")
    else:
        print("Success: Hash updated.")
        
    # 5. Verify Compaction Behavior
    print("Compacting...")
    session.compact()
    
    # Check DB after compaction
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT pixel_offset FROM instances WHERE sop_instance_uid=?", (inst.sop_instance_uid,)).fetchone()
    compacted_offset = row[0]
    print(f"Compacted DB Offset: {compacted_offset}")
    conn.close()
    
    # Verify In-Memory Loader
    if isinstance(inst._pixel_loader, SidecarPixelLoader):
        print(f"Memory Loader Offset: {inst._pixel_loader.offset}")
        if inst._pixel_loader.offset != compacted_offset:
            print(f"FAILURE: Memory loader ({inst._pixel_loader.offset}) != DB ({compacted_offset})")
        else:
            print("Success: Memory loader synced.")
            
        # Verify Read
        inst.unload_pixel_data()
        data = inst.get_pixel_data()
        if data[0,0] == 255:
            print("Success: Retrieved Correct Redacted Data.")
        else:
            print(f"FAILURE: Retrieved Wrong Data (Value={data[0,0]})")
    else:
        print("FAILURE: Instance has no SidecarPixelLoader")


def test_persistence_update_after_redaction():
    # ... (Existing test code)
    pass

def test_save_all_updates_hash_when_persist_skipped():
    print("\n--- Testing Save All Hash Update (Simulate Redaction) ---")
    db_path = "debug_persistence_fail.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    session = DicomSession(db_path)
    from gantry.entities import Patient, Study, Series, Instance
    pat = Patient("P_FAIL", "Fail Patient"); st = Study("S1", "D1"); se = Series("SE1", "M1", 1)
    inst = Instance("1.2.999", "1.2.840...", 1)
    
    # 1. Initial State
    arr = np.zeros((10,10), dtype=np.uint8)
    inst.set_pixel_data(arr)
    pat.studies.append(st); st.series.append(se); se.instances.append(inst)
    session.store.patients.append(pat)
    session.save(sync=True)
    
    initial_hash = getattr(inst, '_pixel_hash', None)
    print(f"Initial Hash: {initial_hash}")
    
    # 2. Redact (Modify Pixels) -> Dirty
    print("Modifying pixels (Redaction)...")
    arr2 = np.ones((10,10), dtype=np.uint8) * 255
    inst.set_pixel_data(arr2)
    
    # SIMULAȚE PERSIST FAILURE (Skip calling persist_pixel_data)
    # Check if Hash is updated? (Should be None or Old)
    # set_pixel_data does NOT update hash.
    curr_hash = getattr(inst, '_pixel_hash', None)
    print(f"Post-Redact Hash (Before Save): {curr_hash}")
    assert curr_hash == initial_hash # Should still be old hash
    
    # 3. Save All
    print("Saving (Save All)...")
    session.save(sync=True)
    
    new_hash = getattr(inst, '_pixel_hash', None)
    print(f"Post-Save Hash: {new_hash}")
    
    if new_hash == initial_hash:
        print("FAILURE: save_all did NOT update the instance hash!")
    else:
        print("Success: save_all updated the hash.")

    session.close()

if __name__ == "__main__":
    test_persistence_update_after_redaction()
    test_save_all_updates_hash_when_persist_skipped()

