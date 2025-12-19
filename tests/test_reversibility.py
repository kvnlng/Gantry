import pytest
import os
import shutil
import pydicom
from gantry.session import DicomSession
from gantry.entities import Patient, Study, Series, Instance
from gantry.crypto import KeyManager

def test_reversible_anonymization_flow(tmp_path):
    db_path = str(tmp_path / "gantry_rev.db")
    key_path = str(tmp_path / "test.key")
    export_dir = str(tmp_path / "export")
    
    session = DicomSession(db_path)
    session.enable_reversible_anonymization(key_path)
    
    # 1. Setup Data
    pid = "REV_123"
    original_name = "Original Name"
    
    p = Patient(pid, original_name)
    from datetime import date
    st = Study("ST_1", date(2023, 1, 1))
    se = Series("SE_1", "CT", 1)
    inst = Instance("SOP_1", "1.2.840.10008.5.1.4.1.1.2", 1)
    inst.file_path = None # In-memory only
    
    # Add mandatory tags for CT Image Storage IOD to pass validation
    inst.set_attr("0018,0050", "1.0") # Slice Thickness
    inst.set_attr("0018,0060", "120") # KVP
    inst.set_attr("0020,0032", ["0", "0", "0"]) # Image Position
    inst.set_attr("0020,0037", ["1", "0", "0", "0", "1", "0"]) # Orientation
    inst.set_attr("0028,0030", ["0.5", "0.5"]) # Pixel Spacing
    
    se.instances.append(inst)
    st.series.append(se)
    p.studies.append(st)
    session.store.patients.append(p)
    session.save()
    
    # 2. Preserve Identity
    session.lock_identities(pid)
    session.save()
    session.persistence_manager.shutdown()
    
    # Verify encrypted blob is in memory
    # 0400,0500 is EncryptedAttributesSequence
    assert "0400,0500" in inst.sequences
    seq = inst.sequences["0400,0500"]
    assert len(seq.items) > 0
    item = seq.items[0]
    # 0400,0510 is EncryptedContent
    assert "0400,0510" in item.attributes
    assert isinstance(item.attributes["0400,0510"], bytes)
    
    # 3. Simulate Anonymization (Change Name)
    p.patient_name = "ANONYMIZED"
    session.save()
    
    # 4. Recover Identity
    recovered = session.reversibility_service.recover_original_data(inst)
    assert recovered is not None
    assert recovered["PatientName"] == original_name
    assert recovered["PatientID"] == pid
    assert p.patient_name == "ANONYMIZED" # Current state is still anon
    
    # 5. Export and Verify Persistence in File
    import numpy as np
    inst.set_pixel_data(np.zeros((10, 10), dtype=np.uint16))
    
    session.export(export_dir)
    
    # Find exported file recursively
    from pathlib import Path
    found_files = list(Path(export_dir).rglob("*.dcm"))
    assert len(found_files) == 1
    exported_file = str(found_files[0])
    
    assert os.path.exists(exported_file)
    
    # Read back with pydicom
    ds = pydicom.dcmread(exported_file)
    
    # Check Standard Sequence
    # Encrypted Attributes Sequence (0400,0500)
    assert (0x0400, 0x0500) in ds
    seq = ds[0x0400, 0x0500].value
    assert len(seq) > 0
    item = seq[0]
    
    # Encrypted Content (0400,0510)
    assert (0x0400, 0x0510) in item
    encrypted_blob = item[0x0400, 0x0510].value
    
    # Pydicom dictionary quirk: incorrectly thinks 0400,0510 is UI (String), so it decodes it.
    # We must handle this by encoding back to bytes if needed.
    if isinstance(encrypted_blob, str):
        encrypted_blob = encrypted_blob.encode('ascii')
    
    assert isinstance(encrypted_blob, bytes)
    assert len(encrypted_blob) > 0
    
    # Verify Decryption from file
    engine = session.reversibility_service.engine # Re-use same key
    decrypted = engine.decrypt(encrypted_blob)
    assert original_name.encode() in decrypted

def test_key_persistence(tmp_path):
    key_path = str(tmp_path / "persistent.key")
    km1 = KeyManager(key_path)
    k1 = km1.load_or_generate_key()
    
    assert os.path.exists(key_path)
    
    km2 = KeyManager(key_path)
    k2 = km2.load_or_generate_key()
    
    assert k1 == k2
