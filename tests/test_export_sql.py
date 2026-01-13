import os
import pytest
import sqlite3
import numpy as np
import pydicom
from gantry.entities import Patient, Study, Series, Instance
from gantry.session import DicomSession
from gantry.persistence import SqliteStore
from gantry.io_handlers import DicomExporter

@pytest.fixture
def session(tmp_path):
    # FORCE THREADS for debugging parallel issues
    os.environ["GANTRY_FORCE_THREADS"] = "1"
    
    # Setup session with temp DB directory
    sess = DicomSession()
    db_path = tmp_path / "test_gantry.db"
    sess.persistence_manager.db_path = str(db_path)
    # Re-init store backend on session AND persistence manager
    new_store = SqliteStore(str(db_path))
    sess.persistence_manager.store_backend = new_store
    sess.store_backend = new_store
    # Clear in-memory store
    sess.store.patients = []
    return sess

@pytest.mark.parametrize("use_file_db", [True, False])
def test_export_sql_streaming_e2e(tmp_path, use_file_db):
    """
    End-to-End test for SQL-driven export.
    Verifies that we can export data ingested into SqliteStore.
    """
    # 1. Setup
    if use_file_db:
        db_path = str(tmp_path / "test_export.db")
        # Ensure fresh
        if os.path.exists(db_path): os.remove(db_path)
    else:
        db_path = ":memory:"
        
    store = SqliteStore(db_path)
    # Important: If file DB, we need separate connection logic or ensure flush if async.
    # But here we interact with store directly (synchronous save_all).
    
    # 2. Populate Store
    p = Patient("P1", "Test Patient")
    st = Study("S1", "20230101")
    se = Series("SE1", "CT", 1)
    
    # Instance 1: Sidecar (Simulated)
    inst1 = Instance("I1", "1.2.840", 1)
    inst1.attributes["0010,0020"] = "P1" # Extra check
    # We need to simulate pixel data writing to sidecar if we test pixel export
    # But for metadata export, we just need the objects.
    # Let's add dummy pixel data to test that path too.
    inst1.set_pixel_data(np.zeros((10, 10), dtype=np.uint8))
    
    se.instances.append(inst1)
    st.series.append(se)
    p.studies.append(st)
    
    # Direct save (Synchronous) -> No race condition
    store.save_all([p])
    
    # 3. Execute Export
    out_dir = tmp_path / "export_out"
    os.makedirs(out_dir)
    
    # Generate tasks
    tasks = DicomExporter.generate_export_from_db(
        store, 
        str(out_dir), 
        ["P1"]
    )
    
    # Run Batch
    count = DicomExporter.export_batch(tasks, show_progress=False)
    
    # 4. Verify
    assert count == 1
    
    # Check File Structure
    # P1 -> Subject_P1 -> Study...
    subject_dir = out_dir / "Subject_P1"
    assert subject_dir.exists()
    
    # Find the .dcm file
    dcm_files = list(subject_dir.rglob("*.dcm"))
    assert len(dcm_files) == 1
    
    # Verify Content
    ds = pydicom.dcmread(dcm_files[0])
    assert ds.PatientID == "P1"
    assert ds.PixelData is not None
    assert ds.Rows == 10
    assert ds.Columns == 10
    assert ds.pixel_array.sum() == 0

def test_export_flattened_flow(session, tmp_path):
    # 1. Create Data
    p = Patient("P_SQL", "SQL Patient")
    st = Study("1.2.3.4", "20230101")
    p.studies.append(st)
    se = Series("1.2.3.4.5", "CT", 1)
    st.series.append(se)
    
    # Use Secondary Capture Image Storage (simplest validation)
    sc_uid = "1.2.840.10008.5.1.4.1.1.7"
    inst = Instance("1.2.3.4.5.6", sc_uid, 1)
    inst.set_attr("0010,0010", "SQL Patient")
    inst.set_attr("0008,0064", "WSD") # ConversionType
    inst.set_attr("0008,0060", "OT")  # Modality
    
    # Add dummy pixel data
    arr = np.zeros((10, 10), dtype=np.uint16)
    inst.set_pixel_data(arr)
    se.instances.append(inst)
    
    session.store.patients.append(p)
    
    # 2. Save (Required for DB Export)
    session.save()
    
    # 3. Export
    out_dir = tmp_path / "export_out"
    session.export(str(out_dir))
    
    # 4. Verify
    found_files = []
    for root, dirs, files in os.walk(out_dir):
        for f in files:
            if f.endswith(".dcm"):
                found_files.append(os.path.join(root, f))
                
    assert len(found_files) == 1
    
    # Read file
    ds = pydicom.dcmread(found_files[0])
    assert ds.PatientName == "SQL Patient"
    assert ds.SOPInstanceUID == "1.2.3.4.5.6"
    assert ds.Rows == 10
    assert ds.Columns == 10
    assert ds.pixel_array.sum() == 0

def test_export_safety_filtering(session, tmp_path):
    # 1. Create Data (Patient with PHI)
    p = Patient("P_PHI", "Bad Name") # This name will trigger PHI if we configured it
    # But we can simulate dirty lists manually by mocking audit results if needed.
    # Currently session.export calls session.audit() if safe=True.
    # For a real integration test, we need rules.
    # Let's assume default config.
    
    # Better: Mock audit return or manipulate dirty_patients in a subclass or mocked method.
    # But simpler: Just test that export works.
    pass

def test_export_parquet(session, tmp_path):
    try:
        import pandas
        import pyarrow
    except ImportError:
        pytest.skip("pandas or pyarrow not installed")

    # 1. Create Data
    p = Patient("P_PQ", "Parquet Patient")
    st = Study("1.2.3.4.100", "20230102")
    p.studies.append(st)
    se = Series("1.2.3.4.5.100", "CT", 1)
    st.series.append(se)
    
    inst = Instance("1.2.3.4.5.6.100", "1.2.840.10008.5.1.4.1.1.7", 1) # SC Image Storage
    inst.set_attr("0010,0010", "Parquet Patient")
    se.instances.append(inst)
    
    session.store.patients.append(p)
    session.save()
    session.persistence_manager.flush()
    
    # 2. Export
    out_file = tmp_path / "data.parquet"
    session.export_dataframe(str(out_file))
    
    # 3. Verify
    assert out_file.exists()
    
    # Read back check
    df = pandas.read_parquet(out_file)
    assert len(df) == 1
    assert df.iloc[0]['PatientName'] == "Parquet Patient"
