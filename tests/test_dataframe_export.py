import pytest
import pandas as pd
import os
import sqlite3
from gantry.session import DicomSession
from gantry.entities import Patient, Study, Series, Instance
from gantry.persistence import GantryJSONEncoder
import json

@pytest.fixture
def session_with_data(tmp_path):
    db_path = tmp_path / "gantry_test.db"
    session = DicomSession(str(db_path))
    
    # Manually populate the database with some hierarchical data
    # We use SQL directly to simulate a populated state, or use the object model if possible.
    # Using SQL directly is more robust for testing the persistence Read path specifically.
    
    # Or better: Create objects and use save_all.
    p = Patient("P1", "Test Patient")
    st = Study("ST1", "20230101")
    se = Series("SE1", "CT", 101, equipment=None)
    inst = Instance("I1", "1.2.840.123", 1)
    inst.file_path = "/tmp/fake.dcm"
    inst.attributes = {"PatientName": "Test Patient", "Modality": "CT", "SliceThickness": 1.5}
    
    se.instances.append(inst)
    st.series.append(se)
    p.studies.append(st)
    
    session.store.patients = [p]
    session.save() # This triggers the full save pipeline
    session.persistence_manager.flush() # Wait for DB write
    
    yield session
    session.close()

def test_export_dataframe_basic(session_with_data):
    df = session_with_data.export_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['patient_id'] == "P1"
    assert df.iloc[0]['sop_instance_uid'] == "I1"
    # Check default columns exist
    expected_cols = ['patient_id', 'study_instance_uid', 'series_instance_uid', 'sop_instance_uid']
    for col in expected_cols:
        assert col in df.columns

def test_export_dataframe_parquet(session_with_data, tmp_path):
    output_path = tmp_path / "export.parquet"
    df = session_with_data.export_dataframe(str(output_path))
    
    assert os.path.exists(output_path)
    
    # Verify we can read it back
    df_read = pd.read_parquet(output_path)
    assert len(df_read) == 1
    assert df_read.iloc[0]['patient_id'] == "P1"

def test_export_dataframe_expand_metadata(session_with_data):
    # This requires us to modify the implementation to actually parse the JSON if expand_metadata=True
    # For now, let's assume we implement it or at least call it.
    df = session_with_data.export_dataframe(expand_metadata=True)
    
    # If expansion works, we should see "SliceThickness" as a column or at least check logic
    # The current plan is to implement it, so let's assert it.
    
    # Note: sqlite persistence stores attributes_json.
    # Our mocked data had SliceThickness = 1.5
    
    assert 'SliceThickness' in df.columns
    assert df.iloc[0]['SliceThickness'] == 1.5
