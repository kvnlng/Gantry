
import pytest
import os
import shutil
import datetime
import numpy as np
from gantry.io_handlers import DicomExporter
from gantry.entities import Patient, Study, Series, Instance

@pytest.fixture
def mock_patient(tmp_path):
    # Create object graph
    p = Patient("PID_001", "Test Subject")
    
    s = Study("STUDY_UID_1", datetime.date(2025, 1, 1))
    p.studies.append(s)
    
    se = Series("SERIES_UID_1", "CT", 1)
    s.series.append(se)
    
    # Create fake instance with minimal attributes
    inst = Instance("SOP_UID_1", "1.2.840.10008.5.1.4.1.1.2", 0)
    inst.attributes = {
        "0008,1030": "Chest CT",       # Study Description
        "0008,103E": "Axial 3mm",      # Series Description
        "0020,0013": "10",             # Instance Number
        "0028,0010": 512,              # Rows
        "0028,0011": 512,              # Cols
    }
    # Mock pixel data
    # We need to monkeypath get_pixel_data because exporters call it
    inst.get_pixel_data = lambda: np.zeros((512, 512), dtype=np.uint16)
    
    se.instances.append(inst)
    
    return p

@pytest.fixture
def mock_validator(monkeypatch):
    from gantry.validation import IODValidator
    # Monkeypatch validate to always return [] (no errors)
    monkeypatch.setattr(IODValidator, "validate", lambda ds: [])

def test_structured_export(mock_patient, mock_validator, tmp_path):
    out_dir = tmp_path / "export_test"
    
    # Run export
    DicomExporter.save_patient(mock_patient, str(out_dir))
    
    # Expected Structure:
    # out_dir / Subject_PID_001 / Study_20250101_Chest_CT / Series_1_Axial_3mm / 0010.dcm
    
    subject_dir = out_dir / "Subject_PID_001"
    assert subject_dir.exists(), "Subject directory missing"
    
    study_dirs = list(subject_dir.glob("Study_*"))
    assert len(study_dirs) == 1
    assert "20250101_Chest_CT" in study_dirs[0].name
    
    series_dirs = list(study_dirs[0].glob("Series_*"))
    assert len(series_dirs) == 1
    assert "1_Axial_3mm" in series_dirs[0].name
    
    files = list(series_dirs[0].glob("*.dcm"))
    assert len(files) == 1
    assert files[0].name == "0010.dcm"

def test_sanitization():
    unsafe = "Bad/Name: With * Characters?"
    safe = DicomExporter._sanitize(unsafe)
    assert "/" not in safe
    assert ":" not in safe
    assert "*" not in safe
    assert "?" not in safe
    assert safe == "BadName_With__Characters" # Depending on implementation details
