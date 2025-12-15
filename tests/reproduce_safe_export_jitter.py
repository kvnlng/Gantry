
import pytest
import os
from gantry.session import DicomSession
from gantry.entities import Patient, Study, Series, Instance
from datetime import date
import numpy as np

def test_reproduce_safe_export_jitter(tmp_path):
    # 1. Setup Session
    session = DicomSession(":memory:")
    
    # 2. Add Data with PHI Date
    p = Patient("P_TEST", "ANONYMIZED") # Name is safe
    s = Study("S1", date(2023, 1, 1))   # Date is PHI (unsafe)
    se = Series("SE1", "CT", 1)
    inst = Instance("I1", "1.2.3", 1)
    inst.set_pixel_data(np.zeros((10,10), dtype=np.uint16))
    
    # Essential attributes
    inst.attributes.update({
        "0008,0020": "20230101", # Study Date
        "0018,0050": "1.0",
        "0020,0032": ["0","0","0"],
        "0020,0037": ["1","0","0","0","1","0"],
        "0028,0030": ["0.5","0.5"]
    })

    se.instances.append(inst)
    s.series.append(se)
    p.studies.append(s)
    session.store.patients.append(p)
    
    # 3. Enable Safe Export / PHI Scan
    # We want to ensure 'safe=True' fails initially
    export_dir_1 = tmp_path / "export_fail"
    session.export(str(export_dir_1), safe=True)
    
    # Expectation: Skipped because of Date finding
    assert not (export_dir_1 / "Subject_P_TEST").exists()
    
    # 4. Apply Jitter Remediation
    # We simulate what the user does:
    # findings = scan() -> remediation()
    findings = session.scan_for_phi()
    assert len(findings) > 0 # Should find the date
    session.apply_remediation(findings)
    
    # Verify date changed
    assert s.study_date != date(2023, 1, 1)
    
    # 5. Export Again (Should Succeed)
    export_dir_2 = tmp_path / "export_success"
    session.export(str(export_dir_2), safe=True)
    
    # CURRENT BEHAVIOR: This should now SUCCEED
    # DESIRED BEHAVIOR: This should pass    
    expected_folder = export_dir_2 / f"Subject_{p.patient_id}"
    if expected_folder.exists():
        # Clean up
        pass
    else:
        # Debug info
        found = list(export_dir_2.glob("*"))
        pytest.fail(f"Fix Failed: Export skipped valid jittered date. Expected {expected_folder}, found {found}")
        
    assert expected_folder.exists()
    
