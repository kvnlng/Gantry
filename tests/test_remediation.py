import pytest
import datetime
from gantry.entities import Patient, Study
from gantry.privacy import PhiInspector
from gantry.remediation import RemediationService

def test_replace_tag_remediation():
    # Setup
    pat = Patient("MRN123", "John Doe")
    inspector = PhiInspector()
    service = RemediationService()
    
    # Act
    findings = inspector.scan_patient(pat)
    service.apply_remediation(findings)
    
    # Assert
    assert pat.patient_name == "ANONYMIZED"
    assert pat.patient_id.startswith("ANON_")
    assert pat.patient_id != "MRN123"

def test_date_shifting_remediation():
    # Setup
    pat_id = "TEST_PATIENT_1"
    # Ensure date shift behaves deterministically
    # Hash of "TEST_PATIENT_1" ... let's just test that it changes and stays changed
    
    pat = Patient(pat_id, "Test Patient")
    study_date = "20230101"
    study = Study("1.2.3.4", study_date)
    pat.studies.append(study)
    
    inspector = PhiInspector()
    service = RemediationService()
    
    # Act
    findings = inspector.scan_patient(pat)
    
    # Verify finding has correct proposal
    date_finding = next(f for f in findings if f.field_name == "study_date")
    assert date_finding.remediation_proposal.action_type == "SHIFT_DATE"
    assert date_finding.remediation_proposal.metadata.get("patient_id") == pat_id
    
    # Apply
    service.apply_remediation(findings)
    
    # Assert
    new_date = study.study_date
    assert new_date != study_date
    assert len(new_date) == 8
    
    # Validate it's a valid date
    orig_dt = datetime.datetime.strptime(study_date, "%Y%m%d")
    new_dt = datetime.datetime.strptime(new_date, "%Y%m%d")
    
    # Check that it is shifted backwards (our logic returns negative offset)
    assert new_dt < orig_dt
    
    # Consistency check
    params_findings = inspector.scan_patient(pat)
    # Applying again should shift it AGAIN if we blindly re-scan the modified object?
    # BUT, the modified object now has a new date.
    # If we run the service again on NEW findings from the MODIFIED object, it will shift again.
    # This is "idempotent" only if we don't re-scan?
    # Actually, standard date shifting is often "shift from original".
    # But here we are mutating the object.
    # If we run the full pipeline twice:
    # 1. Scan -> Findings
    # 2. Apply -> Mutate
    # 3. Scan -> Findings (on mutated date)
    # 4. Apply -> Mutate (double shift)
    # This is expected behavior for a mutator unless we flag it as 'clean'.
    
    # However, let's test consistency for two DIFFERENT objects with SAME patient ID
    pat2 = Patient(pat_id, "Test Patient Copy")
    study2 = Study("1.2.3.5", study_date)
    pat2.studies.append(study2)
    
    inspector2 = PhiInspector()
    service2 = RemediationService()
    findings2 = inspector2.scan_patient(pat2)
    service2.apply_remediation(findings2)
    
    assert study2.study_date == new_date, "Same Patient ID should result in same shifted date for identical input date"

def test_bad_date_format():
    pat = Patient("P1", "N1")
    study = Study("S1", "NOT_A_DATE")
    pat.studies.append(study)
    
    inspector = PhiInspector()
    service = RemediationService()
    findings = inspector.scan_patient(pat)
    
    # Should catch exception/warning but not crash
    service.apply_remediation(findings)
    assert study.study_date == "NOT_A_DATE"

def test_date_object_remediation():
    """Reproduce bug where datetime.date objects crash strptime"""
    pat = Patient("P_DATE", "Date Test")
    # Simulate valid date object (not string)
    study_date = datetime.date(2023, 1, 1) 
    study = Study("S_DATE", study_date)
    pat.studies.append(study)
    
    inspector = PhiInspector()
    service = RemediationService()
    
    findings = inspector.scan_patient(pat)
    
    # Apply -> Should crash here if not fixed
    try:
        service.apply_remediation(findings)
    except TypeError as e:
        pytest.fail(f"Remediation crashed on date object: {e}")
        
    assert study.study_date != study_date

