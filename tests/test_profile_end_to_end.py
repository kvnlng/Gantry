
import pytest
import os
import pydicom
import json
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian
from gantry.session import DicomSession

def create_simple_dicom(path, patient_name="Test^Patient"):
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7" # Secondary Capture (Lenient)
    file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()
    file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
    
    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.PatientName = patient_name
    ds.PatientID = "12345"
    ds.PatientBirthDate = "19900101"
    
    # Must have these for Gantry import
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = file_meta.MediaStorageSOPClassUID
    if not hasattr(ds.file_meta, "MediaStorageSOPClassUID"):
        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
        
    ds.StudyInstanceUID = pydicom.uid.generate_uid()
    ds.SeriesInstanceUID = pydicom.uid.generate_uid()
    ds.Modality = "OT"
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000" # Type 2 for SC
    ds.ConversionType = "WSD" # Type 1 for SC
    
    # Minimal pixel data to pass validation if any
    ds.is_little_endian = True
    ds.is_implicit_VR = True
    ds.Rows = 1
    ds.Columns = 1
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = b'\0'
    
    ds.save_as(str(path), write_like_original=False)
    return ds

def test_profile_remediation_end_to_end(tmp_path):
    # 1. Create DICOM with PHI
    dcm_path = tmp_path / "phi.dcm"
    create_simple_dicom(dcm_path, "John^Doe")
    
    # 2. Create Config with Basic Profile
    config_path = tmp_path / "profile_config.yaml"
    import yaml
    with open(config_path, "w") as f:
        yaml.dump({
            "version": "2.0",
            "privacy_profile": "basic",
            # Empty phi_tags means purely relying on profile
            "phi_tags": {} 
        }, f)
        
    # 3. Setup Session
    session = DicomSession(":memory:")
    session.ingest(str(tmp_path)) # Ingest
    
    # 4. Load Config
    session.load_config(str(config_path))
    
    # 5. Export (Applying Remediation)
    export_dir = tmp_path / "clean_export"
    # Note: 'audit()' or 'export(safe=True)' usually warns. 
    # Here we want to see if applying remediation actually works.
    # We'll use apply_remediation directly or just check if safe export flags it.
    
    # Method A: Check if scan finds it (Verification of config load + Privacy Inspector)
    report = session.scan_for_phi()
    # Basic profile REMOVES PatientName. So scan should FLAG it.
    
    flagged_tags = {f.tag for f in report}
    assert "0010,0010" in flagged_tags # PatientName
    assert "0010,0030" in flagged_tags # BirthDate
    
    # Method B: Apply and Export
    # Generate risk report
    risk_report = session.audit() 
    # Use RemediationService to apply (mocking internal flow if needed, but session usually has helpers)
    session.anonymize(risk_report)
    
    session.export(str(export_dir), safe=False) 
    
    # 6. Verify Exported File
    out_files = list(export_dir.rglob("*.dcm"))
    assert len(out_files) == 1
    ds_out = pydicom.dcmread(out_files[0])
    
    # PatientName should be REMOVED or ANONYMIZED (Gantry safety default)
    # The profile says "REMOVE", but Gantry's semantic layer enforces "ANONYMIZED" for Patient objects
    # to ensure validity. Both are safe.
    val = getattr(ds_out, "PatientName", "")
    assert val == "" or str(val) == "ANONYMIZED"
    assert "John" not in str(val)
    
    # BirthDate has no hardcoded semantic override, so strictly REMOVE should work
    assert "PatientBirthDate" not in ds_out or ds_out.PatientBirthDate == ""
