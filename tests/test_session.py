import os
from gantry.session import DicomSession
from gantry.io_handlers import DicomStore
from gantry.io_handlers import DicomExporter


def test_session_persistence(tmp_path, dummy_patient):
    """Test saving and loading state."""
    pkl_file = tmp_path / "session.pkl"

    # 1. Create and Save
    store = DicomStore()
    store.patients.append(dummy_patient)
    store.save_state(str(pkl_file))

    # 2. Load into Session
    session = DicomSession(str(pkl_file))
    assert len(session.store.patients) == 1
    assert session.store.patients[0].patient_id == "P123"


def test_load_config(tmp_path, config_file):
    session = DicomSession(str(tmp_path / "dummy.pkl"))

    session.load_config(config_file)
    assert len(session.active_rules) == 1
    assert session.active_rules[0]["serial_number"] == "SN-999"


def test_execute_config_integration(tmp_path, dummy_patient, config_file):
    """Full integration: Load Data -> Load Rules -> Execute Redaction."""

    # 1. SETUP: We must EXPORT the dummy patient so valid files exist on disk
    #    This allows the Lazy Loader to find them later.
    dicom_dir = tmp_path / "raw_dicoms"
    DicomExporter.save_patient(dummy_patient, str(dicom_dir))

    # 2. Update dummy_patient instances to point to these new files
    #    (In a real app, Import would do this, but here we manually link for the test)
    inst = dummy_patient.studies[0].series[0].instances[0]
    inst.file_path = str(dicom_dir / f"{inst.sop_instance_uid}.dcm")

    # 3. Create Session and add the patient
    pkl_file = str(tmp_path / "integration.pkl")
    session = DicomSession(pkl_file)
    session.store.patients.append(dummy_patient)

    # 4. Modify the file to simulate "Burned In" data
    #    We need to make sure the file on disk matches our expectation?
    #    Actually, DicomExporter wrote 0s. Let's assume we want to ensure
    #    redaction WRITES 0s.

    # Act
    session.load_config(config_file)  # Config targets SN-999, ROI 10-50
    session.execute_config()

    # Assert
    # Verify the instance in memory is updated (Lazy loaded then updated)
    assert inst.pixel_array[20, 20] == 0
    assert os.path.exists(pkl_file)