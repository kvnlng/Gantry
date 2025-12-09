import os
import pydicom
from gantry.builders import DicomBuilder
from gantry.io_handlers import DicomExporter, DicomImporter, DicomStore


def test_export_import_roundtrip(tmp_path, dummy_patient):
    # 1. Export
    export_dir = tmp_path / "export_test"
    DicomExporter.save_patient(dummy_patient, str(export_dir))

    files = list(export_dir.glob("*.dcm"))
    assert len(files) == 1

    # 2. Import into new store
    store = DicomStore()
    DicomImporter.import_files([str(f) for f in files], store)

    assert len(store.patients) == 1
    imported_inst = store.patients[0].studies[0].series[0].instances[0]

    # Check if file path was captured (for lazy loading)
    assert imported_inst.file_path is not None
    assert str(export_dir) in imported_inst.file_path