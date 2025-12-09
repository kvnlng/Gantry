from gantry.services import MachinePixelIndex, RedactionService
from gantry.io_handlers import DicomStore
import numpy as np


def test_machine_index(dummy_patient):
    store = DicomStore()
    store.patients.append(dummy_patient)

    index = MachinePixelIndex()
    index.index_store(store)

    results = index.get_by_machine("SN-999")
    assert len(results) == 1
    assert results[0].sop_instance_uid == "1.2.840.111.1.1.1"


def test_redaction_service(dummy_patient):
    store = DicomStore()
    store.patients.append(dummy_patient)

    svc = RedactionService(store)

    # Original pixel check (top left is 0)
    inst = store.patients[0].studies[0].series[0].instances[0]
    # Set a value to verify it gets cleared
    inst.pixel_array[20, 20] = 500

    # Act: Redact region 10-50
    svc.redact_machine_region("SN-999", (10, 50, 10, 50))

    # Assert
    assert inst.pixel_array[20, 20] == 0