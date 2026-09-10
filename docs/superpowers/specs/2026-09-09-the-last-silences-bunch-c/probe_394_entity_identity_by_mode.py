"""Does a scan_pixel_content() finding's `entity` mean the same thing
in threads and in processes?"""
import os
import sys
import tempfile
from datetime import date
from unittest.mock import patch

import numpy as np

from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.pixel_analysis import TextRegion
from isocenter.session import DicomSession

CT = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN_PROBE"


def build(session):
    patient = Patient("PAT1", "Probe^Patient")
    study = Study("ST_1", date(2023, 1, 1))
    study.study_time = "120000"
    series = Series("SE_1", "CT", 1)
    series.equipment = Equipment("Probe", "Model", SERIAL)
    inst = Instance("1.2.826.0.1.0", CT, 1)
    inst.file_path = None
    inst.set_attr("0018,1000", SERIAL)
    inst.set_pixel_data(np.full((16, 16), 1000, dtype=np.uint16))
    series.instances.append(inst)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.save(sync=True)
    session.configuration.rules = [
        {"serial_number": SERIAL, "redaction_zones": [[0, 10, 0, 10]]}]
    return inst


def main():
    mode = sys.argv[1]
    os.environ["ISOCENTER_MAX_WORKERS"] = "2"
    os.environ.pop("ISOCENTER_FORCE_THREADS", None)
    os.environ.pop("ISOCENTER_FORCE_PROCESSES", None)
    os.environ["ISOCENTER_FORCE_PROCESSES" if mode == "processes"
               else "ISOCENTER_FORCE_THREADS"] = "1"
    with tempfile.TemporaryDirectory() as tmp:
        with DicomSession(os.path.join(tmp, "p.db")) as session:
            live = build(session)
            with patch("isocenter.verification.analyze_pixels",
                       return_value=[TextRegion("LEAKTEXT", (200, 200, 50, 50), 90.0)]):
                report = session.scan_pixel_content()
            print(f"RESULT[{mode}] findings={len(report)}")
            for f in report:
                print(f"   uid={f.entity_uid} value={f.value!r} "
                      f"entity_is_live={f.entity is live} "
                      f"entity_type={type(f.entity).__name__} "
                      f"entity_has_pixels={getattr(f.entity, 'pixel_array', None) is not None}")


if __name__ == "__main__":
    main()
