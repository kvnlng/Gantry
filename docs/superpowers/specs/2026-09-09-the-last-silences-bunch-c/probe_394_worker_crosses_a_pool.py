"""#394 probe: does scan_pixel_content()'s worker survive a real process pool?"""
import os
import pickle
import sys
import tempfile
from datetime import date

import numpy as np

from isocenter.entities import Equipment, Instance, Patient, Series, Study
from isocenter.session import DicomSession

CT = "1.2.840.10008.5.1.4.1.1.2"
SERIAL = "SN_PROBE"
ZONE = [0, 10, 0, 10]


def populate(session):
    patient = Patient("PAT1", "Probe^Patient")
    study = Study("ST_1", date(2023, 1, 1))
    study.study_time = "120000"
    series = Series("SE_1", "CT", 1)
    series.equipment = Equipment("Probe", "Model", SERIAL)
    for n in range(2):
        inst = Instance(f"1.2.826.0.1.{n}", CT, n + 1)
        inst.file_path = None
        inst.set_attr("0018,1000", SERIAL)
        inst.set_attr("0008,0060", "CT")
        inst.set_pixel_data(np.full((16, 16), 1000, dtype=np.uint16))
        series.instances.append(inst)
    study.series.append(series)
    patient.studies.append(study)
    session.store.patients.append(patient)
    session.save(sync=True)
    session.configuration.rules = [
        {"serial_number": SERIAL, "redaction_zones": [ZONE]}]
    return session


def main():
    mode = sys.argv[1]
    os.environ["ISOCENTER_MAX_WORKERS"] = "2"
    if mode == "processes":
        os.environ["ISOCENTER_FORCE_PROCESSES"] = "1"
    else:
        os.environ["ISOCENTER_FORCE_THREADS"] = "1"
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "probe.db")
        with DicomSession(db) as session:
            populate(session)
            insts = [i for p in session.store.patients for st in p.studies
                     for se in st.series for i in se.instances]
            equip = session.store.patients[0].studies[0].series[0].equipment
            args = (insts[0], equip, session.configuration.rules)
            try:
                blob = pickle.dumps(args)
                print(f"PICKLE OK: {len(blob)} bytes")
                back = pickle.loads(blob)
                print("UNPICKLE OK; pixel data in copy:",
                      None if back[0].get_pixel_data() is None
                      else back[0].get_pixel_data().shape)
            except Exception as exc:  # noqa: BLE001
                print(f"PICKLE FAILED: {type(exc).__name__}: {exc}")
            for inst in insts:
                inst.unload_pixel_data()
            try:
                report = session.scan_pixel_content()
                print(f"SCAN OK ({mode}): {len(report)} findings")
            except Exception as exc:  # noqa: BLE001
                print(f"SCAN RAISED ({mode}): {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
