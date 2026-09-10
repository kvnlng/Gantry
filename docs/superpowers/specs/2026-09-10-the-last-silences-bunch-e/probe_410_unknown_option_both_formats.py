"""#410 -- what each export format does with an option name it does not know.

Two patients, one waveform-bearing instance each. The correct spelling of
the subset filter is `patient_ids`; the typo under test is `patient_id`.

Run:
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<repo> <venv>/bin/python -u \
      docs/superpowers/specs/2026-09-10-the-last-silences-bunch-e/\
probe_410_unknown_option_both_formats.py
"""
import os
import pathlib
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))

from isocenter.session import DicomSession  # noqa: E402
from scripts.generate_waveform_test_data import write_fixture  # noqa: E402


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        src = tmp / "src"
        src.mkdir()
        write_fixture(str(src / "a.dcm"), num_samples=64,
                      patient_id="WFPAT-A", patient_name="Alpha^Ann")
        write_fixture(str(src / "b.dcm"), num_samples=64,
                      patient_id="WFPAT-B", patient_name="Beta^Bob")

        session = DicomSession(persistence_file=str(tmp / "p410.db"))
        try:
            session.ingest(str(src))
            print("patients in store:",
                  sorted(p.patient_id for p in session.store.patients))

            good = session.export(str(tmp / "wfdb_good"), format="wfdb",
                                  patient_ids=["WFPAT-A"])
            print("wfdb patient_ids=['WFPAT-A'] ->",
                  sorted(os.path.basename(p) for p in good))

            typo = session.export(str(tmp / "wfdb_typo"), format="wfdb",
                                  patient_id=["WFPAT-A"])
            print("wfdb patient_id=['WFPAT-A']  ->",
                  sorted(os.path.basename(p) for p in typo))

            nonsense = session.export(str(tmp / "wfdb_junk"), format="wfdb",
                                      zzz_not_an_option=True)
            print("wfdb zzz_not_an_option=True  ->",
                  sorted(os.path.basename(p) for p in nonsense))

            try:
                session.export(str(tmp / "dcm_typo"), format="dicom",
                               patient_id=["WFPAT-A"], show_progress=False)
            except Exception as exc:  # noqa: BLE001
                print(f"dicom patient_id=[...] -> {type(exc).__name__}: {exc}")
            else:
                print("dicom patient_id=[...] -> NO RAISE")
        finally:
            session.close()


if __name__ == "__main__":
    main()
