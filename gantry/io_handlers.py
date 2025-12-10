import os
import pickle
import sys
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian
from pydicom.tag import Tag
from pydicom.datadict import dictionary_VR
from datetime import datetime, date
from typing import List, Set
from .entities import Patient, Study, Series, Instance, Equipment, DicomItem


class DicomStore:
    """Root of the Object Graph + Persistence Logic"""

    def __init__(self):
        self.patients: List[Patient] = []

    def get_unique_equipment(self) -> List[Equipment]:
        """Returns a list of all unique Equipment (Manufacturer/Model/Serial) found in the store."""
        unique = set()
        for p in self.patients:
            for st in p.studies:
                for se in st.series:
                    if se.equipment: unique.add(se.equipment)
        return list(unique)

    def get_known_files(self) -> Set[str]:
        """Returns a set of absolute file paths for all instances currently indexed."""
        files = set()
        for p in self.patients:
            for st in p.studies:
                for se in st.series:
                    for inst in se.instances:
                        if inst.file_path:
                            files.add(os.path.abspath(inst.file_path))
        return files

    def save_state(self, filepath: str):
        print(f"Persisting session metadata to {filepath}...")
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)
        print("Saved.")

    @staticmethod
    def load_state(filepath: str) -> 'DicomStore':
        if not os.path.exists(filepath):
            return DicomStore()
        with open(filepath, 'rb') as f:
            return pickle.load(f)


class DicomImporter:
    """
    Handles scanning of folders/files and ingesting them into the Object Graph.
    Reads metadata only (lazy loading) for performance.
    """
    @staticmethod
    def import_files(file_paths: List[str], store: DicomStore):
        """
        Parses a list of files or directories. Recurses into directories to find all files.
        Skips those already in the store.
        Builds the Patient -> Study -> Series -> Instance hierarchy.
        """
        all_files = []
        for path in file_paths:
            if os.path.isfile(path):
                all_files.append(path)
            elif os.path.isdir(path):
                for root, _, filenames in os.walk(path):
                    for filename in filenames:
                        all_files.append(os.path.join(root, filename))
        
        known_files = store.get_known_files()
        new_files = [fp for fp in all_files if os.path.abspath(fp) not in known_files]
        
        skipped_count = len(all_files) - len(new_files)
        if skipped_count > 0:
            print(f"Skipping {skipped_count} already imported files.")
            
        for fp in new_files:
            try:
                # read metadata only
                ds = pydicom.dcmread(fp, stop_before_pixels=True, force=True)
                DicomImporter._ingest(ds, store, fp)
                print(f"Indexed: {os.path.basename(fp)}")
            except Exception as e:
                print(f"Import Failed {fp}: {e}")

    @staticmethod
    def _ingest(ds, store, filepath):
        # Extract IDs
        pid = str(ds.get("PatientID", "UNKNOWN"))
        pname = str(ds.get("PatientName", "Unknown"))
        sid = str(ds.get("StudyInstanceUID", ""))
        ser_id = str(ds.get("SeriesInstanceUID", ""))
        sop = str(ds.get("SOPInstanceUID", ""))

        # Hierarchy Traversal
        pat = next((p for p in store.patients if p.patient_id == pid), None)
        if not pat:
            pat = Patient(pid, pname)
            store.patients.append(pat)

        study = next((s for s in pat.studies if s.study_instance_uid == sid), None)
        if not study:
            study = Study(sid, date(1900, 1, 1))  # Simplified date parsing
            pat.studies.append(study)

        series = next((s for s in study.series if s.series_instance_uid == ser_id), None)
        if not series:
            series = Series(ser_id, str(ds.get("Modality", "OT")), 0)
            # Equipment
            man = str(ds.get("Manufacturer", ""))
            mod = str(ds.get("ManufacturerModelName", ""))
            sn = str(ds.get("DeviceSerialNumber", ""))
            if man or mod:
                series.equipment = Equipment(man, mod, sn)
            study.series.append(series)

        # Create Instance (Link file path for lazy loading)
        inst = Instance(sop, str(ds.get("SOPClassUID", "")), 0, file_path=filepath)
        DicomImporter._populate_attrs(ds, inst)
        series.instances.append(inst)

    @staticmethod
    def _populate_attrs(ds, item):
        for elem in ds:
            if elem.tag.group == 0x7fe0: continue  # Skip pixels
            tag = f"{elem.tag.group:04x},{elem.tag.element:04x}"
            if elem.VR == 'SQ':
                DicomImporter._process_sequence(tag, elem, item)
            elif elem.VR == 'PN':
                # Sanitize PersonName for pickle safety
                item.set_attr(tag, str(elem.value))
            else:
                item.set_attr(tag, elem.value)

    @staticmethod
    def _process_sequence(tag, elem, parent_item):
        """Recursively parses Sequence (SQ) items."""
        for ds_item in elem:
            seq_item = DicomItem()
            DicomImporter._populate_attrs(ds_item, seq_item)
            parent_item.add_sequence_item(tag, seq_item)

class DicomExporter:
    """
    Handles writing the Object Graph back to standard DICOM files.
    """
    @staticmethod
    def save_patient(patient: Patient, out_dir: str):
        """
        Iterates over a Patient's hierarchy and saves valid .dcm files to out_dir.
        Performs IOD Validation before saving.
        """
        if not os.path.exists(out_dir): os.makedirs(out_dir)
        from gantry.validation import IODValidator

        for st in patient.studies:
            for se in st.series:
                for inst in se.instances:
                    ds = DicomExporter._create_ds(inst)

                    # 1. Patient Level
                    ds.PatientName = patient.patient_name
                    ds.PatientID = patient.patient_id

                    # 2. Study Level
                    ds.StudyInstanceUID = st.study_instance_uid
                    ds.StudyDate = st.study_date.strftime("%Y%m%d") if st.study_date else ""
                    ds.StudyTime = "120000"  # Dummy time to satisfy Type 1

                    # 3. Series Level
                    ds.SeriesInstanceUID = se.series_instance_uid
                    ds.Modality = se.modality
                    ds.SeriesNumber = se.series_number
                    if se.equipment:
                        ds.Manufacturer = se.equipment.manufacturer
                        ds.ManufacturerModelName = se.equipment.model_name

                    # 4. Instance Level and recursive sequences
                    DicomExporter._merge(ds, inst.attributes)

                    # Merge Pixels
                    try:
                        arr = inst.get_pixel_data()
                        ds.PixelData = arr.tobytes()
                        ds.Rows, ds.Columns = arr.shape[-2], arr.shape[-1]
                        ds.SamplesPerPixel = inst.attributes.get("0028,0002", 1)
                        # Ensure Photometric Interpretation is present (Type 1)
                        ds.PhotometricInterpretation = inst.attributes.get("0028,0004", "MONOCHROME2")
                        # Infer depth from numpy array
                        if arr.itemsize == 1:
                            default_bits = 8
                        else:
                            default_bits = 16

                        ds.BitsAllocated = inst.attributes.get("0028,0100", default_bits)
                        ds.BitsStored = inst.attributes.get("0028,0101", default_bits)
                        ds.HighBit = inst.attributes.get("0028,0102", default_bits - 1)
                        ds.PixelRepresentation = inst.attributes.get("0028,0103", 0)
                    except:
                        pass

                    # Validate & Save
                    errs = IODValidator.validate(ds)
                    if not errs:
                        fp = os.path.join(out_dir, f"{inst.sop_instance_uid}.dcm")
                        ds.save_as(fp)
                        print(f"Exported: {fp}")
                    else:
                        # This print statement is why you saw 0 files!
                        print(f"Skipped Invalid {inst.sop_instance_uid}: {errs}")

    @staticmethod
    def _create_ds(inst):
        meta = FileMetaDataset()
        meta.MediaStorageSOPClassUID = inst.sop_class_uid
        meta.MediaStorageSOPInstanceUID = inst.sop_instance_uid
        meta.TransferSyntaxUID = ImplicitVRLittleEndian
        ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
        ds.is_little_endian = True
        ds.is_implicit_VR = True
        return ds

    @staticmethod
    def _merge(ds, attrs):
        for t, v in attrs.items():
            g, e = map(lambda x: int(x, 16), t.split(','))
            try:
                vr = dictionary_VR(Tag(g, e))
                ds.add_new(Tag(g, e), vr, v)
            except:
                pass