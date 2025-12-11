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
from .logger import get_logger
from tqdm import tqdm


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
        logger = get_logger()
        logger.info(f"Persisting session metadata to {filepath}...")
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)
        logger.info("Saved.")

    @staticmethod
    def load_state(filepath: str) -> 'DicomStore':
        if not os.path.exists(filepath):
            return DicomStore()
        with open(filepath, 'rb') as f:
            return pickle.load(f)


from .parallel import run_parallel


def populate_attrs(ds, item):
    """Standalone function to populate attributes for pickle-compatibility in workers."""
    for elem in ds:
        if elem.tag.group == 0x7fe0: continue  # Skip pixels
        tag = f"{elem.tag.group:04x},{elem.tag.element:04x}"
        if elem.VR == 'SQ':
            process_sequence(tag, elem, item)
        elif elem.VR == 'PN':
            # Sanitize PersonName for pickle safety
            item.set_attr(tag, str(elem.value))
        else:
            item.set_attr(tag, elem.value)

def process_sequence(tag, elem, parent_item):
    """Recursively parses Sequence (SQ) items."""
    for ds_item in elem:
        seq_item = DicomItem()
        populate_attrs(ds_item, seq_item)
        parent_item.add_sequence_item(tag, seq_item)

def ingest_worker(fp):
    """
    Worker function to read DICOM and construct Instance object.
    Returns: (metadata_dict, instance_object, error_string)
    metadata_dict contains keys for linking: 'pid', 'pname', 'sid', 'ser_id', etc.
    """
    try:
        ds = pydicom.dcmread(fp, stop_before_pixels=True, force=True)
        
        # Extract Linking Metadata
        meta = {
            'pid': str(ds.get("PatientID", "UNKNOWN")),
            'pname': str(ds.get("PatientName", "Unknown")),
            'sid': str(ds.get("StudyInstanceUID", "")),
            'sdate': str(ds.get("StudyDate", "19000101")),
            'ser_id': str(ds.get("SeriesInstanceUID", "")),
            'modality': str(ds.get("Modality", "OT")),
            'sop': str(ds.get("SOPInstanceUID", "")),
            'sop_class': str(ds.get("SOPClassUID", "")),
            'man': str(ds.get("Manufacturer", "")),
            'model': str(ds.get("ManufacturerModelName", "")),
            'dev_sn': str(ds.get("DeviceSerialNumber", ""))
        }
        
        # Construct Instance
        inst = Instance(meta['sop'], meta['sop_class'], 0, file_path=fp)
        populate_attrs(ds, inst)
        
        return (meta, inst, None)
    except Exception as e:
        return (None, None, str(e))

class DicomImporter:
    """
    Handles scanning of folders/files and ingesting them into the Object Graph.
    optimized for parallel processing.
    """
    @staticmethod
    def import_files(file_paths: List[str], store: DicomStore):
        """
        Parses a list of files or directories. Recurses into directories to find all files.
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
        
        logger = get_logger()
        skipped_count = len(all_files) - len(new_files)
        if skipped_count > 0:
            logger.info(f"Skipping {skipped_count} already imported files.")
            
        if not new_files:
            return

        logger.info(f"Importing {len(new_files)} files (Parallel Ingest)...")
        
        # 1. Build Fast Lookup Maps (O(1))
        patient_map = {p.patient_id: p for p in store.patients}
        study_map = {} # Key: study_uid -> Study
        series_map = {} # Key: series_uid -> Series
        
        # Populate deep maps
        for p in store.patients:
            for st in p.studies:
                study_map[st.study_instance_uid] = st
                for se in st.series:
                    series_map[se.series_instance_uid] = se

        # 2. Parallel Execution
        results = run_parallel(ingest_worker, new_files, desc="Ingesting", chunksize=10)
        
        # 3. Aggregation (Main Thread)
        count = 0
        for meta, inst, err in results:
            if err:
                 logger.error(f"Import Failed: {err}")
                 continue
            if inst:
                try:
                    # Linkage Logic
                    pid = meta['pid']
                    sid = meta['sid']
                    ser_id = meta['ser_id']
                    
                    # Patient
                    pat = patient_map.get(pid)
                    if not pat:
                        pat = Patient(pid, meta['pname'])
                        store.patients.append(pat)
                        patient_map[pid] = pat
                    
                    # Study
                    study = study_map.get(sid)
                    if not study:
                        # Parse date carefully or use fallback
                        try:
                            sdate = datetime.strptime(meta['sdate'], "%Y%m%d").date()
                        except:
                            sdate = date(1900, 1, 1)
                            
                        study = Study(sid, sdate)
                        pat.studies.append(study)
                        study_map[sid] = study
                    
                    # Series
                    series = series_map.get(ser_id)
                    if not series:
                        series = Series(ser_id, meta['modality'], 0)
                        if meta['man'] or meta['model']:
                            series.equipment = Equipment(meta['man'], meta['model'], meta['dev_sn'])
                        study.series.append(series)
                        series_map[ser_id] = series
                    
                    # Instance
                    series.instances.append(inst)
                    count += 1
                except Exception as e:
                    logger.error(f"Linkage Failed: {e}")

        logger.info(f"Successfully ingested {count} instances.")


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
        logger = get_logger()

        for st in patient.studies:
            for se in st.series:
                for inst in se.instances:
                    ds = DicomExporter._create_ds(inst)

                    # 1. Patient Level
                    ds.PatientName = patient.patient_name
                    ds.PatientID = patient.patient_id

                    # 2. Study Level
                    ds.StudyInstanceUID = st.study_instance_uid
                    if st.study_date:
                        if hasattr(st.study_date, 'strftime'):
                             ds.StudyDate = st.study_date.strftime("%Y%m%d")
                        else:
                             ds.StudyDate = str(st.study_date)
                    else:
                        ds.StudyDate = ""
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
                    # Merge Pixels
                    # CRITICAL: Do not swallow errors here. If pixels are missing, we want to know.
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

                    # Validate & Save
                    errs = IODValidator.validate(ds)
                    if not errs:
                        fp = os.path.join(out_dir, f"{inst.sop_instance_uid}.dcm")
                        ds.save_as(fp)
                        logger.info(f"Exported: {fp}")
                    else:
                        # This print statement is why you saw 0 files!
                        logger.warning(f"Skipped Invalid {inst.sop_instance_uid}: {errs}")

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
            # Explicit handling for Gantry Private Tags to ensure correct VR
            if t == "0099,0010":
                ds.add_new(0x00990010, 'LO', v)
                continue
            if t == "0099,1001":
                ds.add_new(0x00991001, 'OB', v)
                continue

            g, e = map(lambda x: int(x, 16), t.split(','))
            
            # Skip Command Set elements (Group 0000) which are illegal for file persistence
            if g == 0x0000:
                continue

            try:
                vr = dictionary_VR(Tag(g, e))
                ds.add_new(Tag(g, e), vr, v)
            except:
                pass