import os
import pickle
import sys
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian
from pydicom.tag import Tag
from pydicom.datadict import dictionary_VR
from datetime import datetime, date
from typing import List, Set, Dict, Any, Optional, Tuple, NamedTuple
import shutil
from .entities import Patient, Study, Series, Instance, Equipment, DicomItem
from .logger import get_logger
from .parallel import run_parallel
from .validation import IODValidator
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
            'pid': ds.get("PatientID", "UnknownPatient"),
            'pname': str(ds.get("PatientName", "Unknown")), # Kept original pname
            'sid': ds.get("StudyInstanceUID", "UnknownStudy"),
            'sdate': str(ds.get("StudyDate", "19000101")), # Kept original sdate
            'ser_id': ds.get("SeriesInstanceUID", "UnknownSeries"),
            'modality': ds.get("Modality", "OT"),
            'sop': ds.SOPInstanceUID,
            'sop_class': str(ds.get("SOPClassUID", "")), # Kept original sop_class
            'man': ds.get("Manufacturer", ""),
            'model': ds.get("ManufacturerModelName", ""),
            'dev_sn': ds.get("DeviceSerialNumber", ""),
            'series_num': ds.get("SeriesNumber", 0) # Added SeriesNumber
        }
        
        if not meta['sop']:
             raise ValueError("Missing SOPInstanceUID. Likely not a valid DICOM file.")

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
                        series = Series(ser_id, meta['modality'], meta['series_num'])
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




class ExportContext(NamedTuple):
    instance: Instance
    output_path: str
    patient_attributes: Dict[str, Any]
    study_attributes: Dict[str, Any]
    series_attributes: Dict[str, Any]
    pixel_array: Optional[Any] = None # Numpy array or None

def _export_instance_worker(ctx: ExportContext) -> str:
    """
    Worker function to export a single instance.
    Returns the output path on success, raises Exception on failure.
    """
    try:
        inst = ctx.instance
        ds = DicomExporter._create_ds(inst)
        
        # 0. Base Attributes
        DicomExporter._merge(ds, inst.attributes)
        
        # 1. Patient Level
        ds.PatientName = ctx.patient_attributes.get("PatientName", "")
        ds.PatientID = ctx.patient_attributes.get("PatientID", "")
        
        # 2. Study Level
        ds.StudyInstanceUID = ctx.study_attributes.get("StudyInstanceUID", "")
        ds.StudyDate = ctx.study_attributes.get("StudyDate", "")
        ds.StudyTime = ctx.study_attributes.get("StudyTime", "")
        
        # 3. Series Level
        ds.SeriesInstanceUID = ctx.series_attributes.get("SeriesInstanceUID", "")
        ds.Modality = ctx.series_attributes.get("Modality", "")
        ds.SeriesNumber = ctx.series_attributes.get("SeriesNumber", None)
        if ctx.series_attributes.get("Manufacturer"):
            ds.Manufacturer = ctx.series_attributes["Manufacturer"]
        if ctx.series_attributes.get("ManufacturerModelName"):
            ds.ManufacturerModelName = ctx.series_attributes["ManufacturerModelName"]

        # 4. Pixel Data
        # Use context-provided pixels (for in-memory objects) or load from file
        arr = ctx.pixel_array
        if arr is None:
             arr = inst.get_pixel_data()
             
        if arr is not None:
            ds.PixelData = arr.tobytes()
            ds.Rows, ds.Columns = arr.shape[-2], arr.shape[-1]
            ds.SamplesPerPixel = inst.attributes.get("0028,0002", 1)
            ds.PhotometricInterpretation = inst.attributes.get("0028,0004", "MONOCHROME2")
            
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
            # Ensure dir exists (race safe)
            os.makedirs(os.path.dirname(ctx.output_path), exist_ok=True)
            ds.save_as(ctx.output_path)
            return ctx.output_path
        else:
            return None
            
    except Exception as e:
        # Re-raise to be caught by parallel wrapper or handled
        raise RuntimeError(f"Export failed for {ctx.output_path}: {e}")


class DicomExporter:
    """
    Handles writing the Object Graph back to standard DICOM files.
    """
    @staticmethod
    def save_patient(patient: Patient, out_dir: str):
        """
        Iterates over a Patient's hierarchy and saves valid .dcm files to out_dir.
        """
        DicomExporter.save_studies(patient, patient.studies, out_dir)

    @staticmethod
    def save_studies(patient: Patient, studies: List[Study], out_dir: str):
        """
        Exports a specific list of studies for a patient using parallel workers.
        """
        if not os.path.exists(out_dir): os.makedirs(out_dir)
        logger = get_logger()
        
        export_tasks: List[ExportContext] = []
        
        # Planning Phase: Generate Contexts
        for st in studies:
            for se in st.series:
                for inst in se.instances:
                    # Prepare Metadata used for directory structure AND overrides
                    
                    # Patient Attributes
                    pat_attrs = {
                        "PatientName": patient.patient_name,
                        "PatientID": patient.patient_id
                    }
                    
                    # Study Attributes
                    s_date_str = ""
                    if st.study_date:
                        if hasattr(st.study_date, 'strftime'):
                            s_date_str = st.study_date.strftime("%Y%m%d")
                        else:
                            s_date_str = str(st.study_date)
                            
                    study_attrs = {
                        "StudyInstanceUID": st.study_instance_uid,
                        "StudyDate": s_date_str,
                        "StudyTime": "120000"
                    }
                    
                    # Series Attributes
                    series_attrs = {
                        "SeriesInstanceUID": se.series_instance_uid,
                        "Modality": se.modality,
                        "SeriesNumber": se.series_number
                    }
                    if se.equipment:
                        series_attrs["Manufacturer"] = se.equipment.manufacturer
                        series_attrs["ManufacturerModelName"] = se.equipment.model_name
                        
                    # Calculate Output Path
                    # 1. Subject Folder
                    subj_name = f"Subject_{DicomExporter._sanitize(patient.patient_id)}"
                    
                    # 2. Study Folder
                    s_date_clean = s_date_str.replace("-", "") or "UnknownDate"
                    
                    s_desc = "Study"
                    if "0008,1030" in inst.attributes:
                        s_desc = inst.attributes["0008,1030"]
                    study_folder = f"Study_{s_date_clean}_{DicomExporter._sanitize(s_desc)}"
                    
                    # 3. Series Folder
                    ser_num = se.series_number if se.series_number is not None else "0"
                    ser_desc = "Series"
                    if "0008,103E" in inst.attributes:
                        ser_desc = inst.attributes["0008,103E"]
                    series_folder = f"Series_{ser_num}_{DicomExporter._sanitize(ser_desc)}"
                    
                    # 4. Filename
                    fname = f"{inst.sop_instance_uid}.dcm"
                    if "0020,0013" in inst.attributes:
                        try:
                            inum = int(inst.attributes["0020,0013"])
                            fname = f"{inum:04d}.dcm"
                        except: pass
                        
                    full_out_path = os.path.join(out_dir, subj_name, study_folder, series_folder, fname)
                    
                    # Handle In-Memory Pixels (e.g. Remediated/Detached instances)
                    # If file_path is None, worker cannot load pixels. send them.
                    p_array = None
                    if inst.file_path is None and inst.pixel_array is not None:
                        p_array = inst.pixel_array

                    # Add to queue
                    ctx = ExportContext(
                        instance=inst, 
                        output_path=full_out_path,
                        patient_attributes=pat_attrs,
                        study_attributes=study_attrs,
                        series_attributes=series_attrs,
                        pixel_array=p_array
                    )
                    export_tasks.append(ctx)

        # Execution Phase
        if not export_tasks:
            logger.warning("No instances found to export.")
            return

        logger.info(f"Starting parallel export of {len(export_tasks)} instances...")
        results = run_parallel(_export_instance_worker, export_tasks, desc="Exporting", chunksize=10)
        
        # results contains paths or Nones
        success_count = sum(1 for r in results if r is not None)
        logger.info(f"Export Complete. Success: {success_count}/{len(export_tasks)}")
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

    @staticmethod
    def _sanitize(filename: str) -> str:
        """
        Removes illegal characters from filenames.
        """
        if not filename:
            return "Unknown"
        # Keep alphanumeric, dashes, underscores, spaces (maybe replace spaces with underscores?)
        # For strictness:
        safe = "".join([c for c in str(filename) if c.isalnum() or c in (' ', '.', '-', '_')])
        return safe.strip().replace(" ", "_")