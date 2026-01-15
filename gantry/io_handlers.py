import os
import pickle
import sys
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian
from pydicom.tag import Tag
from pydicom.datadict import dictionary_VR
from datetime import datetime, date
from typing import List, Set, Dict, Any, Optional, Tuple, NamedTuple, Iterable
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

def populate_attrs(ds, item, text_index: list = None):
    """Standalone function to populate attributes for pickle-compatibility in workers."""
    
    # Text-like VRs that might contain PHI
    TEXT_VRS = {'PN', 'LO', 'SH', 'ST', 'LT', 'UT', 'DA', 'DT', 'TM', 'CS', 'AE', 'UI'} # UI included for linking checks? Maybe not UI usually.
    # Updated VR list based on standard Anonymization profiles
    TEXT_VRS = {'PN', 'LO', 'SH', 'ST', 'LT', 'UT', 'DA', 'DT', 'TM'}
    
    for elem in ds:
        if elem.tag.group == 0x7fe0: continue  # Skip pixels
        tag = f"{elem.tag.group:04x},{elem.tag.element:04x}"
        
        if elem.VR == 'SQ':
            process_sequence(tag, elem, item, text_index)
        elif elem.VR == 'PN':
            # Sanitize PersonName for pickle safety
            val = str(elem.value)
            item.set_attr(tag, val)
            if text_index is not None:
                text_index.append((item, tag))
        else:
            item.set_attr(tag, elem.value)
            # Index if text
            if text_index is not None and elem.VR in TEXT_VRS:
                text_index.append((item, tag))

def process_sequence(tag, elem, parent_item, text_index: list = None):
    """Recursively parses Sequence (SQ) items."""
    for ds_item in elem:
        seq_item = DicomItem()
        populate_attrs(ds_item, seq_item, text_index)
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
            'sop': ds.get("SOPInstanceUID", None),
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
        populate_attrs(ds, inst, inst.text_index)
        
        return (meta, inst, None)
    except Exception as e:
        return (None, None, str(e))

class DicomImporter:
    """
    Handles scanning of folders/files and ingesting them into the Object Graph.
    optimized for parallel processing.
    """
    @staticmethod
    def import_files(file_paths: List[str], store: DicomStore, executor=None):
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
                        if filename.startswith('.'):
                            continue
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
        results = run_parallel(ingest_worker, new_files, desc="Ingesting", chunksize=10, executor=executor)
        
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
    compression: Optional[str] = None # 'j2k' or None
    # Zero-Copy Sidecar Support
    sidecar_path: Optional[str] = None
    pixel_offset: Optional[int] = None
    pixel_length: Optional[int] = None
    pixel_alg: Optional[str] = None

def _export_instance_worker(ctx: ExportContext) -> Optional[bool]:
    """
    Worker function to export a single instance.
    Returns the output path on success, raises Exception on failure.
    """

    try:
        inst = ctx.instance
        ds = DicomExporter._create_ds(inst)
        
        # 0. Base Attributes
        DicomExporter._merge(ds, inst.attributes)
        DicomExporter._merge_sequences(ds, inst.sequences)
        
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
        if ctx.series_attributes.get("DeviceSerialNumber"):
            ds.DeviceSerialNumber = ctx.series_attributes["DeviceSerialNumber"]

        # 4. Pixel Data
        # Use context-provided pixels (for in-memory objects) or load from file
        # 4. Pixel Data
        # Use context-provided pixels (for in-memory objects) or load from file
        arr = ctx.pixel_array
        
        # Zero-Copy Sidecar Loading
        if arr is None and ctx.sidecar_path and ctx.pixel_offset is not None:
             try:
                 # Reconstruct standard SidecarPixelLoader
                 loader = SidecarPixelLoader(ctx.sidecar_path, ctx.pixel_offset, ctx.pixel_length, ctx.pixel_alg, inst)
                 arr = loader()
                 # Ensure attributes are synced (loader usually returns raw array, shaping handled by loader but we double check)
             except Exception as e:

                 raise e

        if arr is None:
             try:
                 arr = inst.get_pixel_data()
             except FileNotFoundError:
                 # Check Modality to decide if we should fail or proceed
                 # Image implementations MUST have pixels.
                 # Non-image (SR, PR, KO, DOC) can proceed without.
                 mod = inst.attributes.get("0008,0060", "OT")
                 IMAGE_MODALITIES = {"CT", "MR", "US", "DX", "CR", "MG", "NM", "PT", "XA", "RF", "SC", "OT"}
                 
                 # If it claims to be an image but has no pixels, fail hard (Safety)
                 if mod in IMAGE_MODALITIES:
                     raise RuntimeError(f"Pixels missing for Image Modality {mod}")
                 
                 # Otherwise (SR, etc.), proceed
                 arr = None
             
        if arr is not None:
            # MEMORY OPTIMIZATION:
            # If compression is requested, DO NOT convert to bytes here.
            # Pass the numpy array to _finalize_dataset -> _compress_j2k directly.
            # Only set PixelData if NOT compressing.
            if not ctx.compression:
                ds.PixelData = arr.tobytes()
            
            # Recalculate dimensions based on array shape
            # Logic mirrored from Instance.set_pixel_data
            shape = arr.shape
            ndim = len(shape)
            
            rows, cols = 0, 0
            # defaults
            if ndim == 2:
                rows, cols = shape
            elif ndim == 3:
                if shape[-1] in [3, 4]:
                    rows, cols, _ = shape
                else:
                    _, rows, cols = shape
            elif ndim == 4:
                _, rows, cols, _ = shape # frames, rows, cols, samples
            
            if rows > 0 and cols > 0:
                ds.Rows = rows
                ds.Columns = cols

            ds.SamplesPerPixel = inst.attributes.get("0028,0002", 1)
            ds.PhotometricInterpretation = inst.attributes.get("0028,0004", "MONOCHROME2")
            
            # Fix for Planar Configuration Mismatch:
            # numpy.tobytes() produces C-contiguous interleaved data (PlanarConfig=0).
            # If the original metadata had PlanarConfig=1, we must override it to 0
            # to match the data we are writing.
            if ds.SamplesPerPixel > 1:
                ds.PlanarConfiguration = 0
            
            if arr.itemsize == 1:
                default_bits = 8
            else:
                default_bits = 16

            ds.BitsAllocated = inst.attributes.get("0028,0100", default_bits)
            ds.BitsStored = inst.attributes.get("0028,0101", default_bits)
            ds.HighBit = inst.attributes.get("0028,0102", default_bits - 1)
            ds.PixelRepresentation = inst.attributes.get("0028,0103", 0)

        # Strip internal Gantry attributes
        if "_GANTRY_REDACTION_HASH" in ds:
            del ds["_GANTRY_REDACTION_HASH"]

        # Validate & Save
        ds = DicomExporter._finalize_dataset(ds, ctx.compression, pixel_array=arr) 
        
        # Ensure dir exists (race safe)
        os.makedirs(os.path.dirname(ctx.output_path), exist_ok=True)
        ds.save_as(ctx.output_path)
        
        return True
    except Exception as e:
        raise RuntimeError(f"Export failed for {ctx.output_path}: {e}")

def _compress_j2k(ds, pixel_array=None):
    """
    Compresses the pixel data of the dataset using JPEG 2000 Lossless (Pillow).
    Updates TransferSyntaxUID and PixelData.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        from pydicom.uid import JPEG2000Lossless
        try:
            from pydicom.encapsulate import encapsulate
        except ImportError:
            from pydicom.encaps import encapsulate
        
        arr = pixel_array
        if arr is None:
            # Fallback to reconstructing from PixelData bytes if array not passed
            if not hasattr(ds, 'PixelData'):
                return
                
            # 1. Get metadata
            rows = ds.Rows
            cols = ds.Columns
            samples = ds.SamplesPerPixel
            bits = ds.BitsAllocated
            
            # 2. Reconstruct Numpy Array from bytes (since we just set it in worker)
            # Assuming Little Endian input for now (as set in _create_ds)
            dt = np.uint16 if bits > 8 else np.uint8
            arr = np.frombuffer(ds.PixelData, dtype=dt)
            
            # Reshape
            # Correctly handle frames
            frames = getattr(ds, "NumberOfFrames", 1)
            
            # Shape logic matching export worker
            if frames > 1:
                if samples > 1:
                     arr = arr.reshape((frames, rows, cols, samples))
                else:
                     arr = arr.reshape((frames, rows, cols))
            else:
                if samples > 1:
                    arr = arr.reshape((rows, cols, samples))
                else:
                    arr = arr.reshape((rows, cols))
        else:
            # Pre-calc frames if not in DS (though export worker sets them)
            frames = getattr(ds, "NumberOfFrames", 1)
            if frames == 1 and len(arr.shape) == 3 and ds.SamplesPerPixel == 1:
                 # Standardize shape for encoding loop
                 # If (Frames, H, W) where Frames=1
                 pass
            elif frames > 1 and len(arr.shape) == 3:
                 # (Frames, H, W)
                 pass

        # 3. Compress
        frames_data = []
        
        # Helper to compress single frame
        def encode_frame(frame_arr):
            # Pillow expects [H, W] or [H, W, C]
            img = Image.fromarray(frame_arr)
            bio = io.BytesIO()
            img.save(bio, format='JPEG2000', compression='lossless')
            return bio.getvalue()


        if frames > 1:
            for i in range(frames):
                frames_data.append(encode_frame(arr[i]))
        else:
            frames_data.append(encode_frame(arr))
            
        ds.PixelData = encapsulate(frames_data)
        # ds.TransferSyntaxUID = JPEG2000Lossless # REMOVE: Group 2 tags must be in file_meta only
        ds.file_meta.TransferSyntaxUID = JPEG2000Lossless
        ds.is_implicit_VR = False # Compressed transfer syntaxes are always Explicit VR
        ds.is_little_endian = True # JPEG 2000 is always Little Endian (in DICOM encapsulation typically)
        
    except ImportError:
        # Fallback or Log? 
        # If requested but dependencies missing, we should probably fail hard or warn.
        # But this is inside a worker. simpler to raise.
        raise RuntimeError("Pillow or pydicom not installed/configured for JPEG 2000.")
    except Exception as e:
        raise RuntimeError(f"Compression failed: {e}")


import json

def gantry_json_object_hook(d):
    if "__type__" in d and d["__type__"] == "bytes":
        import base64
        return base64.b64decode(d["data"])
    return d

class SidecarPixelLoader:
    """
    Functor for lazy loading of pixel data from sidecar.
    Must be a top-level class to be picklable.
    """
    def __init__(self, sidecar_path, offset, length, alg, instance):
        self.sidecar_path = sidecar_path
        self.offset = offset
        self.length = length
        self.alg = alg
        self.instance = instance

    def __call__(self):
        # We need SidecarManager. Importing here to avoid circular dep at top level logic?
        # Actually persistence.py imports it.
        # But SidecarManager might be in a module unrelated to this.
        # Let's hope we can import it.
        from .sidecar import SidecarManager
        mgr = SidecarManager(self.sidecar_path)
        

        raw = mgr.read_frame(self.offset, self.length, self.alg)
        
        # Reconstruct based on attributes
        import numpy as np
        bits = self.instance.attributes.get("0028,0100", 8)
        dt = np.uint16 if bits > 8 else np.uint8
        
        arr = np.frombuffer(raw, dtype=dt)
        
        rows = self.instance.attributes.get("0028,0010", 0)
        cols = self.instance.attributes.get("0028,0011", 0)
        samples = self.instance.attributes.get("0028,0002", 1)
        frames = int(self.instance.attributes.get("0028,0008", 0) or 0)
        
        if frames > 1:
            target_shape = (frames, rows, cols, samples)
            if samples == 1: target_shape = (frames, rows, cols)
        elif samples > 1:
            target_shape = (rows, cols, samples)
        else:
            target_shape = (rows, cols)
        
        try:
            return arr.reshape(target_shape)
        except:
            return arr

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
    def generate_export_from_db(store_backend, out_dir: str, patient_ids: List[str] = None, compression: str = None, instance_uids: List[str] = None):
        """
        Generator that yields ExportContext objects directly from the DB.
        O(1) Memory usage.
        """
        for row in store_backend.get_flattened_instances(patient_ids, instance_uids):
            # 1. Rehydrate Attributes
            attrs = {}
            if row['attributes_json']:
                try:
                    attrs = json.loads(row['attributes_json'], object_hook=gantry_json_object_hook)
                except:
                    pass
            
            # 2. Construct Lightweight Instance
            inst = Instance(
                sop_instance_uid=row['sop_instance_uid'],
                sop_class_uid=row['sop_class_uid'],
                instance_number=row['instance_number'] or 0,
                file_path=row['file_path']
            )
            
            # 3. Handle Sequences
            if '__sequences__' in attrs:
                seq_data = attrs.pop('__sequences__')
                
                # Recursive rehydration helper
                from .entities import DicomSequence, DicomItem
                
                def rehydrate_seq(seq_dict):
                    rehydrated = {}
                    for tag, items in seq_dict.items():
                        ds = DicomSequence(tag=tag)
                        for item_data in items:
                            di = DicomItem()
                            # Item data is a dict (serialized item)
                            # recursion
                            if '__sequences__' in item_data:
                                sub_seqs = item_data.pop('__sequences__')
                                di.sequences = rehydrate_seq(sub_seqs)
                            di.attributes = item_data
                            ds.items.append(di)
                        rehydrated[tag] = ds
                    return rehydrated
                
                inst.sequences = rehydrate_seq(seq_data)
            
            inst.attributes = attrs
            
            # 4. Pixel Loader 
            if row['pixel_offset'] is not None:
                # Use the store's sidecar path, not the instance's original file path
                # Note: This assumes single sidecar file or current one. 
                # If using multiple sidecars, we'd need pixel_file_id lookup.
                sc_path = getattr(store_backend, 'sidecar_path', None)
                if not sc_path and row.get('file_path'): 
                    # Fallback if pixel data is in original file but we have offset (e.g. partial read?)
                    # But usually offset implies SidecarManager format. 
                    # If store_backend doesn't expose sidecar_path, we might be in trouble.
                    # But SqliteStore does.
                    sc_path = row['file_path']
                
                inst._pixel_loader = SidecarPixelLoader(
                    sidecar_path=sc_path, 
                    offset=row['pixel_offset'],
                    length=row['pixel_length'],
                    alg=row['compress_alg'],
                    instance=inst
                )

            # 5. Metadata Overrides (from DB columns)
            pat_attrs = {"PatientName": row['patient_name'], "PatientID": row['patient_id']}
            
            s_date = row['study_date'] or ""
            study_attrs = {
                "StudyInstanceUID": row['study_instance_uid'],
                "StudyDate": s_date, 
                "StudyTime": "120000"
            }
            
            series_attrs = {
                "SeriesInstanceUID": row['series_instance_uid'],
                "Modality": row['modality'],
                "SeriesNumber": row['series_number'],
                "Manufacturer": row['manufacturer'],
                "ManufacturerModelName": row['model_name'],
                "DeviceSerialNumber": row['device_serial_number']
            }
            
            # 6. Output Path Logic
            subj_name = f"Subject_{DicomExporter._sanitize(row['patient_id'])}"
            st_desc = attrs.get("0008,1030", "Study")
            study_folder = f"Study_{s_date}_{DicomExporter._sanitize(st_desc)}"
            se_desc = attrs.get("0008,103E", "Series")
            se_num = row['series_number'] or "0"
            series_folder = f"Series_{se_num}_{DicomExporter._sanitize(se_desc)}"
            fname = f"{row['sop_instance_uid']}.dcm"
            
            full_out_path = os.path.join(out_dir, subj_name, study_folder, series_folder, fname)
            
            yield ExportContext(
                instance=inst,
                output_path=full_out_path,
                patient_attributes=pat_attrs,
                study_attributes=study_attrs,
                series_attributes=series_attrs,
                pixel_array=None, 
                compression=compression
            )

    @staticmethod
    def _generate_export_contexts(patient: Patient, studies: List[Study], out_dir: str, compression: str = None) -> List[ExportContext]:
        """
        Generates ExportContext objects for the given studies.
        """
        contexts = []
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
                        series_attrs["DeviceSerialNumber"] = se.equipment.device_serial_number
                        
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
                    if inst.pixel_array is not None:
                         p_array = inst.pixel_array
                    
                    # Extract Sidecar Info if available (Zero-Copy)
                    sc_path, sc_offset, sc_length, sc_alg = None, None, None, None
                    if hasattr(inst, '_pixel_loader') and inst._pixel_loader:
                        # Check if it's a SidecarPixelLoader
                        # We duck-type check for attributes
                        pl = inst._pixel_loader
                        if hasattr(pl, 'sidecar_path') and hasattr(pl, 'offset'):
                             sc_path = pl.sidecar_path
                             sc_offset = pl.offset
                             sc_length = pl.length
                             sc_alg = pl.alg

                    # Add to queue
                    ctx = ExportContext(
                        instance=inst, 
                        output_path=full_out_path,
                        patient_attributes=pat_attrs,
                        study_attributes=study_attrs,
                        series_attributes=series_attrs,
                        pixel_array=p_array,
                        compression=compression,
                        sidecar_path=sc_path,
                        pixel_offset=sc_offset,
                        pixel_length=sc_length,
                        pixel_alg=sc_alg
                    )
                    contexts.append(ctx)
        return contexts

    @staticmethod
    def save_studies(patient: Patient, studies: List[Study], out_dir: str, compression: str = None, show_progress: bool = True, executor=None):
        """
        Exports a specific list of studies for a patient using parallel workers.
        compression: 'j2k' or None
        """
        if not os.path.exists(out_dir): os.makedirs(out_dir)
        logger = get_logger()
        
        # Planning Phase: Generate Contexts
        export_tasks = DicomExporter._generate_export_contexts(patient, studies, out_dir, compression)
    
        # Execution Phase
        if not export_tasks:
            logger.warning("No instances found to export.")
            return
    
        # Log only if progress is shown, or at least one summary line if hidden?
        # If hidden, the caller (batch export) is logging.
        if show_progress:
            logger.info(f"Starting parallel export of {len(export_tasks)} instances...")
            
        results = run_parallel(_export_instance_worker, export_tasks, desc="Exporting", chunksize=10, show_progress=show_progress, executor=executor)
        
        # results contains paths or Nones
        success_count = sum(1 for r in results if r is not None)
        logger.info(f"Export Complete. Success: {success_count}/{len(export_tasks)}")

    @staticmethod
    def export_batch(export_tasks: Iterable[ExportContext], show_progress: bool = True, total: int = None, executor=None, maxtasksperchild: int = None, disable_gc: bool = False):
        """
        Exports a flat list of ExportContexts using parallel workers.
        """
        logger = get_logger()
        # if not export_tasks: return # Cannot easily check empty iterator without consuming

        if show_progress:
            count_str = str(total) if total else "?"
            logger.info(f"Starting global parallel export of {count_str} instances...")
            
        # Run parallel
        results = run_parallel(_export_instance_worker, export_tasks, desc="Exporting", chunksize=1, show_progress=show_progress, total=total, executor=executor, maxtasksperchild=maxtasksperchild, disable_gc=disable_gc)
        
        success_count = sum(1 for r in results if r is not None)
        logger.info(f"Export Complete. Success: {success_count}/{total or '?'}")
        return success_count
        
    @staticmethod
    def _finalize_dataset(ds, compression=None, pixel_array=None):
        """
        Finalizes the dataset before saving:
        1. Applies compression if requested.
        2. Validates IOD.
        Returns the dataset (modified) or raises RuntimeError if invalid.
        """
        if compression == 'j2k':
            _compress_j2k(ds, pixel_array)
            
        errs = IODValidator.validate(ds)
        if errs:
            # We log but might want to raise? logic in worker returns None on error.
            # But worker expects exception to be raised for error? 
            # In previous logic: "if not errs: save else return None"
            # So here we should probably return None or raise.
            # Let's raise to be clearer in worker catch
            raise ValueError(f"Validation Errors: {errs}")
            
        return ds

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
            
            # Explicit handling for Encrypted Attributes to fix potential dictionary mismatches
            if t == "0400,0510": # Encrypted Content
                ds.add_new(0x04000510, 'OB', v)
                continue
            if t == "0400,0520": # Encrypted Content Transfer Syntax UID
                ds.add_new(0x04000520, 'UI', v)
                continue

            if t.startswith("_") or "," not in t:
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

    @staticmethod
    def _merge_sequences(ds, sequences: Dict[str, Any]):
        """
        Recursively populates sequences into the dataset.
        sequences: Dict[str, DicomSequence]
        """
        from pydicom.sequence import Sequence
        from pydicom.dataset import Dataset
        from pydicom.tag import Tag

        for tag_str, dicom_seq in sequences.items():
            g, e = map(lambda x: int(x, 16), tag_str.split(','))
            tag = Tag(g, e)
            
            pydicom_seq = Sequence()
            for item in dicom_seq.items:
                ds_item = Dataset()
                ds_item.is_little_endian = True
                ds_item.is_implicit_VR = True
                
                # Recursively merge item attributes and sub-sequences
                DicomExporter._merge(ds_item, item.attributes)
                DicomExporter._merge_sequences(ds_item, item.sequences)
                
                pydicom_seq.append(ds_item)
                
            ds.add_new(tag, 'SQ', pydicom_seq)