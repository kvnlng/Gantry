
import pytest
import numpy as np
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian, JPEGLosslessSV1
from pydicom.encaps import encapsulate
import imagecodecs
import os
import yaml
from gantry import Session

def create_compressed_dcm(path, shape, multi_frame=False):
    """
    Creates a DICOM file with JPEG Lossless (SV1) compressed pixel data.
    Uses imagecodecs to encode the data.
    """
    # Create random image data
    if multi_frame:
        frames, rows, cols = shape
        # Create 3D array (Frames, Rows, Cols)
        arr = np.random.randint(0, 255, size=shape, dtype=np.uint8)
    else:
        rows, cols = shape
        arr = np.random.randint(0, 255, size=(rows, cols), dtype=np.uint8)
        frames = 1
        
    # Compress
    # We need to compress each frame individually for encapsulation
    fragments = []
    if multi_frame:
        for i in range(frames):
            frame_data = arr[i]
            encoded = imagecodecs.ljpeg_encode(frame_data)
            fragments.append(encoded)
    else:
        encoded = imagecodecs.ljpeg_encode(arr)
        fragments.append(encoded)
        
    # Create DICOM
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2" # CT
    meta.MediaStorageSOPInstanceUID = "1.2.3.4"
    meta.TransferSyntaxUID = JPEGLosslessSV1
    
    ds = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0"*128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False
    
    ds.SOPClassUID = meta.MediaStorageSOPClassUID
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.PatientName = "Test^Patient"
    ds.PatientID = "TestID"
    ds.StudyInstanceUID = "1.2.3"
    ds.SeriesInstanceUID = "1.2.3.4"
    
    ds.Rows = rows
    ds.Columns = cols
    ds.SamplesPerPixel = 1
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PhotometricInterpretation = "MONOCHROME2"
    
    if multi_frame:
        ds.NumberOfFrames = frames
        
    # Encapsulate
    # pydicom expect list of bytes
    ds.PixelData = encapsulate(fragments)
    
    # Save
    ds.save_as(str(path))
    return arr

# Check if imagecodecs is available to run these tests
try:
    imagecodecs.ljpeg_encode(np.zeros((10,10), dtype=np.uint8))
    HAS_IMAGECODECS = True
except:
    HAS_IMAGECODECS = False

@pytest.mark.skipif(not HAS_IMAGECODECS, reason="imagecodecs not installed or encoding failed")
def test_redact_single_frame_compressed(tmp_path):
    print("\n--- TEST SINGLE FRAME ---")
    dcm_path = tmp_path / "single.dcm"
    original_arr = create_compressed_dcm(dcm_path, (128, 128))
    
    db_path = str(tmp_path / f"gantry_single_{os.urandom(4).hex()}.db")
    session = Session(db_path)
    session.ingest(str(tmp_path))
    
    # Check simple get_pixel_data first
    inst = session.store.patients[0].studies[0].series[0].instances[0]
    try:
        arr = inst.get_pixel_data()
        assert arr.shape == (128, 128)
    except Exception as e:
        print(f"Single Frame get_pixel_data failed: {e}")
        raise e

    config_path = tmp_path / "config.yaml"
    config = {
        "machines": [{
            "serial_number": "DEV001",
            "model_name": "TestModel", 
            "redaction_zones": [[0, 10, 0, 10]] # Top 10 rows
        }]
    }

    ds = pydicom.dcmread(dcm_path)
    ds.DeviceSerialNumber = "DEV001"
    ds.Manufacturer = "TestMan"
    ds.ManufacturerModelName = "TestModel"
    ds.save_as(str(dcm_path))
    
    # Re-ingest (new session to avoid cache/state issues)
    # session.persistence.stop() # Not exposed directly
    del session
    
    if os.path.exists(db_path):
        os.remove(db_path)
        
    db_path_2 = str(tmp_path / f"gantry_single_2_{os.urandom(4).hex()}.db")
    session = Session(db_path_2)
    session.ingest(str(tmp_path))

    with open(config_path, "w") as f:
        yaml.dump(config, f)
        
    session.load_config(str(config_path))
    
    print("Starting Redact...")
    session.redact()
    
    inst = session.store.patients[0].studies[0].series[0].instances[0]
    arr = inst.get_pixel_data()
    assert np.all(arr[0:10, 0:10] == 0)


@pytest.mark.skipif(not HAS_IMAGECODECS, reason="imagecodecs not installed")
def test_redact_multi_frame_compressed(tmp_path):
    print("\n--- TEST MULTI FRAME ---")
    dcm_path = tmp_path / "multi.dcm"
    original_arr = create_compressed_dcm(dcm_path, (5, 128, 128), multi_frame=True)
    
    db_path = str(tmp_path / f"gantry_multi_{os.urandom(4).hex()}.db")
    session = Session(db_path)

    # Update DICOM metadata
    ds = pydicom.dcmread(dcm_path)
    ds.DeviceSerialNumber = "DEV001"
    ds.Manufacturer = "TestMan"
    ds.ManufacturerModelName = "TestModel"
    ds.save_as(str(dcm_path))
    
    session.ingest(str(tmp_path))
    
    # Direct check
    inst = session.store.patients[0].studies[0].series[0].instances[0]
    print(f"Frames: {getattr(inst, 'NumberOfFrames', 'Unknown')}")
    try:
        arr = inst.get_pixel_data()
        assert arr.shape == (5, 128, 128)
    except Exception as e:
        print(f"Multi Frame get_pixel_data failed: {e}")
        # We want to see WHY it failed.
        # Check stderr in captured logs
        raise e
    
    config_path = tmp_path / "config.yaml"
    config = {
        "machines": [{
            "serial_number": "DEV001", 
            "model_name": "TestModel", 
            "redaction_zones": [[0, 10, 0, 10]]
        }]
    }
    with open(config_path, "w") as f:
        yaml.dump(config, f)
        
    session.load_config(str(config_path))
    
    session.redact()
    
    arr = inst.get_pixel_data()
    assert arr.shape == (5, 128, 128)
    for i in range(5):
        assert np.all(arr[i, 0:10, 0:10] == 0)
