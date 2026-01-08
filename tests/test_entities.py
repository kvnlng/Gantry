import numpy as np
import pytest
from unittest.mock import patch
from gantry.entities import Instance, Equipment


def test_equipment_equality():
    """Test that frozen dataclasses hash correctly."""
    e1 = Equipment("GE", "CT", "SN1")
    e2 = Equipment("GE", "CT", "SN1")
    e3 = Equipment("GE", "CT", "SN2")

    assert e1 == e2
    assert e1 != e3
    assert len({e1, e2, e3}) == 2  # Set should deduplicate e1 and e2


def test_pixel_unpacking_2d():
    inst = Instance()
    arr = np.zeros((100, 200))
    inst.set_pixel_data(arr)

    assert inst.attributes["0028,0010"] == 100  # Rows
    assert inst.attributes["0028,0011"] == 200  # Cols
    assert inst.attributes["0028,0002"] == 1  # Samples


def test_pixel_unpacking_rgb():
    inst = Instance()
    # 3D array where last dim is 3 (RGB)
    arr = np.zeros((100, 200, 3))
    inst.set_pixel_data(arr)

    assert inst.attributes["0028,0002"] == 3
    assert inst.attributes["0028,0004"] == "RGB"


def test_lazy_loading(tmp_path):
    """Verify get_pixel_data loads from disk if memory is empty."""
    inst = Instance(sop_instance_uid="1.2.3")

    dummy_file = tmp_path / "dummy.dcm"
    dummy_file.touch()  # <--- THIS WAS MISSING (Creates empty file)
    inst.file_path = str(dummy_file)

    # Mock pydicom.dcmread so we don't need a real file
    with patch("pydicom.dcmread") as mock_read:
        mock_ds = mock_read.return_value
        mock_ds.pixel_array = np.zeros((50, 50))

        # Act
        data = inst.get_pixel_data()

        # Assert
        assert data.shape == (50, 50)
        mock_read.assert_called_once_with(inst.file_path)
        # Verify it cached the result
        assert inst.pixel_array is not None

def test_pixel_bits_allocated():
    """Verify that BitsAllocated is correctly inferred from dtype."""
    inst = Instance()
    
    # Test uint8 (8 bits)
    arr_uint8 = np.zeros((10, 10), dtype=np.uint8)
    inst.set_pixel_data(arr_uint8)
    assert inst.attributes["0028,0100"] == 8
    
    # Test uint16 (16 bits)
    arr_uint16 = np.zeros((10, 10), dtype=np.uint16)
    inst.set_pixel_data(arr_uint16)
    assert inst.attributes["0028,0100"] == 16
    
    # Test int32 (32 bits) - rare for pixel data but good for robustness check
    arr_int32 = np.zeros((10, 10), dtype=np.int32)
    inst.set_pixel_data(arr_int32)
    assert inst.attributes["0028,0100"] == 32