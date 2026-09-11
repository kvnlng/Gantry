from pydicom.dataset import Dataset
from isocenter.validation import IODValidator


def test_validator_valid_ct():
    ds = Dataset()
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"  # CT Image Storage

    # --- 1. Populate ALL Mandatory Fields (Common + CTImage) ---
    # Common Module (Type 1)
    ds.SOPInstanceUID = "1.2.3"
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"  # <--- Was missing
    ds.Modality = "CT"  # <--- Was missing
    ds.SeriesInstanceUID = "1.2.3.4"

    # CT Image Module (Type 1)
    ds.ImagePositionPatient = [0, 0, 0]
    ds.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
    ds.PixelSpacing = [1, 1]

    # CT Image Module (Type 2 - Must exist, even if empty)
    ds.SliceThickness = ""
    ds.KVP = ""

    # --- 2. Verify Baseline (Should be 0 errors) ---
    errors = IODValidator.validate(ds)
    assert len(errors) == 0, f"Expected valid DS, but got: {errors}"

    # --- 3. Test Deletion (Type 1 Error) ---
    del ds.PixelSpacing

    errors = IODValidator.validate(ds)

    # Assert we caught the error
    assert len(errors) > 0
    # Search through ALL errors, not just the first one
    assert any("0028,0030" in e or "PixelSpacing" in e for e in errors)


def _complete_ct():
    """A CT carrying every Type 1 and Type 2 element the validator knows."""
    ds = Dataset()
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"  # CT Image Storage
    ds.SOPInstanceUID = "1.2.3"
    ds.StudyDate = "20230101"
    ds.StudyTime = "120000"
    ds.Modality = "CT"
    ds.SeriesInstanceUID = "1.2.3.4"
    ds.ImagePositionPatient = [0, 0, 0]
    ds.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
    ds.PixelSpacing = [1, 1]
    ds.SliceThickness = "1.0"
    ds.KVP = "120"
    return ds


def test_a_missing_type_2_element_is_reported_and_an_empty_one_is_not():
    """Type 2 means "present, possibly empty" -- both halves pinned (#439).

    The test above covers Type 1 and passes an *empty* Type 2 pair, so
    nothing ever handed the validator a CT *missing* one. Inverting the
    Type 2 check (`req == '2'` to `!=`) or deleting its `errors.append`
    left every test green: two of `validation.py`'s three survivors when
    the probe first ran it. The exact list is asserted, so a message that
    names the wrong tag or module is red as well, and so is a Type 2
    check that starts treating an empty value as missing.
    """
    ds = _complete_ct()
    assert IODValidator.validate(ds) == []

    del ds.KVP
    assert IODValidator.validate(ds) == ["[Type 2 Error] Missing 0018,0060 in CTImage"]

    ds.KVP = ""
    assert IODValidator.validate(ds) == []
