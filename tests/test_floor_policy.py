"""A session that has loaded no configuration de-identifies against a floor
policy (#495), and the floor is the table the scaffold is generated from.

Measured on `168fdd6` with pydicom's `CT_small.dcm`: `Session` -> `ingest`
-> `audit()` -> `anonymize()` -> `export()` wrote a file carrying Study ID
`1CT1`, Series/Acquisition/Content Date `19970430`, Station Name
`CT01_OC0`, Institution Name `JFK IMAGING CENTER` and Study Time `072730`,
graded `PASS`, with a method line reading `Session defaults: 6 tag rules`
-- six rules the scan never applied, because `audit()` read
`configuration.phi_tags` (`{}`) while the report read the shipped
`phi_tags.json`. The documented Quick Start was worse: `create_config` ->
`load_config` -> `anonymize` -> `export` on the same file raised
`ExportError ... ['[Type 1 Error] Missing 0008,0030 in Common']` and wrote
nothing, because the basic profile removed Study Time and `IODValidator`
called it Type 1 (it is Type 2, PS3.3 C.7.2.1).

Every test here names the mutant it kills in its docstring.
"""
import json
import os
import shutil

import pydicom
import pydicom.data
import pytest
from pydicom.dataset import Dataset

from isocenter import Session
from isocenter.validation import IODValidator


CT_IMAGE_STORAGE = "1.2.840.10008.5.1.4.1.1.2"


def _ct_dataset_with_empty_study_time() -> Dataset:
    """A CT dataset carrying every Common/CTImage element the validator
    checks, with Study Time present and empty -- the shape the floor's
    EMPTY action leaves behind."""
    ds = Dataset()
    ds.file_meta = pydicom.dataset.FileMetaDataset()
    ds.file_meta.MediaStorageSOPClassUID = CT_IMAGE_STORAGE
    ds.SOPClassUID = CT_IMAGE_STORAGE
    ds.SOPInstanceUID = "1.2.3.4"
    ds.StudyDate = "20030525"
    ds.StudyTime = ""
    ds.Modality = "CT"
    ds.SeriesInstanceUID = "1.2.3"
    ds.SliceThickness = "1"
    ds.KVP = "120"
    ds.ImagePositionPatient = [0, 0, 0]
    ds.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
    ds.PixelSpacing = [0.5, 0.5]
    return ds


def test_the_floor_does_not_reach_the_validator_as_a_type_1_gap():
    """Study Time is Type 2 in General Study (PS3.3 C.7.2.1): present and
    empty is conformant. Kills `_MODULE_DEFINITIONS["Common"]["0008,0030"]`
    reverted to `'1'`, whose Type-1 arm rejects the empty value and made
    every CT export on the documented path raise."""
    ds = _ct_dataset_with_empty_study_time()

    assert IODValidator.validate(ds) == []

    # The positive control: the same element *absent* is still an error,
    # so the change is Type 1 -> Type 2 and not "stop checking it".
    del ds.StudyTime
    assert IODValidator.validate(ds) == ["[Type 2 Error] Missing 0008,0030 in Common"]
