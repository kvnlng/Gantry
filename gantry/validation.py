from pydicom.dataset import Dataset
from typing import List


class IODValidator:
    _MODULE_DEFINITIONS = {
        'Common': {
            '0008,0016': '1', '0008,0018': '1', '0008,0020': '1',
            '0008,0030': '1', '0008,0060': '1', '0020,000E': '1',
        },
        'CTImage': {
            '0018,0050': '2', '0018,0060': '2',  # SliceThickness, KVP
            '0020,0032': '1', '0020,0037': '1',  # Pos, Orient
            '0028,0030': '1',  # Pixel Spacing
        }
    }

    _SOP_RULES = {
        '1.2.840.10008.5.1.4.1.1.2': ['Common', 'CTImage'],  # CT Image Storage
    }

    @staticmethod
    def validate(ds: Dataset) -> List[str]:
        errors = []
        sop = ds.file_meta.MediaStorageSOPClassUID if hasattr(ds, 'file_meta') else ds.get("SOPClassUID")

        if sop not in IODValidator._SOP_RULES:
            return []

        for module in IODValidator._SOP_RULES[sop]:
            for tag_str, req in IODValidator._MODULE_DEFINITIONS.get(module, {}).items():
                group, elem = map(lambda x: int(x, 16), tag_str.split(','))
                tag = (group, elem)

                if req == '1' and (tag not in ds or ds[tag].value in [None, ""]):
                    errors.append(f"[Type 1 Error] Missing {tag_str} in {module}")
                elif req == '2' and tag not in ds:
                    errors.append(f"[Type 2 Error] Missing {tag_str} in {module}")
        return errors