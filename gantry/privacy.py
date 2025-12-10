from dataclasses import dataclass
from typing import List, Any
from .entities import Patient, Study, Series, Instance

@dataclass
class PhiFinding:
    """Represents a potential PHI breach discovered during a scan."""
    entity_uid: str
    entity_type: str
    field_name: str
    value: Any
    reason: str

class PhiInspector:
    """
    Scans the Object Graph for attributes that are known to contain Protected Health Information (PHI).
    Based on HIPAA Safe Harbor identifier rules.
    """
    HIPAA_TAGS = {
        "0010,0010": "Patient Name",
        "0010,0020": "Patient ID",
        "0010,0030": "Patient Birth Date",
        "0008,0080": "Institution Name",
        "0008,0090": "Referring Physician Name",
        "0008,0050": "Accession Number"
    }

    def scan_patient(self, patient: Patient) -> List[PhiFinding]:
        """
        Recursively scans a Patient and their child studies for PHI.
        """
        findings = []
        
        # 1. Direct Attributes
        if patient.patient_name and patient.patient_name != "Unknown":
             findings.append(PhiFinding(patient.patient_id, "Patient", "patient_name", patient.patient_name, "Names are PHI"))
        
        if patient.patient_id and patient.patient_id != "UNKNOWN":
             findings.append(PhiFinding(patient.patient_id, "Patient", "patient_id", patient.patient_id, "Medical Record Numbers are PHI"))

        # 2. Traverse Children
        for study in patient.studies:
            findings.extend(self._scan_study(study))
            
        return findings

    def _scan_study(self, study: Study) -> List[PhiFinding]:
        findings = []
        uid = study.study_instance_uid
        
        if study.study_date:
            findings.append(PhiFinding(uid, "Study", "study_date", study.study_date, "Dates are Safe Harbor restricted"))
            
        return findings
