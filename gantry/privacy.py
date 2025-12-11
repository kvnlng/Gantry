from dataclasses import dataclass, field
from typing import List, Any, Optional, Union, Dict
import hashlib
from .entities import Patient, Study, Series, Instance

@dataclass
class PhiRemediation:
    """Proposed action to fix a PHI finding."""
    action_type: str  # e.g., "REPLACE_TAG", "REDACT_REGION"
    target_attr: str  # e.g., "patient_name", "study_date"
    new_value: Any = None
    original_value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PhiFinding:
    """Represents a potential PHI breach discovered during a scan."""
    entity_uid: str
    entity_type: str
    field_name: str
    value: Any
    reason: str
    patient_id: Optional[str] = None # Added for linkage
    entity: Any = None # Reference to the actual object (Patient, Study, etc.)
    remediation_proposal: Optional[PhiRemediation] = None

class PhiReport:
    """
    A container for PHI findings that supports analysis and export.
    Acts as a list for backward compatibility.
    """
    def __init__(self, findings: List[PhiFinding]):
        self.findings = findings

    def to_dataframe(self):
        """
        Converts findings to a Pandas DataFrame for analysis.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for this feature. Install it with `pip install pandas`.")
            
        data = []
        for f in self.findings:
            row = {
                "patient_id": f.patient_id,
                "entity_type": f.entity_type,
                "entity_uid": f.entity_uid,
                "field": f.field_name,
                "value": str(f.value),
                "reason": f.reason,
                "action": f.remediation_proposal.action_type if f.remediation_proposal else None
            }
            data.append(row)
        return pd.DataFrame(data)

    def __iter__(self):
        return iter(self.findings)

    def __len__(self):
        return len(self.findings)
    
    def __getitem__(self, index):
        return self.findings[index]
    
    def __repr__(self):
        return f"<PhiReport: {len(self.findings)} findings>"

class PhiInspector:
    """
    Scans the Object Graph for attributes that are known to contain Protected Health Information (PHI).
    Based on HIPAA Safe Harbor identifier rules.
    """
    def __init__(self, config_path: str = None):
        from .config_manager import ConfigLoader
        self.phi_tags = ConfigLoader.load_phi_config(config_path)

    def scan_patient(self, patient: Patient) -> List[PhiFinding]:
        """
        Recursively scans a Patient and their child studies for PHI.
        """
        findings = []
        
        # 1. Direct Attributes
        if patient.patient_name and patient.patient_name != "Unknown" and patient.patient_name != "ANONYMIZED":
             proposal = PhiRemediation(
                 action_type="REPLACE_TAG",
                 target_attr="patient_name",
                 new_value="ANONYMIZED",
                 original_value=patient.patient_name
             )
             findings.append(PhiFinding(
                 entity_uid=patient.patient_id, 
                 entity_type="Patient", 
                 field_name="patient_name", 
                 value=patient.patient_name, 
                 reason="Names are PHI",
                 patient_id=patient.patient_id,
                 entity=patient,
                 remediation_proposal=proposal
             ))
        
        if patient.patient_id and patient.patient_id != "UNKNOWN" and not patient.patient_id.startswith("ANON_"):
             # Simple deterministic anonymization proposal for now (can be refined in Service)
             # The Service will handle the hash calculation if 'new_value' is a placeholder or if logic dictates
             hashed_id = f"ANON_{hashlib.sha256(patient.patient_id.encode()).hexdigest()[:12]}"
             proposal = PhiRemediation(
                 action_type="REPLACE_TAG",
                 target_attr="patient_id",
                 new_value=hashed_id,
                 original_value=patient.patient_id
             )
             findings.append(PhiFinding(
                 entity_uid=patient.patient_id, 
                 entity_type="Patient", 
                 field_name="patient_id", 
                 value=patient.patient_id, 
                 reason="Medical Record Numbers are PHI",
                 patient_id=patient.patient_id,
                 entity=patient,
                 remediation_proposal=proposal
             ))

        # 2. Traverse Children
        for study in patient.studies:
            findings.extend(self._scan_study(study, patient.patient_id))
            
        return findings

    def _scan_study(self, study: Study, patient_id: str = None) -> List[PhiFinding]:
        findings = []
        uid = study.study_instance_uid
        
        if study.study_date:
            proposal = PhiRemediation(
                action_type="SHIFT_DATE", # Special action for the Service to handle
                target_attr="study_date",
                original_value=study.study_date,
                metadata={"patient_id": patient_id}
            )
            findings.append(PhiFinding(
                entity_uid=uid, 
                entity_type="Study", 
                field_name="study_date", 
                value=study.study_date, 
                reason="Dates are Safe Harbor restricted",
                patient_id=patient_id,
                entity=study,
                remediation_proposal=proposal
            ))
            
        return findings
