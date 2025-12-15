import hashlib
from typing import List, Optional
from datetime import datetime, timedelta
from tqdm import tqdm
from .privacy import PhiFinding, PhiRemediation
from .logger import get_logger

class RemediationService:
    """
    Applies remediation proposals found by the PhiInspector.
    Handles data anonymization and date shifting.
    """
    def __init__(self, store_backend=None, date_jitter_config: Optional[dict] = None):
        self.logger = get_logger()
        self.store_backend = store_backend
        self.jitter_config = date_jitter_config or {"min_days": -365, "max_days": -1}

    def apply_remediation(self, findings: List[PhiFinding]):
        """
        Iterates through the findings and applies valid remediation proposals.
        """
        processed_entities = set() # To avoid double-processing if multiple findings point to same entity/attr
        audit_buffer = []

        for finding in tqdm(findings, desc="Anonymizing Metadata", unit="finding"):
            if not finding.remediation_proposal:
                continue
            
            # Simple deduping key
            key = (finding.entity_uid, finding.field_name)
            if key in processed_entities:
                continue
            
            try:
                self._apply_single_remediation(finding, audit_buffer)
                processed_entities.add(key)
            except Exception as e:
                self.logger.error(f"Failed to apply remediation for {finding.entity_uid} ({finding.field_name}): {e}")
        
        # Flush audit logs
        if self.store_backend and audit_buffer:
            self.logger.info(f"Flushing {len(audit_buffer)} audit logs...")
            self.store_backend.log_audit_batch(audit_buffer)

    def _apply_single_remediation(self, finding: PhiFinding, audit_buffer: list = None):
        proposal = finding.remediation_proposal
        entity = finding.entity
        
        if not entity:
             self.logger.warning(f"Finding for {finding.entity_uid} has no entity reference. Skipping.")
             return

        action_type = ""
        details = ""

        if proposal.action_type == "REPLACE_TAG":
            # Direct replacement
            
            # 1. Generic DicomItem support (Instance, Series, etc.)
            if hasattr(entity, "set_attr"):
                # Tag ID is expected in proposal.target_attr (e.g. "0010,0010")
                entity.set_attr(proposal.target_attr, proposal.new_value)
                details = f"Remediated {finding.entity_uid} (Tag {proposal.target_attr}) -> {proposal.new_value}"
                action_type = "REMEDIATION_REPLACE"
                
            # 2. Python Object Attribute support (Patient.patient_name)
            elif hasattr(entity, proposal.target_attr):
                setattr(entity, proposal.target_attr, proposal.new_value)
                details = f"Remediated {finding.entity_uid}: {proposal.target_attr} -> {proposal.new_value}"
                action_type = "REMEDIATION_REPLACE"

            else:
                self.logger.warning(f"Entity {finding.entity_uid} (Type: {type(entity).__name__}) has no attribute or setter for {proposal.target_attr}")
                return

        elif proposal.action_type == "SHIFT_DATE":
            # Deterministic Date Shifting
            patient_id = self._resolve_patient_id(entity, proposal)
            if not patient_id:
                self.logger.warning(f"Could not resolve PatientID for {finding.entity_uid}. Skipping date shift.")
                return

            shift_days = self._get_date_shift(patient_id)
            new_date = self._shift_date_string(proposal.original_value, shift_days)
            
            if new_date:
                setattr(entity, proposal.target_attr, new_date)
                details = f"Date Shifted {finding.entity_uid}: {proposal.target_attr} ({shift_days} days)"
                action_type = "REMEDIATION_SHIFT_DATE"
            else:
                 self.logger.warning(f"Invalid date format for {finding.entity_uid}: {proposal.original_value}")
                 return

        elif proposal.action_type == "REMOVE_TAG":
            # 1. Generic DicomItem support
            if hasattr(entity, "attributes") and isinstance(entity.attributes, dict):
                if proposal.target_attr in entity.attributes:
                    del entity.attributes[proposal.target_attr]
                    details = f"Removed Tag {proposal.target_attr} from {finding.entity_uid}"
                    action_type = "REMEDIATION_REMOVE"
            # 2. Python Object Attribute
            elif hasattr(entity, proposal.target_attr):
                 setattr(entity, proposal.target_attr, None)
                 details = f"Cleared Attribute {proposal.target_attr} on {finding.entity_uid}"
                 action_type = "REMEDIATION_REMOVE"

        # Logging & Auditing
        if action_type:
            self.logger.info(details)
            if self.store_backend:
                if audit_buffer is not None:
                    audit_buffer.append((action_type, finding.entity_uid, details))
                else:
                    self.store_backend.log_audit(action_type, finding.entity_uid, details)

    def _resolve_patient_id(self, entity, proposal: PhiRemediation = None) -> Optional[str]:
        # 1. Check metadata in proposal (Best for Date Shifting logic)
        if proposal and proposal.metadata and "patient_id" in proposal.metadata:
            return proposal.metadata["patient_id"]

        # 2. Check entities directly
        if hasattr(entity, "patient_id") and entity.patient_id:
            return entity.patient_id
        
        # 3. If the entity matches our Patient class structure (it has 'patient_id' field)
        # We already checked hasattr above.

        return None

    def _get_date_shift(self, patient_id: str) -> int:
        """
        Generates a deterministic shift between min_days and max_days based on PatientID.
        """
        # Create a hash of the PatientID
        hash_obj = hashlib.sha256(patient_id.encode())
        # Convert first 8 bytes to int
        val = int(hash_obj.hexdigest()[:8], 16)
        
        min_days = self.jitter_config.get("min_days", -365)
        max_days = self.jitter_config.get("max_days", -1)
        
        # Ensure correct order
        if min_days > max_days:
            min_days, max_days = max_days, min_days
            
        span = max_days - min_days + 1
        if span < 1: 
            span = 1
            
        # Modulo span to get 0..span-1, then add min_days
        offset = (val % span) + min_days
        return offset

    def _shift_date_string(self, date_val, days: int) -> Optional[str]:
        """
        Shifts a date by 'days'. 
        Handles 'YYYYMMDD' strings or datetime.date objects.
        Returns same type as input (str -> str, date -> date).
        """
        # Handles date and datetime objects
        if hasattr(date_val, 'strftime'): 
            return date_val + timedelta(days=days)
            
        # Assume string
        try:
            dt = datetime.strptime(str(date_val), "%Y%m%d")
            new_dt = dt + timedelta(days=days)
            return new_dt.strftime("%Y%m%d")
        except ValueError:
            return None
