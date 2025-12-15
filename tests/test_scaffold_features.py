
import pytest
import os
import json
from gantry.session import DicomSession
from gantry.config_manager import ConfigLoader
from gantry.entities import Instance, Patient, Study, Series
from gantry.privacy import PhiInspector, PhiFinding, PhiRemediation
from gantry.remediation import RemediationService

def test_scaffold_config_structure(tmp_path):
    """Verify that scaffold_config produces valid JSON with new fields."""
    session = DicomSession(persistence_file=":memory:")
    output_path = tmp_path / "unified.json"
    
    session.scaffold_config(str(output_path))
    
    assert output_path.exists()
    
    # Load and check
    with open(output_path, 'r') as f:
        data = json.load(f)
        
    assert "version" in data
    assert "machines" in data
    assert "phi_tags" in data
    assert "date_jitter" in data
    assert "remove_private_tags" in data
    
    assert data["date_jitter"]["min_days"] == -365
    assert data["date_jitter"]["max_days"] == -1
    assert data["remove_private_tags"] is True
    
    # Check if research tags are present (e.g. Study Date with JITTER)
    tags = data["phi_tags"]
    assert "0008,0020" in tags
    assert tags["0008,0020"]["action"] == "JITTER"

def test_private_tag_removal():
    """Verify private tag removal logic."""
    inspector = PhiInspector(remove_private_tags=True)
    
    inst = Instance("1.2.3", "dcm_file")
    # Odd group = Private
    inst.attributes["0011,1010"] = "PrivateData"
    # Even group = Public
    inst.attributes["0010,0010"] = "PublicData"
    # Whitelisted Private
    inst.attributes["0099,0010"] = "GANTRY_SECURE"
    
    findings = inspector._scan_instance(inst, "PAT1")
    
    private_findings = [f for f in findings if "Private Tag" in f.field_name]
    assert len(private_findings) == 1
    assert private_findings[0].tag == "0011,1010"
    assert private_findings[0].remediation_proposal.action_type == "REMOVE_TAG"
    
    # Check whitelist ignored
    whitelist_finding = [f for f in findings if f.tag == "0099,0010"]
    assert len(whitelist_finding) == 0

def test_date_jitter_service():
    """Verify that remediation service respects jitter config."""
    # Config: ONLY shift by -10 days
    config = {"min_days": -10, "max_days": -10}
    svc = RemediationService(date_jitter_config=config)
    
    # With deterministic hashing, the offset is usually (hash % span) + min.
    # Span = (-10) - (-10) + 1 = 1.
    # Offset = (hash % 1) + (-10) = 0 - 10 = -10.
    # So it should ALWAYS be -10.
    
    shift = svc._get_date_shift("PAT123")
    assert shift == -10
    
    shift2 = svc._get_date_shift("OTHER_PAT")
    assert shift2 == -10
    
    # Test Application
    # 20230111 shifted by -10 days -> 20230101
    new_date = svc._shift_date_string("20230111", shift)
    assert new_date == "20230101"
