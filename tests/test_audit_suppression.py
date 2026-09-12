
import pytest
from isocenter.entities import Patient, Study, Series, Instance
from isocenter.privacy import PhiInspector, PhiFinding, PhiReport
from isocenter.remediation import RemediationService

def test_audit_suppresses_a_value_it_shifted():
    """
    Verifies that the PhiInspector does NOT report a SHIFT/JITTER value
    the pipeline already shifted -- and that what makes it stop is the
    per-value record, not an entity-level flag (#510, #513).

    The flag half is asserted in both directions on purpose. The
    suppression used to be `if instance.date_shifted or
    study.date_shifted`, and that is the defect: a flag set by one date
    said nothing about the next one, so a valid date the pipeline never
    touched was skipped for ever (#510) while a date inside a sequence,
    where no flag exists at all, was re-shifted every pass (#513).
    """
    # 1. Setup Instance with a Date that requires shifting
    inst = Instance("I1", "SOP1", 1)
    inst.attributes["0008,0020"] = "20230101" # Study Date

    # 2. Setup Inspector with policy to JITTER dates
    config_tags = {
        "0008,0020": {"name": "Study Date", "action": "JITTER"} # Action is SHIFT/JITTER
    }

    inspector = PhiInspector(config_tags=config_tags)

    # 3. Initial Scan -> Should find it
    findings = inspector._scan_instance(inst, "P1")
    assert len(findings) == 1
    assert findings[0].tag == "0008,0020"
    assert findings[0].remediation_proposal.action_type == "SHIFT_DATE"

    # 4. The entity-level flag alone suppresses nothing: it claims a
    # shift landed somewhere on the instance, never that it landed on
    # this value.
    inst.date_shifted = True
    assert len(inspector._scan_instance(inst, "P1")) == 1

    # 5. The record for the value the tag actually holds suppresses it.
    inst.record_date_shift("0008,0020", "20230101")
    assert inspector._scan_instance(inst, "P1") == []

    # 6. ...and stops the moment the tag stops holding that value.
    inst.set_attr("0008,0020", "20240704")
    assert len(inspector._scan_instance(inst, "P1")) == 1

def test_remediation_service_sets_flag():
    """
    Verifies that RemediationService actually sets the date_shifted flag.
    """
    # 1. Setup Entity
    inst = Instance("I1", "SOP1", 1)
    inst.attributes["0008,0020"] = "20230101"

    # 2. Create Finding regarding this entity
    from isocenter.privacy import PhiRemediation
    proposal = PhiRemediation(
        action_type="SHIFT_DATE",
        target_attr="0008,0020",
        original_value="20230101",
        metadata={"patient_id": "P1"}
    )
    finding = PhiFinding(
        entity_uid="I1", entity_type="Instance", field_name="StudyDate",
        value="20230101", reason="PHI", entity=inst, remediation_proposal=proposal
    )

    # 3. Run Remediation
    service = RemediationService(store_backend=None)
    # We mock _get_date_shift to be deterministic/simple
    service._get_date_shift = lambda pid: 10

    service.apply_remediation([finding])

    # 4. Verify Flag
    assert inst.date_shifted is True
    assert inst.attributes["0008,0020"] == "20230111" # Shifted by 10 days
    # ...and the per-value record, which is what the scan reads (#510).
    assert inst.date_shift_vouches_for("0008,0020", "20230111")
