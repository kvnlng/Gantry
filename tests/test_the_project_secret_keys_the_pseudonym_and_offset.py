"""The pseudonym and the date offset are derived under a project secret.

Until 0.9.7 the `ANON_` pseudonym was the first 12 hex characters of an
unkeyed SHA-256 of the original Patient ID, and the date offset was the
first 8 of those same characters mod the jitter span. Both were therefore
functions of public inputs: the offset could be computed from an exported
file's own PatientID, and the pseudonym inverted by hashing candidate IDs.

**The rule these tests hold.** Both are HMAC-SHA256 under a per-project
secret, with distinct labels, so neither can be computed without the
secret and neither reveals the other. There is no unkeyed fallback: a
keyed derivation asked for without a secret raises.

**Literals.** Every expected pseudonym and offset below was computed once
under `FIXED_A`/`FIXED_B` from the implementation and pasted, so a change
to the arithmetic cannot pass by moving both sides of an equality.

**Why this file imports what it does.** The derivations live in
`isocenter.privacy` and the offset in `isocenter.remediation`, so it
charges both modules' probe rows; see `test_mutation_probe_targets.py`.
"""
import hashlib

import pytest

from isocenter.entities import Patient
from isocenter.privacy import (JITTER_SCHEME_KEYED, PhiInspector,
                               _replacement_id_for)
from isocenter.remediation import RemediationService

from support.project_secret import FIXED_A, FIXED_B


def test_no_secret_no_offset():
    """T17. A keyed derivation with no secret raises; it never falls back.

    Red on the mutant that returns the unkeyed digest when the secret is
    None -- a one-line road back to an offset anyone can compute.
    """
    with pytest.raises(RuntimeError, match="project secret"):
        RemediationService()._get_date_shift("P1", JITTER_SCHEME_KEYED)
    with pytest.raises(RuntimeError, match="project secret"):
        PhiInspector(config_tags={}).scan_patient(Patient("P1", "N"))
    with pytest.raises(RuntimeError, match="project secret"):
        _replacement_id_for("P1", None)


def test_the_pseudonym_depends_on_the_secret():
    """T1. One original, two secrets, two pseudonyms -- neither unkeyed.

    Red on: the HMAC replaced by a plain hash of label and original (the
    literal moves and the two secrets agree); the digest cut back to 12
    hex (the length moves).
    """
    under_a = _replacement_id_for("P1", FIXED_A)
    under_b = _replacement_id_for("P1", FIXED_B)

    assert under_a == "ANON_d932e13c0c2fa7dafbd43b77", under_a
    assert under_b == "ANON_e4abc38a712a6130cce8064f", under_b
    for pseudonym in (under_a, under_b):
        assert len(pseudonym) == 29
        assert pseudonym != "ANON_" + hashlib.sha256(b"P1").hexdigest()[:12]
        assert hashlib.sha256(b"P1").hexdigest()[:12] not in pseudonym


def _offset(secret, patient_id):
    return RemediationService(project_secret=secret)._get_date_shift(
        patient_id, JITTER_SCHEME_KEYED)


def test_the_offset_depends_on_the_secret():
    """T2. Literal offsets under FIXED_A, and FIXED_B moves them.

    The old literals -- `P1` -286, `1CT1` -239 -- are what anyone could
    compute from the id alone; none of the keyed ones is.
    Red on: the secret dropped from the jitter HMAC (the literals move,
    and A and B agree).
    """
    assert _offset(FIXED_A, "P1") == -364
    assert _offset(FIXED_A, "1CT1") == -98
    assert _offset(FIXED_A, "MRN-00-77") == -86
    assert [_offset(FIXED_B, p) for p in ("P1", "1CT1", "MRN-00-77")] == \
        [-120, -359, -153]


def test_the_offset_cannot_be_read_out_of_the_export():
    """T3: the offset is not a readback of the exported id.

    Under FIXED_A, for 1000 synthetic ids, compare the offset the
    pipeline applies with the two readbacks someone holding only the
    export and the documented range can compute from the exported
    PatientID: the 0.9.6 formula (8 hex after the prefix) and the same
    formula over the new 16-hex digest. A readback that agrees by chance
    hits about 1000/365 ~ 3 times; the bound is 3x that.

    What it proves: the offset is neither of those two functions of the
    exported PatientID. What it cannot prove: HMAC's security, the
    absence of other side channels (UIDs that embed dates; weekdays a
    whole-day shift preserves; a guess narrowed to the jitter span), or
    that no other function of exported fields predicts the offset.

    Red on: the readback reintroduced (1000/1000 on the first); the
    offset seeded on the pseudonym HMAC's own digest bits (1000/1000 on
    the second).
    """
    service = RemediationService(project_secret=FIXED_A)
    old_formula = new_digest = 0
    for index in range(1000):
        original = f"MRN{index:07d}"
        exported = _replacement_id_for(original, FIXED_A)
        offset = service._get_date_shift(original, JITTER_SCHEME_KEYED)
        assert offset == service._get_date_shift(exported, JITTER_SCHEME_KEYED)
        old_formula += offset == (int(exported[5:13], 16) % 365) - 365
        new_digest += offset == (int(exported[5:21], 16) % 365) - 365
    assert old_formula <= 9, old_formula
    assert new_digest <= 9, new_digest


#: Ids whose offset must survive replacement (#517). An `ANON_`-prefixed
#: id is absent: the pipeline never mints a pseudonym for one, so there
#: is no "replacement spelling" to agree with -- see the next test.
IDS = ["P1", "1CT1", "", "12345", "MRN-00-77", "patient/with,commas"]


@pytest.mark.parametrize("patient_id", IDS)
def test_either_spelling_gives_one_offset(patient_id):
    """T5, first half (#517). The original and the pseudonym it becomes
    give one offset.

    Red on: the canonical key taken as the id's own text (the original
    and its pseudonym then seed differently).
    """
    replacement = _replacement_id_for(patient_id, FIXED_A)
    assert _offset(FIXED_A, replacement) == _offset(FIXED_A, patient_id), (
        f"the offset moved when {patient_id!r} was replaced")


@pytest.mark.parametrize("patient_id", [
    "ANON_not_hex", "ANON_deadbeefcafe", "ANON_", "ANON_fbeae7c18667"])
def test_a_prefixed_id_seeds_from_its_own_text_under_the_secret(patient_id):
    """T5, second half. Every `ANON_` id seeds from an HMAC of its own
    text, never from characters read out of it.

    `ANON_fbeae7c18667` is the 0.9.6 pseudonym of `P1`; read back the
    unkeyed way it gives -286, and keyed it must not. The expected value
    is recomputed from the documented construction with `hmac` directly
    rather than through the module, so the arithmetic is pinned from
    outside it.

    Red on: a readback of `id[5:13]` for prefixed ids.
    """
    import hmac
    seed = int.from_bytes(hmac.new(
        FIXED_A, b"isocenter/v1/date-jitter\x00" + patient_id.encode(),
        hashlib.sha256).digest()[:8], "big")
    assert _offset(FIXED_A, patient_id) == (seed % 365) - 365
    carried = patient_id[5:13]
    if len(carried) == 8 and all(c in "0123456789abcdef" for c in carried):
        assert _offset(FIXED_A, patient_id) != (int(carried, 16) % 365) - 365
