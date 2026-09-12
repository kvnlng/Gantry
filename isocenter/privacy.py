from dataclasses import dataclass, field
from typing import List, Any, Optional, Dict, Tuple
import hashlib
import re
from .entities import Patient, Study, Instance, iter_item_tree
from .logger import get_logger


# The replacements `scan_patient` proposes, spelled once for the two
# places that ask "is this already the Patient's replacement?":
# `scan_patient`, which raises nothing for a patient holding one, and
# `_scan_instance`, which raises nothing for an instance's top-level copy
# of it (#496). Two spellings of one test would drift, and a drift here
# either re-flags every anonymized instance on a re-audit or stops
# flagging a real identifier that happens to start with the prefix.
def _is_replacement_name(value) -> bool:
    return value == "ANONYMIZED"


def _is_replacement_id(value) -> bool:
    return str(value).startswith("ANON_")


#: The hex run inside an `ANON_` replacement. Used to refuse a value that
#: merely starts with the prefix (`ANON_xyz`, a user's own id) before
#: `jitter_digest` reads eight characters out of it as a digest.
_IS_HEX = re.compile(r"[0-9a-f]+")

#: How many hex characters of the PatientID digest the replacement
#: carries, and how many the date jitter seeds on. The first is a prefix
#: of the second, which is the whole reason `jitter_digest` can read the
#: seed back out of a replacement; shorten `_REPLACEMENT_DIGEST_CHARS`
#: below `_JITTER_DIGEST_CHARS` and the coupling breaks silently, which
#: is why `tests/test_the_jitter_seed_survives_anonymize.py` pins it.
_REPLACEMENT_DIGEST_CHARS = 12
_JITTER_DIGEST_CHARS = 8


def _replacement_id_for(patient_id) -> str:
    """The PatientID replacement `scan_patient` proposes.

    Spelled once, beside the test that recognises it, because the date
    jitter reads its digest back out (`jitter_digest`): two spellings of
    the constructor would let the prefix, the digest length or the hash
    move on one side only.
    """
    digest = hashlib.sha256(str(patient_id).encode()).hexdigest()
    return f"ANON_{digest[:_REPLACEMENT_DIGEST_CHARS]}"


def jitter_digest(patient_id) -> str:
    """The digest that seeds the date jitter, from either spelling of an
    identity (#517).

    `_get_date_shift` used to hash whatever PatientID the scan saw, and
    `anonymize()` replaces PatientID in its first pass -- so every later
    pass hashed `ANON_<digest>` instead of the original and landed on a
    different offset (measured: `P1` gives -286 days, the replacement it
    produces gives -47). A date first shifted in a later pass therefore
    fell off the offset its siblings got, and "jitter is deterministic
    per patient so intervals survive" held only within one pass.

    A replacement already **carries** the original's digest -- it is
    `ANON_` plus its first 12 hex characters, and the jitter reads the
    first 8 -- so the canonical key is read back out of it rather than
    recomputed. That is what makes this fix carry no legacy question:
    for an id that has *not* been replaced the arithmetic is unchanged,
    bit for bit, so every offset this version computes is the offset
    0.9.5 computed and a store's dates stay consistent across the
    upgrade.

    The coupling between the two spellings is load-bearing: it holds only
    while `_replacement_id_for` carries at least
    `_JITTER_DIGEST_CHARS` characters of the *same* hash this function
    would compute. `tests/test_the_jitter_seed_survives_anonymize.py`
    pins it in both directions.

    Known, pre-existing edge, not widened here: a real PatientID that
    literally begins `ANON_` followed by 8 hex characters is read as a
    replacement and seeded from its own text. `_is_replacement_id`
    already treats such an id as anonymized, so this is consistent with
    what the scan does; it is stable, merely arbitrary. An id that starts
    `ANON_` and is *not* 8 hex characters is hashed like any other value.
    """
    text = str(patient_id)
    if _is_replacement_id(text):
        carried = text[5:5 + _JITTER_DIGEST_CHARS]
        if len(carried) == _JITTER_DIGEST_CHARS and _IS_HEX.fullmatch(carried):
            return carried
    return hashlib.sha256(text.encode()).hexdigest()[:_JITTER_DIGEST_CHARS]


def _study_date_is_this_pipelines(study) -> bool:
    """Whether `study.study_date` is a value this pipeline's shift
    produced -- as far as the store can tell (#518).

    Spelled once because two places ask it: `_scan_study`, which raises
    a study date the pipeline did not produce, and
    `_holds_owners_replacement`, which skips an instance's top-level
    copy of a study date only when the owner's value is one this
    pipeline produced (#496). It was `bool(study.date_shifted)` in both,
    and that flag records *that* a shift happened, never *what it
    produced* -- so a fresh original assigned to `study_date` was never
    raised again, and an instance's copy of that fresh original skipped
    as "the owner's replacement". Leaving either on the flag keeps half
    of #518 alive.

    A study shifted before 0.9.6 reads `date_shifted` with no record,
    and what its shift produced is unknowable. That counts as "this
    pipeline's", which keeps the pre-0.9.6 behaviour for such a store
    exactly as the instance half does -- the owner's ruling is that
    nothing in an existing store changes under the user. The load says
    so once, as a WARNING audit row.

    `getattr` throughout because the arm and the scan both fire for any
    object carrying these names, test doubles included.
    """
    if study is None:
        return False
    vouches = getattr(study, "date_shift_vouches_for", None)
    if callable(vouches) and vouches(getattr(study, "study_date", None)):
        return True
    return (bool(getattr(study, "date_shifted", False))
            and getattr(study, "_shifted_study_date", None) is None)


@dataclass(slots=True)
class PhiRemediation:
    """
    Proposed action to fix a PHI finding.

    Attributes:
        action_type (str): The remediation logic code (e.g., "REPLACE_TAG", "SHIFT_DATE").
        target_attr (str): The attribute or tag to modify.
        new_value (Any): The proposed new value (if known).
        original_value (Any): The original value for audit/reversion.
        metadata (Dict[str, Any]): Context metadata (e.g. patient linkage for date shifting).
    """
    action_type: str  # e.g., "REPLACE_TAG", "REDACT_REGION"
    target_attr: str  # e.g., "patient_name", "study_date"
    new_value: Any = None
    original_value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PhiFinding:  # pylint: disable=too-many-instance-attributes
    """
    Represents a potential PHI breach discovered during a scan.

    The attribute count is the point of the record -- it is a report line,
    and every field below is something a reader of that report needs. The
    same reasoning that disables `too-few-public-methods` for DTOs in
    `pylintrc.toml` applies here; splitting it would only move fields
    behind another name.

    Attributes:
        entity_uid (str): Unique identifier of the entity (PatientID, SOPInstanceUID).
        entity_type (str): Type of entity ("Patient", "Instance", etc).
        field_name (str): The specific field or tag description.
        value (Any): The PHI value found.
        reason (str): Why this was flagged (e.g. "Safe Harbor Rules").
        tag (Optional[str]): The DICOM tag (e.g., "0010,0010").
        patient_id (Optional[str]): Linkage for context.
        entity (Any): Reference to the Python object for direct remediation.
        remediation_proposal (Optional[PhiRemediation]): The suggested fix.
        entity_path (Tuple): Route from the Instance to the item this was
            raised against, as `(sequence_tag, index)` steps. Empty means
            the Instance itself. Findings cross a process boundary, and a
            sequence item has no UID to be found again by, so this is the
            only way to rebind one to the item it actually came from.
    """
    entity_uid: str
    entity_type: str
    field_name: str
    value: Any
    reason: str
    tag: Optional[str] = None  # specific DICOM tag if applicable
    patient_id: Optional[str] = None  # Added for linkage
    entity: Any = None  # Reference to the actual object (Patient, Study, etc.)
    remediation_proposal: Optional[PhiRemediation] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    entity_path: Tuple = ()  # Route from the Instance down to a sequence item


class PhiReport:
    """
    A container for PHI findings that supports analysis and export.

    Acts as a list wrapper for backward compatibility but enables
    DataFrame export features.

    `failures` is a list of `(entity_uid, reason)`, one per instance a
    pixel scan could not read in full: its pixels could not be loaded, or
    OCR raised on at least one of its frames. `scan_pixel_content()` fills
    it (#423); an instance that failed on some frames keeps the findings
    of the frames that were read. It is always a list, never `None` -- an
    attribute that is sometimes a list and sometimes `None` is a trap for
    every caller that iterates it. `audit()`'s is always empty: a failure
    in its workers raises instead.
    """

    def __init__(self, findings: List[PhiFinding],
                 failures: Optional[List[Tuple[str, str]]] = None):
        self.findings = findings
        self.failures: List[Tuple[str, str]] = list(failures or [])

    def to_dataframe(self):
        """
        Converts findings to a Pandas DataFrame for analysis.

        Returns:
            pd.DataFrame: A dataframe containing flattened finding details.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "Pandas is required for this feature. Install it with `pip install pandas`.")

        data = []
        for f in self.findings:
            row = {
                "patient_id": f.patient_id,
                "entity_type": f.entity_type,
                "entity_uid": f.entity_uid,
                "tag": f.tag,
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
        return (f"<PhiReport: {len(self.findings)} findings, "
                f"{len(self.failures)} failures>")


class PhiInspector:
    """
    Scans the Object Graph for attributes that are known to contain Protected Health Information (PHI).

    Implements rules based on HIPAA Safe Harbor identifiers and Configurable PHI Tags.
    Also handles Private Tag scanning/flagging.
    """

    def __init__(self,
                 config_path: str = None,
                 config_tags: Dict[str,
                                   str] = None,
                 remove_private_tags: bool = False):
        """
        Initializes the inspector.

        Args:
            config_path (str, optional): Path to a JSON/YAML config file.
            config_tags (Dict[str, Union[str, Dict]], optional): Direct
                config dictionary, taking precedence over `config_path`.
                A tag's value is either a **rule** --
                `{"name": ..., "action": "REMOVE"|"EMPTY"|"SHIFT"|"JITTER"}`
                -- or a plain string, which is the tag's **display name**
                and leaves the action as `REPLACE`. The string form names
                the tag; it does not choose what happens to it. Passing a
                bare action name (`{"0008,0020": "SHIFT"}`) therefore
                replaces the value instead of shifting it, and is warned
                about at construction (#111).
            remove_private_tags (bool): If True, scans all attributes for non-whitelisted private tags.
        """
        from .config_manager import ConfigLoader

        self.remove_private_tags = remove_private_tags

        if config_tags is not None:
            self.phi_tags = config_tags
        elif config_path:
            self.phi_tags = ConfigLoader.load_phi_config(config_path)
        else:
            self.phi_tags = ConfigLoader.load_phi_config()

        # Normalize tag-key casing HERE, once, at the boundary between
        # "however phi_tags got built" (a built-in PRIVACY_PROFILES entry,
        # a user's own YAML, an external custom-profile file, or the
        # floor policy) and "how it's looked up"
        # (`_scan_instance`'s `self.phi_tags.get(tag)` below). Every
        # ingested attribute key on the object graph is lowercased
        # (`io_handlers.py`'s `populate_attrs`,
        # `f"{elem.tag.group:04x},{elem.tag.element:04x}"`), so a config
        # source that spells a tag with an uppercase hex letter (A-F) --
        # as `isocenter/profiles.py`'s Basic profile did for Series
        # Description, `0008,103E` -- would otherwise never match and
        # silently disable a declared policy with no error anywhere.
        # Normalizing at this single choke point, rather than fixing the
        # one known offending key, means this class of bug cannot recur
        # regardless of which future profile or config file introduces it.
        self.phi_tags = self._normalize_tag_keys(self.phi_tags)
        self._warn_on_bare_action_values()

    # Action names a caller is most likely to write where a description
    # belongs, having read `Dict[str, str]` and reasonably concluded the
    # string chooses the behaviour. `REPLACE` is deliberately absent: it
    # is what the string form already does, so there is no gap between
    # what was asked and what happens, and the warning would only advise
    # writing a rule to obtain the behaviour already in effect.
    _ACTION_WORDS = frozenset({"REMOVE", "EMPTY", "SHIFT", "JITTER"})

    def _warn_on_bare_action_values(self) -> None:
        """Report tags whose value names an action rather than the tag.

        A string value is the tag's display name and always leaves the
        action as `REPLACE`, so `{"0008,0020": "SHIFT"}` replaces the date
        with `ANONYMIZED` instead of shifting it -- destroying the
        interval information shifting exists to preserve, with nothing
        raised and nothing logged.

        Warned rather than raised: the string form works as designed, and
        rejecting a call that succeeds today would be a breaking change.
        A caller may also legitimately have a tag *described* as "Shift".
        """
        offenders = sorted(
            tag for tag, val in self.phi_tags.items()
            if isinstance(val, str) and val.strip().upper() in self._ACTION_WORDS)
        if not offenders:
            return

        for tag in offenders:
            get_logger().warning(
                "config_tags[%r] is %r, which is read as the tag's display "
                "name, not its action -- the action stays REPLACE. To %s "
                "this tag, write {'action': %r, 'name': ...}.",
                tag, self.phi_tags[tag], self.phi_tags[tag].strip().lower(),
                self.phi_tags[tag].strip().upper())

    @staticmethod
    def _normalize_tag_keys(phi_tags: Dict[str, Any]) -> Dict[str, Any]:
        """Lowercase every string PHI-tag key so it matches the
        lowercased 'gggg,eeee' keys the object graph actually uses.

        Non-string keys (defensive only -- config_tags is caller-supplied
        and its shape isn't strictly validated elsewhere) pass through
        unchanged.
        """
        if not phi_tags:
            return phi_tags

        normalized: Dict[str, Any] = {}
        for key, value in phi_tags.items():
            norm_key = key.lower() if isinstance(key, str) else key
            if norm_key in normalized:
                # Two source keys collided after lowercasing (e.g. both
                # "0008,103E" and "0008,103e" were present). Last one
                # wins, matching ordinary dict.update()/dict-literal
                # overwrite semantics -- but log it, since a silent
                # overwrite here is exactly the kind of thing this
                # normalization exists to make loud.
                get_logger().warning(
                    "PHI tag key collision after case normalization: "
                    "%r overwrites the earlier entry for %r.", key, norm_key)
            normalized[norm_key] = value
        return normalized

    def scan_patient(self, patient: Patient) -> List[PhiFinding]:
        """
        Recursively scans a Patient and their child studies for PHI.

        Args:
            patient (Patient): The patient object to scan.

        Returns:
            List[PhiFinding]: A list of all identified PHI findings.
        """
        findings = []

        # 1. Direct Attributes
        if (patient.patient_name and patient.patient_name != "Unknown"
                and not _is_replacement_name(patient.patient_name)):
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
                tag="0010,0010",
                patient_id=patient.patient_id,
                entity=patient,
                remediation_proposal=proposal
            ))

        if (patient.patient_id and patient.patient_id != "UNKNOWN"
                and not _is_replacement_id(patient.patient_id)):
            # Simple deterministic anonymization proposal for now (can be refined in Service)
            # The Service will handle the hash calculation if 'new_value' is a
            # placeholder or if logic dictates
            # Through the constructor, not spelled here: the date jitter
            # reads this value's digest back out as its seed (#517), so
            # the prefix and the digest length have one home.
            hashed_id = _replacement_id_for(patient.patient_id)
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
                tag="0010,0020",
                patient_id=patient.patient_id,
                entity=patient,
                remediation_proposal=proposal
            ))

        # 2. Traverse Children & Scan Instances (Generic Unified Config)
        for study in patient.studies:
            findings.extend(self._scan_study(study, patient.patient_id))

            for series in study.series:
                for instance in series.instances:
                    findings.extend(self._scan_instance(instance, patient.patient_id,
                                                        study=study, patient=patient))

        return findings

    def _scan_instance(self, instance: Instance, patient_id: str,
                       study: Study = None, patient: Patient = None) -> List[PhiFinding]:
        """
        Scans a single instance for PHI based on configured tags and private tag rules.

        Walks the instance structurally to reach every text node,
        including nested sequence items. See the comment below before
        reintroducing an index.

        `patient` and `study` are the instance's owners. With them, a
        top-level copy of a tag they own that already holds their
        replacement is not a finding (#496); without them nothing is
        skipped, and the policy judges every copy.
        """
        findings = []

        # Appended last, and that is the point: these delete a whole
        # sequence, and a configured-tag finding raised *inside* one
        # holds a live reference to an item within it. Remediating the
        # contents before removing the container means every audit row
        # describes an item that was still in the graph when it was
        # written.
        seq_removals = []

        # 0. Determine Scan Targets
        # Walked from the instance itself, not read off a prebuilt index.
        # It was read off `Instance.text_index` until 0.8.0, and that
        # index was built once at ingest: not rebuilt when a session was
        # loaded from the store, and not carried into the worker copies
        # `session.audit()` scans. So every route into this method except
        # a direct call arrived with it empty and took a top-level-only
        # scan, reporting clean on sequence content it never opened
        # (#57). The index itself was retired in #84, once it was clear
        # nothing had read it since.
        #
        # An index would have to be derived here, per scan, to be worth
        # having -- a stored one is a second answer to "where does text
        # live" that can disagree with the graph, which is what #57 was.
        # Note also that walking drops the index's text-VR filter, which
        # the top-level path never applied either: a configured PHI tag
        # is a configured PHI tag wherever it sits and whatever its VR.
        scan_targets = [
            (item, tag, path)
            for item, path in iter_item_tree(instance)
            for tag in list(item.attributes.keys())
        ]

        # 1. Private Tag Removal Logic
        if self.remove_private_tags:
            # Private tags live in odd groups. Remove all of them except
            # two, which are NOT the reversibility service's tags -- a
            # previous version of this comment said they were, and that
            # has been false since v0.5.0 (47278f8). Reversibility uses
            # the Encrypted Attributes Sequence (0400,0510), an *even*
            # group, so it never reaches this sweep and was never at risk
            # from it. See `reversibility.py`.
            #
            # (0099,0010) and (0099,1001) were the encrypted-identity
            # payload in exactly one release -- `gantry` v0.4.1, before
            # both the migration to (0400,0500) and the rename to
            # Isocenter. The exemption exists so `remove_private_tags`
            # does not strip the identities out of a store written by
            # that version, leaving it unrecoverable with its own key.
            #
            # This is one of two halves. `DicomExporter._merge`
            # (io_handlers.py) gives the same two tags explicit VRs on
            # the way out, because they are private and `dictionary_VR`
            # raises for them. Removing either half alone leaves the
            # exporter carefully preserving a tag the sweep now strips.
            WHITELIST_TAGS = {"0099,0010", "0099,1001"}

            for item, tag, path in scan_targets:
                try:
                    group_str, _ = tag.split(',')
                    group = int(group_str, 16)
                    if group % 2 != 0:  # Odd group = Private
                        if tag not in WHITELIST_TAGS:
                            findings.append(PhiFinding(
                                entity_uid=instance.sop_instance_uid,
                                entity_type="Instance",
                                field_name=f"Private Tag {tag}",
                                value="<PRIVATE>",
                                reason="Private Tag Removal Requested",
                                tag=tag,
                                patient_id=patient_id,
                                entity=item,
                                entity_path=path,
                                remediation_proposal=PhiRemediation(
                                    action_type="REMOVE_TAG",
                                    target_attr=tag
                                )
                            ))
                except ValueError:
                    pass  # Malformed tag?

            # A private *sequence* is a private tag. `scan_targets` is
            # built from `attributes` alone, so before #167 a private SQ
            # was swept only when it arrived as an opaque `UN` blob --
            # i.e. only under Implicit VR, and only because the scan
            # could not see it was a sequence. Restoring the structure
            # (#167) removes that accident, so the sweep has to ask the
            # question directly, at every depth, or the default
            # configuration starts exporting private sequences.
            for owner, path in iter_item_tree(instance):
                for seq_tag in list(owner.sequences.keys()):
                    try:
                        if int(seq_tag.split(',')[0], 16) % 2 == 0:
                            continue
                    except ValueError:
                        continue
                    if seq_tag in WHITELIST_TAGS:
                        continue
                    seq_removals.append(PhiFinding(
                        entity_uid=instance.sop_instance_uid,
                        entity_type="Instance",
                        field_name=f"Private Sequence {seq_tag}",
                        value="<PRIVATE>",
                        reason="Private Tag Removal Requested",
                        tag=seq_tag,
                        patient_id=patient_id,
                        entity=owner,
                        entity_path=path,
                        remediation_proposal=PhiRemediation(
                            action_type="REMOVE_TAG",
                            target_attr=seq_tag)))

            # Deepest first, for the same reason the whole block is
            # appended last: a private sequence can hold another one,
            # and `iter_item_tree` yields the container before the thing
            # inside it. In that order remediation deletes the outer
            # sequence, and the inner finding -- whose `entity` was
            # resolved before either ran -- then deletes from a dict
            # that is no longer reachable from the instance and files a
            # `REMEDIATION_REMOVE` row for it. The export is right
            # either way; the audit trail is not, and "every row
            # describes an item that was still in the graph" is the
            # claim this ordering exists to keep. Stable, so sequences
            # at equal depth keep the walk's order (#167).
            seq_removals.sort(key=lambda f: len(f.entity_path),
                              reverse=True)

        # 2. Configured PHI Tags
        if not self.phi_tags:
            return findings + seq_removals

        for item, tag, path in scan_targets:
            # Parse config
            config_val = self.phi_tags.get(tag)
            if not config_val:
                continue

            if isinstance(config_val, dict):
                description = config_val.get("name", "Unknown Tag")
                action_code = config_val.get("action", "REPLACE").upper()
            else:
                description = str(config_val)
                action_code = "REPLACE"

            # Check if tag exists in item items
            val = item.attributes.get(tag)

            if val is None:
                continue

            # The owner's replacement is not PHI (#496). Since #492 an
            # owner's remediation writes its value onto every instance's
            # top-level copy, and without this a re-audit raised the
            # instance's own rule against it: the instance went
            # IDENTIFIED, the manifest read false, and a second
            # `anonymize()` wrote a second value over the owner's.
            # Top-level only, because that is as far as the owner's write
            # and the exporter's stamp reach. Not for REMOVE, which the
            # remediation side exempts from the fold for the same reason:
            # removing a copy writes no second value, and it is the
            # policy's explicit request.
            if (action_code != "REMOVE" and not path
                    and self._holds_owners_replacement(tag, val, patient, study)):
                continue

            # Determine if remediation is needed
            needs_remediation = False
            remediation_action = "REPLACE_TAG"
            new_val = None

            if action_code == "REMOVE":
                # If user wants it gone, and it exists (val is not None), finding!
                needs_remediation = True
                remediation_action = "REMOVE_TAG"
            elif action_code == "EMPTY":
                if val != "":
                    needs_remediation = True
                    remediation_action = "REPLACE_TAG"
                    new_val = ""
            elif action_code in ["SHIFT", "JITTER"]:
                # Date shifting, decided **per value** (#510, #513).
                #
                # This used to read `instance.date_shifted` and, failing
                # that, `study.date_shifted`. Neither flag speaks for a
                # value: each says a shift landed somewhere on an entity,
                # so the shortcut over-suppressed a valid date the
                # pipeline never touched (a rule first named in pass 2,
                # or a value left out of `anonymize(findings=[...])`, was
                # skipped forever while the instance read CLEARED --
                # #510) and under-suppressed a date inside a sequence (no
                # flag exists on a `DicomItem`, so a nested date was
                # re-shifted on every pass with a
                # `REMEDIATION_SHIFT_DATE` row each time -- #513).
                # Neither flag is read here any more; a second reading of
                # them beside the record would be a second answer, and
                # the legacy branch below already carries the only
                # persisted evidence that a shift ever ran.
                if item.date_shift_vouches_for(tag, val):
                    # This item shifted this tag to this value, and the
                    # tag still holds it. Shifting again would move it
                    # twice.
                    needs_remediation = False
                elif not str(val).strip():
                    # A blank value is not a `SHIFT`/`JITTER` finding at
                    # all. `val` cannot be `None` here -- the walk above
                    # skips a tag the item does not hold -- so this tests
                    # blank, not absent. `EMPTY` already tests `val != ""` and
                    # `REPLACE` tests `val != "ANONYMIZED" and val !=
                    # ""`, so three of the four value-writing actions
                    # skip blank; and the arm's own reasoning is that an
                    # empty value is not retained PHI -- there is nothing
                    # to shift and nothing left behind, which is why it
                    # is the one non-success path that writes no decline
                    # row. Spelled as the arm spells it
                    # (`str(...).strip()`), so a multi-valued element is
                    # not mistaken for a blank one. Before the record
                    # this asymmetry was invisible, because the flag
                    # suppressed every pass after the first; with a
                    # per-value record every pass looks like pass 1, so a
                    # blank would otherwise raise a finding the arm
                    # declines to act on for ever and #491's pass-end
                    # demotion would leave a clean instance IDENTIFIED.
                    # This branch is *above* the legacy one, so
                    # `_date_shift_declines`' own blank guard is no
                    # longer reachable from here; it is kept because the
                    # predicate is also read directly and must answer
                    # the same way alone as it does in place.
                    needs_remediation = False
                elif getattr(instance, "_legacy_shift_provenance", False):
                    # A pre-0.9.6 store has no per-value records for the
                    # dates it already holds, so this instance keeps the
                    # entity-level rule for them, permanently: reading
                    # "no record" as "not shifted" here would shift every
                    # already-shifted date in the archive a second time.
                    # The load says so once, as a WARNING audit row.
                    #
                    # "The entity-level rule" is #498's version of it,
                    # not the older one: a value the arm could not parse
                    # is raised again so its decline recurs and the
                    # pass-end demotion keeps the instance IDENTIFIED,
                    # while a value the shift could apply to is skipped
                    # because it has moved once already. The import is
                    # local because remediation imports this module.
                    #
                    # This whole branch dies when no pre-0.9.6 store
                    # remains.
                    from .remediation import (  # pylint: disable=import-outside-toplevel
                        _date_shift_declines)
                    needs_remediation = _date_shift_declines(val)
                else:
                    needs_remediation = True
                if needs_remediation:
                    remediation_action = "SHIFT_DATE"
            elif action_code == "KEEP":
                needs_remediation = False
            else:  # REPLACE (Default)
                if val != "ANONYMIZED" and val != "":
                    needs_remediation = True
                    remediation_action = "REPLACE_TAG"
                    new_val = "ANONYMIZED"

            if needs_remediation:
                proposal = PhiRemediation(
                    action_type=remediation_action,
                    target_attr=tag,
                    new_value=new_val,
                    original_value=val,
                    metadata={
                        "patient_id": patient_id} if remediation_action == "SHIFT_DATE" else {})

                findings.append(PhiFinding(
                    entity_uid=instance.sop_instance_uid,
                    entity_type="Instance",
                    field_name=f"{description} (Deep)" if item != instance else description,
                    value=val,
                    reason=f"Matched PHI Tag {tag} ({description})",
                    tag=tag,
                    patient_id=patient_id,
                    entity=item,  # Point to the specific deep item!
                    entity_path=path,
                    remediation_proposal=proposal
                ))
        return findings + seq_removals

    @staticmethod
    def _holds_owners_replacement(tag: str, value: Any, patient: Patient,
                                  study: Study) -> bool:
        """Whether `value`, an instance's top-level copy of `tag`, is the
        replacement its owner already holds (#496).

        Agreement alone is not enough: before anything is anonymized every
        copy equals its owner's *original*, and that is PHI. The owner's
        value has to be a replacement by the scan's own test --
        `_is_replacement_name` / `_is_replacement_id`, the ones
        `scan_patient` stops raising on -- or, for the date, a study date
        this pipeline's shift produced (`_study_date_is_this_pipelines`).
        No owner, no skip.

        The date arm read `study.date_shifted` until 0.9.6, and that flag
        cannot tell the shift's own output from a fresh original assigned
        over it -- so an instance's copy of a hand-replaced study date
        skipped here as "the owner's replacement", which is #518 reached
        through #496's door.
        """
        if tag == "0008,0020":
            if study is None:
                return False
            # Lazy: io_handlers is the heavy module, and this is the one
            # spelling of "a Study's date as a DA string" (#189) -- the
            # spelling the owner's write put on the copy.
            from .io_handlers import format_study_date
            return (_study_date_is_this_pipelines(study)
                    and value == format_study_date(study.study_date))
        if patient is None:
            return False
        if tag == "0010,0010":
            return (value == patient.patient_name
                    and _is_replacement_name(value))
        if tag == "0010,0020":
            return (value == patient.patient_id
                    and _is_replacement_id(value))
        # No StudyTime (0008,0030) arm, though `ENTITY_FIELD_TAGS` carries
        # one. The skip needs a value *known* to be a replacement, and a
        # time has no such test: no `date_shifted` flag, no `ANON_`
        # prefix, and no shipped scan remediates `Study.study_time`. An
        # arm could only skip on agreement, and agreement with an original
        # is PHI -- the case this function exists to refuse.
        return False

    def _scan_study(self, study: Study, patient_id: str = None) -> List[PhiFinding]:
        """
        Scans a Study entity for study-level PHI (e.g. StudyDate).
        """
        findings = []
        uid = study.study_instance_uid

        # A date this pipeline's own shift produced is not raised again;
        # anything else under `study_date` is (#518).
        #
        # This read `study.date_shifted` and returned no findings at all
        # while it was set. The flag records *that* a shift happened,
        # never *what it produced*, so it could not tell its own output
        # from a new input: shift a study's date, assign a fresh
        # original to `study.study_date`, re-audit, and the real date was
        # never raised again and was exported. Measured on `927cb2b`:
        # `2023-01-01` -> `2022-03-21`, then `study.study_date =
        # date(2024, 7, 4)`, then `raised=0` with `date_shifted=True`.
        if _study_date_is_this_pipelines(study):
            return findings

        if study.study_date:
            proposal = PhiRemediation(
                action_type="SHIFT_DATE",  # Special action for the Service to handle
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
                tag="0008,0020",
                patient_id=patient_id,
                entity=study,
                remediation_proposal=proposal
            ))

        return findings
