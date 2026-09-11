
"""
Standard Privacy Profiles for Isocenter.

This module defines built-in privacy profiles that can be referenced in the
Isocenter configuration file using the "privacy_profile" key. These profiles
provide a baseline set of PHI actions (e.g. REMOVE, EMPTY) which can be
overridden by the user's specific "phi_tags" configuration.
"""

# Based on DICOM PS3.15 Annex E (Basic Profile) - Reduced for common usage
#
# NOTE: keys must be lowercase 'gggg,eeee'. Ingested attribute keys on the
# object graph are always lowercased (isocenter/io_handlers.py's populate_attrs),
# and PhiInspector.__init__ (isocenter/privacy.py) now normalizes any phi_tags
# dict to lowercase keys as a defensive backstop -- but do not rely on that
# backstop when adding new entries here; write them lowercase directly so
# a mismatch never has a chance to silently disable a tag. (0008,103E,
# Series Description, shipped uppercase for a while and was never
# remediated on any documented path as a result.)
BASIC_PROFILE = {
    # Patient Identity
    "0010,0010": {"action": "REMOVE", "name": "Patient's Name"},
    "0010,0020": {"action": "REMOVE", "name": "Patient ID"},
    "0010,0030": {"action": "REMOVE", "name": "Patient's Birth Date"},
    "0010,0032": {"action": "REMOVE", "name": "Patient's Birth Time"},
    "0010,0040": {"action": "REMOVE", "name": "Patient's Sex"},
    "0010,1000": {"action": "REMOVE", "name": "Other Patient IDs"},
    "0010,1001": {"action": "REMOVE", "name": "Other Patient Names"},
    "0010,1040": {"action": "REMOVE", "name": "Patient's Address"},
    "0010,2160": {"action": "REMOVE", "name": "Ethnic Group"},
    "0010,4000": {"action": "REMOVE", "name": "Patient Comments"},

    # Study / Series Information
    "0008,0020": {"action": "REMOVE", "name": "Study Date"},
    "0008,0021": {"action": "REMOVE", "name": "Series Date"},
    "0008,0022": {"action": "REMOVE", "name": "Acquisition Date"},
    "0008,0023": {"action": "REMOVE", "name": "Content Date"},
    # DT-valued twins of the dates above. (0008,002A) carries the same
    # information as (0008,0022) Acquisition Date; omitting it meant raw
    # acquisition timing survived a full anonymize() pass while the plain
    # date was stripped.
    "0008,002a": {"action": "REMOVE", "name": "Acquisition DateTime"},
    # EMPTY rather than REMOVE: PS3.15 Table E.1-1 says Z, and Study Time
    # is Type 2 in General Study (PS3.3 C.7.2.1), so a CT export with the
    # element absent fails validation. REMOVE here plus a validator that
    # called it Type 1 meant the documented Quick Start exported nothing
    # on any CT file (#495).
    "0008,0030": {"action": "EMPTY", "name": "Study Time"},
    "0008,0031": {"action": "REMOVE", "name": "Series Time"},
    "0008,0032": {"action": "REMOVE", "name": "Acquisition Time"},
    "0008,0033": {"action": "REMOVE", "name": "Content Time"},
    # Procedure step timing: same shape of leak as the acquisition dates.
    "0040,0244": {"action": "REMOVE", "name": "Performed Procedure Step Start Date"},
    "0040,0245": {"action": "REMOVE", "name": "Performed Procedure Step Start Time"},
    "0040,0250": {"action": "REMOVE", "name": "Performed Procedure Step End Date"},
    "0040,0251": {"action": "REMOVE", "name": "Performed Procedure Step End Time"},
    "0008,0050": {"action": "REMOVE", "name": "Accession Number"},
    "0008,0090": {"action": "REMOVE", "name": "Referring Physician's Name"},
    # Often contains PHI, but structural
    "0008,1030": {"action": "EMPTY", "name": "Study Description"},
    "0008,103e": {"action": "EMPTY", "name": "Series Description"},
    "0020,0010": {"action": "REMOVE", "name": "Study ID"},
    "0008,0080": {"action": "REMOVE", "name": "Institution Name"},
    # PS3.15 Table E.1-1: X. Absent until #495, so CT_small's `CT01_OC0`
    # survived even the documented path.
    "0008,1010": {"action": "REMOVE", "name": "Station Name"},
    "0008,0081": {"action": "REMOVE", "name": "Institution Address"},
    "0008,1040": {"action": "REMOVE", "name": "Institutional Department Name"},
    "0008,1050": {"action": "REMOVE", "name": "Performing Physician's Name"},
    "0008,1070": {"action": "REMOVE", "name": "Operators' Name"},
    # Free-text annotation commentary. Reaches annotations.json `note` when
    # a caller opts in via include_annotation_text; remediated here so that
    # opting in on a configured session still does not surface raw text.
    "0070,0006": {"action": "EMPTY", "name": "Unformatted Text Value"},
}

# The three entries where a research export deliberately departs from the
# basic profile: the study date is jittered so intervals survive, and sex
# and age are kept because analyses stratify on them. `create_config()`
# writes exactly these beneath `privacy_profile: basic` -- it diffs the
# floor against `BASIC_PROFILE`, so the scaffold cannot drift from this
# table -- and they are half of what a bare session applies. Keys
# lowercase, for the reason the header comment above gives.
RESEARCH_DEFAULTS = {
    "0008,0020": {"action": "JITTER", "name": "Study Date"},
    "0010,0040": {"action": "KEEP", "name": "Patient's Sex"},
    "0010,1010": {"action": "KEEP", "name": "Patient's Age"},
}

# What a session applies when it has loaded no configuration, and what a
# config file with no `privacy_profile` line extends (#495). Until then a
# bare session scanned against `{}`: it remediated patient name, ID and
# study date through the hardcoded entity checks and left Study ID,
# Institution Name, Station Name and every series/acquisition/content
# date in the exported file, graded PASS.
#
# Not a named profile. `PRIVACY_PROFILES` is what a config can spell; the
# floor is what spelling nothing means, so it has no name to collide with
# a user's. Callers that hand it out copy it (`copy.deepcopy`) -- a
# session's `set_phi_tag` writes into its own `phi_tags`, and a shared
# dict would carry that edit into every later session. The dict is built
# from copies of each entry for the same reason: `BASIC_PROFILE` is what
# `privacy_profile: basic` reads.
FLOOR_POLICY = {tag: dict(rule)
                for tag, rule in {**BASIC_PROFILE, **RESEARCH_DEFAULTS}.items()}

PRIVACY_PROFILES = {
    "basic": BASIC_PROFILE
}
