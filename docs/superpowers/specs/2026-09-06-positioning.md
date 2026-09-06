# Isocenter positioning (#350)

**Status:** proposed; adopted when the pull request carrying it merges, which is the review gate
**Base:** `origin/main` @ `e244c0a` ("docs: the TestPyPI rehearsal becomes a release step")
**Issue:** #350 (the promotion kit). Adjacent: #348 (relicense to Apache-2.0, PR #349), #29 (no CLI, closed as not planned), #153 (report after export), #57/#84 (nested-sequence scan), #36 (multi-group waveform loss is audited).
**Superseded in part:** section 9's "paid support" clause, the same day it was written; see the struck text there.
**Scope:** internal. Excluded from the docs site by `exclude_docs`, and the source every public artifact in #350 derives from. A dated record: when a later change falsifies a clause, add a `**Superseded in part:**` line here and strike the clause in place rather than rewriting it.

This document exists so that the README, the docs landing page, the PyPI
page, an announcement post, and a one-page brief for a compliance officer
all say the same thing. It fixes four things: who the material is for and
in what order, the claims we make everywhere, the claims we do not make
until we can back them, and the voice. Everything else in the promotion
kit is a rendering of this page for one audience.

## 1. What Isocenter is

Isocenter is a Python library for indexing, de-identifying, and exporting
DICOM datasets at cohort scale. It never modifies the source files. It
builds a SQLite metadata index and a binary pixel/waveform sidecar over a
read-only source tree, applies de-identification profiles and
machine-specific pixel redaction to an in-memory object graph, grades its
own output, and writes clean copies to a new directory as DICOM or
PhysioNet WFDB.

There is no command-line tool and none is planned (#29). The Python API
is the whole interface. That is a positioning fact, not an omission: the
people who de-identify cohorts write scripts, and a library that lives in
their script can be resumed, inspected, and audited in ways a batch
command cannot.

## 2. Who it is for, in priority order

The order is a priority order. When two audiences want different things
from the same page, the earlier one wins.

1. **Research data engineers and imaging core labs.** They de-identify
   cohorts for a study or for multi-site sharing, under a protocol an IRB
   approved. They read documentation, run the quickstart, and judge by
   whether the audit trail would survive a question from compliance. What
   they need to see: the pipeline, the report, and what the library
   refuses to do silently.
2. **The pydicom and Python medical-imaging community.** They find the
   project through PyPI, GitHub, Show HN, or a conference lightning talk,
   and judge by the API and the README in under a minute. What they need
   to see: an object model over pydicom, a persistent session, and a
   licence they can import.
3. **Institutional decision makers**: compliance officers, IRB staff,
   imaging-informatics leads. They never run code. What they need to see:
   the licence, the DOI, what the compliance report contains, and a plain
   statement that de-identification is protocol-conformant and the
   institution's determination, not the library's.
4. **Cardiology and ECG researchers**, reached through PhysioNet and
   through Murmur Studio's users. What they need to see: DICOM waveform
   IODs in, WFDB records out, with the annotation bridge.

## 3. The claims we make everywhere

Each claim names the evidence in the repository that backs it. A claim
whose evidence is deleted is deleted with it.

### Claim 1. It never modifies source files.

The source tree is read-only to Isocenter by construction. Ingest builds
an index and a sidecar beside it; anonymize and redact mutate an
in-memory graph; nothing reaches disk until `export()` writes copies to a
directory you name. A crashed or abandoned run leaves the originals as
they were.

Evidence: the architecture in `CLAUDE.md` and `docs/architecture.md`;
`tests/test_export_atomic_write.py` and `tests/test_safe_export.py` for
the write path; the absence of any write to the ingest path anywhere in
`io_handlers.py`.

### Claim 2. It grades its own output and records what it could not do.

Every step that can lose data writes an audit row, and the compliance
report reads those rows. A cohort that lost a file, a private tag, a
waveform group, or a pixel frame cannot grade `PASS`; it grades
`REVIEW_REQUIRED` and names what was lost. An export that wrote nothing
raises rather than returning. A report generated before export says so
in its own text.

Evidence: `reporting.py` (grades are `PENDING`, `PASS`, `REVIEW_REQUIRED`;
there is deliberately no `FAIL`); #153 (report after export); #191
(`ExportError`); #181 and #166 (export boundary rows); the
`DATA_LOSS` audit rows written by ingest (#36) and export;
`tests/test_export_failure_audit.py`, `tests/test_export_loss_audit.py`,
`tests/test_data_loss_reporting.py`, `tests/test_worker_loss_is_reported.py`.

### Claim 3. It is tested where it says it runs, including inside sequences.

Every pull request runs the suite on Python 3.12 and on the free-threaded
3.14t build, and a release runs all four supported versions. The
classifiers on PyPI list only the versions CI runs, and a test fails if
the matrix is narrowed without removing the classifier. PHI detection
walks nested sequences structurally rather than reading a cached index,
because the cached index went stale on every real path once (#57, #84).

Evidence: `.github/workflows/tests.yml` and `publish.yml`;
`tests/test_packaging_contract.py`; `tests/test_nested_phi_audit.py`;
the `Programming Language :: Python :: 3.14` and free-threading
classifiers in `setup.py` with their comments.

### Supporting claims, stated where they fit

These are true and evidenced but are not the headline. They appear on
the page whose audience they serve.

- **De-identification is protocol-conformant, not maximal.** The profile
  decides what goes; a field the protocol permits stays. Optional
  reversible anonymization keeps the original identities under a Fernet
  key, and the export discloses when recoverable identities are present.
  (`profiles.py`, `crypto.py`, `reversibility.py`,
  `tests/test_reversibility.py`, `tests/test_check_reversibility.py`.)
- **Pixel redaction is machine-specific**, by manufacturer and model, with
  optional OCR to find burned-in text, and it reads CTP
  `DicomPixelAnonymizer.script` files so an existing rule set carries
  over. (`isocenter/utils/ctp_parser.py`, `pixel_analysis.py`,
  `tests/test_redaction_*.py`.)
- **The session is persistent and resumable.** Ten thousand instances load
  from the index without ten thousand joins, a background thread drains
  saves, and `compact()` reclaims sidecar space. (`persistence.py`,
  `sidecar.py`, `persistence_manager.py`.)
- **Waveforms go in as DICOM and out as WFDB**, with a
  `<record>.annotations.json` bridge to Murmur Studio, and multi-group
  records that cannot yet be represented are warned about and audited
  rather than silently truncated (#36). (`exporters/wfdb.py`, `murmur.py`,
  `tests/test_wfdb_conformance.py`, `tests/test_waveform_dicom_roundtrip.py`.)
- **Free-threaded Python takes the threads path.** `run_parallel()` uses
  threads instead of processes when there is no GIL to escape, and 3.14t
  is a blocking check on every pull request. (`parallel.py`,
  `tests/test_parallel_contract.py`.)
- **Licence: Apache-2.0** from the release after 0.9.2 (#348). Releases
  through 0.9.2 remain AGPL-3.0-or-later. A concept DOI on Zenodo and a
  `CITATION.cff` make it citable as a methods reference.

## 4. The claims we do not make

The rule: **a number needs a reproducible run behind it, a guarantee
needs a test behind it, and an adoption claim needs something to
count.** The following appear in the current materials and are removed
in the rewrite, each with the reason.

| Current wording | Where | Why it goes |
| --- | --- | --- |
| "Guarantees zero memory leaks" | README | No test asserts it. Process isolation makes worker memory reclaimable; that is the honest sentence. |
| "Enterprise-Grade Scalability" | README | A label, not a claim. Nothing would falsify it. |
| "Validated sub-linear memory scaling on 100GB+ datasets" | README | The published run is 100 multi-frame files, about 50GB raw, on an `n2-highmem-16`, January 2026. Say that. |
| "101,000 files (50GB Single-Frame + 50GB Multi-Frame)" | `docs/performance.md` | The table on the same page shows 100 files. No 101,000-file run is recorded anywhere. |
| "O(1) memory streaming, ensuring it never runs out of RAM even when processing terabytes" | `docs/performance.md` | No terabyte run exists; the runbook's 412GB three-phase design is a plan, not a result. Peak RSS grew 0.5GB to 11.3GB across the run we have. |
| "Generate a signed Compliance Report" | README | The report carries a blank Data Protection Officer signature line. It is not cryptographically signed. Say "a compliance report with a signature block". |
| Report as checkpoint 9, Export as checkpoint 10 | README | Wrong order. Export writes the `DATA_LOSS` rows the report grades; #153 fixed the code and the README was not updated. |
| "Fully compatible with Python 3.13t+" | `docs/index.md` | CI runs 3.14t only. 3.13t is untested and unclaimed. |
| "HIPAA compliant" or "guarantees compliance" | Anywhere | Compliance is the covered entity's determination against its protocol. Isocenter supports a de-identification workflow and produces the record a reviewer needs. That sentence is the ceiling. |
| Any adoption, user, site, or community count | Anywhere | Zero stars and two forks today. Nothing to count; nothing claimed. |
| "This ensures 100% PHI safety" | README, codec section | Strict codec validation refuses to pass through pixels it could not decode. That is a refusal, and a good one; it is not a percentage. |
| "Compliance & Certification" | README, section heading | Nothing is certified by anyone. The section describes a report. Call it "The compliance report". |
| "audit-ready Markdown reports for HIPAA/GDPR documentation" | README | The report is evidence a reviewer reads under whatever regime applies. Naming regimes implies conformance was assessed against them, and it was not. |

Two related rules. **Benchmark numbers carry their machine and date**
wherever they appear, and live on the performance page rather than the
README. **"Validated", "verified", and "guaranteed" are reserved** for
statements a named test or a recorded run backs.

## 5. Voice

- **Problem first.** Open with the situation the reader is in, then what
  the library does about it. The README's first sentence should be
  about a cohort and an IRB, not about an object model.
- **Say what it refuses to do.** The strongest sentences in this project
  are the refusals: it will not touch your source files, it will not
  grade a lossy export `PASS`, it will not write a nested tag onto the
  instance as a decoy. Lead with those.
- **Specific over superlative.** "High-performance", "robust",
  "enterprise-grade", and "massive" are cut on sight. A number with its
  provenance, or a named behaviour, replaces each one.
- **The CHANGELOG register.** Breaking entries there state the exact
  exception a previously-working call now raises and why the old
  behaviour was wrong. Public prose matches that depth of honesty at a
  shorter length.
- **No exclamation marks, no emoji, no "simply", no "just".**
- **"License", not "licence", in new public material**, for the US
  audience. The existing British spellings in test docstrings and the
  developer guide are left alone; a spelling sweep is not a deliverable.

## 6. What we are not

A reader will compare Isocenter with what they already use. Name the
neighbours by their own stated shape so the comparison is fair and
survives their next release. Do not count features.

- **RSNA CTP (Clinical Trial Processor).** A Java pipeline application
  configured by scripts, and the incumbent in imaging research. Isocenter
  is a library rather than an application, and reads CTP's
  `DicomPixelAnonymizer.script` so a site's pixel rules carry over.
- **`deid` (pydicom organisation).** A Python library driven by recipe
  files, from the same ecosystem Isocenter builds on. Isocenter differs
  in owning the cohort as a persistent, resumable session with an audit
  log and a graded report, rather than processing files one at a time.
- **`dicognito`.** A Python library and CLI that anonymizes metadata
  consistently across a set of files. It does not address pixel data or
  waveforms, which are half of Isocenter's surface.
- **DicomCleaner (PixelMed)** and **Orthanc's anonymization**. A desktop
  application and a server, respectively. Different shape; not
  competitors for a script.
- **Cloud de-identification services** (Google Cloud Healthcare API,
  Microsoft Presidio's image redactor). Hosted or ML-driven. Isocenter
  runs where the data is and its decisions are a configuration a human
  reads, which is what a protocol review needs.

The sentence that follows from this list: Isocenter is the one that
treats the cohort, not the file, as the unit of work, and that writes
down what it could not do.

## 7. One line per audience

1. Core labs: *De-identify a cohort under your protocol, keep the
   originals untouched, and hand compliance a report that names anything
   the run lost.*
2. Python community: *An object model over pydicom with a persistent
   session, nested-sequence PHI detection, pixel redaction, and WFDB
   export, under Apache-2.0, tested on free-threaded 3.14t.*
3. Institutions: *A citable, permissively licensed library whose output
   is a report your reviewer can read, and whose de-identification is
   whatever your protocol says it is.*
4. ECG researchers: *DICOM waveforms in, PhysioNet WFDB records out, with
   annotations that open in Murmur Studio.*

## 8. Artifact map

| Artifact | Audience | Lives at | Carries |
| --- | --- | --- | --- |
| README | 2, then 1 | `README.md`, rendered on PyPI and GitHub | Claims 1 to 3, the pipeline, the quickstart, licence and citation |
| Docs landing page | 1 | `docs/index.md` | Claims 1 to 3 in problem-first form; not a copy of the README |
| Institutional brief | 3 | `docs/for-institutions.md` | Licence, DOI, report contents, the compliance sentence from section 4 |
| Performance page | 1, 2 | `docs/performance.md` | The one recorded run, with machine and date; the architecture that produced it |
| Waveform page | 4 | `docs/waveforms.md` | The WFDB path and its documented limits |
| Announcement post | 2 | `docs/superpowers/specs/2026-09-06-announcement-draft.md` until 1.0 tags | Claim 3 leads; the refusals; the licence |

**Contact.** Every public page that offers a contact offers
`support@isocenter.net`, and nothing else. The docs site footer currently
carries a literal `mailto:[EMAIL_ADDRESS]` placeholder in `mkdocs.yml`;
the docs landing page PR replaces it.

## 9. What this document does not decide

- Whether to seek a JOSS or similar software paper. A paper is a
  citation channel for audience 1 and 4; it is out of scope until 1.0.
- The 1.0 date. The zero-bug-bounce process decides that.
- ~~Paid support. Not planned near term; a permissive licence keeps it
  possible. Nothing public mentions it until it exists.~~ **Superseded
  the same day (#350, later PR):** paid consulting is offered from the
  start. Bug reports and questions about documented behaviour are free
  and public on GitHub Issues; help beyond that (protocol configuration,
  pipeline integration, report review, features a study needs) is paid
  consulting via `support@isocenter.net`. Every public contact statement
  says so.
