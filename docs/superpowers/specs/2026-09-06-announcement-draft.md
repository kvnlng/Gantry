# Announcement draft (#350)

**Status:** held until the 1.0 tag. Not published anywhere.
**Base:** the positioning document, `2026-09-06-positioning.md`.
**Audience:** 2 (the pydicom and Python medical-imaging community), with audience 1 reading over their shoulder.
**Before posting:** replace every `1.0` with the tagged version, re-check the three numbers against the release, and re-read section 4 of the positioning document. Nothing here may claim a run, a test, or a user that does not exist on the day it is posted.

---

## Show HN

**Title** (80 characters is the limit; both fit):

> Show HN: Isocenter – de-identify DICOM cohorts without touching the source files

or

> Show HN: Isocenter – a Python library that grades its own DICOM de-identification

**Body:**

I maintain a Python library for de-identifying DICOM imaging cohorts for research, and it just reached 1.0.

The problem it solves: you have a few thousand studies, a protocol an IRB approved, and a collaborator waiting. You need to remove what the protocol says to remove, keep what it permits, scrub identifiers that equipment burned into the pixels, and hand your compliance office a record of what happened. Most tools in this space are either a desktop application, a Java pipeline (CTP), or a script that processes one file at a time and forgets.

Isocenter treats the cohort as the unit of work. It builds a SQLite index and a pixel sidecar beside a read-only source tree, applies your profile and redaction rules to an in-memory object graph, and writes clean copies to a new directory. Three things it refuses to do, which are the reasons it exists:

- It never modifies a source file. A crashed run leaves the originals as they were.
- It never grades a lossy export PASS. Every step that can lose data writes an audit row; a cohort that lost a file, a tag, a frame, or a waveform group grades REVIEW_REQUIRED and the report names the loss. An export that wrote nothing raises.
- It never claims a Python version it does not test. Every PR runs on 3.12 and on free-threaded 3.14t, where the parallel dispatcher uses threads because there is no GIL to escape.

Other things: PHI detection walks nested sequences structurally (the cached-index version went stale silently, which was a bug worth a changelog entry). Pixel redaction is keyed by device, with an OCR pass to find where text lands, and it imports CTP's pixel scripts. Optional reversible anonymization under a Fernet key, with the export disclosing when recoverable identities are present. DICOM waveforms in, PhysioNet WFDB out.

Apache-2.0. Python API only, no CLI; the people who do this write scripts. Docs: https://kvnlng.github.io/Isocenter/ Source: https://github.com/kvnlng/Isocenter

I would especially like to hear from anyone running CTP or pydicom's deid at a site: what does your compliance reviewer actually ask for, and does the report answer it?

---

## Longer version (docs site post, or r/Python)

### Isocenter 1.0: de-identify a DICOM cohort and get a report that says what went wrong

If you prepare imaging data for research, the de-identification step has a shape that tools rarely match. It is not "anonymize this file". It is: here is a cohort, here is a protocol that says which fields must go and which may stay, here are the machines that burn names into the corner of the image, and here is a reviewer who will ask what you did. The unit of work is the cohort, and the deliverable is two things: the clean copies and the record.

Isocenter is a Python library for that step, and 1.0 is the release where the record became something I would defend in front of a reviewer.

#### What it does

You point a `Session` at a directory. Ingest builds a SQLite index of the metadata and appends pixel and waveform bytes to a sidecar; the source tree is never written to. You examine the cohort, scaffold a configuration from what it contains, edit that configuration to match your protocol, and audit. The audit walks every instance, including nested sequences, and tells you what the run will change before it changes anything.

Then anonymize, redact, export. Anonymize applies the profile in memory. Redact scrubs configured pixel regions, keyed by manufacturer and model, because the same machine in the same room burns identifiers into the same place every time; an OCR pass on a sample of one machine's images will tell you where. Export writes clean copies to a new directory, as DICOM or as PhysioNet WFDB for waveforms, and validates that every compressed frame it wrote can be decoded.

Then the report. It comes last, deliberately, because export is where the final data-loss rows are written and a report generated before export would grade a run that had not finished.

#### What the report says

A manifest of what was processed. Counts of every action. Every exception, listed. A grade: `PASS` or `REVIEW_REQUIRED`. There is no `FAIL`, because a run that lost something is a run a person must look at, and the job of the report is to say what to look at. A cohort that lost a file, a private tag, a pixel frame, or a waveform group cannot grade `PASS`.

That last sentence is the one I care about most. The 0.9 series was three milestones of closing issues whose common shape was "reports success while wrong": a lost record graded PASS, a redaction recorded by configuration hash rather than by pixels, a worker that died and took its findings with it. Each was fixed by making the loss a row in the audit log that the grade reads. The changelog carries the reasoning for every one.

#### What it will not do

- Modify a source file.
- Grade a lossy export `PASS`.
- Copy through pixels it could not decode.
- Advertise a Python version CI does not run. The classifiers list 3.12, 3.13, 3.14, and free-threaded 3.14t; a test fails if the matrix narrows without removing the classifier.
- Claim compliance. Whether a configured profile satisfies a protocol or a regulation is the institution's determination. The report exists to make that determination possible.

#### What it is not

It is not CTP, which is a Java pipeline application; Isocenter reads CTP's pixel-anonymizer scripts so a site's rules carry over. It is not a desktop tool. It is not a service; it runs where the data is and sends nothing anywhere. And it is not a command-line tool: the Python API is the whole interface, because a library that lives in your script can be paused, resumed, inspected, and audited in ways a batch command cannot.

#### License, citation, contact

Apache-2.0 from 1.0. Each release is archived on Zenodo under a concept DOI, and the repository carries a `CITATION.cff`; if Isocenter prepared a dataset, it belongs in the methods section. Issues and questions on GitHub; anything that should not be public to support@isocenter.net.

`pip install isocenter`. The quick start walks the pipeline end to end.

---

## Not for this post

- Any number of users, sites, or stars.
- Any benchmark beyond the one recorded run, and that one only with its machine and date.
- Murmur Studio beyond a single mention as the WFDB annotation consumer. This post is about the library.
