# MIDI-B as a gap-finding run (#356)

**Status:** proposed; adopted when the pull request carrying it merges, which is the review gate. Nothing in it runs before 1.0 tags.
**Base:** `origin/main` @ `e193ea7` ("docs: paid consulting in every contact statement, and inline highlighted code made legible on the API reference")
**Issue:** #356. Adjacent: #357 (`research_tags.json` is shipped and unread, found while writing this), #350 (the promotion kit, whose claims policy this run serves), #235 (SOP UID regeneration on redaction, the only UID rewrite the pipeline does today), #153 (report after export).
**Scope:** internal. Excluded from the docs site by `exclude_docs`. A dated record: when a later change falsifies a clause, add a `**Superseded in part:**` line here and strike the clause in place rather than rewriting it.

This document fixes what the MIDI-B benchmark is used for, what it is
not used for, what the scorer actually measures, what we already know it
will find, how the findings are triaged, and what the public record of
a run must state. It is written before any run so that the run cannot
quietly redefine its own purpose after the number is known.

## 1. Decisions already made

These were settled in conversation on 2026-09-06 and are the constraints
on everything below.

- **It is a gap-finding instrument**, not a gate. The purpose is to find
  what the pipeline cannot express or gets wrong against an external,
  public expectation of what de-identification means.
- **It is not part of CI.** Inputs are 19 GB, the pixel half of the
  scorer is OCR-bound, and the second-place team's run took about two
  days. It lives at the tier of the GCP stress benchmark, off the
  release path.
- **It is not a 1.0 blocker.** 1.0 is a code-stability position. How the
  1.0 release scores is a later question, and the answer does not reopen
  1.0.
- **A score under 100% is a list to read, not a verdict.** The best
  challenge score was 99.93%. TCIA's own curated test set has 29,656
  images against 29,660 synthetic inputs, so the curators themselves
  dropped four. The scorer's manual says outright that many answers "are
  simply marked according to how TCIA handled it when the collection
  ... was originally curated." Judgement calls are not measurable here
  and the record must not pretend they are.
- **The profile is documented, not shipped.** The TCIA-rules
  configuration the run needs lives once, as the benchmark's fixture,
  and the docs page embeds it with `pymdownx.snippets`. It is not a
  resource the package loads by name. A name is API surface and gets
  expensive the day 1.0 tags; a documented file is prose plus a file and
  can be revised with a changelog line.
- **The public record carries the version, commit, Zenodo version DOI,
  date, machine, and scorer commit** it was produced with, on the same
  page as the number. A number without those is not published.

## 2. What MIDI-B is

The Medical Image De-Identification Benchmark, run by NCI and TCIA at
MICCAI 2024. Real de-identified radiology studies from TCIA and IDC had
synthetic PHI planted in headers and pixels; teams de-identified them
and were scored per action against an answer key built from TCIA's
curation rules (HIPAA Safe Harbor, DICOM PS3.15 confidentiality
profiles, and TCIA's practice of keeping research-critical metadata).
Ten teams finished, scoring 97.91% to 99.93%.

Everything needed to reproduce it is public:

| Item | Where | Terms |
| --- | --- | --- |
| Images (synthetic inputs, curated references) and answer keys | TCIA collection, DOI `10.7937/cf2p-aw56` | CC BY 4.0 |
| Scoring script (`run_validation.py`, `run_import.py`, `run_reports.py`, `run_dciodvfy.py`) | `github.com/CBIIT/MIDI_validation_script` | Apache-2.0 |
| Report paper | arXiv 2507.23608 | |
| Second-place team's paper (the most detailed public account of the scoring) | arXiv 2508.07538 | |

| Set | Subjects | Studies | Series | Images | Size | Download |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Synthetic validation (input) | 216 | 241 | 280 | 23,921 | 7.63 GB | NBIA Data Retriever manifest |
| Synthetic test (input) | 322 | 364 | 428 | 29,660 | 11.09 GB | NBIA Data Retriever manifest |
| Curated validation (reference) | 216 | 241 | 280 | 23,921 | 18.45 GB | NBIA Data Retriever manifest |
| Curated test (reference) | 322 | 364 | 428 | 29,656 | 27.91 GB | NBIA Data Retriever manifest |
| Validation answer key | | | | | 2.6 GB zip (SQLite) | Aspera |
| Test answer key | | | | | 3.38 GB zip (SQLite) | Aspera |
| UID and patient ID mapping CSVs (TCIA's own, per set) | | | | | 3 MB, 8 KB | direct |

Modalities: CR, CT, DX, MG, MR, PT, SR, US.

The curated sets are far larger than the synthetic inputs they were made
from (18.45 GB from 7.63 GB). That is the signature of a curation
pipeline that decompressed pixel data on the way through, and it matters
for section 4.

## 3. What the scorer measures

Read from the scorer's source at the commit current on 2026-09-06; the
runbook pins the commit the run used.

**Inputs.** A directory of our output DICOM, one of TCIA's answer-key
SQLite files, and two CSVs *we* produce, each with columns
`id_old,id_new`: a UID mapping (every Study, Series, and SOP Instance
UID, old to new) and a patient ID mapping. The scorer indexes our
output, reads each file's SOP Instance UID, looks it up in our UID
mapping to find the old UID, and joins to the answer key on that. An
output file whose SOP UID is not in our mapping is logged as an error
and scored as absent. Filenames and directory layout do not matter.

**Missing files.** An answer-key instance with no output file scores as
a failure on every retain, not-null, and consistency action, and as a
pass on every removal, date-shift, and UID-change action. Dropping a
file is therefore not free.

**Actions.** Each answer-key record is one action on one tag or region
of one instance:

| Action | Pass when | Scorer category |
| --- | --- | --- |
| `tag_retained` | the tag is present in the output | `dicom` / the IOD subcategory |
| `text_notnull` | the tag is present and non-empty | `dicom` / the IOD subcategory |
| `text_retained` | every non-stopword token of the expected value appears in the output value | `tcia` / `tcia.p15`, `tcia.ptkb`, or `tcia.rev` |
| `text_removed` | no token of the planted value survives (partial credit by token fraction; numbers compared as floats) | `hipaa` (`hipaa.m` or `hipaa.z`) else `tcia` |
| `date_shifted` | the original date string does not appear in the output value | `hipaa` / `HIPAA-C` |
| `uid_changed` | the original UID does not appear in the output value | `hipaa` / `HIPAA-R` |
| `uid_consistent` | the output UID equals what *our* mapping says the old UID became | `dicom` / `DICOM-P15-BASIC-U` |
| `patid_consistent` | the output PatientID equals what *our* mapping says the old ID became | `dicom` / `DICOM-P15-BASIC-C` |
| `pixels_retained` | MD5 of the output file's `PixelData` bytes equals the digest TCIA recorded from its curated file | `tcia` / `TCIA-P15-PIX-K` |
| `pixels_hidden` | EasyOCR over the answer key's bounding box in our output finds none of the planted text | `hipaa` / `HIPAA-A` |

The category vocabulary is the answer key's, not ours: `hipaa.z` and
`hipaa.m` are Safe Harbor identifiers, `dicom.p15` and `dicom.iod` are
the standard, and the three `tcia.*` subcategories (`ptkb`, `p15`,
`rev`) are TCIA's private-tag knowledge base, TCIA's reading of PS3.15,
and TCIA reviewer decisions. Those three are where the judgement calls
live, and the scorer labels them for us.

**Score.** `run_reports.py` produces a discrepancy list, pass/fail
counts per action, pass/fail counts per category, and one aggregate:
passes over passes-plus-fails, all actions weighted equally. There is no
per-category score in the shipped report; we derive it from the category
table.

**The human step.** `run_validation.py` writes `pixel_validation.xlsx`
listing every `pixels_hidden` check with EasyOCR's verdict. The manual
says a person is meant to open each region, enter 1 or 0, and run
`run_import.py` to overwrite the verdicts before reporting. We will
report EasyOCR's verdict as the automatic number and say so; a manually
reviewed number, if we produce one, is a second line on the page with
who reviewed it and when.

**Environment.** The scorer requires Python 3.8 and pins `pydicom==2.2.1`
and `easyocr==1.7.1`. It cannot share a venv with Isocenter (3.12+,
pydicom 3). It gets its own, created by the runbook.

**`run_dciodvfy.py`.** A separate step that runs David Clunie's
`dciodvfy` over the output and tabulates IOD conformance warnings and
errors per modality. It is not part of the score. It is a free second
signal about our exporter, and the run collects it.

## 4. What we already know it will find

These come from reading the scorer against the pipeline before any run.
They are stated now so that finding them later cannot be presented as a
discovery.

1. **No UID remapping.** `uid_changed` and `uid_consistent` require
   every Study, Series, and SOP Instance UID replaced consistently
   across the cohort, and a mapping file that says how. The only UID
   rewrite in the pipeline is `regenerate_uid()` on an instance that
   redaction rewrote (#235). Nothing touches Study or Series UIDs, and
   nothing emits a mapping. Every `HIPAA-R` and `DICOM-P15-BASIC-U`
   action on a non-redacted instance fails. This is a **missing
   capability**, the largest one, and the run is the evidence for
   scoping it. Whether a cohort-wide UID map belongs in the
   reversibility service, the exporter, or a new step is the design
   question the run does not answer.
2. **PatientID is removed, not mapped.** `patid_consistent` wants a new
   ID that is the same in every file of a patient, and a mapping file.
   `BASIC_PROFILE` removes (0010,0020). The date shift already seeds on
   PatientID, so a stable pseudonym is a precondition for consistent
   dates too. **Missing capability**, or a configuration question if
   `REPLACE` with a per-patient value already exists; the run decides
   which.
3. **Pixel bytes.** `pixels_retained` is a byte-for-byte digest against
   TCIA's curated file. Isocenter decompresses at ingest, stores raw
   bytes in the sidecar, and exports Implicit VR Little Endian unless
   `compression='j2k'`. TCIA's curated set is 2.4x the size of its
   input, so TCIA also decompressed; if its output is little-endian
   uncompressed with the same bit depth and planar configuration, the
   digests match. If not, every pixel-retention action fails at once
   and the category count will make that obvious. **Unknown until the
   first validation run**; not a bug in either outcome, since TCIA's
   encoding is TCIA's choice.
4. **Retention.** `tag_retained`, `text_notnull`, and `text_retained`
   score keeping things: sex, age, modality-specific acquisition
   parameters, private tags TCIA has vetted as safe (`tcia.ptkb`).
   `BASIC_PROFILE` removes Patient's Sex. `research_tags.json` is the
   closest thing the package has to a retention list and nothing reads
   it (#357). The TCIA profile has to express "keep these", and the
   configuration model's answer to that is what the run checks.
5. **Free text.** `text_removed` on descriptions, comments, and
   sequences is where the second-place team lost most of its 487
   failures. Our detection is regex-first with an optional spaCy path.
   Expect a long tail here, and expect most of it to be **policy**:
   which tokens count as identifying is exactly the judgement the manual
   disclaims.
6. **Burned-in text.** `pixels_hidden` checks bounding boxes the answer
   key knows about. Our redaction is zones per device plus OCR-assisted
   discovery. A device with no configured zone is not a bug; it is
   configuration we did not write. Whether the OCR discovery path finds
   the planted text on its own is a real question about the `ocr` extra
   and worth its own line in the write-up.
7. **Ten thousand files per run means the export has to keep every
   instance.** An `ExportError` or a lossy export is already graded
   `REVIEW_REQUIRED` by our own report; here it also costs every retain
   action on the missing file. The run is a large end-to-end exercise of
   #153's ordering and of the delivery counters, on data we did not
   generate.

## 5. Triage

Every failed action goes into exactly one bin. The bin is decided by
reading the discrepancy row, not by the category label alone, but the
label is the first sort key:

| Bin | Means | Scorer categories that usually land here | Disposition |
| --- | --- | --- | --- |
| **Bug** | The profile said one thing and the export did another. | `HIPAA-C` (a date the profile shifts still carries the original), `DICOM-P15-BASIC-U` and `-C` (a mapping we emitted and did not honour), `dicom.iod` retention of a tag the profile keeps, `TCIA-P15-PIX-K` once the encoding question is settled | GitHub issue, zero-bug-bounce rules, fixed before the number is published or listed as open beside it |
| **Missing capability** | The rule could not be expressed. | `HIPAA-R` and `-U` until UID remapping exists; `-C` until patient pseudonyms exist; retention rules with no configuration spelling | GitHub issue, feature-shaped, milestone decided by Kevin, listed beside the number |
| **Disagreement** | We would not do what the key wants, and can say why. | `tcia.ptkb`, `tcia.rev`, `tcia.p15` where our reading of PS3.15 differs, `text_removed` on tokens we judge non-identifying, `HIPAA-A` on devices with no zone | Written up on the page, one line each, never "fixed" |

A row that could be two bins is the more expensive one: a suspected
policy disagreement that could be a bug is a bug until shown otherwise.

The published aggregate is the scorer's own number, uncorrected. The
page also shows the per-category table, and the reader can subtract the
disagreement rows themselves. We do not publish a "corrected" score.

## 6. Run tiers

| Tier | Data | Pixels | Where | Purpose |
| --- | --- | --- | --- | --- |
| A | Synthetic validation (7.63 GB) | off (no `redact()`, `pixels_hidden` expected to fail wholesale, ignored) | laptop | Inner loop. Answers questions 1, 2, 3, 4, 5, 7 of section 4 in an afternoon. Rerun after every fix. |
| B | Synthetic validation | on, with the `ocr` extra | laptop overnight or GCP | Answers question 6. Produces the first `pixel_validation.xlsx`. |
| C | Synthetic test (11.09 GB) | on | GCP, per runbook | Run once per version that is published. The only run whose number goes on the page. |

Rules:

- **Publish the test number only.** A profile tuned on validation and
  scored on validation is not a benchmark. Tier A and B numbers appear
  nowhere public.
- **One tier C per version.** Rerunning C until the number improves is
  tuning on test. If a bug is fixed after C, the fix ships in a version
  and that version gets its own C.
- **Every run records** the Isocenter commit, the scorer commit, the
  answer-key file checksum, the profile file checksum, the machine, the
  wall time, and whether the pixel review was manual. The runner writes
  these into the output directory before the export starts.

## 7. Artifacts

Everything below lives in the repo. Nothing is a new import path in the
package.

```
tests/benchmarks/midi_b/
    README.md            what this is, one paragraph, pointer to the runbook and the spec
    profile.yaml         the TCIA-rules configuration; the single source the docs page embeds
    run_midi_b.py        ingest -> examine -> load_config -> audit -> anonymize -> [redact] -> export -> report,
                         then writes uid_mapping.csv, patid_mapping.csv, and run_record.json
    scorer_config.json   template for the scorer's config file, paths filled by the runbook
    scorer-requirements.txt
                         the scorer's pins, copied, so the scorer venv is reproducible without its repo's future
.agent/workflows/midi_b_benchmark.md
                         the runbook: download (NBIA Data Retriever for images, Aspera for keys),
                         venvs, tiers A/B/C, scoring, reports, what to copy to the docs page
docs/midi-b.md           the public record (section 8); nav under Project Info
```

`run_midi_b.py` follows `run_stress_test.py`'s shape: a module under
`tests.benchmarks`, invoked with `python -m`, no CLI in the package
(#29). It uses only public `Session` methods; if it needs something
private to produce the mapping files, that is a section 4 finding, not
a reason to reach in.

`profile.yaml` is an ordinary configuration file in the format
`IsocenterConfiguration.save()` writes, so a reader can copy it. If the
TCIA rules need a spelling the configuration model does not have, the
file carries a comment saying what could not be expressed, and the gap
goes in the section 4 list. Being unable to write the profile completely
is itself a result.

## 8. The public page

`docs/midi-b.md` states, in this order:

1. What MIDI-B is, in one paragraph, with the DOI and the citation TCIA
   asks for.
2. **The run**: Isocenter version, git commit, Zenodo version DOI (the
   version DOI for that release, alongside the concept DOI
   `10.5281/zenodo.22104298`), date, machine, wall time, scorer commit,
   answer-key checksum, and whether the pixel review was automatic or
   manual and by whom.
3. **The number**, as the scorer reports it, with the pass and fail
   counts it is made of.
4. **The per-category table**, verbatim from the scorer's category
   report.
5. **The three bins**: every open bug and missing capability by issue
   number, and every disagreement in one line each with the reason.
6. **The profile**, embedded from `tests/benchmarks/midi_b/profile.yaml`
   with `--8<--`, with the version it was scored under stated above it
   and a note that a later profile is not the one that produced the
   number until the page says it is.
7. **What the run does not tell you**, in the same voice as the
   performance page: it is one machine, one run, one answer key that
   encodes one archive's practice, and the judgement categories are not
   a measurement of anything about Isocenter.

Until tier C has run once, the page does not exist. A page that says
"results to come" is a promise, and the positioning doc forbids those.

The docs tests apply: any `session.*` call in a python fence on the page
must exist, and any output the page shows must match what the code
produces. The embedded profile is YAML, not python, and is not
executed; a test that loads it through `load_config()` and asserts it
parses is the drift guard, and belongs beside the runner rather than in
the docs tests.

## 9. Sequence

Nothing here starts before 1.0 tags. Then, in order:

1. Download the validation set, the validation answer key, and TCIA's
   own mapping CSVs (to learn the expected shape, not to use). Build
   the scorer venv. Score TCIA's *curated* validation set against its
   own key with its own mappings to confirm the tooling produces a
   near-100% number on the reference answer. This is the calibration
   step and it costs nothing but time.
2. Write `profile.yaml` as far as the configuration model allows.
   Record what it cannot express.
3. Write `run_midi_b.py`. Tier A. Triage. File issues.
4. Fix bugs found. Re-run tier A until the bug bin is empty or every
   remaining bug has an issue. Do not touch the missing-capability bin
   yet.
5. Tier B. Triage the pixel rows.
6. Decide, with Kevin, which missing capabilities are built before the
   first tier C and which are listed beside it. This is the point where
   UID remapping and patient pseudonyms get a milestone or do not.
7. Tier C on GCP. Write the page. Open the PR with the page, the
   runner, the profile, and the runbook together, so the number and the
   thing that produced it merge in one commit.

## 10. What this document does not decide

- Where UID remapping lives, if it is built. That is a design of its
  own, and the run's discrepancy rows are its input.
- Whether the profile grows into a shipped resource later. Section 1
  says not now; a later spec can supersede this one when there is a
  reason.
- Whether to submit anything back to TCIA or the challenge organisers.
  The data terms do not require it, and nothing here depends on it.
- The 1.0 date. This document waits for it and does not move it.
