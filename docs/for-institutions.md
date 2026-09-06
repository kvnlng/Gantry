# For institutions

This page is for the people who decide whether Isocenter may be used at a site and never run it: compliance officers, IRB and privacy-office staff, and imaging-informatics leads. It answers the questions that review usually asks, in the order they are usually asked, without code.

## What it is

Isocenter is an open-source Python library that a research team's own script imports to de-identify a cohort of DICOM studies. It reads the source files, builds an index and a working copy of the pixel data beside them, applies the de-identification rules the team configured, and writes de-identified copies to a new directory. It also writes a report of what it did.

It is a library, not a service. It runs on the machine the team runs it on, inside the institution's network, and sends nothing anywhere. There is no account, no telemetry, and no cloud component.

## What it never does

- **It never modifies the source files.** The originals are read and left as they were. A crashed or abandoned run leaves them untouched.
- **It never certifies compliance.** De-identification under Isocenter is whatever the team's configured profile says it is. Whether that profile satisfies a protocol, an IRB determination, or a regulation is the institution's judgement, and the report exists to make that judgement possible.
- **It never grades a run that lost data as passing.** If a file, a tag, a pixel frame, or a waveform group could not be carried through, the report says so and grades the run `REVIEW_REQUIRED`.

## What the report contains

The team generates a Markdown report at the end of a run. It contains:

- **A cohort manifest**: the patients, studies, and series that were processed.
- **An audit trail**: counts of every action taken (tags removed or replaced, dates shifted, pixel regions redacted, files exported) and every loss recorded.
- **Exceptions**: every warning and error the run raised, listed individually.
- **A grade**: `PASS` or `REVIEW_REQUIRED`. There is deliberately no `FAIL`. A run that lost something is a run a person must look at, and the report names what to look at.
- **A signature block** for the reviewer who accepts the report.

The report is evidence for whatever review the institution runs. It is not itself a determination.

## How de-identification is configured

The team writes a configuration file that names the de-identification profile, the tags to remove, replace, or date-shift, and the pixel regions to redact on each make and model of equipment. A field the protocol permits is kept; nothing is removed by default beyond what the profile names, so the configuration is the document a reviewer reads to see what will happen to the data.

Two options bear on review:

- **Date shifting** is deterministic per patient, so intervals between a patient's studies survive while absolute dates do not.
- **Reversible anonymization** is optional and off by default. When a team turns it on, original identities are encrypted under a key the team holds and stored inside the de-identified files, recoverable only with that key. The export warns and records in the audit trail when it has written files that carry recoverable identities, so a cohort cannot be shared under the impression that it does not.

## Burned-in text

Some equipment writes patient identifiers into the image pixels themselves. Isocenter redacts rectangular regions of the pixels for each make and model the team configures, and can use optical character recognition on a sample of one machine's images to find where text actually lands before the team writes those regions. Images the source marks as carrying burned-in annotations are flagged for manual review rather than silently exported.

## License

Apache License 2.0 from the release after version 0.9.2. It is a permissive license with an explicit patent grant; it permits internal use, modification, and redistribution, and does not require an institution to publish its own code. Releases up to and including 0.9.2 were published under the GNU Affero General Public License v3.0 or later and remain available under it.

The full text is in the repository's `LICENSE` file.

## Citation and provenance

Each release is archived on Zenodo, a repository operated by CERN for research outputs, under the concept DOI [10.5281/zenodo.22104298](https://doi.org/10.5281/zenodo.22104298), which always resolves to the latest version. The repository carries a `CITATION.cff` file that reference managers read. Work that used Isocenter to prepare a dataset should cite it in the methods section.

The source, the issue tracker, and the full change history are public at [github.com/kvnlng/Isocenter](https://github.com/kvnlng/Isocenter). Every release runs its test suite on the supported Python versions before it is published.

## Where it runs

Python 3.12 or later, on Linux, macOS, or Windows, on the team's own hardware. Sizing guidance for large cohorts is on the [Performance](performance.md) page.

## Contact

Questions that can be public go to [GitHub Issues](https://github.com/kvnlng/Isocenter/issues). Anything that should not be public goes to <support@isocenter.net>.
