# Session API

Every method on this page is **frozen at 1.0** — see
[API stability](stability.md), whose table is the list this page
renders, in the pipeline order the README teaches: lifecycle first, then
ingest → examine → config → audit → anonymize → redact → verify → export
→ report. `tests/test_frozen_surface.py` asserts the `members:` block
below renders every frozen method; it rendered 16 of the 28 until 0.9.4.

::: isocenter.session.DicomSession
    handler: python
    options:
      members:
        - ingest
        - save
        - examine
        - create_config
        - load_config
        - preview_config
        - configuration
        - audit
        - auto_remediate_config
        - anonymize
        - enable_reversible_anonymization
        - lock_identities
        - lock_identities_batch
        - recover_patient_identity
        - redact
        - redact_by_machine
        - scan_pixel_content
        - discover_redaction_zones
        - reconcile_private_tags
        - export
        - export_dataframe
        - get_cohort_report
        - phi_status_summary
        - generate_report
        - generate_manifest
        - save_analysis
        - compact
        - release_memory
        - close
