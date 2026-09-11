"""Sampled mutation probe: does the suite notice when behaviour changes?

Coverage says a line ran. It does not say a test would have failed had
that line done something else. This asks the second question: mutate one
decision in a module, run only the tests that import it, and see whether
anything goes red. A mutant that survives is a change to production
behaviour the suite cannot see.

Run it against the de-identification core, where a test that cannot fail
is not an inconvenience but a leak nobody is watching for:

    python -m scripts.mutation_probe                 # default targets, each at its own budget
    python -m scripts.mutation_probe 10              # one budget for every module: the cheap pass
    python -m scripts.mutation_probe 30 isocenter/session.py tests/test_session.py

A default run is long, and survivors are what make it so: a surviving
mutant pays its module's whole test list, where a kill exits at the
first red test. Measured on 3.12.14, as upper bounds -- every sampled
mutant surviving:

    default run, before the session/entities/imagecodecs rows   ~3.7 h
    default run, with them (#414, #419)                         ~8.4 h
    `python -m scripts.mutation_probe 10`, the cheap pass       ~3.0 h

`session.py` and `entities.py` each list over 120 test files, one full
pass of ~300s and ~190s. A mutant that hangs costs a further 15 minutes,
because `run()` times out at 900s and reports it as skipped. A real run
is much shorter, since most mutants die in seconds, but the survival
rates any such estimate rests on were sampled too thinly to print here.
The positional budget is the override for every module at once; nothing
in CI runs this script.

A default run ends by printing `NOT_PROBED`: the modules it does not
measure, and why. `tests/test_mutation_probe_targets.py` fails when a
module under `isocenter/` is in neither `TARGETS` nor `NOT_PROBED`.

**Sampled, not exhaustive.** It walks mutation sites at a fixed stride,
so the output is "of N representative mutations, M survived" -- evidence
about whether the suite bites, not a mutation score to track over time.
Adding an operator renumbers the sites, so two runs across such a change
are not comparable sample-for-sample.

**The operator set decides what can be measured.** Three operators see
decisions -- comparison flips, `and`/`or`, boolean constants -- and three
see straight-line code: dropping a `not`, replacing a returned value with
None, and deleting a bare expression statement. That second group exists
because the first reported `crypto.py` as 0 sites and 0 survivors: 73
lines of key derivation, encrypt and decrypt with almost no branching,
so there was nothing for a comparison flip to find. A module with 0
sites is unmeasured, not clean, and the run says so rather than printing
a zero and letting it be read as a pass (#106).

Still unreached: argument swaps between same-typed parameters, string and
bytes constant mutation (salts, encodings, key-derivation parameters),
and exception-handler removal.

**A survivor is a question, not a verdict.** Some mutants are equivalent
-- they change the code without changing behaviour, and no test could
tell. Read each one before acting on it. The ones that matter are where
the mutated code is still plausible *and* the behaviour differs.

**The control run is load-bearing.** Before mutating, the module is
unparsed from its own AST and the tests are run against that. If the
control fails, the harness itself is perturbing the module and every
result below it is an artefact -- so it stops rather than reporting.

The mutated file is written in place and restored in a `finally`. Run it
on a clean tree so `git checkout isocenter/` is always a way out.

**A verdict is only about the mutation if the mutation was compiled.**
Runs carry `PYTHONDONTWRITEBYTECODE=1` and every write is checked against
CPython's own `.pyc` validation rule before the tests see it, because a
stale bytecode cache made this tool report mutations that were never in
the code it tested. `assert_fresh` has the mechanism (#174). The cache it
inspects is the one `PYTEST[0]` itself names, because the entry that
matters belongs to the interpreter that runs the tests, not the one that
launched the probe (#201).
"""

import ast, importlib.util, os, struct, subprocess, sys, time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PYTEST = [str(REPO / ".venv/bin/python"), "-m", "pytest", "-x", "-q", "--no-header", "-p", "no:randomly"]

# The `(tests, budget)` to use for each target. Complete-ness of the
# test list matters more than it looks: a mutant that a test in this
# repo would kill, but which is not in that list, is reported as
# SURVIVED -- a phantom gap in the de-identification core that costs a
# human a real investigation.
# `tests/test_mutation_probe_targets.py` fails if a test file imports a
# target module and is not listed here. Extra entries are allowed but
# must have earned their runtime with a measured kill: every listed file
# runs for every sampled mutant. `test_remediation_actions.py` exercises
# `remediation.py` without importing it, which no import scan can see;
# `test_redaction_export.py` and `test_reversibility.py` do the same for
# `io_handlers.py` (the `apply_redaction_to_array` call and the
# `(0400,0510)` write, both verified kills).
# The budget is per module because the knob does two jobs at once
# globally: `io_handlers.py` has ~5x the sites of any other target, so
# raising its sampling density with one shared number forces the
# already-measured modules to re-pay at stride 1. The positional CLI
# budget overrides every module for one run.
TARGETS = {
    # 68 sites; budget 80 is stride 1 with headroom (#365). Five files
    # rather than the issue's three: test_every_test_that_imports_a_target_module_is_listed
    # demands every file whose text names `isocenter.parallel`, and
    # test_logging.py and test_shared_executor_lifecycle.py do.
    #
    # The last two are listed although the guard does not demand them
    # (#384, #400): they cover the threads-or-processes decision through
    # `redact()`, which is where its two attribution fields are read, and
    # a probe that did not run them would report a survivor for exactly
    # the mutation those files exist to kill -- an attribution computed
    # after the `force_threads` short-circuit. Extras cost the guard
    # nothing.
    "isocenter/parallel.py": (["tests/test_logging.py",
                               "tests/test_packaging_contract.py",
                               "tests/test_parallel_config.py",
                               "tests/test_parallel_contract.py",
                               "tests/test_redaction_worker_count.py",
                               "tests/test_shared_executor_lifecycle.py",
                               "tests/test_redaction_names_its_strategy.py",
                               "tests/test_memory_store_reports_its_processes_lever.py",
                               "tests/test_duplicate_sop_uid_at_ingest.py"],
                              80),
    "isocenter/crypto.py": (["tests/test_crypto.py", "tests/test_reversibility.py"], 30),
    "isocenter/privacy.py": (["tests/test_analysis.py", "tests/test_analysis_persistence.py",
                              "tests/test_audit_suppression.py", "tests/test_automation.py",
                              "tests/test_config_tags_shapes.py",
                              "tests/test_declined_remediation_is_recorded.py",
                              "tests/test_multiprocessing.py",
                              "tests/test_mutation_gaps.py", "tests/test_ocr_formal.py",
                              "tests/test_persistence.py", "tests/test_privacy.py",
                              "tests/test_private_sequence_implicit_vr.py",
                              "tests/test_profile_end_to_end.py", "tests/test_remediation.py",
                              "tests/test_remediation_actions.py",
                              "tests/test_remediation_invariants.py",
                              "tests/test_scaffold_features.py",
                              "tests/test_shipped_resource_is_required.py",
                              "tests/test_sr_anonymization.py"], 30),
    "isocenter/remediation.py": (["tests/test_audit_suppression.py",
                                  "tests/test_declined_remediation_is_recorded.py",
                                  "tests/test_deid_tags.py",
                                  "tests/test_mutation_gaps.py", "tests/test_persistence.py",
                                  "tests/test_private_sequence_implicit_vr.py",
                                  "tests/test_remediation.py",
                                  "tests/test_remediation_accounting.py",
                                  "tests/test_remediation_actions.py",
                                  "tests/test_remediation_dates.py",
                                  "tests/test_remediation_invariants.py",
                                  "tests/test_phi_retention.py",
                                  "tests/test_scaffold_features.py"], 30),
    "isocenter/io_handlers.py": (["tests/test_api_coherence.py",
                                  "tests/test_audit_read_barrier.py",
                                  "tests/test_binary_retention_threshold.py",
                                  "tests/test_check_reversibility.py",
                                  "tests/test_codecs_strict.py",
                                  "tests/test_colour_space_at_ingest.py",
                                  "tests/test_compact_refuses_during_a_pass.py",
                                  "tests/test_compaction_races_a_concurrent_write.py",
                                  "tests/test_compaction_reclaims_a_row_instances_does_not_carry.py",
                                  "tests/test_compress_handlers.py",
                                  "tests/test_compress_j2k_coverage.py",
                                  "tests/test_data_loss_reporting.py",
                                  "tests/test_descriptor_edit_with_pixels_unloaded.py",
                                  "tests/test_dtype_only_replacement_survives_the_dedup.py",
                                  "tests/test_duplicate_sop_uid_at_ingest.py",
                                  "tests/test_empty_sequence_roundtrip.py",
                                  "tests/test_export_atomic_write.py",
                                  "tests/test_export_contract.py",
                                  "tests/test_export_date_error.py",
                                  "tests/test_export_delivery_counters.py",
                                  "tests/test_export_error.py",
                                  "tests/test_export_failure_audit.py",
                                  "tests/test_export_flushes_before_it_sweeps.py",
                                  "tests/test_export_loss_audit.py",
                                  "tests/test_export_merge_shape.py",
                                  "tests/test_export_pixels.py",
                                  "tests/test_export_readback.py",
                                  "tests/test_export_redaction_hash_warning.py",
                                  "tests/test_export_worker_graph_purity.py",
                                  "tests/test_float_pixel_data_export.py",
                                  "tests/test_ingest_failure_audit.py",
                                  "tests/test_ingest_imagecodecs_fallback.py",
                                  "tests/test_io.py",
                                  "tests/test_legacy_waveform_hydration.py",
                                  "tests/test_logging.py",
                                  "tests/test_metadata_refactor_full.py",
                                  "tests/test_missing_study_date.py",
                                  "tests/test_multiprocessing.py",
                                  "tests/test_murmur_annotations.py",
                                  "tests/test_naming_structure.py",
                                  "tests/test_nested_phi_audit.py",
                                  "tests/test_nested_pixel_carriage.py",
                                  "tests/test_offset_table_frame_count.py",
                                  "tests/test_pixel_dtype_roundtrip.py",
                                  "tests/test_pixel_geometry_pipeline.py",
                                  "tests/test_planar_configuration_roundtrip.py",
                                  "tests/test_private_binary_ingest.py",
                                  "tests/test_private_tag_arity_roundtrip.py",
                                  "tests/test_private_tag_empty_value_roundtrip.py",
                                  "tests/test_private_tag_export.py",
                                  "tests/test_private_tag_vr_roundtrip.py",
                                  "tests/test_pydicom_deprecations.py",
                                  "tests/test_recursive_import.py",
                                  "tests/test_redaction_export.py",
                                  "tests/test_redaction_optimization.py",
                                  "tests/test_redaction_rgb.py",
                                  "tests/test_redaction_robustness.py",
                                  "tests/test_redaction_wildcard.py",
                                  "tests/test_remediation_accounting.py",
                                  "tests/test_reporting_features.py",
                                  "tests/test_reversibility.py",
                                  "tests/test_safe_export.py",
                                  "tests/test_services.py",
                                  "tests/test_session.py",
                                  "tests/test_shared_executor_lifecycle.py",
                                  "tests/test_signed_lossless_jpeg_decode.py",
                                  "tests/test_signed_pixels_survive_a_compressed_export.py",
                                  "tests/test_single_frame_encapsulated_decode.py",
                                  "tests/test_sidecar_gate_crosses_processes.py",
                                  "tests/test_sidecar_gate_order.py",
                                  "tests/test_sr_anonymization.py",
                                  "tests/test_structured_export.py",
                                  "tests/test_study_date_roundtrip.py",
                                  "tests/test_waveform_dicom_roundtrip.py",
                                  "tests/test_waveform_ingest.py",
                                  "tests/test_waveform_model.py",
                                  "tests/test_wfdb_conformance.py",
                                  "tests/test_wfdb_writer.py",
                                  "tests/test_worker_loss_is_reported.py"], 30),
    # 453 sites. Until #383 this module had no row at all, so no mutant
    # of `_hold_sidecar_gate`, `_hold_pass_lock`, `_refuse_while_pass_open`,
    # `_flock_within`, `_SIDECAR_GATE_TIMEOUT_S`, the `:memory:` temp-file
    # ownership flag or `compact_sidecar()` was ever generated -- the
    # sidecar gate and the pass-lock #368 put here were invisible to the
    # probe.
    #
    # Budget 30 matches io_handlers.py deliberately: 453 sites at 30 is a
    # stride of 15, and io_handlers.py's 519 at 30 is a stride of 17, so
    # the two largest modules are sampled at comparable density. A
    # different number here would need a reason.
    #
    # This row roughly doubles a default probe run: 14 of the 42 files
    # alone take 19s (77 tests, 3.12.14), and all 42 run well over a
    # minute, so this target is ~30-45 minutes at budget 30. That is the
    # same order as io_handlers.py, and the probe is a by-hand tool
    # rather than CI, so it is affordable -- written down because an
    # unexplained doubling of the run time is the kind of thing someone
    # later "fixes" by cutting the budget.
    #
    # The list is measured, not curated: every file whose text matches
    # `isocenter\.persistence\b` (the `\b` correctly excludes
    # persistence_manager). #383 named eight; all eight are here and so
    # are 34 others, each of which the guard demands.
    "isocenter/persistence.py": (["tests/test_api_coherence.py",
                                  "tests/test_async_persistence.py",
                                  "tests/test_audit_drop_accounting.py",
                                  "tests/test_audit_read_barrier.py",
                                  "tests/test_audit_worker_does_not_pin_its_store.py",
                                  "tests/test_blob_storage.py",
                                  "tests/test_bytes_persistence.py",
                                  "tests/test_close_does_not_drop_an_orphaned_save.py",
                                  "tests/test_compact_refuses_during_a_pass.py",
                                  "tests/test_compaction_races_a_concurrent_write.py",
                                  "tests/test_compaction_reclaims_a_row_instances_does_not_carry.py",
                                  "tests/test_concurrency_stress.py",
                                  "tests/test_dataframe_export.py",
                                  "tests/test_declined_remediation_is_recorded.py",
                                  "tests/test_dtype_only_replacement_survives_the_dedup.py",
                                  "tests/test_export_flushes_before_it_sweeps.py",
                                  "tests/test_float_pixel_data_export.py",
                                  "tests/test_flush_orphan_recovery.py",
                                  "tests/test_json_serialization.py",
                                  "tests/test_legacy_waveform_hydration.py",
                                  "tests/test_memory_store_unlinks_its_temp_files.py",
                                  "tests/test_packaging_contract.py",
                                  "tests/test_persistence.py",
                                  "tests/test_persistence_concurrency.py",
                                  "tests/test_persistence_incremental.py",
                                  "tests/test_persistence_manager.py",
                                  "tests/test_persistence_worker_does_not_pin_its_manager.py",
                                  "tests/test_phi_retention.py",
                                  "tests/test_pixel_divergence.py",
                                  "tests/test_pixel_geometry_check.py",
                                  "tests/test_planar_configuration_roundtrip.py",
                                  "tests/test_private_tag_arity_roundtrip.py",
                                  "tests/test_private_tag_empty_value_roundtrip.py",
                                  "tests/test_private_tag_reload.py",
                                  "tests/test_private_tag_vr_roundtrip.py",
                                  "tests/test_save_all_contract.py",
                                  "tests/test_save_redact_race.py",
                                  "tests/test_save_reparenting.py",
                                  "tests/test_services.py",
                                  "tests/test_sidecar_gate_crosses_processes.py",
                                  "tests/test_sidecar_gate_order.py",
                                  "tests/test_study_date_roundtrip.py",
                                  "tests/test_vertical_table.py",
                                  "tests/test_worker_start_is_serialised.py"], 30),
    # 566 sites. Until #414 the facade had no row, so no mutant of
    # `Session` -- the ingest/audit/anonymize/redact/export ordering,
    # `_make_lightweight_copy`, `_verify_worker`, the report's boundary
    # note -- was ever generated.
    #
    # Budget 30 is a stride of 18, the same density as io_handlers.py
    # (533/30, stride 17) and persistence.py (453/30, stride 15); a
    # different number here would need a reason.
    #
    # The list is measured, not curated: every file `_importers` in
    # tests/test_mutation_probe_targets.py demands, which is 126 of the
    # suite's 221 -- nearly all of it, because nearly every test drives
    # the facade. One full pass is ~300s (3.12.14), so a surviving mutant
    # costs five minutes and a kill costs seconds.
    #
    # Its survivors at budget 30 are NOT yet classified: this row landed
    # without a full budget-30 run, and classifying what one prints is
    # #445. Until then a survivor from this row is a question, not a
    # finding. What is known (4d34c64, 3.12.14): at budget 3 all four
    # sampled mutants are killed, among them the deleted WARNING
    # `reconcile_private_tags()` logs when it drops stored rows -- a real
    # gap until #414, now pinned by tests/test_private_tag_reload.py.
    # Deleting the `print(f"  {line}")` that echoes each safe-export
    # feedback line (then at line 488) is killed by
    # test_safe_export_feedback.py. Deleting the INFO log "Batch preserved
    # identity for N patients" in the chunked identity-lock path (then at
    # line 2807) survives a full pass.
    #
    # Cost: at budget 30 this row is ~2.7 h of a default run as an upper
    # bound (every mutant surviving). Written down because an unexplained
    # tripling of the run time is the kind of thing someone later "fixes"
    # by cutting the budget.
    "isocenter/session.py": (["tests/test_analysis.py",
                              "tests/test_analysis_persistence.py",
                              "tests/test_api_coherence.py",
                              "tests/test_async_persistence.py",
                              "tests/test_automation.py",
                              "tests/test_binary_retention_threshold.py",
                              "tests/test_check_reversibility.py",
                              "tests/test_close_does_not_drop_an_orphaned_save.py",
                              "tests/test_close_warns_about_unsaved_instances.py",
                              "tests/test_colour_space_at_ingest.py",
                              "tests/test_compact_refuses_during_a_pass.py",
                              "tests/test_compact_rewiring_is_locked.py",
                              "tests/test_compaction.py",
                              "tests/test_compaction_races_a_concurrent_write.py",
                              "tests/test_compaction_recovery.py",
                              "tests/test_configuration_manual.py",
                              "tests/test_configuration_persistence.py",
                              "tests/test_dataframe_export.py",
                              "tests/test_date_shifted_roundtrip.py",
                              "tests/test_declined_remediation_is_recorded.py",
                              "tests/test_descriptor_edit_with_pixels_unloaded.py",
                              "tests/test_discovery_integration.py",
                              "tests/test_doc_anchors.py",
                              "tests/test_dtype_only_replacement_survives_the_dedup.py",
                              "tests/test_duplicate_sop_uid_at_ingest.py",
                              "tests/test_empty_sequence_roundtrip.py",
                              "tests/test_export_atomic_write.py",
                              "tests/test_export_contract.py",
                              "tests/test_export_delivery_counters.py",
                              "tests/test_export_failure_audit.py",
                              "tests/test_export_flushes_before_it_sweeps.py",
                              "tests/test_export_loss_audit.py",
                              "tests/test_export_readback.py",
                              "tests/test_feature_regression.py",
                              "tests/test_float_pixel_data_export.py",
                              "tests/test_frozen_surface.py",
                              "tests/test_full_logging.py",
                              "tests/test_import_validation.py",
                              "tests/test_ingest_failure_audit.py",
                              "tests/test_ingest_imagecodecs_fallback.py",
                              "tests/test_ingestion_normalization.py",
                              "tests/test_io_no_pixels.py",
                              "tests/test_legacy_waveform_hydration.py",
                              "tests/test_lock_identities_signature.py",
                              "tests/test_logging.py",
                              "tests/test_manifest.py",
                              "tests/test_memory_redaction.py",
                              "tests/test_memory_store_redaction_strategy.py",
                              "tests/test_memory_store_reports_its_processes_lever.py",
                              "tests/test_metadata_refactor_full.py",
                              "tests/test_missing_study_date.py",
                              "tests/test_multiprocessing.py",
                              "tests/test_murmur_annotations.py",
                              "tests/test_naming_structure.py",
                              "tests/test_nested_pixel_carriage.py",
                              "tests/test_ocr_leaves_frames_where_it_found_them.py",
                              "tests/test_ocr_unavailable_refuses.py",
                              "tests/test_offset_table_frame_count.py",
                              "tests/test_optimization.py",
                              "tests/test_packaging_contract.py",
                              "tests/test_parallel_contract.py",
                              "tests/test_parallel_export.py",
                              "tests/test_persistence.py",
                              "tests/test_persistence_manager.py",
                              "tests/test_phi_retention.py",
                              "tests/test_phi_status.py",
                              "tests/test_pixel_divergence.py",
                              "tests/test_pixel_dtype_roundtrip.py",
                              "tests/test_pixel_export.py",
                              "tests/test_pixel_geometry_check.py",
                              "tests/test_pixel_geometry_pipeline.py",
                              "tests/test_pixel_integrity.py",
                              "tests/test_private_binary_ingest.py",
                              "tests/test_private_tag_arity_roundtrip.py",
                              "tests/test_private_tag_empty_value_roundtrip.py",
                              "tests/test_private_tag_export.py",
                              "tests/test_private_tag_reload.py",
                              "tests/test_private_tag_vr_roundtrip.py",
                              "tests/test_profile_end_to_end.py",
                              "tests/test_query_export.py",
                              "tests/test_redact_error.py",
                              "tests/test_redact_reports_outcome.py",
                              "tests/test_redaction_attestation.py",
                              "tests/test_redaction_consistency.py",
                              "tests/test_redaction_export.py",
                              "tests/test_redaction_failure_is_reported.py",
                              "tests/test_redaction_multizone.py",
                              "tests/test_redaction_names_its_strategy.py",
                              "tests/test_redaction_parallel.py",
                              "tests/test_redaction_reaches_the_exported_file.py",
                              "tests/test_redaction_uid_capture.py",
                              "tests/test_redaction_wildcard.py",
                              "tests/test_redaction_worker_count.py",
                              "tests/test_reingest_after_redact.py",
                              "tests/test_release_memory.py",
                              "tests/test_relock_identity_token.py",
                              "tests/test_remediation_actions.py",
                              "tests/test_report_action_evidence.py",
                              "tests/test_report_export_boundary.py",
                              "tests/test_reporting_features.py",
                              "tests/test_reversibility.py",
                              "tests/test_safe_export.py",
                              "tests/test_safe_export_feedback.py",
                              "tests/test_safe_export_jitter.py",
                              "tests/test_save_all_contract.py",
                              "tests/test_save_redact_race.py",
                              "tests/test_scaffold_features.py",
                              "tests/test_scaffold_profiles.py",
                              "tests/test_scaffolding.py",
                              "tests/test_scan_pixel_content_dispatches_its_worker.py",
                              "tests/test_scan_pixel_findings_name_the_live_graph.py",
                              "tests/test_scan_reports_what_it_could_not_read.py",
                              "tests/test_session.py",
                              "tests/test_shared_executor_lifecycle.py",
                              "tests/test_shipped_resource_is_required.py",
                              "tests/test_sidecar.py",
                              "tests/test_sidecar_gate_order.py",
                              "tests/test_signed_lossless_jpeg_decode.py",
                              "tests/test_signed_pixels_survive_a_compressed_export.py",
                              "tests/test_study_date_roundtrip.py",
                              "tests/test_suggested_config.py",
                              "tests/test_sync_save_does_not_overlap_an_async_one.py",
                              "tests/test_waveform_dicom_roundtrip.py",
                              "tests/test_waveform_ingest.py",
                              "tests/test_wfdb_conformance.py",
                              "tests/test_wfdb_option_strictness.py",
                              "tests/test_wfdb_partial_export_is_audited.py",
                              "tests/test_wfdb_privacy.py",
                              "tests/test_wfdb_start_date_honesty.py",
                              "tests/test_wfdb_writer.py",
                              "tests/test_worker_loss_is_reported.py"],
                             30),
    # 196 sites. Until #419 this module had no row, so the persistence
    # bookkeeping every CLAUDE.md trap is about -- `mark_modified`,
    # `mark_persisted`'s `max`, `phi_status`'s revision comparison,
    # `record_phi_status`'s short-circuit -- and `unload_pixel_data()`'s
    # #293 refusal were never mutated.
    #
    # The sample does not reach that bookkeeping, and no budget here
    # would make it: budget 30 is a stride of 6, sites 0, 6, 12, ..., and
    # of the bookkeeping mutants only site 12 -- `phi_status`'s `is None`
    # flipped to `is not None` -- is generated by default. The
    # `mark_persisted` flip, `phi_status`'s `!=` flip and its `or -> and`,
    # and `record_phi_status`'s short-circuit flip never are. What pins
    # the bookkeeping is the tests named after it (test_phi_status.py and
    # the #173/#307 pins in test_remediation_invariants.py), not this
    # row's sample.
    #
    # So the budget is a cost decision: 30 is ~1.8 h of upper bound for
    # 33 mutants, 12 would be ~0.74 h for 13. It stays at 30, because the
    # classification below is of exactly this stride-6 sample. A stride
    # of 16 shares five of its sites (0, 48, 96, 144, 192), so a default
    # run at 12 would print survivors nobody has read -- the cost this row
    # exists to avoid -- to save an hour on a tool nothing in CI runs.
    #
    # The list is measured, not curated (125 files, the `_importers`
    # demand), and here completeness was shown to matter: flipping the
    # `or` in `phi_status` to `and` passes the six files that pin the
    # bookkeeping by name and is killed only by tests/test_phi_status.py
    # in the full list (two of its tests go red: the edit-after-a-scan
    # invalidation and the stale status that must not persist as
    # current). A curated list would have reported a false survivor. One full pass is ~190s (3.12.14).
    #
    # Measured at budget 30 (4d34c64, 3.12.14): 25 of 33 killed. The
    # eight survivors, each classified:
    #   - four `@dataclass(...)` / `field(...)` keyword flips (`slots`
    #     twice, and `repr=False` twice): layout and repr, nothing any
    #     test or caller reads -- equivalent. None is a `frozen` flip,
    #     which would not be: `Equipment` is frozen so value-hashing works;
    #   - the `ds is not None and hasattr(ds, "file_meta")` flip in the
    #     pixel fallback, which only builds the transfer-syntax UID quoted
    #     in an error message -- equivalent;
    #   - `_write_int_if_changed`'s `return True` flipped, whose result only
    #     decides whether a DEBUG "BitsAllocated ... corrected" line is
    #     logged: a DEBUG log with no reader and no contract -- equivalent;
    #   - `_write_str_if_changed`'s `return False` -> None, whose only
    #     caller discards the result -- equivalent;
    #   - `unload_waveform_data()`'s `return True` for samples that are
    #     already absent, flipped to False. NOT classed equivalent: a caller
    #     told False believes the samples could not be released. Pinned
    #     since by `tests/test_waveform_ingest.py::
    #     test_already_absent_samples_report_as_released` (#443).
    # At budget 3, deleting the DEBUG "Identity regenerated" log also
    # survives: no reader, no contract -- equivalent.
    #
    # Cost: ~1.8 h of a default run as an upper bound.
    "isocenter/entities.py": (["tests/test_analysis.py",
                               "tests/test_api_coherence.py",
                               "tests/test_async_persistence.py",
                               "tests/test_audit_suppression.py",
                               "tests/test_blob_storage.py",
                               "tests/test_bytes_persistence.py",
                               "tests/test_check_reversibility.py",
                               "tests/test_close_does_not_drop_an_orphaned_save.py",
                               "tests/test_close_warns_about_unsaved_instances.py",
                               "tests/test_codecs_strict.py",
                               "tests/test_colour_space_at_ingest.py",
                               "tests/test_compact_refuses_during_a_pass.py",
                               "tests/test_compact_rewiring_is_locked.py",
                               "tests/test_compaction.py",
                               "tests/test_compaction_races_a_concurrent_write.py",
                               "tests/test_compaction_reclaims_a_row_instances_does_not_carry.py",
                               "tests/test_compaction_recovery.py",
                               "tests/test_compression_deps.py",
                               "tests/test_concurrency_stress.py",
                               "tests/test_config_tags_shapes.py",
                               "tests/test_create_config_output.py",
                               "tests/test_dataframe_export.py",
                               "tests/test_declined_remediation_is_recorded.py",
                               "tests/test_deid_tags.py",
                               "tests/test_descriptor_edit_with_pixels_unloaded.py",
                               "tests/test_empty_sequence_roundtrip.py",
                               "tests/test_entities.py",
                               "tests/test_entity_state_vocabulary.py",
                               "tests/test_export_atomic_write.py",
                               "tests/test_export_contract.py",
                               "tests/test_export_date_error.py",
                               "tests/test_export_delivery_counters.py",
                               "tests/test_export_error.py",
                               "tests/test_export_failure_audit.py",
                               "tests/test_export_flushes_before_it_sweeps.py",
                               "tests/test_export_loss_audit.py",
                               "tests/test_export_merge_shape.py",
                               "tests/test_export_pixels.py",
                               "tests/test_export_readback.py",
                               "tests/test_export_worker_graph_purity.py",
                               "tests/test_float_pixel_data_export.py",
                               "tests/test_flush_orphan_recovery.py",
                               "tests/test_frozen_surface.py",
                               "tests/test_ingest_imagecodecs_fallback.py",
                               "tests/test_io.py",
                               "tests/test_io_no_pixels.py",
                               "tests/test_legacy_waveform_hydration.py",
                               "tests/test_lock_identities_signature.py",
                               "tests/test_memory_redaction.py",
                               "tests/test_memory_store_redaction_strategy.py",
                               "tests/test_memory_store_reports_its_processes_lever.py",
                               "tests/test_murmur_annotations.py",
                               "tests/test_mutation_gaps.py",
                               "tests/test_nested_phi_audit.py",
                               "tests/test_ocr_formal.py",
                               "tests/test_ocr_leaves_frames_where_it_found_them.py",
                               "tests/test_ocr_unavailable_refuses.py",
                               "tests/test_offset_table_frame_count.py",
                               "tests/test_optimization.py",
                               "tests/test_parallel_export.py",
                               "tests/test_persistence.py",
                               "tests/test_persistence_concurrency.py",
                               "tests/test_persistence_incremental.py",
                               "tests/test_persistence_manager.py",
                               "tests/test_phi_retention.py",
                               "tests/test_phi_status.py",
                               "tests/test_pixel_analysis.py",
                               "tests/test_pixel_divergence.py",
                               "tests/test_pixel_dtype_roundtrip.py",
                               "tests/test_pixel_geometry_check.py",
                               "tests/test_pixel_geometry_pipeline.py",
                               "tests/test_planar_configuration_roundtrip.py",
                               "tests/test_privacy.py",
                               "tests/test_private_binary_ingest.py",
                               "tests/test_private_tag_reload.py",
                               "tests/test_pydicom_deprecations.py",
                               "tests/test_query_export.py",
                               "tests/test_recursive_import.py",
                               "tests/test_redact_error.py",
                               "tests/test_redact_reports_outcome.py",
                               "tests/test_redaction_consistency.py",
                               "tests/test_redaction_failure_is_reported.py",
                               "tests/test_redaction_multizone.py",
                               "tests/test_redaction_names_its_strategy.py",
                               "tests/test_redaction_optimization.py",
                               "tests/test_redaction_parallel.py",
                               "tests/test_redaction_rgb.py",
                               "tests/test_redaction_robustness.py",
                               "tests/test_redaction_roi.py",
                               "tests/test_reingest_after_redact.py",
                               "tests/test_release_memory.py",
                               "tests/test_relock_identity_token.py",
                               "tests/test_remediation.py",
                               "tests/test_remediation_accounting.py",
                               "tests/test_remediation_actions.py",
                               "tests/test_remediation_invariants.py",
                               "tests/test_reporting_features.py",
                               "tests/test_reversibility.py",
                               "tests/test_reversibility_coverage.py",
                               "tests/test_safe_export.py",
                               "tests/test_safe_export_jitter.py",
                               "tests/test_save_all_contract.py",
                               "tests/test_save_redact_race.py",
                               "tests/test_save_reparenting.py",
                               "tests/test_scaffold_features.py",
                               "tests/test_scaffolding.py",
                               "tests/test_scan_pixel_content_dispatches_its_worker.py",
                               "tests/test_scan_pixel_findings_name_the_live_graph.py",
                               "tests/test_scan_reports_what_it_could_not_read.py",
                               "tests/test_sidecar.py",
                               "tests/test_sidecar_gate_crosses_processes.py",
                               "tests/test_sidecar_gate_order.py",
                               "tests/test_signed_lossless_jpeg_decode.py",
                               "tests/test_single_frame_encapsulated_decode.py",
                               "tests/test_sr_anonymization.py",
                               "tests/test_structured_export.py",
                               "tests/test_study_date_roundtrip.py",
                               "tests/test_sync_save_does_not_overlap_an_async_one.py",
                               "tests/test_tag_key_normalisation.py",
                               "tests/test_uid_regeneration.py",
                               "tests/test_verification_logic.py",
                               "tests/test_voi_lut_integration.py",
                               "tests/test_waveform_dicom_roundtrip.py",
                               "tests/test_waveform_ingest.py",
                               "tests/test_waveform_model.py",
                               "tests/test_wfdb_conformance.py",
                               "tests/test_wfdb_start_date_honesty.py",
                               "tests/test_wfdb_writer.py",
                               "tests/test_worker_loss_is_reported.py",
                               "tests/test_worker_start_is_serialised.py",
                               "tests/test_ybr_jpegls_read_doors.py"],
                              30),
    # 61 sites; budget 60 is stride 1 with headroom, exhaustive because it
    # is cheap, like parallel.py's 80.
    #
    # Six files, and it takes the widened `_importers` (#419) to see
    # them: several reach this module as `from isocenter import
    # imagecodecs_handler`, which the old stem-only scan could not read.
    # Without test_offset_table_frame_count.py, nine of the #418
    # frame-count helpers' mutants survive; without
    # test_signed_lossless_jpeg_decode.py, the #446 sign rule and the
    # lj92 pad have no witness at all.
    #
    # Measured at stride 1 on the #446/#447 branch (3.12.14): 54 of 61
    # killed, and a seventh (line 312, the sign rule's `or` -> `and`) was
    # then pinned by that file's S6 and killed by a real edit. The six
    # survivors are all known, and all equivalent or dead:
    #   - the decode-error print in `get_pixel_data` is equivalent: the
    #     exception it describes is re-raised carrying the same text;
    #   - the print in `is_available()` is equivalent now. It was the only
    #     place the import failure's cause reached anyone until #444 put
    #     the cause in the raise itself;
    #   - two mutants each in `needs_to_convert_to_RGB` and
    #     `should_change_PhotometricInterpretation_to_RGB`, which return
    #     False and have no caller: dead code, and deleting it is #440.
    # The RLE arm, which no mutant could reach through a real decode, is
    # gone (#447).
    "isocenter/imagecodecs_handler.py": (["tests/test_codecs_strict.py",
                                          "tests/test_imagecodecs_edge_cases.py",
                                          "tests/test_ingest_imagecodecs_fallback.py",
                                          "tests/test_offset_table_frame_count.py",
                                          "tests/test_signed_lossless_jpeg_decode.py",
                                          "tests/test_single_frame_encapsulated_decode.py",
                                          "tests/test_ybr_jpegls_read_doors.py"],
                                         60),
}

# Every module under `isocenter/` that has no TARGETS row, and why. Rowed
# or listed here, never neither and never both:
# tests/test_mutation_probe_targets.py fails otherwise, so a module that
# grows into behaviour cannot go unprobed silently again -- which is how
# persistence.py (#383), session.py (#414), entities.py and
# imagecodecs_handler.py (#419) each spent releases without a row, found
# each time by a person noticing rather than by a check.
#
# It lives here rather than in the test so the person running the probe
# sees what it does not measure: a default run prints it last, because the
# tail of a multi-hour run is what gets read.
#
# The 24 deferred entries are #439. Two of their obstacles have issues of
# their own: reach by class name or format string, which no import scan
# sees (#441), and the flat 900s per-mutant timeout a hang costs (#442).
#
# A reason that starts "0 sites" is recomputed by that test and must stay
# true. The other numbers are dated notes (measured at 4d34c64 on 3.12.14:
# sites by `count_ops`, importers by the test's `_importers`, seconds for
# one pass of those importers), not checked -- an edit that moves them
# does not make the reason wrong.
NOT_PROBED = {
    # Permanently excluded.
    "isocenter/__init__.py":
        "1 site, but 217 of 221 test files import the package, so its row "
        "would run the whole suite for each mutant: a full-suite probe of "
        "one re-export, not a measurement",
    "isocenter/_version.py": "0 sites: a version string",
    "isocenter/utils/__init__.py": "0 sites: an empty package marker",
    "isocenter/profiles.py":
        "0 sites: data only, the shipped profile tables; what reads them "
        "is probed where it lives",

    # Deferred: no test names the module, so the scan demands no list and
    # a row needs one written by hand (#439).
    "isocenter/store.py":
        "deferred: 16 sites, 0 importers -- 12 test files reach DicomStore "
        "by class name, which the import scan cannot see (#441)",
    "isocenter/logger.py":
        "deferred: 13 sites, 1 importer -- reached through get_logger() "
        "and describe_exception(), whose spelling "
        "tests/test_ingest_failure_audit.py pins directly (#435); a row "
        "would still need its list written by hand",
    "isocenter/exporters/dicom.py":
        "deferred: 2 sites, 0 importers -- reached through "
        "export(format=\"dicom\"), which the import scan cannot see (#441)",

    # Deferred: at least 5s per pass of the demanded list (#439).
    "isocenter/services.py": "deferred: 151 sites, 21 importers, 36.9s per pass",
    "isocenter/pixel_geometry.py": "deferred: 99 sites, 7 importers, 22.8s per pass",
    "isocenter/persistence_manager.py":
        "deferred: 99 sites, 7 importers, 18.8s per pass",
    "isocenter/exporters/wfdb.py": "deferred: 97 sites, 6 importers, 17.6s per pass",
    "isocenter/waveform.py": "deferred: 69 sites, 5 importers, 15.2s per pass",
    "isocenter/pixel_analysis.py": "deferred: 34 sites, 10 importers, 13.6s per pass",
    "isocenter/sidecar.py": "deferred: 19 sites, 5 importers, 17.9s per pass",
    "isocenter/builders.py": "deferred: 16 sites, 12 importers, 11.2s per pass",
    "isocenter/blob_kind.py": "deferred: 11 sites, 2 importers, 17.6s per pass",
    "isocenter/reporting.py": "deferred: 7 sites, 3 importers, 23.5s per pass",
    "isocenter/exporters/__init__.py":
        "deferred: 5 sites, 7 importers, 17.6s per pass",

    # Deferred: cheap (seconds per pass), but a row would put unclassified
    # survivors in every default run's output; classifying them is its own
    # piece of work (#439).
    "isocenter/automation.py":
        "deferred: 17 sites, 1 importer, ~0s per pass; killed 17/17 at "
        "stride 1, rowable as it stands",
    "isocenter/manifest.py":
        "deferred: 7 sites, 1 importer, ~0s per pass; killed 6/7, one "
        "survivor unclassified",
    "isocenter/configuration.py":
        "deferred: 31 sites, 4 importers, 0.6s per pass; killed 21/31, ten "
        "survivors unclassified",
    "isocenter/discovery.py":
        "deferred: 77 sites, 4 importers, 0.3s per pass; over 20 survivors "
        "unclassified, and flipping `visited = [False] * n` to True does "
        "not terminate, which costs run()'s full 900s timeout (#442)",
    "isocenter/reversibility.py": "deferred: 18 sites, 2 importers, 0.8s per pass; not run",
    "isocenter/verification.py": "deferred: 23 sites, 4 importers, 0.5s per pass; not run",
    "isocenter/utils/ctp_parser.py":
        "deferred: 22 sites, 2 importers, 0.3s per pass; not run",
    "isocenter/config_manager.py":
        "deferred: 40 sites, 9 importers, 2.6s per pass; not run",
    "isocenter/murmur.py": "deferred: 52 sites, 1 importer, 3.0s per pass; not run",
    "isocenter/validation.py": "deferred: 13 sites, 5 importers, 3.6s per pass; not run",
}

class Mut(ast.NodeTransformer):
    """Applies exactly the nth mutation opportunity found."""
    def __init__(self, target): self.target, self.n, self.desc = target, 0, None
    def _hit(self):
        self.n += 1
        return self.n - 1 == self.target
    def visit_Compare(self, node):
        self.generic_visit(node)
        flip = {ast.Eq: ast.NotEq, ast.NotEq: ast.Eq, ast.Lt: ast.GtE, ast.Gt: ast.LtE,
                ast.LtE: ast.Gt, ast.GtE: ast.Lt, ast.In: ast.NotIn, ast.NotIn: ast.In,
                ast.Is: ast.IsNot, ast.IsNot: ast.Is}
        if len(node.ops) == 1 and type(node.ops[0]) in flip:
            if self._hit():
                new = flip[type(node.ops[0])]
                self.desc = f"line {node.lineno}: {type(node.ops[0]).__name__} -> {new.__name__}"
                node.ops = [new()]
        return node
    def visit_BoolOp(self, node):
        self.generic_visit(node)
        if self._hit():
            new = ast.Or if isinstance(node.op, ast.And) else ast.And
            self.desc = f"line {node.lineno}: {type(node.op).__name__} -> {new.__name__}"
            node.op = new()
        return node
    def visit_Constant(self, node):
        if isinstance(node.value, bool):
            if self._hit():
                self.desc = f"line {node.lineno}: {node.value} -> {not node.value}"
                return ast.copy_location(ast.Constant(value=not node.value), node)
        return node

    # The three operators above only see *decisions*. A module of
    # straight-line calls has none, so it reported 0 sites and 0
    # survivors -- which reads like a clean bill of health next to
    # `privacy.py 11/36` and actually meant "not measured" (#106).
    # crypto.py, the reversible-anonymisation core, was in that state.
    # The three below reach code that decides nothing.
    def visit_UnaryOp(self, node):
        self.generic_visit(node)
        if isinstance(node.op, ast.Not) and self._hit():
            self.desc = f"line {node.lineno}: dropped `not`"
            return node.operand
        return node

    def visit_Return(self, node):
        self.generic_visit(node)
        already_none = (isinstance(node.value, ast.Constant)
                        and node.value.value is None)
        if node.value is not None and not already_none and self._hit():
            self.desc = f"line {node.lineno}: return <value> -> return None"
            return ast.copy_location(ast.Return(value=ast.Constant(value=None)), node)
        return node

    def visit_Expr(self, node):
        # A bare expression statement is there for its side effect, so
        # dropping it is the cheapest way to ask whether anything checks
        # that the side effect happened. Becomes `pass` rather than being
        # deleted: removing the only statement in a body leaves an AST
        # that will not unparse, and the failure would look like a
        # skipped mutant rather than a bug in this file.
        if isinstance(node.value, ast.Constant):
            return node  # a docstring; deleting it is an equivalent mutant
        self.generic_visit(node)
        if self._hit():
            self.desc = f"line {node.lineno}: deleted statement"
            return ast.copy_location(ast.Pass(), node)
        return node

def count_ops(src):
    n = 0
    while True:
        m = Mut(n); m.visit(ast.parse(src))
        if m.n <= n: return n
        n += 1

#: Answers from `subprocess_cache_path`, keyed on (interpreter, source
#: path). Per *path*, not per cache tag: the subprocess honours
#: `PYTHONPYCACHEPREFIX`/`sys.pycache_prefix`, so two sources under one
#: tag can cache into different directories and a tag-keyed memo would
#: hand one module the other's answer.
_SUBPROCESS_CACHE_PATHS = {}

def subprocess_cache_path(path):
    """The `__pycache__` entry `PYTEST[0]` would read for `path`.

    Asked of that interpreter, never derived here: a local
    `cache_from_source` names the file from the *parent's*
    `sys.implementation.cache_tag`, and the parent is routinely not the
    interpreter that runs the tests -- a pyenv shim beside the hardcoded
    `.venv` is the ordinary case, not the exotic one. Inspecting the
    parent-tag file degrades one direction only, never a false abort and
    always a false pass, so nothing about running the probe would ever
    say the guard had gone quiet (#201).

    The full path is requested rather than just the tag because the
    subprocess honours `PYTHONPYCACHEPREFIX`: a hand-assembled
    `<dir>/__pycache__/<stem>.<tag>.pyc` looks in the wrong directory
    under a cache prefix. One queried path covers every execution lever
    the run has -- `PYTEST[0]` is the process that imports the mutant,
    threads share it, and process workers spawn from the same
    `sys.executable` (so the same tag, with `PYTHONDONTWRITEBYTECODE`
    inherited either way).

    An interpreter that is missing or cannot answer is a `SystemExit`
    naming it, not a guess: the probe prefers aborting over reporting,
    and the raw FileNotFoundError this replaces was how a worktree
    without a `.venv` used to die mid-run.
    """
    resolved = Path(path).resolve()
    key = (PYTEST[0], str(resolved))
    if key not in _SUBPROCESS_CACHE_PATHS:
        query = ("import importlib.util, sys; "
                 "print(importlib.util.cache_from_source(sys.argv[1]))")
        try:
            r = subprocess.run([PYTEST[0], "-c", query, str(resolved)],
                               capture_output=True, text=True, timeout=60)
        except FileNotFoundError:
            raise SystemExit(
                f"ABORT: the test interpreter {PYTEST[0]} does not exist, so "
                f"no verdict it produced could be vouched for. Create the "
                f".venv (pip install -e '.[dev]') or point PYTEST at the "
                f"interpreter that should run the tests.") from None
        answer = r.stdout.strip()
        if r.returncode != 0 or not answer:
            raise SystemExit(
                f"ABORT: the test interpreter {PYTEST[0]} could not name its "
                f"bytecode cache for {resolved} (exit {r.returncode}: "
                f"{r.stderr.strip() or 'no output'}). A guard pointed at a "
                f"guessed path is #201's silent no-op again, so the probe "
                f"stops instead.")
        _SUBPROCESS_CACHE_PATHS[key] = Path(answer)
    return _SUBPROCESS_CACHE_PATHS[key]

def assert_fresh(path, cache):
    """Stop the run if a cached `.pyc` would be reused for what was just written.

    CPython validates a timestamp-based `.pyc` against the source's
    `(mtime, size)` pair, with the mtime truncated to whole seconds.
    Both halves collide far more easily here than they look.

    *Size* collides by construction. The probe writes `ast.unparse`
    output, so consecutive mutants differ from each other only by the
    mutation delta -- and two `Eq -> NotEq` flips, two dropped `not`s,
    or two `True -> False`s differ by exactly zero bytes. Comparison
    flips dominate most modules, so equal-size neighbours are the norm,
    not the exception.

    *Seconds* collide whenever a run is quick. `crypto.py`'s tests take
    0.6s, so consecutive writes land in the same second about half the
    time; the 15s runs on `privacy.py` are what makes this intermittent
    rather than constant.

    When both match, the interpreter hands back the *previous* mutant's
    bytecode and pytest never sees the mutation being scored. The verdict
    is then about code that was not there. It is not biased toward
    survivors either: it repeats the neighbour's verdict, so it invents
    a coverage gap or hides one depending on which way the neighbour
    went (#174).

    `PYTHONDONTWRITEBYTECODE=1` in `run()` is the fix -- not because it
    stops a `.pyc` being *read* (it does not) but because it stops each
    run planting the trap the next one falls into. A cache left behind by
    something else cannot spring it: its recorded mtime is in the past
    and the probe's writes are always later.

    This asserts that rather than trusting it, and aborts instead of
    printing a verdict. A probe that cannot tell "the suite did not
    notice" from "the suite was never shown" is exactly the silent
    failure it exists to hunt for.

    `cache` is required, with no default, on purpose. The cache that
    matters belongs to the interpreter that RUNS the tests -- callers
    pass `subprocess_cache_path(path)` -- and this function must never
    derive one itself: a `cache_from_source` fallback resurrects the
    parent interpreter's tag, and a pyenv shim launching the probe
    beside the hardcoded `.venv` is the routine case, not the edge. A
    guard built that way inspects a `.pyc` the subprocess never reads,
    finds nothing, and returns -- never a false abort, always a false
    pass, so no run would ever reveal the check had been off (#201).
    """
    cache = Path(cache)
    if not cache.exists():
        return
    head = cache.read_bytes()[:16]
    if len(head) < 16:
        return
    # Two deliberate divergences from `_validate_timestamp_pyc`: the
    # `& 0xFFFFFFFF` masking on both halves is omitted, which matters in
    # 2106 or on a 4GB source, and the magic number CPython checks first
    # is not, which matters only across a pre-release magic bump inside
    # one cache tag. Neither can reach this script.
    flags, mtime, size = struct.unpack("<III", head[4:16])
    if flags & 0b1:
        # Hash-based `.pyc`. CHECKED_HASH is verified against the source's
        # own hash and cannot go stale; UNCHECKED_HASH is trusted blind,
        # which is worse than the timestamp case, not better.
        if flags & 0b10:
            return
        why = "an UNCHECKED_HASH .pyc is reused without looking at the source"
    else:
        st = path.stat()
        if not (mtime == int(st.st_mtime) and size == st.st_size):
            return
        why = (f"its recorded (mtime={mtime}, size={size}) matches the file "
               f"just written")
    raise SystemExit(
        f"ABORT: {cache} is stale bytecode that CPython would reuse -- {why}. "
        f"pytest would execute the previous mutant and the verdict would not "
        f"be about this mutation. See assert_fresh() and #174.")

def run(tests):
    # PYTHONDONTWRITEBYTECODE rather than `-B`: `run_parallel()` spawns
    # worker processes, and a grandchild that writes a `.pyc` plants the
    # same trap the parent avoided. The variable is inherited
    # unconditionally; an interpreter flag reaches a spawned child only
    # through `_args_from_interpreter_flags`, which is not a promise this
    # script should rest on. `os.environ` is copied, not replaced -- a bare
    # `env=` dict loses PATH and the failure looks like a killed mutant.
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run(PYTEST + tests, cwd=REPO, capture_output=True, text=True,
                       timeout=900, env=env)
    return r.returncode == 0

def main():
    argv = sys.argv[1:]
    override = int(argv[0]) if argv and argv[0].isdigit() else None
    rest = argv[1:] if argv and argv[0].isdigit() else argv
    if len(rest) >= 2:
        targets = {rest[0]: (list(rest[1:]), override if override is not None else 30)}
    else:
        targets = TARGETS

    for mod, (tests, budget) in targets.items():
        if override is not None:
            budget = override
        path = REPO / mod
        original = path.read_text()
        total = count_ops(original)

        # Control: unparsed-but-unmutated must still pass, or every
        # result below is an artefact of the harness rather than a finding.
        path.write_text(ast.unparse(ast.parse(original)))
        try:
            assert_fresh(path, subprocess_cache_path(path))
            ok = run(tests)
        finally:
            path.write_text(original)
        print(f"\n### {mod}  ({total} mutation sites, sampling {budget})")
        print(f"    control (unparsed, unmutated): {'PASS' if ok else 'FAIL -- results unusable'}")
        if not ok:
            continue
        # A module with no sites was NOT MEASURED. Left to speak for
        # itself, "0 survived" sits in a table next to "11/36" and reads
        # as the healthiest row (#106).
        if total == 0:
            print("    => NOT MEASURED: no operator in this probe can see "
                  "this module. This is not a clean result -- it is the "
                  "absence of one. Add an operator that reaches it.")
            continue

        step = max(1, total // budget)
        survived, killed = [], 0
        for i in range(0, total, step):
            m = Mut(i); tree = m.visit(ast.parse(original))
            if m.desc is None: continue
            try:
                path.write_text(ast.unparse(ast.fix_missing_locations(tree)))
                # Not caught by the `except Exception` below on purpose: a
                # stale cache invalidates the whole run, not one sample.
                # (`subprocess_cache_path`'s aborts ride the same exit.)
                assert_fresh(path, subprocess_cache_path(path))
                t0 = time.time()
                if run(tests):
                    survived.append(m.desc)
                    print(f"    SURVIVED  {m.desc}  ({time.time()-t0:.0f}s)")
                else:
                    killed += 1
            except Exception as e:
                print(f"    skipped   {m.desc}: {type(e).__name__}")
            finally:
                path.write_text(original)
        n = killed + len(survived)
        print(f"    => killed {killed}/{n}, SURVIVED {len(survived)}/{n}")

    # What the run did not measure, last, where the tail of a long run is
    # read. Not on a single-module CLI run, which says what it measured.
    # Deliberately unchecked here against TARGETS or the filesystem:
    # tests/test_mutation_probe_targets.py does that, and tests patch
    # TARGETS and REPO under main() with a ledger that matches neither.
    if len(rest) < 2:
        print(f"\n### NOT PROBED ({len(NOT_PROBED)} modules -- see NOT_PROBED "
              f"in scripts/mutation_probe.py)")
        for mod, why in NOT_PROBED.items():
            print(f"    {mod} -- {why}")

if __name__ == "__main__":
    main()
