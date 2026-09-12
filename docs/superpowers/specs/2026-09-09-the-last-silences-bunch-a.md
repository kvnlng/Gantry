# The Last Silences, bunch A — implementation brief

**Date:** 2026-09-09
**Milestone:** v0.9.5 — The Last Silences
**Issues:** #386 (the loader's dtype table), #388 (a shipped resource that goes missing quietly), #392 (an empty sequence that vanishes at ingest)
**Base:** `main` at `1ab5f1a` (release: 0.9.4)
**Author:** architect (bunch A). Written for a TDD developer.
**Status:** design brief. No production code was changed while writing it.
**Superseded in part:** §0.3, §2.2 (the helper signature), §2.3, §2.4, §2.5 T1
and T6, §5 (#388), all five of §7, and §11.6 items 2 and 3 — by owner rulings
and closing reviews of 2026-09-09, recorded in **§11 Amendments**, which also
adds work to §1.4, §1.5 and §3.6 and adds **§11.13 (#404)**, a defect found by
§11.6's own step P1. Read §11 before implementing; where it disagrees with the
text above, §11 wins.
**Superseded in part (later):** #449 (v0.9.6) — the clause in §11.13.1 that
`export(verify_readback=True)` "would not have caught it either, and cannot be
made to … `_verify_readback` never decodes pixels", and review item 19 in
§11.13 ("`_verify_readback` cannot catch this family … it never decodes
pixels"). Since #449 the readback decodes every written file and compares it
bit for bit with the samples written. Both clauses are struck in place.
**Superseded in part (later):** #495 (v0.9.6) — §12.5's clause that "a bare
session already audits against an empty policy and says so with its own
`"PHI Scan Warning: No PHI tags defined"` — a different question, and not a
silence". A bare session now applies the floor policy (`FLOOR_POLICY`), and the
empty policy was the silence: Study ID, Station Name and Institution Name
reached the export under a PASS. #495 also deleted `resources/phi_tags.json`,
so this brief's `phi_tags.json` arm of #388 no longer exists. The clause is
struck in place.

**Superseded in part, by implementation:** §11.12's "no 3.14.7t run is
required" (struck in place — #404 added an unguarded module-scope C-extension
import whose encoder runs inside export workers, so the gate is the
measurement); §11.13.3's support matrix (no row for a 16-bit **multi-sample**
frame, which the old encoder refused before the question could be asked) and
owner ruling 2's stated scope of "32/64-bit only" (the refused set is wider —
measured, a 16-bit colour frame encodes exactly and produces a file this
project cannot re-ingest); §2.5 T6's second arm (factually wrong — `audit()`
never reaches `load_phi_config()`); and §11.5's stated ordering of the dtype
check and the byte-order normalization (inverted, for cost). All four are
recorded with their measurements in **§12 Implementation addendum**, §12.5 and
§12.6.

The three are one bunch because they are the same failure at the ingest/export
boundary: the pipeline drops or corrupts something and the session's own
accounting says nothing happened. In every case below the *speech* is the
defect — a wrong dtype with a `wrote 1 of 1` row beside it, an empty
`redaction_zones` list with no log line, an absent element with `losses == []`.
Every test in this brief must assert the speech, not only the behaviour.

---

## §0 How everything below was measured

### 0.1 The project `.venv` is broken. Read this before you run anything.

`.venv/pyvenv.cfg` names `/Users/kevin/.pyenv/versions/3.14.6`, and pyenv no
longer has 3.14.6:

```
cat .venv/pyvenv.cfg
    home = /Users/kevin/.pyenv/versions/3.14.6/bin
    version = 3.14.6
ls /Users/kevin/.pyenv/versions/
    3.12.14
    3.14.7
    3.14.7t
.venv/bin/python -VV
    zsh: no such file or directory: .../.venv/bin/python
```

`.venv/bin/python` is a dangling symlink; the *site-packages* are intact and are
ABI-compatible with 3.14.7 (`cp314` extension modules). The task brief's pyenv
list (3.12.13 / 3.14.6 / 3.14.6t / 3.14.7 / 3.14.7t) is stale in both
directions. Until someone rebuilds the venv, this is the rig — and it satisfies
CLAUDE.md's runbook, because `PYTHONPATH` puts the worktree ahead of the
editable finder and `isocenter.__file__` is printed and read:

```
PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=<worktree>:/Users/kevin/Developer/Isocenter/.venv/lib/python3.14/site-packages \
  /Users/kevin/.pyenv/versions/3.14.7/bin/python -u -c \
  'import isocenter,sys;print(isocenter.__file__);print(sys._is_gil_enabled())'

  .../worktrees/agent-a2aa736e2121ec77b/isocenter/__init__.py
  True
```

The same line with `-m pytest -v` runs the suite (verified:
`tests/test_private_tag_empty_value_roundtrip.py` gives `51 passed in 9.52s`).
Never `-q` piped.

**One trap the probes hit and you will too:** a standalone probe script that
opens a `Session` must have an `if __name__ == '__main__':` guard. `spawn`
re-imports `__main__` in every worker, and a script that ingests at module
scope re-enters ingest in each child and dies as
`concurrent.futures.process.BrokenProcessPool` with no child traceback.

### 0.2 Versions every number below was measured on

CPython **3.14.7** (GIL build, `sys._is_gil_enabled() is True`), numpy 2.5.2,
pydicom 3.0.2, macOS (darwin 25.6.0), worktree
`.claude/worktrees/agent-a2aa736e2121ec77b` at `1ab5f1a`.

**No 3.14.7t run is required for this bunch, and that is a claim, not an
omission.** Nothing designed here touches `parallel.py`, the sidecar gate, the
pass-lock, or any lock: the change list adds and moves **zero** `write_frame`
sites (the count stays **six**), so `tests/test_sidecar_gate_order.py`'s
seventh-writer AST detector and its recording-proxy order assertions stay green
*by design*, and the order pass-lock to `_sidecar_gate` to `_pixel_swap_lock`
to sqlite is untouched. If you find yourself adding a writer, stop: the design
has changed and this brief no longer describes it.

### 0.3 The probes

Scripts live in the session scratchpad subdirectory named after this worktree
(`.../scratchpad/agent-a2aa736e2121ec77b/`):

| Script | Answers |
| --- | --- |
| `probe_386_dtype.py` | what dtype comes back after a round trip, per source dtype and per `set_pixel_data` dtype |
| `probe_386_export.py` | what the *exported file* and the *audit log* say about it |
| `probe_392_empty_sq.py` | does a zero-item `SQ` in the source reach the graph, the store, the file |
| `probe_392_hops.py` | with an empty sequence planted in the graph, which hop drops it |
| `probe_392_implicit.py` | the adjacent Implicit-VR population, so it is not mistaken for this one |
| `probe_388_resource.py` | what a session does when the shipped resource is absent |

Their outputs are quoted inline below. ~~Re-run them before you start; a
brief's numbers are only as good as the tree they were taken on.~~
**[Superseded by §11.7: the six scripts are committed at
`docs/superpowers/specs/2026-09-09-the-last-silences-bunch-a/`, beside this
file. Run them from there; the scratchpad path above is session-scoped and is
gone.]**

---

## §1 #386 — the dtype table, and the signedness nothing recorded

### 1.1 Diagnosis, re-measured

The issue's numbers came from a reviewer's probe and had never been
re-measured. They reproduce, with two corrections.

**Ingest arm** (source file to sidecar to `save()` to `close()` to reopen to
`get_pixel_data()`), one 4x4 frame per row:

| Source dtype | BitsAllocated | PixelRepresentation | pydicom `pixel_array` | reloaded from the sidecar |
| --- | --- | --- | --- | --- |
| uint8 | 8 | 0 | uint8 | **uint8**, values equal |
| int8 | 8 | 1 | int8 | **int8**, values equal |
| uint16 | 16 | 0 | uint16 | **uint16**, values equal |
| int16 | 16 | 1 | int16 | **int16**, values equal |
| uint32 | 32 | 0 | uint32 | **`RuntimeError`** |
| int32 | 32 | 1 | int32 | **`RuntimeError`** |

```
RELOAD RAISED: RuntimeError Pixel Loader failed for 1.2.826.0....007: Integrity
Error: frame for 1.2.826.0....007 holds 32 samples; geometry (4, 4) needs 16
(one trailing pad byte is tolerated, nothing else)
```

That is #373's bound doing exactly what its CHANGELOG entry says it does, on
exactly the population that entry names. Confirmed on 0.9.4, not inherited.

**Setter arm** (`set_pixel_data(arr)` on an ingested uint8 instance, then
`save()`, `close()`, reopen, `get_pixel_data()`):

| Array handed to `set_pixel_data` | after the call: carrier / `0028,0100` / `0028,0103` | reloaded | values |
| --- | --- | --- | --- |
| int16 `[-8,-7,-6,-5,...]` | `None` / 16 / **0** | **uint16** | `[65528, 65529, 65530, 65531]` — **wrong** |
| int8 | `None` / 8 / **0** | **uint8** | `[248, 249, 250, 251]` — **wrong** |
| bool | `None` / 8 / 0 | **uint8** | equal (`True == 1`) — **dtype lost, values not** |
| uint32 | `None` / 32 / 0 | **`RuntimeError`** | — |
| float32 | **`float32`** / 32 / 0 | float32 | equal |

**The export speech, which is the part that makes this a Last Silences item.**
An int16 array set in memory, saved, exported:

```
=== B: ingested uint8, set_pixel_data(int16), exported
  files written: 1
  file BitsAllocated: 16 PixelRepresentation: 0
  file pixel_array dtype: uint16 first 4: [65528, 65529, 65530, 65531]
  file values == intended: False
  AUDIT EXPORT: DICOM export to .../out: wrote 1 of 1 planned instances from 1 patients.
```

The file on disk declares `PixelRepresentation 0` beside int16 bytes, a reader
gets 65528 where the caller wrote -8, and the audit log says the export wrote
everything it planned. No `DATA_LOSS` row, no `ERROR` row, no warning.

The 32-bit ingest case, by contrast, is loud all the way out:

```
=== A: ingested uint32 (RTDOSE-shaped), untouched, exported
ERROR: Export failed for ...: Pixel Loader failed for ...: Integrity Error: ...
WARNING: Export finished with failures: 0 of 1 instances exported.
isocenter.io_handlers.ExportError: Export to .../out wrote 0 of 1 planned
instances; 1 failed and nothing reached disk.
```

### 1.2 Two corrections to the issue text

1. **The issue frames gap 1 as a read-side gap and gap 2 as a write-side one.
   The 32/64-bit gap is on both paths**: `set_pixel_data(uint32_array)` raises
   the same `RuntimeError` on reload, because the loader has no 32-bit arm
   whatever put the bytes there. One table fix serves both.
2. **The signedness loss is a WRITE-side defect, and the loader cannot fix
   it.** `set_pixel_data` writes `BitsAllocated` from `array.itemsize` and
   writes *nothing* about signedness — not `PixelRepresentation`, not the
   dtype carrier. There is nothing recorded for the loader to read. The same
   omission is what makes the exported file wrong, because
   `_export_instance_worker` writes
   `ds.PixelRepresentation = inst.attributes.get("0028,0103", 0)`
   (io_handlers.py:3207). Fix the write side and the file and the sidecar are
   both corrected by one edit; fix only the loader and the file keeps lying.

### 1.3 Design decision

**D1. `set_pixel_data` writes `PixelRepresentation (0028,0103)` from the
array's dtype kind, through `_write_int_if_changed`.** Kind `'i'` gives 1;
`'u'` and `'b'` give 0; kind `'f'` is **left alone** (see below). This is the
same argument the method's own comment already makes for `BitsAllocated`: the
export writes `arr.tobytes()`, so a descriptor that disagrees with the array
cannot be honoured, and "the attributes win" is not one of the options. Mirror
the existing `BitsAllocated` debug log when it corrects a previously declared
value.

Why floats are excluded: PS3.5 Section 8.2 says Bits Stored, High Bit and Pixel
Representation *shall not be present* beside a float pixel element, and
io_handlers.py:3082 already deletes all three on that arm. Writing a 0 there
would be a write the export immediately deletes.

**D2. The dtype carrier widens by exactly one dtype: `bool`.** Rename
`FLOAT_DTYPE_NAMES` (pixel_geometry.py:81) to `SIDECAR_DTYPE_NAMES` and make it
`frozenset({"float16", "float32", "float64", "bool"})`. In `set_pixel_data`,
record the name when `array.dtype.kind in ('f', 'b')` and `pop` otherwise —
the existing write-*and*-delete discipline is unchanged and still load-bearing.

**Rejected: the issue's own proposal, "carry the dtype name for every
non-default dtype".** Once D1 lands, `BitsAllocated` plus `PixelRepresentation`
name every integer dtype exactly. A carrier recorded *as well* is a second
answer to a question the descriptor pair already answers — precisely the field
the #210 comment condemns ("a field nothing reads is a second answer waiting to
disagree with this one") — and it would be the *authoritative* one, so a graph
whose descriptors were later corrected would decode against the stale carrier.
`bool` is the one dtype no DICOM descriptor can name (numpy `bool_` and `uint8`
both declare `BitsAllocated 8`, `PixelRepresentation 0`), which is the same
argument #183 makes for float16 and the reason the carrier exists at all.

**D3. The loader's integer arm becomes a `BitsAllocated`-keyed table with the
old rule as its fallback.** At module scope beside the other tables:

```
_INTEGER_DTYPE_BY_BITS = {
    8:  (np.uint8,  np.int8),
    16: (np.uint16, np.int16),
    32: (np.uint32, np.int32),
    64: (np.uint64, np.int64),
}
```

and in `SidecarPixelLoader.__call__` (io_handlers.py:3513), when no carrier
applies:

```
unsigned, signed = _INTEGER_DTYPE_BY_BITS.get(
    self.bits, (np.uint16, np.int16) if self.bits > 8 else (np.uint8, np.int8))
dt = signed if self.pixel_representation == 1 else unsigned
```

**The fallback is not tidiness, and a dict-only rewrite is a regression.**
`BitsAllocated 1` is a real ingested population — a binary Segmentation, whose
bytes pydicom unpacks to one uint8 per pixel — and `BitsAllocated 12` arrives
as uint16. Both must keep decoding as they do today.
`tests/test_pixel_geometry_pipeline.py::test_export_writes_bits_allocated_the_array_actually_has`
(fixture at `:759`, `ds.BitsAllocated = 1`) is the existing guard; add an
explicit loader-level one anyway (T3 below), because that test asserts the
export's width, not the dtype the loader produced.

**D4. No new stored field, and no schema column. This is a deliberate
departure, with the precedent read first.** The precedent is
`SqliteStore._add_missing_columns` (persistence.py:1014): `phi_status`,
`loss_scope` (#146), `element_tag` (#167), `source_path` (#238),
`date_shifted` (#182), `value_count` (#328) — each guarded by a
`PRAGMA table_info` column list rather than a version number, each with a
comment saying what NULL means, and **none back-filled**, on the stated ground
that answering for rows that never had an answer is the fabrication the column
exists to stop.

No column is needed here because #183 already chose the other channel: the
carrier rides `attributes_json` as an underscore key (`_merge` skips
`t.startswith("_")`), which #183 weighed against a schema change and preferred.
Every `SidecarPixelLoader` that hydration builds is built with `instance=`
(persistence.py:1129), so it reads the hydrated `attributes` — the carrier and
both descriptors arrive together, for old and new rows alike. An old row
carries no carrier and reaches the extended table, where it decodes *better*
than today (a 32-bit frame stops raising).

The one thing a column could have bought is the one thing no migration can: a
store written by a **pre-fix** `set_pixel_data(int16_array)` holds int16 bytes
beside `0028,0103 = 0`, and nothing anywhere recorded the signedness. Those
frames still reload as uint16 after this fix. Say so in the CHANGELOG in
`value_count`'s own direction: NULL is ungraded, not guessed.

**D5. `write_frame` sites: unchanged at six.** No writer added, none moved, no
lock touched. `tests/test_sidecar_gate_order.py` must stay green; if it goes
red, you have changed the design.

### 1.4 Change list

| File | Function / line | Change |
| --- | --- | --- |
| `isocenter/pixel_geometry.py` | `FLOAT_DTYPE_NAMES` (`:81`) | rename to `SIDECAR_DTYPE_NAMES`, add `"bool"`; update the comment block above `PIXEL_DTYPE_ATTR` (`:55`-`:75`), which currently says the carrier is about floats. `FLOAT_DTYPE_BY_ELEMENT` is untouched — it maps the two float *elements* at ingest and is a different question. |
| `isocenter/entities.py` | import at `:12`, `set_pixel_data` (`:1160`-`:1169`) | import the renamed constant; `name = array.dtype.name if array.dtype.kind in ('f', 'b') else None`. |
| `isocenter/entities.py` | `set_pixel_data`, beside the `BitsAllocated` write (`:1176`-`:1183`) | new `_write_int_if_changed("0028,0103", ...)` for kinds `i`/`u`/`b`; skipped for `f`, with the PS3.5 8.2 reason in a comment. |
| `isocenter/io_handlers.py` | import at `:163`; new `_INTEGER_DTYPE_BY_BITS` near `_NESTED_GEOMETRY_TAGS` (`:318`) | the table. |
| `isocenter/io_handlers.py` | `SidecarPixelLoader.__call__` (`:3505`-`:3520`) | carrier check against the renamed allow-list; table plus fallback for the integer arm. Keep the #373 byte check and the #343/#373 bounds exactly where they are. |
| `isocenter/io_handlers.py` | comment at `~:3190` | it states `SidecarPixelLoader` buckets dtype as `uint16 if bits > 8 else uint8`. That becomes false for {8,16,32,64}; correct it in place and keep the reason (the 1-bit Segmentation case is *why* the fallback survives). |
| `isocenter/builders.py` | `:104` | **no change** — verified: it delegates to `Instance.set_pixel_data`. It is not a seventh dtype site. |
| `isocenter/entities.py` | `set_pixel_data`, new first statement | *(§11.5)* byte-order normalization, then the positive accepted-dtype check, **before any tag is written**. |
| `isocenter/entities.py` | `set_pixel_data` docstring | *(§11.9)* the "Updates tags" list gains `PixelRepresentation (0028,0103)`; a `Raises: ValueError` section is added. The API reference is generated from docstrings, so this ships. |
| `isocenter/io_handlers.py` | `_compress_j2k` fallback branch (`~:3344`) | *(§11.6)* a **third** copy of `uint16 if bits > 8 else uint8`, found after this brief was written. Same table, same legacy fallback, and it must honour `PixelRepresentation`. |
| `CHANGELOG.md` | new 0.9.5 entry | §5, plus the separate breaking entry §11.5 requires. |

### 1.5 TDD test plan

New file `tests/test_pixel_dtype_roundtrip.py`. Add it to
`scripts/mutation_probe.py`'s `TARGETS` for `io_handlers.py` **and** to
CLAUDE.md's module-to-tests table — `tests/test_mutation_probe_targets.py` goes
red otherwise, and that is the guard's whole point.

Each test names the single production edit that turns it red.

- **T1 `test_a_32_bit_integer_frame_reloads_as_the_dtype_it_was_ingested_as`**,
  parametrized `(uint32, pixrep=0)` and `(int32, pixrep=1)`. Ingest, `save()`,
  `close()`, reopen, `get_pixel_data()`. Assert
  `got.dtype == np.dtype("int32")` — an absolute dtype, never
  `got.dtype == arr.dtype` of an array derived in the same test — **and**
  `got.tolist()` against a literal. *Red when:* the `32` row is removed from
  `_INTEGER_DTYPE_BY_BITS` (reverts to the `RuntimeError` measured above).
- **T2 `test_a_64_bit_integer_frame_reloads_as_the_dtype_it_was_ingested_as`**.
  Measured: pydicom 3.0.2 writes and reads `BitsAllocated 64` fine
  (`pixel_array dtype uint64`), so this is a full-pipeline test, not a
  loader-level one. *Red when:* the `64` row is removed.
- **T3 `test_a_one_bit_frame_still_reloads_as_uint8`**. The fallback. Build the
  binary-Segmentation fixture the way
  `tests/test_pixel_geometry_pipeline.py:_write_binary_segmentation` does
  (`:735`), and assert the **dtype** (`np.uint8`) plus the frame count, at
  `get_pixel_data()`. *Red when:* the fallback is replaced by a bare
  `_INTEGER_DTYPE_BY_BITS[self.bits]` (`KeyError`) or by a raise.
- **T4 `test_an_int16_array_set_in_memory_records_its_signedness`**. Immediately
  after `set_pixel_data`, assert `inst.attributes["0028,0103"] == 1` and
  `inst.attributes["0028,0100"] == 16`. *Red when:* the `0028,0103` write is
  removed. This is the smallest test in the plan and the one that pins the root
  cause.
- **T5 `test_an_int16_array_set_in_memory_reloads_signed`**. `save()`,
  `close()`, reopen; assert `dtype == np.dtype("int16")` **and**
  `got.tolist() == [[-8, -7, -6, -5], ...]`. *Red when:* the `0028,0103` write
  is removed (today: uint16 and 65528).
- **T6 `test_the_exported_file_declares_the_signedness_of_an_array_set_in_memory`**.
  Export, `dcmread`, assert `ds.PixelRepresentation == 1`,
  `ds.BitsAllocated == 16`, and `ds.pixel_array.tolist() == [[-8, ...]]`.
  **This is the milestone test**: today the file is wrong and the audit row
  says `wrote 1 of 1 planned instances`. Call `flush_audit_queue()` **before**
  reading the log (§11.8 — unflushed, the `SELECT` returns `[]` and a pass is
  indistinguishable from a failure), then assert that row is still present and
  still says 1 of 1 — the point is that the sentence becomes *true*, not that
  it disappears. *Red when:* the `0028,0103` write is removed.
- **T7 `test_a_bool_array_set_in_memory_reloads_as_bool`**. Assert
  `got.dtype == np.dtype(bool)`. **Do not assert `np.array_equal`** — measured,
  a bool array already round-trips to uint8 with `array_equal` True, so a
  values-only test passes on unfixed code. *Red when:* `"bool"` is dropped from
  `SIDECAR_DTYPE_NAMES`, or the kind test narrows back to `== 'f'`.
- **T8 `test_an_integer_array_deletes_the_dtype_carrier_a_float_array_left`**.
  Set float32, assert the carrier is present; set int16 on the same instance,
  assert `PIXEL_DTYPE_ATTR not in inst.attributes`, then reload and assert
  `int16` (not float32). *Red when:* the `pop` becomes a no-op. This pins the
  half of #183 the widening could most easily break.
- **T9 `test_a_uint32_array_set_in_memory_reloads_unsigned`**. Covers the setter
  half of the 32-bit gap that the issue does not mention. *Red when:* the `32`
  row is removed.

Regression watch (run these; they should stay green):
`tests/test_pixel_geometry_pipeline.py`, `tests/test_float_pixel_data_export.py`,
`tests/test_export_pixels.py`, `tests/test_redaction_export.py`,
`tests/test_redaction_rgb.py`, `tests/test_planar_configuration_roundtrip.py`,
`tests/test_nested_pixel_carriage.py`, `tests/test_sidecar_gate_order.py`.
`_write_int_if_changed` means the new `0028,0103` write is a no-op on every
frame whose signedness already matches, so the redaction path should not dirty
one extra entity — check that, because `tests/test_services.py` counts writes.

### 1.6 Ordering

D1 (the `0028,0103` write) is independent of D2/D3 and fixes T4/T5/T6 on its
own. D3 (the table) fixes T1/T2/T9. D2 (bool) fixes T7. Land them as three
commits in that order if you want the bisect to read cleanly; the tests
partition exactly.

---

## §2 #388 — a shipped resource that goes missing quietly

### 2.1 Diagnosis, measured

`_load_redaction_knowledge_base` (session.py:150) and `_load_ctp_rules`
(session.py:164) both `return []` when the file is not there. With
`isocenter.session.RESOURCES_DIR` pointed at an empty directory:

```
direct call, real dir: 2 machines
direct call, missing dir: []
direct call ctp, missing dir: []
```

Through the public API — ingest one instance whose `DeviceSerialNumber` is
`SN-SCANNER-01`, a serial the shipped knowledge base names, then
`create_config()`:

```
--- WITH the resource, scaffolded config:
machines:
- serial_number: SN-SCANNER-01
  model_name: Revolution CT
  # Redact burned-in Patient Name top-left
  redaction_zones: [{roi: [50, 100, 50, 200], note: 'RowStart, RowEnd, ColStart, ColEnd'}]
--- WITHOUT the resource, scaffolded config:
machines:
- manufacturer: GE MEDICAL SYSTEMS
  model_name: Revolution CT
  serial_number: SN-SCANNER-01
  redaction_zones: []
--- configs identical: False
--- log lines while the resource was missing: []
--- audit rows while missing: []
```

Zero log lines at `DEBUG` on the `Isocenter` logger, zero audit rows, and a
config that instructs the pipeline to redact nothing. The session then runs,
redacts nothing by serial, and reports a clean run.

### 2.2 Design decision: raise, and raise once, from one helper

**Raise, not warn**, on the #400 reasoning quoted in that issue's own ruling: a
warning in front of a run that then succeeds is a line nobody reads, and there
is nothing to *annotate* because a missing shipped resource is never a correct
state. CLAUDE.md's degrade-gracefully rule is about the optional extras (`ocr`,
`nlp`, `docs`); a shipped package resource is the opposite kind of thing.
`setup.py`'s `package_data` promises it and `publish.yml` refuses to publish a
wheel without it — this is the runtime half of a promise CI already makes.

**On "match #357's exception shape": #357 has no exception to match, and this
brief says so rather than inventing a lineage.** Its CHANGELOG entry states it
outright — "No config changes, no behaviour changes, **no exception changes**:
no loader ever opened the file, so no call can notice its absence." #357
deleted an *unread* resource; #388 is about a read one. The decision this must
read as one with is **#400's** ("refuse, not warn"), and the exception should
be shaped the way #400's ruling shapes its own: naming the things the reader
needs, plus the remedy.

**The exception is `RuntimeError`.** Not a new public class — `__all__` is
frozen at five names (`docs/api/stability.md:25`) and adding one is an owner
call for no gain. Not `FileNotFoundError`: `ConfigLoader._load_yaml` already
raises that for a *user's* config file, which is a different failure with a
different remedy, and a caller writing `except FileNotFoundError` around
`load_config` would silently swallow "your install is broken". There is a
second, sharper reason: `_load_redaction_knowledge_base`'s existing handler is
`except (OSError, json.JSONDecodeError)`, and `FileNotFoundError` **is** an
`OSError` — a raise that drifted inside the `try` would be caught and turned
straight back into `return []`, with every test still green. Raise **before**
the `try`, and see T3 for the test that kills that drift.

**One helper, not three call sites.** New in `config_manager.py` — private
wholesale per `docs/api/stability.md:242`, and already imported by
`session.py`, so there is no import cycle:

**[Superseded by §11.3: the signature takes a third parameter,
`consequence: str`, which supplies the clause this message leaves as a
placeholder. A refusal must be accurate, not generic.]**

```
def require_package_resource(directory: str, basename: str) -> str:
    """The path to a resource this package ships, or a refusal."""
    path = os.path.join(directory, basename)
    if not os.path.exists(path):
        raise RuntimeError(
            f"Isocenter's shipped resource {basename} is missing from this "
            f"installation (looked in {path}). setup.py packages it and "
            f"publish.yml refuses to release a wheel without it, so its "
            f"absence is a broken install rather than a configuration "
            f"choice -- reinstall isocenter. Continuing would have "
            f"<what is lost>, and reported a clean run.")
    return path
```

`directory` is a **parameter**, not read from a module global inside the
helper: `session.RESOURCES_DIR` is what tests monkeypatch, and a helper that
closed over its own copy would make every such test pass against the real tree.

Call sites:

- `_load_redaction_knowledge_base`:
  `path = require_package_resource(RESOURCES_DIR, "redaction_rules.json")`,
  replacing the `os.path.join` plus `if not exists: return []`. The
  `"redaction_rules.json"` string literal must survive as a literal in
  `session.py` — the packaging test
  `test_every_shipped_resource_is_named_by_the_package` walks the AST for
  `ast.Constant` basenames, and an f-string or a `Path` join makes the resource
  read as unnamed.
- `_load_ctp_rules`: the YAML branch keeps its `os.path.exists`, because
  `ctp_rules.yaml` is *deliberately* not shipped and its absence is the
  ordinary case. Only the JSON fallback goes through the helper. Keep both
  literals.
- The existing `except (OSError, ...)` blocks stay: a resource that is present
  but unreadable or malformed is a different failure and its warning-plus-`[]`
  is out of scope here. Say that in the CHANGELOG so a reviewer does not read
  the silence as an oversight.

### 2.3 Scope: the third loader, ~~recommended and separable~~ **ruled IN SCOPE (§11.1)**

`ConfigLoader.load_phi_config` (config_manager.py:209) has the identical shape
for `phi_tags.json`, and it is the **louder** silence: `publish.yml`'s own
comment says a wheel without it "audited against an empty PHI tag list and
reported clean", and `tests/test_packaging_contract.py:465`'s docstring
currently records the degrade as intended behaviour — "When it is absent,
`load_phi_config()` returns `{}` rather than raising". Leaving it on the old
shape is two spellings for one behaviour, in the same PR that argues there
should be one.

Recommendation: take it, in the same PR, through the same helper, and update
that docstring — a test whose prose asserts the opposite of the code is the
next silence. ~~Marked separable because it is a strictly larger blast radius
(`load_phi_config` is called on the audit path, not only the scaffold path) and
because #388's text names only session.py's two. **This is an owner call; see
§7.**~~ **[Ruled in scope by the owner — §11.1, which also names the module-level
constant the hoist needs, the six call sites the raise now reaches, and the
packaging-contract docstring that must be corrected in the same commit.]**

### 2.4 Change list

| File | Function | Change |
| --- | --- | --- |
| `isocenter/config_manager.py` | new `require_package_resource(directory, basename)` | the helper above. |
| `isocenter/session.py` | `_load_redaction_knowledge_base` (`:150`) | helper call before the `try`; delete `return []`. |
| `isocenter/session.py` | `_load_ctp_rules` (`:164`) | YAML `exists` check kept; JSON path through the helper; delete `return []`. |
| `isocenter/config_manager.py` | new module-level `RESOURCES_DIR` | *(§11.1)* hoist the `os.path.dirname(__file__)`/`resources` join out of `load_phi_config`'s body, so a test has something to monkeypatch. |
| `isocenter/config_manager.py` | `load_phi_config` (`:207`-`:216`) | ~~*(separable, §2.3)*~~ **in scope (§11.1)** — helper call; delete `return {}`. |
| `tests/test_packaging_contract.py` | `:465` docstring **and `:327` comment** | ~~*(with the separable leg)*~~ **required (§11.1.3)** — the sentence becomes false while the test stays green. |
| `tests/test_config.py` | `test_phi_config_default` (`:56`) | *(§11.2)* keep the test and the assertion; replace the "it shouldn't crash" comment. It is the positive control. |
| `docs/` | — | check `docs/configuration.md` and `docs/redaction.md` for a sentence promising the graceful degrade before you ship. |

### 2.5 TDD test plan

New file `tests/test_shipped_resource_is_required.py`.

- **T1 `test_a_missing_redaction_knowledge_base_refuses_instead_of_returning_empty`**.
  `monkeypatch.setattr(isocenter.session, "RESOURCES_DIR", str(tmp_path))`;
  `pytest.raises(RuntimeError)` on `_load_redaction_knowledge_base()`. Assert
  the message contains **`"redaction_rules.json"`**, `str(tmp_path)`, **and the
  `consequence` clause (§11.3)** — ~~two~~ **three**
  anchors, because a one-word match like `"missing"` is satisfiable by half the
  strings in this codebase. *Red when:* `return []` is restored.
- **T2 `test_a_broken_install_cannot_scaffold_a_config`**. The speech, through
  the public API: ingest one instance, `pytest.raises(RuntimeError)` on
  `session.create_config(path)`, and assert **no file was written** at
  `output_path`. *Red when:* `return []` is restored — today it writes a config
  with `redaction_zones: []` and prints `Scaffolded Unified Config to ...`.
- **T3 `test_the_refusal_is_not_swallowed_by_the_loaders_own_handler`**. Call
  the loader with the directory missing and assert the `RuntimeError` escapes.
  *Red when:* the raise is moved inside the `try` **and** the exception type is
  changed to `FileNotFoundError`. That is a two-part mutation, so decide
  deliberately: either keep this test with a docstring saying which pair of
  edits it kills, or fold its assertion into T1 and put the reasoning in a
  comment above the raise. Do not keep it because it looks thorough.
- **T4 `test_a_missing_ctp_yaml_is_not_a_broken_install`**. A directory holding
  `ctp_rules.json` and `redaction_rules.json` but no `.yaml`: both loaders
  return their rules, nothing raises. *Red when:* the YAML `exists` check is
  routed through the helper too — the over-eager version of this fix.
- **T5 `test_a_missing_ctp_json_is_a_broken_install`**. A directory holding
  neither: `pytest.raises(RuntimeError)` naming `ctp_rules.json`. *Red when:*
  the JSON path keeps its `return []`.
- **T6** ~~*(separable leg)*~~ **(in scope — §11.1)**
  **`test_a_missing_phi_tag_policy_refuses_instead_of_auditing_against_nothing`**.
  Same shape, through `ConfigLoader.load_phi_config(None)`, plus a second arm
  asserting a session cannot `audit()` on that install. *Red when:*
  `return {}` is restored.

Any test that reads the audit log must call `flush_audit_queue()` first — but
none of these need to: the design writes **no** audit row, on the ground that
there is nothing to annotate. If you find yourself adding one, the design has
changed.

---

## §3 #392 — an empty sequence, and the hop that drops it

### 3.1 Diagnosis, measured

Source: one Explicit VR instance carrying a private empty `SQ` `(0009,1005)`, a
standard empty `SQ` `(0008,1140)`, and a standard `SQ` with one item
`(0008,1110)` as a control.

```
SOURCE: priv empty SQ present: True len 0 VR SQ
SOURCE: std empty SQ present: True len 0
ingest() returned: IngestSummary(ingested=1, failures=[], declined=0, skipped=0)
GRAPH sequences keys: ['0008,1110']
GRAPH has 0009,1005 in attributes: False
GRAPH has 0008,1140 in attributes: False
EXPORT: priv empty SQ present: False
EXPORT: std empty SQ present: False
EXPORT: std full SQ present: True 1
audit action counts: Counter({'EXPORT': 1})
```

One audit row, and it is the `EXPORT` row. `losses == []`, no `DATA_LOSS`, no
`ERROR`. Two elements the source asserted are absent from the file and nothing
anywhere says so.

### 3.2 The hop trace — this is the whole design

With an empty `DicomSequence` **planted directly in the graph** after ingest
(`probe_392_hops.py`), each hop measured separately:

| Hop | Code | Verdict |
| --- | --- | --- |
| A. source to graph | `process_sequence` (io_handlers.py:1215) | **DROPS.** The only way a sequence reaches the graph is one `add_sequence_item` per item; zero items make zero calls. |
| B. graph to `attributes_json` | `_serialize_item` / `_serialize_dicom_item` (persistence.py:2095, :2114) | **PRESERVES.** Measured: `STORED __sequences__: {"0009,1005": [], "0008,1140": []}` |
| C. `attributes_json` to graph | `_deserialize_into` (persistence.py:2140) | **DROPS.** `for item_data in items_list` over an empty list never calls `add_sequence_item`. Measured: `HYDRATED sequences: {}` |
| D. graph to file | `_merge_sequences` (io_handlers.py:4674) | **PRESERVES.** `ds.add_new(tag, 'SQ', Sequence())` writes an empty SQ. Measured: `FRESH EXPORT: priv empty SQ present: True VR SQ len 0` |

So: **preservation is possible end to end, and a loss row is not needed.** Two
hops drop, two already carry. Two one-line edits, at A and C.

### 3.3 The population this is *not*, measured so you do not chase it

An empty **private** `SQ` under **Implicit VR** never reaches
`process_sequence` at all. The transfer syntax carries no VR and the standard
dictionary has no entry, so pydicom hands it over as `UN` with value `None`, it
lands in `attributes`, and #344's zero-length arm exports it as a zero-length
`UN`:

```
SOURCE (implicit): VR UN value None
GRAPH attributes has 0009,1005: True None
GRAPH sequences: []
EXPORT has 0009,1005: True (UN, None)
```

Present in the file, under `UN` rather than `SQ`, exactly as PS3.5 6.2.2
prescribes for an element whose VR was never known. That is #344's arm working;
it is not this fix, it does not become an `SQ` after this fix, and a test that
asserts `VR == 'SQ'` on an Implicit VR source will be red for the wrong reason.
Pin the measured behaviour instead (T7).

Likewise, #367's `_merge` arm handles an empty *attribute* value (`None`, `[]`,
`()`) and never sees a sequence. It is a solved hop; do not widen it.

### 3.4 Design decision

**D1. `DicomItem.add_sequence(tag) -> DicomSequence`** — canonicalise the tag,
create a `DicomSequence` if absent, return it. `mark_modified()` **only when it
creates**: a sequence that newly exists is a change the store must hold, and a
second call on an existing tag is not.

**D2. `add_sequence_item` delegates to it**:
`self.add_sequence(tag).items.append(item)` then `self.mark_modified()`.
Adding the first item to a brand-new sequence therefore advances `_revision`
twice where it advanced once. That is harmless — `_revision` is a monotonic
counter and `has_unsaved_changes` is a comparison, not arithmetic — but it is a
real change and you should know it before a dirty-count assertion surprises
you. The alternative, leaving `add_sequence_item`'s body alone and letting two
functions create sequences, is rejected on "one spelling per behaviour".
`add_sequence_item` is frozen at tier 2 (`docs/api/stability.md:207`); its
*signature and behaviour* are unchanged, and `add_sequence` is an addition,
which `tests/test_frozen_surface.py` does not forbid — it asserts
`callable(DicomItem.set_attr)` and nothing about the rest of the class.

**D3. Hop A** — `process_sequence` calls `parent_item.add_sequence(tag)` once,
**before** the `for index, ds_item in enumerate(elem)` loop, unconditionally.
Not inside an `if not len(elem)` guard: the unconditional call is the same
statement for both cases and cannot go stale.

**D4. Hop C** — `_deserialize_into` calls `target_item.add_sequence(tag)` at the
top of its `for tag, items_list in sequences_data.items()` loop, before the
item loop. Hydration's marking is unchanged in kind: `add_sequence_item`
already marks here, and the load path calls `mark_subtree_persisted()` after
(persistence.py:1924, :2089).

**D5. No `DATA_LOSS` row, and that is an assertion the tests must make.** A fix
that preserved the element *and* filed a loss row would be a new lie, and a
presence-only test would not catch it.

Rejected alternatives: (a) a loss row instead of preservation — the issue's
fallback, ruled out by the hop trace, since D is already correct and B already
stores it; (b) representing the empty sequence as an `attributes` entry with
`[]` — that would put the same tag through both `_merge` and `_merge_sequences`
and make their order decide the outcome, which is the failure the UN-recovery
comment at io_handlers.py:1081 already names; (c) special-casing the root item —
both edits sit in recursive functions and cover nesting for free, and T6 is
what proves it.

### 3.5 Change list

| File | Function | Change |
| --- | --- | --- |
| `isocenter/entities.py` | new `DicomItem.add_sequence` beside `add_sequence_item` (`:331`) | D1. |
| `isocenter/entities.py` | `add_sequence_item` (`:331`-`:343`) | D2, delegate. |
| `isocenter/io_handlers.py` | `process_sequence` (`:1186`, loop at `:1215`) | D3, plus a docstring line saying a zero-item sequence is carried and why. |
| `isocenter/persistence.py` | `_deserialize_into` (`:2140`, loop at `:2158`) | D4. |
| `docs/api/stability.md` | tier-2 entities bullet (`:207`) | optional: name `add_sequence` alongside `add_sequence_item`. Adding a name to the tier-2 list is not a freeze change, but leaving it out means the next reader cannot tell whether it is deliberate. |
| `CHANGELOG.md` | new 0.9.5 entry | §5. |

### 3.6 TDD test plan

New file `tests/test_empty_sequence_roundtrip.py`, built on
`tests/test_private_tag_empty_value_roundtrip.py`'s shape — a `_write_src`, an
`_export_fresh`, an `_export_reloaded`, module-scoped fixtures. That file is
the worked precedent for fresh-versus-reloaded pairs and its `_data_loss_tags`
helper is directly reusable. Add the new file to `TARGETS` and to CLAUDE.md's
table for both `io_handlers.py` and `privacy.py`/`remediation.py` only if it
imports them; `io_handlers.py` alone is the honest entry.

- **T1 `test_an_empty_sequence_from_the_source_is_in_the_graph_after_ingest`**,
  parametrized over the private `(0009,1005)` and standard `(0008,1140)` tags,
  **plus a third parameter added by §11.10: `(0008,1140)` in an *Implicit VR*
  Little Endian source, which T2 and T3 inherit. A standard tag resolves its VR
  from the dictionary, so it reaches `process_sequence` and is covered — the
  boundary is private-versus-standard, not the transfer syntax.**
  Assert the tag is a key of `inst.sequences`, that
  `inst.sequences[tag].items == []`, and that the tag is *not* in
  `inst.attributes` — the two tiers must not both claim it. *Red when:* the
  `add_sequence` call is removed from `process_sequence`.
- **T2 `test_an_empty_sequence_reaches_the_exported_file[fresh]`**. `dcmread`
  the written file; assert the tag is present, `VR == 'SQ'`, `len(value) == 0`.
  *Red when:* the `process_sequence` edit is removed. **Green** when only the
  `_deserialize_into` edit is removed — that asymmetry is the point.
- **T3 `test_an_empty_sequence_reaches_the_exported_file[reloaded]`**. Ingest,
  `save()`, `close()`, reopen, export. *Red when:* **either** edit is removed.
  T2 and T3 together discriminate the two hops; neither alone does.
- **T4 `test_an_empty_sequence_survives_the_store`**. Reopen and assert the
  hydrated `inst.sequences[tag].items == []` — the graph, not just the file, so
  a future export-side workaround cannot make this pass. *Red when:* the
  `_deserialize_into` edit is removed.
- **T5 `test_no_loss_row_is_filed_for_an_empty_sequence`**. **Build this one
  carefully.** An assertion that a `SELECT` returns nothing passes identically
  when the audit queue simply has not been flushed. So: call
  `flush_audit_queue()`, then in the *same* query assert a **positive
  control** — the `EXPORT` row is present and names the output folder — and
  only then assert that no `DATA_LOSS` row's `details` names either tag. *Red
  when:* the fix is implemented as a loss row instead of preservation. The
  positive control is what makes the negative assertion mean anything.
- **T6 `test_an_empty_sequence_nested_in_a_sequence_item_survives`**. Source
  carries `(0008,1110)` with one item, and that item carries an empty
  `(0008,1140)`. Fresh and reloaded. *Red when:* either edit is special-cased
  to the root item.
- **T7 `test_an_implicit_vr_empty_private_sequence_is_still_the_un_population`**.
  Pins §3.3's measured behaviour: `dcmread` gives `VR == 'UN'` and
  `value is None`, and the tag is in `attributes`, not `sequences`. *Red when:*
  someone "extends" the fix into `_sequence_from_un_bytes`. This is a
  characterization test; say so in its docstring, the way
  `tests/test_compaction_races_a_concurrent_write.py:357` does.
- **T8 `test_a_sequence_with_items_is_unchanged`**. The control: `(0008,1110)`
  still has exactly one item, fresh and reloaded, with its inner attribute
  values intact. *Red when:* `add_sequence_item`'s delegation loses the
  `append`.

Regression watch: `tests/test_private_sequence_implicit_vr.py`,
`tests/test_nested_phi_audit.py`, `tests/test_sr_anonymization.py`,
`tests/test_waveform_ingest.py`, `tests/test_reversibility.py`,
`tests/test_persistence.py`. Every consumer of `.sequences` that could now see
a zero-item sequence was checked and already guards on `.items`
(`reversibility.py:126`, `persistence.py:1266`, `io_handlers.py:1588` and
`:3665`, `session.py:3607`, `waveform.py:250`, `murmur.py:183` and `:243`) —
verify that claim rather than trusting it, since it is the one place this fix
could change behaviour somewhere nobody was looking.

---

## §4 Sequencing

The three are independent: different files, no shared function, no shared test.
Land them in any order, but if you want one: **#392 first** — two one-line
edits, the clearest hop trace, and the test file that teaches the
fresh/reloaded pattern the rest of the bunch borrows — then **#386**, the
largest change list, then **#388**, the smallest diff but the largest number of
"does any doc promise the old behaviour" checks.

Run the full suite before pushing, per CLAUDE.md's local tier. The PR gate is
3.12 and 3.14t; **you cannot run 3.12 in this venv** — it is 3.14-only — so
either build a 3.12 venv from `setup.py`'s `.[dev]` or rely on the gate, and
say which you did in the PR body.

---

## §5 CHANGELOG entry shapes

`CHANGELOG.md` is the primary design record; breaking entries state the exact
exception a previously-working call now raises and why the old behaviour was
wrong. Three entries, under a new `0.9.5` heading.

**#386.** Open on the corrected diagnosis, not the issue's: *"`set_pixel_data()`
recorded a frame's width and never its signedness, so an `int16` array set in
memory reloaded as `uint16` and exported as a file declaring
`PixelRepresentation 0` beside signed bytes — measured, `-8` read back as
`65528` — while the audit log said `wrote 1 of 1 planned instances` (#386)."*
Then carry: the measured table of §1.1, both arms; that the fix is on the
**write** side and the loader table follows it; that `BitsAllocated`
{8,16,32,64} crossed with `PixelRepresentation` replaces
`uint16 if bits > 8 else uint8` **with the old rule kept as the fallback,
because `BitsAllocated 1` is a real Segmentation population and a table-only
rewrite is a regression**; that the dtype carrier gains exactly `bool`, and why
not the integers; **exact exceptions** — the
`RuntimeError("Integrity Error: frame for <uid> holds 32 samples; geometry
(4, 4) needs 16 ...")` that #373 introduced for 32/64-bit integer frames no
longer fires for them, and no call that returned now raises; and the honest
limit: *a store written by a pre-fix `set_pixel_data(int16)` holds int16 bytes
beside `0028,0103 = 0` and still reloads as `uint16`; nothing recorded the
signedness and no migration can invent it, the same direction `value_count`
(#328) took.* Reference #373's entry and **do not rewrite it** — it named this
population and cited this issue, and it was right.

**#388.** *"A missing shipped resource is a broken install, and now says so
(#388)."* Carry: the measured before-state — the two scaffolded configs side by
side, `redaction_zones: []` against a real ROI, zero log lines, zero audit
rows; **exact exception**: `RuntimeError` with the message shape, raised from
`_load_redaction_knowledge_base`, `_load_ctp_rules` **and `load_phi_config`**
~~(and `load_phi_config`, if the third leg is taken)~~ **(§11.1)**, so `create_config()` on a broken wheel now raises
where it previously wrote a config that redacted nothing; why `RuntimeError`
and not `FileNotFoundError` — the `except OSError` that would swallow it, and
`_load_yaml`'s different failure; that a resource which is *present but
malformed* keeps its warning-and-`[]`, and why that is a different question;
that `ctp_rules.yaml`'s absence is still legitimate and the JSON's is not; and
the one-line answer to *what a `pip install` on a broken wheel now does that it
did not*: it raises on the first call that needs the resource instead of
reporting a clean run. Name #400 as the ruling this reads as one with, ~~and
#357 as the neighbour that introduced **no** exception~~ **and do not cite #357
at all (§11.11) — a changelog citing a precedent that does not exist is itself
a small silence.**

**#392.** *"A zero-item sequence is carried end to end; it used to vanish at
ingest with `losses == []` (#392)."* Carry: the hop table of §3.2 verbatim —
which two hops dropped it and which two already carried it — because that is
the whole argument for preservation over a loss row; that no `DATA_LOSS` row is
filed **because nothing is lost**, in #344's own words; the Implicit VR `UN`
population that is *not* this fix and stays as it is; and that `add_sequence()`
is a new name on `DicomItem` with `add_sequence_item` delegating to it, the
first item on a new sequence now advancing `_revision` twice.

---

## §6 What the reviewer should attack

**#386**

1. **The fallback.** Delete `_INTEGER_DTYPE_BY_BITS`'s fallback and run the
   suite. If
   `tests/test_pixel_geometry_pipeline.py::test_export_writes_bits_allocated_the_array_actually_has`
   and the new T3 do not both go red, the fallback is unguarded and a future
   cleanup will remove it.
2. **T7's shape.** If it asserts `np.array_equal` rather than `dtype`, it
   passes on unfixed code — measured. Check it by hand.
3. **The float exclusion.** `set_pixel_data` deliberately does not touch
   `0028,0103` for a float array, so an instance ingested as signed int16 and
   then given a float32 array keeps a stale `PixelRepresentation 1` in the
   graph. Argued harmless: the carrier wins on read, and the export's float arm
   deletes all three descriptors. Attack it — is there a path where that stale
   1 is read? If yes, the answer is a `pop`, not a write.
4. **Exotic dtypes.** `set_pixel_data` accepts anything with a `.dtype`.
   `complex64` records no carrier, declares `BitsAllocated 128`, and reloads
   through the fallback as `uint16`, silently. Big-endian (`>i2`) writes BE
   bytes and reads native. Neither is in scope here and neither is fixed by
   this brief. **Adjacent design call: should `set_pixel_data` refuse a dtype
   the sidecar cannot round-trip, with a `ValueError`?** Named for the owner,
   not solved — §7.
5. **The compressed export branch with a 32-bit array.** Now that 32-bit frames
   load instead of raising, they reach the J2K/compression path for the first
   time. Does it refuse, or narrow silently? Measure before merging; a new
   silent narrowing would be this milestone shipping its own defect.
6. **Dirtying.** Confirm the new `0028,0103` write is a no-op on the redaction
   path (`_write_int_if_changed`), and that `tests/test_services.py`'s
   write-count assertions are unmoved.

**#388**

7. **Does the helper read the directory at call time?** If it closes over
   `RESOURCES_DIR` at import, every monkeypatched test passes against the real
   source tree and the whole file is decoration.
8. **Is T3 killed by a single edit?** As written it needs a two-part mutation.
   Either accept it as a comment rather than a test, or make the argument
   explicitly. Do not keep it because it looks thorough.
9. **The blast radius of the third leg.** If `load_phi_config` is included,
   grep for every caller and for every test that constructs a session with a
   monkeypatched resources dir. And check that
   `tests/test_packaging_contract.py:465`'s docstring was updated — a test
   whose prose contradicts the code is exactly the drift this milestone is
   about.
10. **Message anchors.** T1 must match on the basename *and* the resolved path.
    A `match="missing"` passes against half the strings in this codebase.

**#392**

11. **Does removing one edit turn exactly one of T2/T3 red?** If both go red on
    the `process_sequence` edit alone, the reloaded fixture is not actually
    reloading — a session that never closed, or a `save()` that never ran.
12. **T5's positive control.** Without it, a missing `flush_audit_queue()` and
    "no loss row" look identical: the known correct-by-accident shape.
13. **The double `mark_modified`.** Check that no test counts revisions rather
    than dirty entities, and that `mark_persisted(revision=captured)` cannot be
    straddled by the extra bump.
14. **The empty sequence in a worker clone.** `clone_sequences`
    (entities.py:453) iterates `item.sequences.items()` and builds a
    `DicomSequence` per tag, so it should carry an empty one by construction —
    verify, because the PHI scan and `_rehydrate_findings` walk those clones
    and an empty sequence is a new shape for them.
15. **`remove_private_tags=True` with an empty private sequence.** It is now in
    `sequences` rather than absent. Confirm it is stripped with the rest of the
    vendor block and does not survive into a de-identified export.

---

## §7 Owner calls — ~~please decide these; I have not~~ **all five ruled 2026-09-09; see §11**

1. **#388, the raise site.** **[Ruled — §11.4: `RuntimeError` from the loader,
   surfacing from `create_config()` unwrapped; no new exception class, no raise
   at construction.]** Recommended: raise from the loader, at the moment
   the resource is needed. The alternative in the issue is raising at
   `Session()` construction, which is louder — a broken install cannot open a
   session at all — but puts a fourth place in the codebase that has to know
   the resource list, and changes a behaviour `docs/api/stability.md:29`
   documents in prose (`Session(persistence_file=None)` ... "accepted"). The
   signature is unchanged either way, so `tests/test_frozen_surface.py` is not
   affected — but "what a released version claimed about construction" is your
   call, not mine.
2. **#388, the scope.** **[Ruled — §11.1: in scope. The recommendation below
   was accepted in full.]** Recommended: take `config_manager.load_phi_config` in
   the same PR, through the same helper, and update
   `tests/test_packaging_contract.py:465`'s docstring. #388's text names only
   session.py's two loaders; leaving the third is two spellings for one
   behaviour, and it is the loudest of the three silences. Say if you want it
   held back to its own issue.
3. **#386, exotic dtypes at `set_pixel_data`.** **[Ruled — §11.5: yes,
   `ValueError`, in this bunch, with the accepted set defined positively and
   byte order normalized rather than refused.]** `complex64`, `object`, and
   big-endian dtypes round-trip wrongly or unreadably and nothing refuses them.
   Should `set_pixel_data` raise `ValueError` for a dtype the sidecar cannot
   carry? That is a user-visible contract change on a frozen-surface method's
   behaviour. Not filed; say the word and I will.
4. **#386, 32-bit frames on the compressed export path.** **[Ruled — §11.6:
   measured in this bunch, not filed as a suspicion. A three-armed decision rule
   and a third copy of the defect, in `_compress_j2k`, are recorded there.]** They could not reach
   it before, because they raised at load. If it narrows or refuses, that is a
   second issue and I have not measured it. Worth filing alongside.
5. **Environment.** **[Confirmed — §11.12: broken, and 3.12.14 is the floor now,
   not 3.12.13.]** The project `.venv`'s base interpreter (3.14.6) no longer
   exists on this machine, and the CI floor (3.12) cannot be run here at all.
   Rebuilding it is outside this bunch, but every measurement in this brief was
   taken on 3.14.7 through the workaround in §0.1, and the developer will be
   working the same way unless you rebuild first.

---

## §11 Amendments

Amendments record decisions taken *after* this brief's date — five owner
rulings and one closing review, all on 2026-09-09. Per CLAUDE.md's convention
on dated records nothing above §11 has been rewritten to agree with them: the
clauses they falsify are struck in place and point here. **Where an amendment
and the text above disagree, the amendment wins.**

**Scope after these rulings.** #386 grows two arms (a dtype guard at
`set_pixel_data`, and a measured verdict on the compressed export path, which
has already yielded a third copy of the defect). #388 grows a third call site
(`load_phi_config`) and a helper parameter. #392 is unchanged except for one
extra test parameter. Still true and unchanged: **no new stored field, no
schema column, and zero `write_frame` sites added or moved — the count stays
six**, so `tests/test_sidecar_gate_order.py`'s AST detector and the
`pass-lock -> _sidecar_gate -> _pixel_swap_lock -> sqlite` order are untouched
by design.

### §11.1 `load_phi_config` is in scope (owner). Supersedes §2.3, §2.4, §2.5 T6, §5 (#388), §7.2.

Ruling: take it, in this bunch, through the same helper. The reasoning is the
one §2.3 gave: an empty PHI tag list makes `audit()` report success on data
full of PHI, which is the more dangerous of the two silences and squarely the
milestone's theme. The bunch therefore has **three** `require_package_resource`
call sites, not two, and every "separable" marker above is void.

Three consequences the developer should not have to discover:

**1. `load_phi_config` has no directory constant to monkeypatch.** Measured —
`config_manager.py:207`-`:216` computes it *inline in the function body*:

```
base = os.path.dirname(os.path.abspath(__file__))
filepath = os.path.join(base, "resources", "phi_tags.json")
```

`config_manager.py` has no module-level resources constant; `session.py` does
(`RESOURCES_DIR`, `:107`). So the change list gains a step **before** the helper
call: hoist that to a module-level
`RESOURCES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources")`
in `config_manager.py` and pass it in. Without the hoist there is nothing for
T6 to monkeypatch, and the test either passes against the real tree — the
correct-by-accident shape §2.2 warns about — or is written against the
`filepath` argument instead, which exercises the *user-config* branch and never
enters the arm under test. Keep `"phi_tags.json"` as a bare literal in
`config_manager.py` so `test_every_shipped_resource_is_named_by_the_package`
still sees it in the AST.

**2. Blast radius, measured.** Six call sites: `session.py:474` (tag display
names, on the report path), `:1691` (remediation defaults), `:1761` (an explicit
user path — the other branch, unaffected), `:2253` (audit's effective tags),
`privacy.py:159`/`:161` (`PhiInspector.__init__`), and `config_manager.py:77`
(profile parsing). **None is `Session.__init__`** — a broken install can still
construct a `Session`, which is what keeps §11.4's ruling coherent and leaves
`docs/api/stability.md:29`'s prose about construction true. Four of the six sit
inside `except (OSError, ValueError)` handlers that fall back to `{}` or warn.
`RuntimeError` is neither, so it propagates — deliberately. **Do not widen those
handlers to catch it.** They exist for a resource that is present but malformed,
which keeps its warning-and-fallback per §2.2; widening them would reproduce
the exact bug this issue is about, one layer up.

**3. `tests/test_packaging_contract.py` must be corrected in the same commit.**
`test_the_wheel_ships_every_resource_the_package_reads`'s docstring (`:465`)
reads: *"isocenter/resources/phi_tags.json is the whole default PHI policy.
When it is absent, load_phi_config() returns {} rather than raising, so audit()
finds nothing and reports success on data full of PHI."* After this change the
second sentence is false. It is **prose, not an assertion — the test stays
green while its stated reason becomes a lie**, which is this milestone's own
failure mode occurring inside its own fix. Rewrite it to the post-fix reason:
the resource is still release-blocking, but a wheel without it now refuses at
first use instead of auditing against nothing. The same claim appears as a
comment at `:327`; fix both.

### §11.2 `test_config.py::test_phi_config_default`: keep it, rewrite its comment.

The owner asked for change-or-delete and for the brief to say which. It is
neither, and the reason matters. `tests/test_config.py:56`-`:59` is:

```
def test_phi_config_default():
    # Calling with None should attempt to load default.
    # We can't easily assert content unless we know it, but it shouldn't crash.
    tags = ConfigLoader.load_phi_config(None)
    assert isinstance(tags, dict)
```

Delete it and the suite loses its only assertion that the shipped
`phi_tags.json` is present, parseable and returns a mapping in a real checkout —
the **positive control** without which §11.1's negative test proves only that
*something* raises. Convert it to `pytest.raises` and the same refusal is
asserted in two files, which is the duplicate-spelling this project deletes on
sight. So: **keep the test and its assertion exactly as they are, and replace
the comment.** The code is still true; the comment "it shouldn't crash" is the
clause that becomes false. New comment: this is the positive control — in a
correct install the default policy loads; the refusal when it is absent is
`tests/test_shipped_resource_is_required.py`'s T6.

### §11.3 The refusal names its consequence (closing review). Supersedes §2.2's helper signature.

§2.2 gives `require_package_resource(directory, basename)`, but its message ends
`"Continuing would have <what is lost>, and reported a clean run."` and nothing
supplies that clause. Left as written, the developer hardcodes a generic
sentence — and "accurate, not generic" is the standard this milestone applies to
loss rows, which applies no less to refusals. Add a third parameter:

```
def require_package_resource(directory: str, basename: str, consequence: str) -> str:
```

interpolated as `f"Continuing would have {consequence}, and reported a clean run."`

| Call site | `consequence` |
| --- | --- |
| `session._load_redaction_knowledge_base` | `"scanned every frame with no machine redaction rules"` |
| `session._load_ctp_rules` (JSON fallback only) | `"matched no CTP de-identification rules"` |
| `config_manager.load_phi_config` | `"audited against an empty PHI tag list"` |

The third is `publish.yml`'s own wording for the same failure, so the runtime
refusal and the release gate say the same thing about the same file. **§2.5 T1
gains a third anchor**: the message must contain the basename, the searched
path, *and* the consequence clause. Three anchors is the difference between a
test that pins the speech and one satisfied by any string containing "missing".

### §11.4 `create_config()` raises `RuntimeError` unwrapped (owner). Supersedes §7.1.

No domain exception class, no translation layer, and no raise at `Session()`
construction. A broken install is not a domain condition, and with the API
about to freeze, a new exception class is surface the project would then be
stuck with. `__all__` stays at five names and `tests/test_frozen_surface.py` is
unaffected. The message carries the file, the searched path and the consequence
(§11.3) — that is the whole user-facing contract. §2.5 T2 is the test for it and
needs no change.

### §11.5 `set_pixel_data` refuses dtypes the sidecar cannot honestly carry (owner). Extends §1.3, §1.4, §1.5, §5.

Ruling: in this bunch, at the call, `ValueError`. Define the accepted set
**positively**, never as a blacklist:

- `dtype.kind in ('u', 'i')` **and** `dtype.itemsize in (1, 2, 4, 8)` — exactly
  the domain of `_INTEGER_DTYPE_BY_BITS`, so the accept rule and the reload
  table are one statement rather than two that can drift apart.
- `dtype.kind == 'b'` — carried by name under D2.
- `dtype.kind == 'f'` **and** `dtype.name in SIDECAR_DTYPE_NAMES`
  (`float16`/`float32`/`float64`).

Everything else raises: `complex64`/`complex128` (kind `c`), `object` (`O`),
strings (`U`/`S`), structured and void (`V`), datetimes (`M`/`m`), and
`float128`/`longdouble` — which is kind `f` but absent from the name set, so the
positive rule already catches it with no special case. That is the point of the
positive spelling: a numpy release that adds a kind is refused by default rather
than admitted by omission.

**The "merely unusual but round-trippable" case is byte order, and it is
normalized, not refused.** A big-endian `>i2` is kind `'i'`, itemsize 2, and
passes every clause above — yet the sidecar stores raw bytes and the loader
reads them with a native-order dtype, so it reloads byte-swapped. Refusing it
would reject an array that is exactly representable and merely spelled
unusually. So, **before** the accept check:

```
if array.dtype.byteorder not in ('=', '|'):
    array = array.astype(array.dtype.newbyteorder('='))
```

`>i2` and `<i2` then produce byte-identical frames, which is what a caller
means. Say so in the CHANGELOG: byte order is normalized and the reload is
byte-exact.

**The refusal must not half-write.** `set_pixel_data` writes several tags and
the dtype carrier; a `ValueError` raised after some of them leaves an instance
describing an array it does not hold — a new silence inside the fix. **Validate
first, before any mutation**, and pin that with a test rather than a comment.

Three tests, added to `tests/test_pixel_dtype_roundtrip.py`:

- **T10 `test_set_pixel_data_refuses_a_dtype_the_sidecar_cannot_carry`**,
  parametrized over `complex64`, `object`, a structured dtype, and `float128`
  guarded by `pytest.mark.skipif(not hasattr(np, "float128"), ...)`.
  `pytest.raises(ValueError)` with the message naming the offending dtype **and**
  the accepted set. *Red when:* the guard is removed.
- **T11 `test_a_refused_dtype_leaves_the_instance_exactly_as_it_was`**. Set a
  valid `int16` frame; snapshot `dict(inst.attributes)` and `inst._revision`;
  attempt `complex64`; assert `ValueError`, then assert the attributes dict
  **equals the snapshot** (including `0028,0100`, `0028,0103` and the
  presence-or-absence of the carrier), that `inst._revision` is unchanged, and
  that `get_pixel_data()` still returns the `int16` frame. *Red when:* the guard
  is moved below the first `_write_int_if_changed`. This is what makes "validate
  first" a fact instead of a comment, and it is the speech test for the refusal:
  the instance must not describe an array it never took.
- **T12 `test_a_big_endian_array_is_normalized_not_refused`**. `dtype='>i2'`;
  assert no raise, `inst.attributes["0028,0103"] == 1`, and after
  `save()`/reopen that `get_pixel_data().tolist()` equals a **literal** list —
  never a comparison against a `>i2` array built in the same test, which is true
  even when both sides are wrong. *Red when:* the normalization is deleted
  (values return byte-swapped) **or** the accept rule is tightened to refuse
  non-native order.

**CHANGELOG: this is a breaking entry of its own**, separate from #386's main
entry, because CLAUDE.md requires a breaking entry to state the exact exception
a previously-working call now raises and why the old behaviour was wrong. Shape:
*"`Instance.set_pixel_data()` now raises `ValueError` for a dtype the sidecar
cannot round-trip — `complex64`, `object`, structured, `float128` and the rest —
instead of accepting it and handing back a different array, or failing later
with a `RuntimeError` from the loader that named a geometry mismatch rather than
the dtype (#386)."* Carry: the accepted set stated positively; that byte order
is normalized rather than refused, so `>i2` and `<i2` now store identical
bytes; that the check runs before any tag is written, so a caught `ValueError`
leaves the instance untouched; and the honest limit — this guards the entrance
and migrates nothing, so a pre-fix store holding an exotic frame is unaffected.

### §11.6 The compressed export path is measured in this bunch, not suspected (owner). Supersedes §7.4, extends §1.4.

Two facts found while writing this amendment change the shape of the question,
and neither was in the brief above.

**1. Compression is the default.** `Session._export_dicom(folder,
use_compression=True, ...)` (session.py:3358) defaults to True and maps to
`compression='j2k'` at session.py:3820. A 32-bit frame on a plain
`session.export(folder)` therefore goes through `_compress_j2k`
(io_handlers.py:3325). This is the ordinary path, not an exotic arm, which
raises the stakes on the answer.

**2. `_compress_j2k` holds a third copy of the defect.**
**[Superseded by §11.13.4: the branch is measured **unreachable** in the export
flow, so it is deleted rather than corrected. The reachability argument is
there; do not fix a branch that no longer exists.]** Its fallback
reconstruction branch (io_handlers.py:~3344), taken when `pixel_array` is not
passed and the array must be rebuilt from `ds.PixelData`, reads:

```
dt = np.uint16 if bits > 8 else np.uint8
```

— the same bucket §1.3 replaces in `SidecarPixelLoader`, in a second function
§1.4 does not list. ~~**Add it to the change list**: same
`_INTEGER_DTYPE_BY_BITS` lookup, same legacy fallback, and it must honour
`PixelRepresentation` too~~ **[Superseded by §11.13.4: deleted, not
corrected — the branch is unreachable in the export flow.]** — a signed
frame rebuilt here today comes back
unsigned for exactly the reason the loader's did.

**Step P1, before writing any #386 test.** Build a `(uint32, pixrep=0)` and an
`(int32, pixrep=1)` instance; export twice, once with `use_compression=True`
(the default) and once with `use_compression=False`; and record for each: whether
a file was written; `dcmread`'s `BitsAllocated`, `PixelRepresentation`,
`pixel_array.dtype` and values; `ExportSummary.written` and `.failures`; and —
**after `flush_audit_queue()`** — every audit row for that UID with its status
and details. Without the flush, "no row" and "row not yet written" are the same
observation and the probe cannot distinguish its own arms.

Decision rule, three arms, one of which must be written into this file with its
numbers before the bunch is called done:

- **The file is correct.** Negative result. Record the numbers as a §11
  addendum and turn P1 into a characterization test
  (`test_a_32_bit_frame_survives_the_compressed_export_path`) so the next reader
  does not re-ask.
- **The export refuses and says so accurately** — an `ERROR`/`DATA_LOSS` row
  naming the instance *and* the dtype or bit depth, with `written` not counting
  it. Also a negative result for #386, also a characterization test; note in the
  CHANGELOG that 32-bit frames are uncompressible on this path and that the run
  says so.
- **The export refuses or degrades and the accounting does not say why.**
  **[This is the arm P1 landed on, and worse than predicted — see §11.13. The
  informative refusal survives, narrowed to 32- and 64-bit, in §11.13.3.]** The
  likely arm: `_compress_j2k` ends
  `except Exception as e: raise RuntimeError(f"Compression failed: {e}")`, which
  reports the codec's message and not the dtype, and most J2K encoders cap below
  32 bits. Then it **is** part of #386 and is fixed here — either the refusal
  names the dtype and the width, or the path falls back to uncompressed for
  widths the codec cannot take and files a row saying it did. Choose one, say
  which in the CHANGELOG, and do not leave a `RuntimeError` whose text is
  Pillow's.

"May have a separate gap" is not an acceptable finding for this bunch.

**P1 has been run. The result is §11.13 (#404), and it is larger than the
question P1 asked: signed 16-bit pixel data — every CT and MR study — could
not be exported at all on the default path.**

### §11.7 The probes are committed beside this brief (closing review). Supersedes §0.3.

§0.3 told the reader to re-run six scripts living in a session-scoped scratchpad
that exists for nobody but their author — a false promise in a document whose
whole claim is measured evidence. The six are now committed at
`docs/superpowers/specs/2026-09-09-the-last-silences-bunch-a/`, beside this file.
Checked before committing: all six carry the `if __name__ == '__main__':` guard
§0.1 warns about, and none hardcodes a scratchpad or session path. They are kept
off the docs site by `mkdocs.yml`'s `exclude_docs: superpowers/`, and cannot be
collected by pytest because `pytest.ini` sets `testpaths = tests`. Run them with
the §11.12 invocation, from the worktree root.

### §11.8 §1.5 T6 must flush before it reads the audit log (closing review).

T6 is the milestone test for #386 — the one asserting that the exported file is
right *and* that the `wrote 1 of 1 planned instances` row is still there and has
become true. As written it reads the log without `flush_audit_queue()`. The
audit writer is a background thread, so an unflushed `SELECT` returns `[]` and
"the row is missing" is indistinguishable from "the row has not landed yet" —
the test then passes or fails for reasons unrelated to the fix. Call
`flush_audit_queue()` before the read. §2.5 and §3.6 T5 already say this; T6 was
the omission.

### §11.9 §1.4 gains the `set_pixel_data` docstring (closing review).

`Instance.set_pixel_data`'s docstring carries an explicit "Updates tags" list.
D1 adds `PixelRepresentation (0028,0103)` to what it writes, and §11.5 adds a
`Raises: ValueError` section. The API reference is generated from docstrings and
`docs.yml` deploys on any `isocenter/**.py` push, so this is documentation that
ships, not cosmetics.

### §11.10 §3.6 T1/T2 gain a standard empty `SQ` under Implicit VR (closing review).

§3.3 establishes that an Implicit VR **private** empty `SQ` arrives as a
zero-length `UN` and is not this fix. A developer can read that as "Implicit VR
is out of scope", which is wrong: a **standard** tag under Implicit VR resolves
its VR from the dictionary, reaches `process_sequence`, and is covered. Add a
third parameter to T1 (which T2 and T3 inherit): `(0008,1140)` in an Implicit VR
Little Endian source. The boundary the tests draw is then private-versus-standard,
which is the real one, rather than a property of the transfer syntax. T7 stays
as the characterization test for the `UN` population.

### §11.11 #357 is dropped as a precedent, confirmed by the owner.

The task that commissioned this brief named v0.9.4's #357 as the neighbour whose
exception shape #388 should match. §2.2 records the measurement that there is
none: #357's own CHANGELOG entry says *"no exception changes: no loader ever
opened the file, so no call can notice its absence."* The owner has confirmed
this was an error in the commissioning brief, not in the reading.

§2.2's paragraph **stands as written** — it is the record of why the precedent
was dropped, and erasing it would leave the next reader to re-derive the same
measurement. What is struck is §5's instruction to *name* #357 in the CHANGELOG
entry: the entry names **#400** as the ruling this reads as one with, and does
not mention #357 at all. A changelog citing a precedent that does not exist is a
small silence of exactly the kind this milestone is about.

### §11.12 The environment prerequisite, verbatim.

Owner-verified in the main checkout: `.venv/bin/python` symlinks to
`/Users/kevin/.pyenv/versions/3.14.6/bin/python`, which does not exist, and
`.venv/bin/python -V` fails outright. `pyenv versions` is now **3.12.14, 3.14.7,
3.14.7t** — 3.12.13, 3.14.6 and 3.14.6t are all gone, so **the CI floor to build
against is 3.12.14, not 3.12.13**. Until the venv is rebuilt this is the
invocation for every command in this brief, copy-pasteable from the worktree
root:

```
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$(pwd):/Users/kevin/Developer/Isocenter/.venv/lib/python3.14/site-packages" \
/Users/kevin/.pyenv/versions/3.14.7/bin/python -u -c \
'import isocenter, sys; print(isocenter.__file__); print(sys._is_gil_enabled())'
```

**Read the printed path.** It must end
`.../worktrees/<your worktree>/isocenter/__init__.py`. If it names the main
checkout, the editable install won and every measurement taken after it is about
a tree you did not edit. Substitute `-m pytest -v <file>` for the `-c '...'` to
run tests; never `-q` piped. `sys._is_gil_enabled()` prints `True` here, and for
this bunch that is fine: ~~nothing in it touches a thread, a lock or a
`write_frame` site, so no 3.14.7t run is required. If a later change touches one,
that sentence stops being true and the 3.14.7t rig comes back.~~

**Superseded by §11.13 (#404), struck above rather than rewritten.** That
reasoning was written before #404 was in this bunch, and #404 adds an
**unguarded module-scope import of a C extension** (`imagecodecs`) whose
encoder then runs *inside export workers* — on the threads path under a
free-threaded build. "Nothing here touches a thread" stopped being true the
moment that landed, and the premise was never restated. It is not restated
now either: the 3.14.7t gate on the PR is the measurement, and it passed
(`test (3.14t)` SUCCESS, 12m35s, run 34391330672). Cite that run rather than
the argument.

---


### §11.13 #404 — signed pixel data cannot be exported at all. Owner: fix it properly, inside bunch A. Supersedes §11.6 items 2 and 3.

#404 is what step P1 (§11.6) was written to find, and it is larger than the
32-bit question P1 asked. Everything below was measured on the rebuilt venv —
Python 3.12.14, the CI floor — with `isocenter.__file__` printed and read; the
scripts are `probe_404_codecs.py`, `probe_404_detail.py`, `probe_404_32bit.py`,
`probe_404_matrix.py`, `probe_404_shapes.py`, `probe_404_export.py` and
`probe_404_container.py`, committed beside this file.

#### §11.13.1 Diagnosis, re-measured

`session.export(folder)` writes **nothing** for signed 16-bit pixel data — CT
and MR. Compression is the default (`_export_dicom(..., use_compression=True)`,
frozen at `docs/api/stability.md:73`), `_finalize_dataset` calls
`_compress_j2k`, and `Image.fromarray(...).save(..., format="JPEG2000")` at
io_handlers.py:3417 raises for every dtype Pillow's encoder does not accept.

End to end, a 64x64 CT frame of Hounsfield units (`int16`,
`PixelRepresentation 1`), on unmodified code:

```
1. DEFECT: default export (use_compression=True)
   export raised : ExportError: ... wrote 0 of 1 planned instances; 1 failed
                   and nothing reached disk.
   files on disk : 0
   AUDIT ERROR   : Export failed for .../<uid>.dcm: Compression failed:
                   broken data stream when writing image file
   AUDIT EXPORT  : wrote 0 of 1 planned instances from 1 patients.

2. CONTROL: use_compression=False, same data, same code
   export raised : None
   written       : 1     files on disk: 1
   TransferSyntaxUID : 1.2.840.10008.1.2 (Implicit VR Little Endian)
   BitsAllocated 16   PixelRepresentation 1   BitsStored 16
   pixel_array dtype : int16      BIT-EXACT: True
```

Two things this settles. **The sidecar is not implicated** — case 2 is
bit-exact from the same store, so the loader's `pixel_representation == 1` arm
(io_handlers.py:3517-3519) is doing its job and the array reaching the exporter
is already correct. **The axis is signedness, not width.** Measured against
Pillow 12.3.0's JPEG 2000 encoder, one array per dtype:

| dtype | Pillow J2K |
| --- | --- |
| `uint8` | EXACT |
| `int8` | `OSError: broken data stream when writing image file` |
| `uint16` | EXACT |
| `int16` | `OSError: broken data stream when writing image file` |
| `uint32`, `int32`, `float32`, `bool` | `OSError: broken data stream ...` |
| `uint64`, `int64` | `TypeError: Cannot handle this data type` (in `fromarray`) |

Exactly two dtypes work, and `tests/conftest.py:257` makes the shared fixture
`np.zeros((512, 512), dtype=np.uint16)` — one of the two. Every export test on
the default path rides it. That is the blind spot, and §11.13.7 is its
disposition.

The refusal is also mute: the `ERROR` row carries Pillow's sentence and names
neither the dtype, nor `BitsAllocated`, nor `PixelRepresentation`, nor the
encoder, nor a remedy. A reader is told a data stream broke.

~~`export(verify_readback=True)` would not have caught it either, and cannot be
made to: `_READBACK_DESCRIPTORS` is `("Rows", "Columns", "SamplesPerPixel",
"NumberOfFrames", "BitsAllocated")`, it does not include
`PixelRepresentation`, and `_verify_readback` never decodes pixels.~~ Noted for
§6, not proposed as a fix here. — superseded by #449 (v0.9.6): the readback
now decodes every written file and compares it bit for bit with the samples
written.

#### §11.13.2 The approach, chosen by measurement

Three candidates, all encoding the same arrays to JPEG 2000 lossless
(`probe_404_codecs.py`):

| dtype | A: Pillow (today) | B: imagecodecs | C: pydicom `ds.compress()` |
| --- | --- | --- | --- |
| `uint8` | EXACT | EXACT | unavailable |
| `int8` | broken data stream | EXACT | unavailable |
| `uint16` | EXACT | EXACT | unavailable |
| `int16` | broken data stream | **EXACT** | unavailable |
| `uint32` | broken data stream | encodes, see §11.13.3 | unavailable |
| `int32` | broken data stream | encodes, see §11.13.3 | unavailable |
| `uint64`/`int64` | `TypeError` in `fromarray` | `ValueError: item size not supported by codec` | unavailable |
| `float32`/`float64` | broken data stream | `ValueError: sample format not supported by codec` | unavailable |
| `bool` | broken data stream | `ValueError: sample format not supported by codec` | unavailable |

**C is rejected on measurement, not on taste.** `ds.compress(JPEG2000Lossless,
arr)` raises, in full:

```
RuntimeError: The pixel data encoder for 'JPEG 2000 Image Compression
(Lossless Only)' is unavailable because all of its plugins are missing
dependencies:
    pylibjpeg - requires numpy, pylibjpeg>=2.0 and pylibjpeg-openjpeg>=2.2
```

Neither `pylibjpeg` nor `pylibjpeg-openjpeg` is installed, and neither is in
`setup.py`. **The task brief's premise that "#372 already established installs
and works in this environment" does not hold on the rebuilt venv** — #372's own
CHANGELOG entry says its result was "identical with Pillow alone and with the
pylibjpeg plugins installed", which records a comparison made with them
temporarily present, not a dependency the project took. Taking C means adding
**two** runtime dependencies, one of which builds a C extension, to fix a
defect that a dependency already in `install_requires` fixes.

**The signed-to-unsigned shift carried by `RescaleIntercept` is rejected**, and
this is the more tempting of the two. It fails the owner's own bar — pixels
that read back bit-exact with `PixelRepresentation` correct in the written
file. A shifted export writes `PixelRepresentation 0` over data that was
signed, so the file's own descriptor is a lie that only a reader honouring
`RescaleIntercept` can undo; every reader that shows stored values, and every
tool that recomputes a hash of the pixels, sees different numbers. It also
collides with real data: `RescaleIntercept` is already populated on CT (the
population this defect is about), so the fix would have to compose with an
existing value rather than set one, and #182's date-shift precedent is the
wrong analogy — a jittered date is still a date, where a shifted pixel under a
flipped descriptor is a different image. Rejected.

**B is the fix: `imagecodecs.jpeg2k_encode(arr, level=0, codecformat="J2K")`.**
It is not a new dependency. `setup.py` already carries
`imagecodecs>=2023.9.18` in `install_requires`, and
`isocenter/imagecodecs_handler.py` already uses it for the **decode** side —
so the project already trusts this library with pixel fidelity in the other
direction. `level=0` is lossless; measured reversible on a random full-range
`int16` frame, and identically with the explicit `reversible=True`.

Verified through the real `DicomExporter._finalize_dataset(ds, "j2k",
pixel_array=arr)`, IOD validation included, with `_compress_j2k` replaced
in-process (export's workers are spawned processes, so a session-level
monkeypatch cannot reach them — CLAUDE.md, #185; the process boundary is not
what is under test):

```
3. THE UNIT: _finalize_dataset(ds, 'j2k', pixel_array=arr)
   current  (Pillow)      : RuntimeError: Compression failed: broken data
                            stream when writing image file
   proposed (imagecodecs) : WROTE proposed.dcm
       TransferSyntaxUID  : 1.2.840.10008.1.2.4.90
       BitsAllocated 16   BitsStored 16   PixelRepresentation 1
       decoded dtype      : int16
       BIT-EXACT          : True    dtype match: True
```

Shapes, through the same call — the matrix has no unmeasured column
(`probe_404_shapes.py`):

| shape | proposed | current |
| --- | --- | --- |
| `int16` 3-frame stack | EXACT | `RuntimeError: Compression failed` |
| `int16` single frame | EXACT | `RuntimeError: Compression failed` |
| `uint8` RGB `(H,W,3)` | EXACT | EXACT |
| `uint8` RGB 2-frame `(F,H,W,3)` | EXACT | not run (uint8 works either way) |

RGB needs no `colorspace` or `planar` argument: the axis order survives as
written. There is no colour regression to design around.

**One encoder, not two.** A dual path — Pillow for the two dtypes it handles,
imagecodecs for the rest — is rejected under CLAUDE.md's one-spelling-per-behaviour
rule: it would leave two encoders whose output can diverge for the same input,
and the divergence would be invisible because both produce readable files.
`from PIL import Image` at io_handlers.py:136 becomes unused **in this module**
and should go with it; Pillow stays in `install_requires` because
`isocenter/pixel_analysis.py` still imports it.

#### §11.13.3 The dtype matrix the fix must satisfy

Measured end to end — encode, wrap as the export worker wraps, `save_as`,
`dcmread`, compare (`probe_404_matrix.py`). This is the table the parametrized
tests come from.

| dtype | BitsAllocated | PixelRepresentation | after the fix | why |
| --- | --- | --- | --- | --- |
| `uint8` | 8 | 0 | **compressed, bit-exact** | measured EXACT |
| `int8` | 8 | 1 | **compressed, bit-exact** | measured EXACT; fails today |
| `uint16` | 16 | 0 | **compressed, bit-exact** | measured EXACT |
| `int16` | 16 | 1 | **compressed, bit-exact** | measured EXACT; **this is #404** |
| `bool` | 8 | 0 | **compressed, bit-exact, encoded as `uint8`** | the codec refuses kind `b`; `astype(np.uint8)` is exact and is what the uncompressed path already writes |
| `uint32` | 32 | 0 | **refused** | see below |
| `int32` | 32 | 1 | **refused** | see below |
| `uint64`/`int64` | 64 | 0/1 | **refused** | `ValueError: item size not supported by codec` |
| `float32`/`float64` | — | — | **never reaches the encoder** | the float branch writes (7fe0,0008)/(7fe0,0009) and deletes (7fe0,0010), so `_compress_j2k` returns at its `hasattr(ds, "PixelData")` guard and the file is written uncompressed, per PS3.5 8.2. Unchanged, and pinned by a test so it stays that way. |

**32-bit must be refused, and this is the answer §11.6's P1 was asking for.**
The codec does not refuse it — it encodes, and the result is wrong twice over:

```
int32 holding 16 bits of data : exact
int32 holding 24 bits of data : exact
int32 holding 25 bits of data : exact
int32 holding 26 bits of data : NOT exact
int32 holding 32 bits of data : NOT exact
```

`bitspersample=31`, `bitspersample=32` and `reversible=True` do not rescue it;
imagecodecs' own docstring carries the matching TODO ("(u)int32 must contain
26 bits or fewer?"). And the DICOM file built from a 32-bit codestream is
**unreadable**: `pydicom.dcmread(path).pixel_array` raises `RuntimeError:
Unable to decode as exceptions were raised by all available plugins`. So an
unguarded switch to imagecodecs would turn today's loud failure into a
`wrote 1 of 1` beside a file no reader can open — this milestone's own defect,
introduced by its own fix. The width guard is not optional.

**Refusal message.** §11.6's arm 3 informative refusal is not deleted; it is
**narrowed to the widths that remain unsupported**, and it is the only thing
left of arm 3. It must name the dtype, `BitsAllocated`, `PixelRepresentation`,
the encoder and the remedy:

```
raise RuntimeError(
    f"Compression failed: JPEG 2000 lossless cannot carry {arr.dtype} "
    f"pixel data (BitsAllocated {bits}, PixelRepresentation {pixrep}). "
    f"The encoder (imagecodecs {imagecodecs.__version__}) is exact only "
    f"to 25 bits, so a 32-bit frame would be written wrong and read "
    f"back wrong, and a 64-bit frame is refused by the codec outright. "
    f"Export this study with use_compression=False, which writes the "
    f"same pixels uncompressed and bit-exact.")
```

Raised **before** the encode, from the dtype, so nothing is written first. It
keeps the exception type and the outcome the default path already has — an
`ExportError`, `wrote 0 of N`, nothing on disk — so this is **not** a change to
what `use_compression=True` promises; only the speech improves. A reviewer will
ask; that sentence is the answer.

**Rejected pending the owner (§11.13.9, call 2):** falling back to an
uncompressed write plus an audit row saying so. It would deliver a file where
today the export fails, and it is not silent — but it changes what
`use_compression=True` *means* on a frozen option, which is the owner's call
under the escalation rule, not mine.

#### §11.13.4 The third `uint16 if bits > 8` copy is dead code, and is deleted. Supersedes §11.6 item 2.

§11.6 recorded the copy at io_handlers.py:~3344 as a defect to correct. Measured
since: in the export flow **it cannot be reached**, and the correct disposition
is deletion rather than repair.

The branch runs only when `pixel_array is None` **and** `ds.PixelData` exists.
`_compress_j2k` has exactly two callers — `_finalize_dataset`
(io_handlers.py:4275) and nothing else — and `_finalize_dataset` has exactly
one, the export worker at io_handlers.py:3276, which always passes
`pixel_array=arr`. When `ctx.compression` is set the worker never assigns
`ds.PixelData` at all: the assignment at io_handlers.py:3116 sits under
`if not ctx.compression`, and the comment at :3154-:3155 already says so in
those words. The one path that reaches `_compress_j2k` with `arr is None` is
the float branch, which has deleted (7fe0,0010) — so the guard above returns
first. `tests/test_compress_handlers.py` calls `_compress_j2k(ds,
pixel_array=...)` directly and always passes an array, so the branch has never
executed in the suite either.

It is a silent-corruption sibling, which is why it must not simply be left:
reading bytes as `uint16` regardless of `PixelRepresentation` would compress
signed data to wrong values without raising. Delete the reconstruct-from-bytes
arm and keep the early return, so `pixel_array is None` means "nothing to
compress" and nothing else. The file stays uncompressed and coherent, which is
exactly what the float branch already relies on.

#### §11.13.5 Dependency consequences

**No new dependency.** `imagecodecs>=2023.9.18` is already in `setup.py`'s
`install_requires`. `tests/test_packaging_contract.py` — the single-source rule
— is satisfied as it stands.

**The import is unguarded, at module scope**, beside the other unguarded
imports in `io_handlers.py`. Not `try/except ImportError` like today's
`from PIL import Image` (io_handlers.py:136-139) and not like
`imagecodecs_handler.py`'s guarded import: a guarded import whose absence turns
into `Compression failed` is the same shape as `_load_redaction_knowledge_base`
returning `[]`, and this bunch is removing that shape, not adding one. The
precedent in `setup.py`'s own comments is `python-dotenv` — "Imported unguarded
by `isocenter/config_manager.py`" — a declared dependency imported plainly.
Consequence for the CHANGELOG: `import isocenter` now fails on an installation
missing `imagecodecs`, which is an installation `pip` cannot produce.

**`publish.yml`** checks that the built wheel carries its own `resources/*.json`
and can `ConfigLoader.load_phi_config()`. It does not exercise the export path,
so it will not catch a codec problem; nothing there needs changing and no new
release gate is proposed. Say that explicitly rather than leaving it unsaid.

**Not `extras_require`.** An optional extra that degrades would mean a
`pip install isocenter` whose compressed export silently stops working — the
#388 silence, reintroduced one issue later in the same bunch.

**The floor is unverified, and this is a numbered step, not a footnote.**
Everything above was measured on imagecodecs **2026.8.16**; `setup.py` floors
at **2023.9.18**, and signed-integer support, `codecformat="J2K"` and `level=0`
exactness on that release are unknown. A floor that does not support signed
`int16` ships #404 as fixed. **Step D1:** build a scratch venv on
`imagecodecs==2023.9.18` and run `probe_404_codecs.py` against it. If it passes,
record the number here and leave the floor alone. If it fails, raise the floor
to the first release that passes and say so in the CHANGELOG — CLAUDE.md's rule
for `setup.py` cuts that way ("a bound we cannot back with a passing matrix is
a bound we should not widen"), and by the same logic a bound we cannot back is
one we should not keep.

#### §11.13.6 What a compressed export actually contains today

Measured through the real export path on a `uint16` frame — a dtype that works
today — by reading the encapsulated fragments of the written file
(`probe_404_container.py`):

```
TransferSyntaxUID : 1.2.840.10008.1.2.4.90
fragment count    : 2          (fragment 0 is the 4-byte basic offset table)
codestream head   : 0000000c6a5020200d0a870a
VERDICT           : JP2 box (a file format), not a bare codestream
readback ok       : True
```

`Image.save(bio, format="JPEG2000")` with no filename wraps the codestream in a
JP2 box; `no_jp2=True` produces the bare codestream, and imagecodecs'
`codecformat="J2K"` produces `ff4f ff51`, the SOC marker. **Every compressed
file Isocenter has ever exported carries a JP2 box under a transfer syntax that
names a codestream.** Lenient decoders read it — pydicom does, through Pillow —
which is why nothing noticed. The project already knows the distinction on the
fixture side: #372's entry records its J2K fixtures being written `no_jp2=True`
"since Pillow's default wraps them in a JP2 box".

The fix changes this as a side effect, because `codecformat="J2K"` is the right
argument for DICOM. That makes it **three things at once** — a conformance
correction, a change to the bytes of every compressed export including the ones
that work today, and a statement about what released versions wrote. All three
are the owner's under the escalation rule, so this brief measures it and does
not decide it. See §11.13.9, call 1.

#### §11.13.7 The `conftest.py:257` blind spot

Blast radius, measured: `dummy_pixel_array_2d` and `dummy_patient` are
referenced **21 times across 5 test files** — `test_unified_config.py` (4),
`test_session.py` (7), `test_services.py` (6), `test_io.py` (3),
`test_recursive_import.py` (1) — not "the whole suite". That changes the
disposition.

**Recommendation: change the fixture's dtype to `int16` in the #404 commit, and
make it the first red test.** It is one line, its reach is five files, and on
unmodified code it turns every export test riding it red with
`Compression failed` — the defect, stated as a test, for free. Doing it here
rather than in its own issue also means the fix is proved by tests that already
existed, not only by new ones written to pass.

The decision rule for what comes back red, so this is not "see what breaks":
a red that is a `Compression failed` on the export path **is** #404 and goes
green with the fix. A red that is anything else is a test that was quietly
depending on unsignedness — a value comparison, a hash, a byte count — and each
needs its own look and its own line in the PR body. If more than a couple turn
up, stop and file them rather than absorbing them here.

Not recommended: parametrizing the fixture over signedness. It doubles five
files' worth of tests to prove one thing, and that one thing is better proved
by the dedicated `int16` cases in §11.13.8.

#### §11.13.8 Change list and TDD test plan

| File | Function | Change |
| --- | --- | --- |
| `isocenter/io_handlers.py` | module imports (`:136`-`:139`) | `import imagecodecs`, unguarded, at module scope; remove the guarded `from PIL import Image` once nothing else in this module uses it. |
| `isocenter/io_handlers.py` | `_compress_j2k` (`:3325`) | new positive width/kind guard raising the §11.13.3 message **before** any encode; `bool` normalized with `astype(np.uint8)`; `encode_frame` becomes `imagecodecs.jpeg2k_encode(frame, level=0, codecformat="J2K")`; the reconstruct-from-bytes arm (`:3333`-`:3363`) deleted per §11.13.4, keeping the early return. |
| `isocenter/io_handlers.py` | `_compress_j2k` docstring | it says "using Pillow"; it will not be. State the encoder, the accepted widths, and that `level=0` is lossless. |
| `setup.py` | `install_requires` | **no change unless step D1 fails**, in which case raise the `imagecodecs` floor and carry the reason in the comment beside it, as the other pins do. |
| `tests/conftest.py` | `dummy_pixel_array_2d` (`:257`) | `np.zeros((512, 512), dtype=np.int16)` per §11.13.7. |
| `tests/test_compress_handlers.py` | whole file | it exercises `_compress_j2k` directly and is where the shape cases belong; check nothing in it asserts a Pillow-specific error string. |
| `CHANGELOG.md` | 0.9.5 | §11.13.10. |

New file `tests/test_signed_pixels_survive_a_compressed_export.py`, registered
in `scripts/mutation_probe.py`'s `TARGETS` for `io_handlers.py` and in
CLAUDE.md's module-to-tests table, or `tests/test_mutation_probe_targets.py`
goes red. Every test names the single production edit that reddens it.

- **P1 `test_a_signed_16_bit_study_exports_with_the_default_options`**. The
  milestone test: ingest a CT-shaped `int16` frame, `save()`, `export(out)` with
  **no** keyword arguments, then assert three things together — a file exists;
  `dcmread(...).pixel_array` is bit-exact against a **literal** array and
  `dtype == np.int16`; and, after `flush_audit_queue()`, the `EXPORT` row says
  `wrote 1 of 1 planned instances` with no `ERROR` row for that UID. Today all
  three fail. *Red when:* the encoder is reverted to `Image.fromarray`.
- **P2 `test_the_written_file_declares_the_signedness_it_holds`**. Same export;
  assert `ds.PixelRepresentation == 1`, `ds.BitsAllocated == 16`,
  `ds.file_meta.TransferSyntaxUID == JPEG2000Lossless`. *Red when:* the
  descriptors are written from anything but the array's dtype.
- **P3 `test_every_supported_dtype_round_trips_through_the_compressed_path`**,
  parametrized over `uint8, int8, uint16, int16, bool` from §11.13.3's table.
  Compare to a literal per case, and assert the dtype, never
  `got.dtype == src.dtype` of an array built in the same test. *Red when:* the
  `bool` normalization is dropped (that arm alone), or the encoder is reverted
  (all arms).
- **P4 `test_a_32_bit_frame_is_refused_by_name_rather_than_written_wrong`**,
  parametrized `uint32`/`int32`. Assert the export fails **and** that the
  message names the dtype, `BitsAllocated`, `PixelRepresentation`, the encoder
  and `use_compression=False`; then assert **no file reached disk**. *Red when:*
  the width guard is removed — at which point the encode succeeds, a file is
  written, and `wrote 1 of 1` appears beside a file `pixel_array` cannot
  decode. This is the test that stops the fix from creating a new silence.
- **P5 `test_a_64_bit_frame_is_refused_by_name`**. Same shape; the codec's own
  `ValueError` must not be what the user sees. *Red when:* the guard is
  narrowed to 32-bit only.
- **P6 `test_float_pixel_data_still_exports_uncompressed_under_the_default`**.
  A `float32` instance exported with defaults: assert a file exists, that it
  carries (7fe0,0008) or (7fe0,0009) and no (7fe0,0010), and that the transfer
  syntax is **not** JPEG 2000. *Red when:* the `hasattr(ds, "PixelData")` early
  return is removed, or the deleted fallback is restored.
- **P7 `test_a_multi_frame_signed_stack_survives`**. Three `int16` frames;
  assert shape `(3, H, W)`, dtype, and values against a literal. *Red when:*
  the per-frame loop is replaced by a single whole-array encode.
- **P8 `test_an_rgb_frame_still_round_trips`**. `uint8` `(H, W, 3)`, one frame
  and two: assert exact values **and** `PhotometricInterpretation == "RGB"` and
  `PlanarConfiguration == 0`. The control against a colour regression from the
  encoder swap. *Red when:* the encoder is given the frame with its axes
  reordered.
- **P9 `test_the_encapsulated_fragment_is_a_codestream_not_a_jp2_box`**. Read
  the fragments of a written file and assert the payload starts `ff4f`. *Red
  when:* `codecformat="J2K"` is dropped. **Hold this test until the owner has
  answered §11.13.9 call 1** — it pins a behaviour change the owner may want
  scoped differently.
- **P10 `test_compress_j2k_without_an_array_writes_nothing_and_raises_nothing`**.
  Call `_compress_j2k(ds, pixel_array=None)` on a dataset carrying `PixelData`
  and assert the transfer syntax is unchanged and `PixelData` is untouched —
  the characterization pin for §11.13.4's deletion. Docstring says it is a
  characterization test, as `tests/test_compaction_races_a_concurrent_write.py:357`
  does. *Red when:* the reconstruct-from-bytes arm is restored.

Regression watch: `tests/test_compress_handlers.py`,
`tests/test_compress_j2k_coverage.py`, `tests/test_export_pixels.py`,
`tests/test_export_readback.py`, `tests/test_float_pixel_data_export.py`,
`tests/test_redaction_rgb.py`, `tests/test_planar_configuration_roundtrip.py`,
`tests/test_colour_space_at_ingest.py`, `tests/test_nested_pixel_carriage.py`,
plus the five files §11.13.7 names.

#### §11.13.9 Owner calls raised by #404

1. **The JP2 box (§11.13.6).** Every compressed file Isocenter has exported
   carries a JP2 box under `1.2.840.10008.1.2.4.90`, which names a codestream.
   The fix produces a bare codestream instead. Should the CHANGELOG name that
   as a conformance fix in its own right — a claim about what released versions
   wrote — or should the exporter keep writing the JP2 wrapping for
   compatibility with whatever has been reading Isocenter's output? Downstream
   readers are a fact I cannot measure, so this brief does not choose. P9 is
   held until you answer.
2. **Refuse or fall back, for 32/64-bit.** The design refuses, which keeps
   `use_compression=True` meaning what it means and keeps the same exception
   and the same `wrote 0 of N`. The alternative is to write the file
   uncompressed and file an audit row saying the codec could not carry that
   width — louder in the useful direction, and not silent, but it changes what
   a frozen option does. Your call, not mine.

#### §11.13.10 CHANGELOG shape for #404

A **breaking-adjacent** entry — no signature changes, but a previously failing
call now succeeds and a previously succeeding one now raises a different
message. Open on the corrected diagnosis, not the width the issue was found
through: *"Signed pixel data could not be exported at all. `session.export()`
compresses by default, and the JPEG 2000 encoder it used accepted exactly
`uint8` and `uint16`, so every CT and MR study — `int16`,
`PixelRepresentation 1` — failed with `Compression failed: broken data stream
when writing image file`, `wrote 0 of 1`, and nothing on disk (#404)."*

Carry: that the axis is **signedness, not width**, with the measured Pillow
table; that the shared test fixture was `uint16`, one of the exactly two dtypes
that worked, which is why the suite was green; that the encoder is now
`imagecodecs`, **already an `install_requires` dependency and already used for
decoding**, imported unguarded, so `import isocenter` now fails on an install
missing it; the supported matrix and the **exact exception** for what is not —
the `RuntimeError` naming dtype, `BitsAllocated`, `PixelRepresentation`, the
encoder and `use_compression=False`; that 32-bit is refused **because the codec
encodes it silently wrong above 25 bits and the resulting file cannot be
decoded at all**, so a permissive version of this fix would have replaced a
loud failure with `wrote 1 of 1` beside an unreadable file; that
`_compress_j2k`'s reconstruct-from-bytes branch is deleted as unreachable
rather than corrected, with the reachability argument; that float pixel data is
unaffected and still exports uncompressed per PS3.5 8.2; and — if the owner
rules that way on call 1 — that the encapsulated payload is now a bare
codestream where it was a JP2 box. Reference §11.6's P1 as the step that found
it, and do not rewrite §11.6.

#### §11.13.11 What the reviewer should attack, added to §6

16. **The width guard is the whole safety of this fix.** Delete it and the
    suite must go red on P4; if it does not, P4 is decoration. Check that P4
    asserts *no file on disk*, not merely that something raised.
17. **`level=0` is assumed to mean lossless.** It is measured exact on
    full-range `int16` and identical to `reversible=True`, but that is a
    property of one imagecodecs release. Step D1 is the floor check; ask
    whether it was actually run and what number came back.
18. **The bool arm writes `uint8`.** Confirm the uncompressed path writes the
    same bytes for the same array, or the two export paths disagree for one
    dtype.
19. ~~**`_verify_readback` cannot catch this family.** `_READBACK_DESCRIPTORS`
    is `("Rows", "Columns", "SamplesPerPixel", "NumberOfFrames",
    "BitsAllocated")` — no `PixelRepresentation` — and it never decodes pixels,
    so `verify_readback=True` passes on a file whose codestream is wrong.~~
    Naming it here; widening it is not this bunch's work. — superseded by
    #449 (v0.9.6), which widened it: the readback decodes every written file.
20. **The conftest flip is a test-quality change with a blast radius.** Every
    red it produces must be explained in the PR body, not absorbed.

---

*End of amendments. Nothing above §11 was rewritten; the struck clauses and
their markers are the whole of the change to the dated text.*
## §12 Implementation addendum — step P1, measured

**Superseded in part: §12.4's disposition, by §11.13.** This section was
written when P1's escalation was still open; the owner then ruled neither
of §11.6 arm 3's options but "fix it properly, at the encoder", the
architect designed that as §11.13, and #404 is implemented in this same
bunch. §12.4's "left exactly as it was in this bunch" is therefore no
longer true and is struck below; §12.1-§12.3 and §12.5 stand as measured.

**§11.13.3's support matrix is incomplete and owner ruling 2's stated
scope ("32/64-bit only") is departed from — see §12.6.** The matrix has no
row for a 16-bit *multi-sample* frame, because Pillow refused that shape at
`Image.fromarray` and the question could not be asked; measured after the
encoder swap, it encodes exactly and produces a DICOM file no pydicom
decoding plugin will read. The refused set is therefore wider than the
ruling names. This is recorded here rather than argued in place: the
departure and its evidence are §12.6, and the owner may re-rule it.

Added by the TDD developer during implementation, on 2026-09-09, because
§11.6 requires one of its three arms to be written into this file with its
numbers before the bunch is called done. Nothing above is rewritten.
Reproduce with
`docs/superpowers/specs/2026-09-09-the-last-silences-bunch-a/probe_386_p1_compressed.py`,
committed beside the six of §11.7.

### §12.1 The rig changed mid-implementation

The project `.venv` was rebuilt to **3.12.14 — the CI floor** — while this
bunch was in progress, so §11.12's 3.14.7 workaround is superseded for
anyone reading this later. Every number below was taken on 3.12.14,
numpy 2.5.3, pydicom 3.0.2, Pillow 12.3.0, with the worktree ahead of the
editable install on `PYTHONPATH` and `isocenter.__file__` printed and read
each time. The suite baseline on that interpreter is **1685 passed,
1 skipped**; the one skip is `test_discovery_integration.py::
TestDiscoveryIntegration::test_proper_noun_merging` (spacy absent).

### §12.2 The four cells

One 4x4 frame per row, ingest to `save()` to `close()` to reopen to
`export()`, with the `_INTEGER_DTYPE_BY_BITS` table in place so the frames
load at all:

| dtype | pixrep | `use_compression` | files | file BitsAllocated / PixelRepresentation | `pixel_array` | `written` | audit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| uint32 | 0 | **False** | 1 | 32 / 0 | `uint32`, values exact | 1 | `EXPORT ... wrote 1 of 1 planned instances` |
| int32 | 1 | **False** | 1 | 32 / 1 | `int32`, values exact | 1 | `EXPORT ... wrote 1 of 1 planned instances` |
| uint32 | 0 | **True** (the default) | **0** | — | — | **0**, `ExportError` raised | `ERROR ... Compression failed: broken data stream when writing image file` + `EXPORT ... wrote 0 of 1` |
| int32 | 1 | **True** (the default) | **0** | — | — | **0**, `ExportError` raised | same |

Read after `flush_audit_queue()`, so "no row" and "row not yet written"
are distinguishable.

**Verdict: arm 3.** The export refuses, and refuses *loudly* about
whether — `written_uids == []`, an `ExportError` raised, nothing on disk —
but the accounting does not say *why*: the `ERROR` row carries Pillow's
own sentence and names neither the dtype nor the width. The uncompressed
path is entirely correct for both, which is the second half of the
finding: nothing is wrong with the pixels or the descriptors.

### §12.3 The population is wider than §11.6 assumed, and the axis is not width

Measured directly against Pillow 12.3.0's JPEG 2000 encoder:

| array | `Image.fromarray` mode | `save(format='JPEG2000', compression='lossless')` |
| --- | --- | --- |
| uint8 (H,W) | `L` | OK |
| uint16 (H,W) | `I;16` | OK |
| uint8 (H,W,3) / (H,W,4) | `RGB` / `RGBA` | OK |
| **int8, int16, int32, uint32** | `I` | **OSError: broken data stream when writing image file** |
| float32 | `F` | same |
| bool | `1` | same |
| uint16 (H,W,3) / (H,W,4) | — | `TypeError: Cannot handle this data type` at `fromarray` |
| uint64, int64 | — | `TypeError: Cannot handle this data type` at `fromarray` |

So **signedness is the axis, not width**, and `int16` is CT and MR: a
plain `session.export(folder)` — compression is the default — fails
outright for the most ordinary medical dtype there is, writing nothing.
Confirmed end to end: `int16` and `int8` raise `ExportError` while
`uint16` and `uint8` write correct J2K files.

**This is pre-existing, not introduced here.** The identical probe run
against the main checkout at `c925829`, unmodified, gives identical
results. The loader is correct — it honours `pixel_representation == 1`
and returns `int16` — and the frame dies at the encoder. The suite never
caught it because `tests/conftest.py:257`'s shared pixel fixture is
`np.zeros((512, 512), dtype=np.uint16)`, one of the exactly two dtypes
that work.

### §12.4 What was done about it — *superseded by §11.13*

Escalated rather than decided. **Owner ruling: neither of §11.6 arm 3's
two options — fix it properly, at the encoder, as its own issue.** The
measurement is now **#404**, on the v0.9.5 milestone.

~~and goes back through the architect because the encoder fix is a design
call with a probable dependency change. `_compress_j2k`'s error path is
therefore left exactly as it was in this bunch: an informative refusal
written here would be written and then deleted by #404's fix.~~
**[Superseded by §11.13: the architect's design landed inside this bunch
and #404 is implemented in this PR. The encoder is `imagecodecs` — already
in `install_requires` and already driving the decode side, so not a new
dependency. §11.6's informative refusal survives, narrowed to 32- and
64-bit, which is the one width range that stays unsupported. The error
path is therefore rewritten here after all.]**

The fall-back-to-uncompressed-and-file-a-row option was **not** taken, and
the owner did not take it either: a `DATA_LOSS` row for a fallback that
loses nothing would be exactly the kind of false statement this milestone
exists to remove.

Two consequences handled inside this bunch regardless, because both are
about work this PR does rather than about #404:

1. **`_compress_j2k` views a bool frame as `uint8` before encoding.** The
   `bool` carrier of D2 would otherwise have been a regression: while a
   mask reloaded as `uint8` the default export worked, and the moment it
   began reloading as `bool` it reached `Image.fromarray` as mode `1`.
2. **§1.5 T6 exports with `use_compression=False`**, with a comment citing
   this section and #404. On the default path it would be red for a reason
   with nothing to do with recording signedness, and the choice stays
   correct after #404 lands.

### §12.5 Two corrections to the brief, found by implementing it

- **§2.5 T6's second arm is wrong as written.** It asks for "a second arm
  asserting a session cannot `audit()` on that install". Measured:
  `session.audit()` passes `config_tags=self.configuration.phi_tags` to
  `PhiInspector`, and `{}` is not `None`, so `__init__` takes its first
  branch and `load_phi_config()` is never reached. ~~A bare session already
  audits against an empty policy and says so with its own
  `"PHI Scan Warning: No PHI tags defined"` — a different question, and
  not a silence.~~ *(Superseded by #495: the empty policy was a silence,
  and a bare session now applies the floor policy.)* The test instead asserts the two paths that do reach the
  loader: `PhiInspector()` with no policy (`privacy.py:161`), and
  `create_config()` through `_scaffold_phi_tags`, which is also the arm
  that proves the `except (OSError, ValueError)` handlers do not swallow a
  `RuntimeError`.
- **§11.5 phrases byte-order normalization before the accept check;
  implemented the other way round.** The check is O(1) on `dtype` and
  `astype(dtype.newbyteorder('='))` copies the whole frame, so normalizing
  first would fully copy a large `complex64` array immediately before
  rejecting it. The behaviour is identical for everything accepted.

### §12.6 Step D1, measured — and one cell §11.13.3 never measured

`probe_404_floor.py` is committed beside the other thirteen probes, per
§11.7. It imports no isocenter code so it can be pointed at a scratch venv
holding only `imagecodecs` and `numpy`, which is how the declared floor was
measured.

**The floor moves, and it had to.** `imagecodecs==2023.9.18` — the floor
`setup.py` declared before this change — publishes no cp312 wheel and does
not build from source here, so it cannot be installed on this project's own
`python_requires` floor at all. `2024.6.1` is the next release, installs
from a wheel, and returns the same verdict as 2026.8.16 on every cell:

| dtype | 2024.6.1 | 2026.8.16 |
| --- | --- | --- |
| `uint8`, `int8`, `uint16`, `int16` (1 sample) | exact | exact |
| `uint8`, `int8` (3 samples) | exact | exact |
| `uint16`, `int16` (3 samples) | exact at the codec | exact at the codec |
| `uint32`, `int32` | encodes, **inexact** | encodes, **inexact** |
| `uint64`, `int64` | `Jpeg2kError: opj_encode or opj_write_tile failed` | `ValueError: item size not supported by codec` |
| `float32`, `bool` | `ValueError: invalid data shape or dtype` | `ValueError: sample format not supported by codec` |

Every codestream starts `ff4fff51` on both releases. The two releases give
the *same verdicts* and *different sentences*, which is the measured reason
the refusal's wording is ours rather than the codec's.

**The cell §11.13.3 did not measure: 16-bit multi-sample.** Pillow refused
it at `Image.fromarray`, so the question could not previously be asked, and
the matrix in §11.13.3 has no row for it. Measured after the swap: the
codestream is *exact*, the export succeeds, a file is written — and
`ds.pixel_array` raises `RuntimeError: Unable to decode as exceptions were
raised by all available plugins` / `Pillow cannot decode 16-bit
multi-sample data correctly`. Pillow is the only JPEG 2000 decoding plugin
this project installs, so the library could not re-ingest its own export --
verified in review of #405 through `session.ingest()` rather than bare
`dcmread`: a `uint16` RGB export comes back `ingested=0` with a
`Decompression Failed` row, while the `uint8` equivalent ingests with exact
pixels.

**And the claim is narrower than "JPEG 2000 cannot do this", deliberately.**
`imagecodecs.jpeg2k_decode` reads those frames bit-exactly and so does
pylibjpeg-openjpeg. The standard applied here is *what this library can read
back*, which is the same standard the 32-bit cell is judged by — and the
refusal's own sentence says that, rather than "no plugin reads it", which
would be a false statement in the one place a user reads it. Review of #405
caught the first wording; the runtime message and the docstring now name
Pillow and this installation.

That is 32-bit's silence arriving through a different door, and it is
reachable *only* because of this fix — which makes it a hole in this PR's
own code, not an adjacent issue. So the itemsize tuple `§11.13.6` names,
`_J2K_ENCODABLE_ITEMSIZES`, is **renamed** `_J2K_ENCODABLE_FRAMES` and
becomes a matrix keyed on `(itemsize, samples > 1)` rather than a list of
widths -- with `_J2kWidthRefusal` renamed `_J2kFrameRefusal` for the same
reason, since "width" is no longer the whole rule:

    _J2K_ENCODABLE_FRAMES = frozenset({(1, False), (1, True), (2, False)})

`int8` multi-sample, which Pillow also refused, *is* exact and is now
supported — so the swap widens the accepted set as well as fixing it.
Owner ruling 2's shape is unchanged: positive rule, raised before any
encode and before any `ds` mutation, naming the dtype, `BitsAllocated`,
`PixelRepresentation` and `use_compression=False`, not re-wrapped by the
outer handler. Only its *reason* clause is now selected by which cell was
refused, because "the encoder is exact only to 25 bits" is false for the
16-bit case and would send a reader after the wrong thing.

*End of implementation addendum.*
