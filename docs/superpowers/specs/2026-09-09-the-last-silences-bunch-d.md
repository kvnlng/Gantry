# The Last Silences, bunch D: what the file carries versus what the caller set

**Date:** 2026-09-09
**Milestone:** v0.9.5 -- The Last Silences
**Issues:** #406 (a dtype-only `set_pixel_data()` is swallowed by the frame
dedup), #407 (`imagecodecs_handler` joins the Basic Offset Table into the
codestream)
**Base:** `main` at `124e9e5` (bunch C: #396, #397, #401, #394, #383)
**Status:** design brief for a TDD developer. Three architect decisions are
recorded in §2, §3 and §4; four owner questions are in §9.
**Superseded in part:** #416 (v0.9.6). §3's ruling that
`_J2K_ENCODABLE_FRAMES`' `(2, True)` refusal does not move, and §8's
CHANGELOG sentence saying so. Both were true of this spec's change: #407
gave `Instance.get_pixel_data()` a working imagecodecs decode and gave
ingest none. §3 filed the fallback that would move the cell as a
capability decision (§9 Q2); the owner ruled it in for v0.9.6, and #416
added it to `_decode_pixels` and `(2, True)` to the frozenset in the same
change. Both clauses are struck in place below.
**Superseded in part:** #447 (v0.9.6). §6.2's ruling to leave
`README.md:34` alone because the fix "makes the sentence true" -- it
never did for RLE, whose handler arm called a function imagecodecs does
not have -- and §6.3's rewrite of `test_rle_lossless_handling`, which
#447 deletes with the arm. Both are struck in place.

Both issues are about a **snapshot that stopped describing the thing it was
taken of**, and in both the snapshot is between the caller's pixels and the
bytes on disk.

- #406: `SidecarPixelLoader` captures the descriptors at construction. The
  save's dedup arm decides "these bytes are already stored" and returns
  without rebuilding it, so a `set_pixel_data()` that changes only the
  *type* of the pixels -- leaving the bytes bit-identical -- is written off
  by a commit that never re-read the instance. On the float carrier that
  reaches the exported file: the caller sets `int32`, the file carries
  `FloatPixelData`, and the audit log says `wrote 1 of 1`.
- #407: the single-frame decode arm joins **every item** of the encapsulated
  pixel data, the Basic Offset Table included, and hands the result to
  `imagecodecs`. Four zero bytes in front of `ff4f ff51` and the codec
  refuses it. That arm has never worked.

Unlike bunch C, both fixes are production code. One of them changes what a
default `session.export()` does, and that change needs a CHANGELOG breaking
entry (§8).

---

## 1. How this was measured

Nothing below is inherited from the issue text. Where an issue disagrees with
the code, the code is what is reported and the correction is called out in
that issue's own section.

- **Floor:** `/Users/kevin/Developer/Isocenter/.venv/bin/python` -- 3.12.14.
- **Free-threaded gate:** `/Users/kevin/Developer/Isocenter/.venv314t/bin/python` -- 3.14.7t.
- **Worktree:** `/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-acad1b70700ba80e4`.
- **Third parties:** `imagecodecs 2026.8.16`, `pydicom 3.0.2`, `numpy 2.5.3`.

Every invocation carried `PYTHONDONTWRITEBYTECODE=1` (#174) and an explicit
`PYTHONPATH=<worktree>`. The resolved import was checked first and read:

```
/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-acad1b70700ba80e4/isocenter/__init__.py
```

so the editable install did not serve the main checkout. Every candidate fix
and every mutation below was applied **in the worktree itself** and reverted
with `git checkout`; a scratchpad copy is not a mutation sandbox, because
`PYTHONPATH` loses to the process cwd and the copy measures the unmutated
tree.

**The suite was run whole, twice, with candidate fixes in place**, because
"which existing test notices this?" is the question both issues turn on:

| tree | result |
| --- | --- |
| `124e9e5` + the #406 fix | `1813 passed, 2 skipped in 280.43s` |
| `124e9e5` + the #406 fix + the #407 fix | `1 failed, 1812 passed, 2 skipped in 284.45s` |
| the same, plus deleting the now-unused `generate_fragments` import | `tests/test_imagecodecs_edge_cases.py`: `2 failed, 5 passed` |

The single failure is `tests/test_imagecodecs_edge_cases.py::test_rle_lossless_handling`,
and it is a *mock* failure, not a behaviour failure; §6.3 says what to do
with it. **The #406 fix breaks nothing at all** -- which is the finding, not
the reassurance: no test in this repository can see the defect (§5.7).

Seven probes are committed beside this brief in
`docs/superpowers/specs/2026-09-09-the-last-silences-bunch-d/`, for bunch A,
B and C's reason -- a document whose whole claim is measured evidence cannot
point at a directory that exists for nobody but its author. They are kept off
the docs site by `mkdocs.yml`'s `exclude_docs` and cannot be collected by
pytest (`pytest.ini` sets `testpaths = tests`). Each derives the repo root
from its own `__file__`, so they run from any checkout:

- `probe_406_dedup_stale_loader.py` -- the three carriers, end to end (§5.1--5.3).
- `probe_406_severe_export_both_ways.py` -- the float case at
  `use_compression=True` and `False`, with the audit log and the sidecar
  growth (§5.2).
- `probe_406_sibling_arr_is_none.py` -- the sibling arm this bunch does
  **not** fix (§5.8).
- `probe_407_bot_join.py` -- the fragment structure and the refusal, on
  three frame shapes (§6.1).
- `probe_407_generate_frames_edges.py` -- five encapsulation shapes against
  `generate_fragments` and `generate_frames` (§6.4).
- `probe_407_404_interaction.py` -- five separate readers of one 16-bit
  RGB JPEG 2000 file, before and after the fix (§3).
- `probe_407_fallback_arm_and_edges.py` -- the `entities.py` fallback arm
  and the undeclared-`NumberOfFrames` edge (§6.5, §6.7).

**Line-number drift in the issues, recorded rather than corrected silently.**
#406 cites `persistence.py:3512`-`:3521` for the dedup arm; at `124e9e5` the
arm is **3508--3519** (comment 3508--3510, condition 3511--3512, body
3513--3519), and 3521 is the `write_frame` call *below* it. #406 cites
`io_handlers.py:3705`-`:3708` for the loader's snapshot; the snapshot is
**3699--3711** -- 3705--3708 is its middle. Both windows name the same code,
and neither miscitation changes the argument. #407's `imagecodecs_handler.py:143`
is exact.

---

## 2. Architect decision 1: bunch membership

**Ruling: the two-issue cut stands. #410 does not join it. Recommend #410 and
#411 as a bunch E.**

#406 and #407 share one mental model -- *the exported file versus the pixels
the caller set* -- and one machinery: the sidecar loader on one side and the
codec that reads its frames back on the other. A developer holding "does the
file carry what was asked for?" can do both, and the PR body is one paragraph.
They also share a reviewer question, which is the better test: both fixes are
checked by decoding a file and comparing an array, never by the absence of an
exception.

**#410 is an export defect and still does not belong here.** `export(format="wfdb")`
silently ignoring an unknown option is a defect in what a *call* refuses, not
in what a *file* carries; the pixels are irrelevant to it, and so is every
line of machinery this bunch touches. Three concrete reasons, beyond the
theme:

1. **It is blocked on an owner ruling.** #410's own text says the open
   question is "whether wfdb should raise on *any* unrecognised option or
   only warn". A defect-fix PR that also decides an API-strictness policy is
   two PRs wearing one hat, which is the judgement bunch C already made about
   this same issue.
2. **It carries a breaking CHANGELOG entry of its own**, naming an exception a
   previously-working call now raises. #406 already brings one (§8). Two
   unrelated breaking entries in one release note is how a reader loses the
   thread of either.
3. **It pairs naturally with #411** (five user-visible audit strings outside
   the freeze). Both are "the surface promises something and nothing enforces
   it, one layer above the data" -- a real theme, and the right home for the
   `raise`-versus-`warn` ruling once the owner makes it.

#409 (the stale notebook) and #399 (identity-token re-lock) stay where they
were placed. #412, #414 and #415 are bunch C's own filed follow-ons and are
not touched here.

---

## 3. Architect decision 2: the #407 / #404 interaction

~~**Ruling: `_J2K_ENCODABLE_FRAMES`'s `(2, True)` refusal does NOT move, and
this PR must not move it. #407's claim that the refusal is "contingent on this
bug" is measured false for the door the refusal is about. The docstring gains
one paragraph so that a future reader does not undo it on the strength of the
fallback.**~~ **Superseded by #416 (v0.9.6):** true of #407's change, which
did not move the cell; #416 gave `_decode_pixels` the imagecodecs fallback
this section files below, and moved `(2, True)` with it. Left struck rather
than rewritten -- see the front matter.

#407's last section says:

> So the `(2, True)` refusal is **correct today and contingent on this bug**.
> Fixing this decode path would make 16-bit colour readable here and would
> move that cell.

`probe_407_404_interaction.py` builds the exact file `_J2K_ENCODABLE_FRAMES`
refuses to write -- a 4x4 `uint16` RGB frame, hand-encoded with
`imagecodecs.jpeg2k_encode(..., codecformat="J2K")` and wrapped by
`encapsulate()` -- and asks **five separate readers** for its pixels, on
`124e9e5` and again with the #407 fix applied in the worktree:

| reader | on `124e9e5` | with #407 fixed |
| --- | --- | --- |
| A. `pydicom.dcmread(f).pixel_array` | `RuntimeError`: `pillow: Pillow cannot decode 16-bit multi-sample data correctly` | **unchanged** |
| B. `io_handlers._decode_pixels` (`io_handlers.py:1319`, `get_decoder(ts).as_array(ds)`) | the same `RuntimeError` | **unchanged** |
| C. `imagecodecs_handler.get_pixel_data` | `RuntimeError`: `not a J2K or JP2 data stream` | **`uint16 (4, 4, 3)`, bit-exact** |
| D. `Instance.get_pixel_data()` on a file-backed instance | `RuntimeError: Lazy load failed ...` | **`uint16 (4, 4, 3)`, bit-exact** |
| E. `session.ingest(folder)` | `IngestSummary(ingested=0, ...)` + an `ERROR` audit row | **unchanged** |

The decisive row is **B**. `ingest_worker` reads pixels through
`_decode_pixels` (`io_handlers.py:1519`), which is `get_decoder(ts).as_array(ds)`
-- pydicom's own `pydicom.pixels` backend, whose plugins here are Pillow and
nothing else. **It has no imagecodecs fallback and #407 does not give it
one.** So row E does not move either: `session.ingest()` on such a file still
returns `ingested=0` with a `Decompression Failed` row.

And row E is precisely the ground `_J2K_ENCODABLE_FRAMES` states for the
refusal (`io_handlers.py:3407`-`:3430`):

> `session.ingest()` on our own export returns `ingested=0` with a
> `Decompression Failed` row.

That sentence is still literally true after #407 is fixed. **The cell does not
move.**

What *does* change is that the library becomes able to read this file at one
door (`Instance.get_pixel_data()`'s imagecodecs fallback) and not at another
(`session.ingest()`). That asymmetry is exactly the kind of fact a future
reader "cleans up" -- they will find `imagecodecs.jpeg2k_decode` working in
row C, conclude the refusal is stale, and delete a guard that is not. So:

**Required, in the same PR:** add a short paragraph to the
`_J2K_ENCODABLE_FRAMES` comment (`io_handlers.py:3407`-`:3430`) saying, in the project's comments-explain-the-trap register:

> #407 fixed `imagecodecs_handler`'s single-frame decode, so
> `Instance.get_pixel_data()`'s imagecodecs fallback **does** now read a
> 16-bit multi-sample J2K frame back bit-exactly. That does not move this
> cell. `ingest_worker` reads through `_decode_pixels` ->
> `get_decoder(ts).as_array(ds)`, pydicom's own backend, whose only J2K
> plugin here is Pillow -- measured at `124e9e5` + #407: `session.ingest()`
> on such a file still returns `ingested=0` with a `Decompression Failed`
> row. The round trip this rule is about is export -> ingest, and it is
> still broken. Widening it means giving `_decode_pixels` a fallback, which
> is #(new), not a change to this frozenset.

**Not required, and ruled against:** editing the refusal *message* at
`io_handlers.py:3453`-`:3462`. It reads "the file would be written and then
unreadable by this library itself", which after #407 is true of the ingest
door and false of `Instance.get_pixel_data()`. Leave it. Three reasons: the
round trip a user performs after an export is an ingest, so the sentence
describes the path they will actually take; the remedy it offers
(`use_compression=False`) is unchanged and still correct; and the message is
user-facing text on the export path, so changing its wording is a claims
decision rather than a defect fix. It is Q1 in §9 if the owner wants it
tightened to "cannot be ingested by this library".

**File, do not decide:** should `_decode_pixels` fall back to
`imagecodecs_handler` when every pydicom plugin has failed? That is what would
move `(2, True)`, and it is a capability decision -- a file the library
rejects today would start ingesting, `_J2K_ENCODABLE_FRAMES` would gain a
row, and `use_compression=True`'s behaviour on 16-bit colour would change.
Draft in §9, Q2.

---

## 4. Architect decision 3: what "frozen" obliges here

`docs/api/stability.md:73` freezes the `dicom` export options as
`use_compression=True, check_burned_in=False, check_reversibility=True,
patient_ids=None, show_progress=True, subset=None, verify_readback=False`,
and `tests/test_frozen_surface.py` pins them through `_export_dicom`'s
signature. Three distinctions the developer must hold:

1. **The freeze is over names, defaults and shapes -- not over outcomes.**
   No option is added, removed, renamed or re-defaulted by either fix. Nothing
   in `docs/api/stability.md` may change.
2. **`use_compression=True` keeps its name and its default, and a call that
   previously wrote a wrong file will now raise.** That is not a freeze
   violation; it is the freeze doing its job, because the *signature* is what
   was promised and the promise was never "this always writes a file". It is
   still a breaking change and needs the CHANGELOG entry in §8.
3. **No new audit vocabulary.** Neither fix invents an `action_type`.
   #406's severe case, once fixed, either exports correctly (uncompressed) or
   fails through the existing `ERROR` + `EXPORT` rows that
   `_J2K_ENCODABLE_FRAMES` already produces. If a developer finds themselves
   wanting a new `DATA_LOSS` reason string, stop -- that is #411's territory
   and an owner call.

---

## 5. #406 -- the dedup swallows a dtype-only replacement

### 5.1 The defect, measured

The dedup arm, `isocenter/persistence.py:3508`-`:3519`:

```python
            # Deduplication: identical bytes already in the sidecar.
            # Appending them again would grow the file by a full frame
            # per save.
            if (getattr(inst, '_pixel_hash', None) == digest
                    and isinstance(loader, SidecarPixelLoader)):
                inst._pixel_hash = digest
                # These exact bytes are already in the sidecar and the
                # loader already points at them, so the resident array
                # is recoverable and freeable again (#293).
                inst._pixel_array_unwritten = False
                return _StoredFrame(loader.offset, loader.length,
                                    loader.alg, digest)
```

`loader` is not rebuilt. `SidecarPixelLoader.__init__`'s instance branch
(`io_handlers.py:3699`-`:3711`) snapshots `rows`, `cols`, `samples`, `frames`,
`bits`, `pixel_representation`, `pixel_hash` and `pixel_dtype` **once**, at
construction, and `__call__` reconstructs the frame from that snapshot -- so
every one of those eight fields can go stale here.

`probe_406_dedup_stale_loader.py`, on `124e9e5`:

```
================ MILD: uint16 ingested, set_pixel_data(int16 view)
  after ingest: carrier = None  0028,0100 = 16  0028,0103 = 0
  loader dtype after first save: None bits: 16 pixrep: 0
  after set_pixel_data: carrier = None  0028,0100 = 16  0028,0103 = 1  unwritten = True
  loader object unchanged: True  hash unchanged (dedup hit): True
  loader dtype after second save: None bits: 16 pixrep: 0
  unload_pixel_data() -> True
  LIVE after unload: dtype uint16  equals what the caller set: False
  export: OK
   EXPORTED: BitsAllocated 16  PixelRepresentation 1
   EXPORTED dtype: int16  equals the array the caller set: True
  REOPENED dtype: int16  equal: True
```

The loader still says `pixrep 0` after the second save while `attributes` say
`PixelRepresentation 1`, so a reload reads the same bytes unsigned. The source
values are `40000..40015`, deliberately
`>= 32768`, so a `uint16` reading and an `int16` reading of the same bytes
differ; below 32768 they agree and the assertion would be `0 == 0`.

**The issue is right about the boundary.** The store, a reopened session and
the exported file are all correct in this case, on both compression settings,
because the export declares its descriptors from `attributes` and the bytes
are bit-identical. What lies is the live object, for the rest of the session.

### 5.2 The severe case: the exported file lies

`probe_406_severe_export_both_ways.py`, on `124e9e5`. A `float32` instance
(carrier `float32` from `io_handlers.py:1580`), then
`inst.set_pixel_data(floats.view(np.int32))` -- bit-identical bytes, so the
dedup hits:

```
=== use_compression=True
  sidecar grew by: 0 bytes (0 == the dedup arm ran)
  export: OK
  files: 1
   FloatPixelData: True  PixelData: False  BitsAllocated: 32  PixelRepresentation: None
   dtype: float32  equals the int32 array set: False
   AUDIT EXPORT: DICOM export to .../out: wrote 1 of 1 planned instances from 1 patients.

=== use_compression=False
  sidecar grew by: 0 bytes (0 == the dedup arm ran)
  export: OK
  files: 1
   FloatPixelData: True  PixelData: False  BitsAllocated: 32  PixelRepresentation: None
   dtype: float32  equals the int32 array set: False
   AUDIT EXPORT: DICOM export to .../out: wrote 1 of 1 planned instances from 1 patients.
```

**One `EXPORT` row saying `wrote 1 of 1`, and nothing else.** No `ERROR`, no
`DATA_LOSS`, no `WARNING`. The caller set an `int32` array and the file
carries `FloatPixelData` with float values, on both compression settings.

Why the exported file follows the loader rather than `attributes`: the export
picks the pixel container from the **array's dtype**, at
`io_handlers.py:2942` --

```python
        if arr is not None and arr.dtype.kind == 'f':
```

-- and `arr` is what the loader produced. `session.export()` runs
`release_memory()` before dispatching (`Memory Cleanup: Released 1 pixel
arrays` in the probe output), so the resident `int32` array is dropped and
the stale loader's `pixel_dtype='float32'` is what rebuilds it. The carrier
in `attributes` has correctly been *deleted* by `set_pixel_data`
(`entities.py:1316`); the loader's copy of it has not.

`REOPENED dtype: int32  equal: True` -- the store row is right. Only the live
graph and the file it produces are wrong.

### 5.3 A third case the issue does not name: geometry

The stale snapshot is not only about dtype. Same probe, third case: a 4x4
`uint8` instance, then `set_pixel_data(same_bytes.reshape(2, 8))` --
bit-identical bytes, new `Rows`/`Columns`:

```
================ GEOMETRY: uint8 4x4 ingested, set_pixel_data(same bytes as 2x8)
  loader object unchanged: True  hash unchanged (dedup hit): True
  unload_pixel_data() -> True
  LIVE after unload: dtype uint8  equals what the caller set: False
```

The loader reshapes to `(4, 4)` from its own stale `rows`/`cols` while
`attributes` say `2 x 8`. Export is correct here for the same reason the mild
case is (descriptors win, bytes are identical), so this is a live-object lie
-- but it settles the **shape of the fix**: the answer is to rebuild the
loader, not to patch its `pixel_dtype`.

### 5.4 The fix

One statement, in the dedup arm, immediately after `inst._pixel_hash = digest`
(`persistence.py:3513`):

```python
                # The bytes are the loader's bytes; the DESCRIPTORS may not
                # be. `SidecarPixelLoader` snapshots rows, cols, samples,
                # frames, BitsAllocated, PixelRepresentation and the
                # `_ISOCENTER_PIXEL_DTYPE` carrier at construction
                # (`SidecarPixelLoader.__init__`), so a `set_pixel_data()` that
                # changes only the *type* of the pixels -- a `float32`
                # frame handed back as `int32`, an unsigned frame as
                # signed -- leaves the bytes bit-identical, hits this
                # dedup, and is written off by a commit that never re-read
                # the instance. The loader then rebuilds the frame under
                # the superseded dtype, and because the export picks its
                # pixel container from `arr.dtype.kind`
                # (`_export_instance_worker`'s `arr.dtype.kind == 'f'`
                # test) rather than from `attributes`, a
                # float instance whose pixels were replaced with integers
                # exported as `FloatPixelData` beside an audit row reading
                # `wrote 1 of 1` (#406).
                #
                # Rebuilt, not patched: the same window is open on every
                # field the snapshot holds, geometry included -- a
                # replacement of the same byte length at a new
                # Rows/Columns reloads at the old shape. One rebuild
                # answers all eight; a `loader.pixel_dtype = ...` answers
                # one and leaves the rest.
                inst._pixel_loader = self._create_pixel_loader(
                    loader.offset, loader.length, loader.alg, inst,
                    pixel_hash=digest)
```

**The comment cites by name, not by line, and that is deliberate.** §11
sequences #407 first, and #407's comment edit inserts roughly ten lines into
`io_handlers.py` above `SidecarPixelLoader`, so an `io_handlers.py:3699`
written into this comment would be wrong before the developer finished
typing it -- and still *in range*, so `tests/test_source_citations.py` would
stay green while the reader was sent to the wrong function. That is Rule 1's
blind spot. Name the function; let the reader grep. The same applies to any
line number this brief quotes: the brief is dated `124e9e5`, and the
developer re-resolves before pasting.

`_create_pixel_loader` (`persistence.py:1126`-`:1129`) re-reads every field
from the live instance, which is the whole point. `pixel_hash=digest` is
passed explicitly for the reason the write arm below already gives at
`:3541`-`:3547`.

Nothing else changes. In particular:

- **Do not touch `set_pixel_data()`.** #293 weighed clearing `_pixel_loader`
  there and rejected it -- the loader is what lets a partially-redacted array
  be dropped and the original reloaded. The loader is legitimately stale
  between `set_pixel_data()` and the write; the defect is that the *write*
  does not clear the staleness.
- **Do not move the rebuild above or below the revision guard.** The dedup arm
  returns before `if revision is not None and inst._revision != revision`
  (`persistence.py:3525`), so the rebuild reads `inst.attributes` as they are
  *now* rather than as they were at the caller's capture. That is pre-existing
  and harmless: if the revision has moved, the instance is still dirty against
  the capture and the next save rebuilds again from the same source, and the
  offsets returned are the loader's own so nothing is poisoned. Stated here so
  the reviewer does not file it against this PR.

### 5.5 The probe: one new test file, five tests

`tests/test_dtype_only_replacement_survives_the_dedup.py`. Every test must
assert on a **decoded array or a written file**, never on the absence of an
exception, and every test must prove it entered the dedup arm.

1. **`test_the_second_save_appends_nothing_so_this_is_the_dedup_arm`**
   Ingest, `save(sync=True)`, `set_pixel_data(<same bytes, new dtype>)`,
   `save(sync=True)`; assert `os.path.getsize(<db>_pixels.bin)` is **unchanged
   across the second save**, and `inst._pixel_hash` is unchanged. This is the
   fixture-never-enters-the-arm guard, and it is load-bearing for a reason
   named in §5.6 (M3) -- without it, a "fix" that defeats the dedup passes
   every other test in this file.

2. **`test_a_dtype_only_replacement_reloads_as_the_dtype_the_caller_set`**
   `uint16` source with values `>= 32768`; `set_pixel_data(src.view(np.int16))`;
   `save(sync=True)`; `assert inst.unload_pixel_data() is True`;
   `get_pixel_data()`. Assert `got.dtype == np.int16`, `got.min() < 0`, and
   `np.array_equal(got, replacement)`. The `min() < 0` assertion is what stops
   this passing on a fixture whose two readings agree.
   *Red at `124e9e5`: returns `uint16`.*

3. **`test_a_geometry_only_replacement_reloads_at_the_geometry_the_caller_set`**
   4x4 `uint8`; `set_pixel_data(src.reshape(2, 8))`; `save(sync=True)`;
   `assert inst.unload_pixel_data() is True`; `get_pixel_data()`.
   Assert `got.shape == (2, 8)` and `np.array_equal(got, replacement)`.
   *Red at `124e9e5`: shape `(4, 4)`.*

   **`unload_pixel_data() is True` is a precondition in both, not a
   courtesy.** It refuses an array replaced through `set_pixel_data()` and not
   since written (#293), so if the save did not clear
   `_pixel_array_unwritten` it returns False, the array stays resident,
   `get_pixel_data()` hands back the resident array without ever consulting
   the loader, and *both tests pass on unfixed code*. Assert the return value
   with `is True`; a bare `inst.unload_pixel_data()` is the fixture that never
   enters the arm under test.

4. **`test_a_float_instance_whose_pixels_become_integers_exports_as_integers`**
   Ingest a `FloatPixelData` instance; `set_pixel_data(floats.view(np.int32))`;
   save; `export(out, use_compression=False)`. Read the written file back and
   assert `"FloatPixelData" not in ds`, `"PixelData" in ds`,
   `ds.PixelRepresentation == 1`, `ds.BitsAllocated == 32`, and
   `np.array_equal(ds.pixel_array.reshape(want.shape), want)`.
   *Red at `124e9e5`: the file carries `FloatPixelData` with float values.*
   `use_compression=False` deliberately -- see the next test for why the
   default cannot assert on a file.

5. **`test_the_same_export_compressed_refuses_instead_of_writing_a_float_file`**
   The same graph, `export(out)` with the frozen default `use_compression=True`.
   Assert `pytest.raises(ExportError)`, that the message names `int32` and
   `PixelRepresentation 1`, that **no file was written**, and that the audit
   log carries an `ERROR` row for the instance. Match the dtype with
   `re.search(r"\bint32\b", msg)`, **not** `"int32" in msg`: `"int32" in
   "uint32"` is `True`, and under M2 (§5.6) the stale `PixelRepresentation 0`
   makes the refusal read `uint32`, so a plain `in` check passes on the
   mutant. This is the milestone's own substring shape, inside the
   milestone's own brief. Drain with
   `flush_audit_queue()` before the `SELECT` -- a `SELECT` without it is this
   repo's standing correct-by-accident shape.
   *Red at `124e9e5`: `export: OK`, `wrote 1 of 1`, one `EXPORT` row, a file on
   disk carrying `FloatPixelData`.*

**Registry obligation.** The new file names `isocenter.persistence` and
`isocenter.io_handlers`, so it must be added to **both** rows of
`scripts/mutation_probe.py`'s `TARGETS`, or
`tests/test_mutation_probe_targets.py::test_every_test_that_imports_a_target_module_is_listed`
goes red. (Whether the file names those modules depends on how it imports; the
guard matches file *text*, so it will match if the test imports
`SidecarPixelLoader` from `isocenter.io_handlers` as the probes do. Add the
rows regardless -- extras cost the guard nothing and the file genuinely covers
both modules.)

### 5.6 The mutations that must kill it

The reviewer will apply each of these to the fixed tree, run
`tests/test_dtype_only_replacement_survives_the_dedup.py`, and expect red.

- **M1 -- delete the fix.** Remove the three-line
  `inst._pixel_loader = self._create_pixel_loader(...)` statement.
  *Expected: tests 2, 3, 4 and 5 red.* If any of them stays green, that test is
  not measuring what it says.

- **M2 -- the narrow fix.** Replace the rebuild with a `pixel_dtype`-only
  patch:
  ```python
  loader.pixel_dtype = inst.attributes.get(PIXEL_DTYPE_ATTR)
  ```
  *Expected: test 3 (geometry) red, test 2 (signedness) red, and test 5 red*
  -- neither `rows`/`cols` nor `bits`/`pixel_representation` is
  `pixel_dtype`, so the frame comes back at the right *kind* and the wrong
  *signedness*, and the compressed refusal names `uint32` where the caller
  set `int32`. Test 5 is a kill here only if it uses the `\bint32\b` match
  §5.5 requires; with a bare substring it stays green and the mutant
  survives. Test 4 may stay green, which is the point of the mutant: it
  proves the suite demands a full rebuild rather than the single field the
  issue title names.

- **M3 -- defeat the dedup instead.** Add `self._pixel_hash = None` to
  `Instance.set_pixel_data()`. Every dtype and geometry test in §5.5 goes
  **green** -- the write arm below rebuilds the loader anyway -- and only
  **test 1 goes red**, because the sidecar grows by a full frame on every
  save of unchanged bytes. This is the mutant that makes the growth assertion
  load-bearing rather than decorative, and it is a fix a developer could
  plausibly reach for. *Expected: test 1 red, tests 2--5 green.*

- **M4 -- re-snapshot the stale values.** Change the rebuild's `inst` argument
  to build from the old loader's own metadata instead of the live instance
  (`SidecarPixelLoader(..., metadata={"pixel_dtype": loader.pixel_dtype,
  "bits": loader.bits, ...})`). *Expected: tests 2, 3, 4 red.* This kills the
  "rebuilt an object, therefore fixed" reading.

### 5.7 Collateral: the suite is green, and that is the finding

With the #406 fix applied and nothing else changed, the whole suite is
`1813 passed, 2 skipped in 280.43s` on 3.12.14, and the probe's three cases
all pass on 3.14.7t as well. **Not one existing test changes verdict.**

That is not a licence to skip the tests in §5.5 -- it is the reason they are
required. `tests/test_export_pixels.py`, `tests/test_pixel_dtype_roundtrip.py`,
`tests/test_float_pixel_data_export.py` and
`tests/test_pixel_geometry_pipeline.py` all exist and none of them constructs
a *dtype-only replacement of bit-identical bytes*, which is the only shape
that reaches this arm. The blind spot is structural and was named in the
brief for this bunch: `tests/conftest.py:257` returns
`np.zeros((512, 512), dtype=np.uint16)` -- all-zero bytes, one dtype, and its
21 users route through `write_tree`/`execute_config` rather than
`session.export()`. Do not build these tests on that fixture.

### 5.8 Scope boundaries, and one sibling to file

**Out of scope, deliberately:**

- **`_persist_pixels`' `arr is None` arm (`persistence.py:3467`-`:3503`)**
  returns the loader's frame without rebuilding it too, and the same staleness
  is reachable there without any `set_pixel_data()`.
  `probe_406_sibling_arr_is_none.py`, on `124e9e5`:
  ```
  loader bits/pixrep after save: 16 0
  unload: True  resident: False
  set_attr(0028,0103 -> 1); dirty: True
  loader bits/pixrep after second save: 16 0
  get_pixel_data dtype: uint16  attributes say PixelRepresentation 1
  values match a signed read: False
  ```
  It is a live-object lie only (a reopened session is right, and the export
  declares from `attributes`), and rebuilding there is *not* obviously
  correct: a `BitsAllocated` changed by hand while the pixels are unloaded
  would give the rebuilt loader a byte count its geometry cannot hold, turning
  a quiet wrong shape into an `Integrity Error`. That is arguably better and is
  a different decision. **File it (§9, Q3); do not fix it here.**
- **The redaction path.** `persist_pixel_data` /
  `_swap_pixels_under_gate` (`persistence.py:2769`, `:2793`) has no dedup and
  always rebuilds the loader. Nothing to do.
- **`unload_pixel_data()` / `discard_pixel_data()`.** #293's two names stay
  two names. This fix does not touch either.
- **#412** (`PhiFinding.entity` in threads vs processes) shares no code with
  this; if a test in §5.5 wanders near it, stop and leave it.

---

## 6. #407 -- the Basic Offset Table in the codestream

### 6.1 The defect, measured

`isocenter/imagecodecs_handler.py:143`, the single-frame encapsulated arm:

```python
                codestream = b"".join(generate_fragments(pixel_bytes))
```

`probe_407_bot_join.py` builds each frame with the project's own encoder
(`io_handlers._compress_j2k`, bare J2K post-#404) and reports:

```
=== 8-bit grayscale  dtype=uint8 shape=(4, 4)
  fragments: 2 lens: [4, 138]
  joined head: 00000000ff4fff51
  last-fragment head: ff4fff5100290000
  generate_frames count: 1 head: ff4fff5100290000
  handler RAISED: RuntimeError imagecodecs failed to decode 1.2.840.10008.1.2.4.90: not a J2K or JP2 data stream
  hand-decoded last fragment: dtype uint8 shape (4, 4) equal: True

=== 16-bit grayscale  dtype=uint16 shape=(4, 4)      ... identical shape of failure
=== 8-bit RGB        dtype=uint8 shape=(4, 4, 3)     ... identical shape of failure
```

Every cell `_J2K_ENCODABLE_FRAMES` permits fails, and `imagecodecs` decodes
each of them bit-exactly when handed the fragment instead of the join. The
issue is exactly right.

**One refinement worth knowing before writing a fixture.**
`probe_407_generate_frames_edges.py` shows the join is wrong only when the
Basic Offset Table is **populated**:

```
=== 1. encapsulate() one frame (populated BOT)
  generate_fragments: 2 [4, 64] ['00000000', 'ff4fff51']   joined head: 00000000ff4fff51
=== 2. one fragment, EMPTY BOT
  generate_fragments: 2 [0, 64] ['', 'ff4fff51']           joined head: ff4fff5141414141
```

With an empty BOT the join is accidentally correct. `pydicom.encaps.encapsulate`
writes a populated BOT by default, and `_compress_j2k` calls it that way, so
**every file this project compresses hits the bug** -- but a hand-built
fixture using `item(b"") + item(codestream)` would pass without the fix. The
test must use `encapsulate()`, and §6.3's second test asserts the BOT is
there so the fixture cannot drift.

### 6.2 What it costs, and what it does not

`Instance.get_pixel_data()` reaches this handler only when pydicom's own read
has already raised (`entities.py:923`-`:942`). With Pillow installed, pydicom
reads every J2K cell this project writes, so the fallback is not on the
ordinary path. Where it *is* on the path is the population pydicom cannot
decode -- and 16-bit multi-sample JPEG 2000 is exactly that population (§3,
row D). So the practical cost of #407 is: **the one case the fallback exists
for is the one case it has never handled.**

`README.md:34` -- "JPEG Lossless, JPEG 2000, JPEG-LS, RLE, and baseline JPEG,
through `imagecodecs`, with strict validation on the way out" -- is the
sentence #407 calls an overstatement. ~~**Ruling: leave `README.md` alone.** The
fix makes the sentence true rather than requiring it to be softened; editing
it in the same PR would be a claims change made on the strength of a defect
that no longer exists. Recorded here so a reviewer does not file its absence.~~
*[Superseded in part, #447: the fix never made the RLE clause true -- the
handler's RLE arm called `imagecodecs.rle_decode`, which does not exist --
and #447 rewrites the README line.]*

### 6.3 The fix

Replace `imagecodecs_handler.py:143` with:

```python
                # `generate_fragments` yields EVERY item of the
                # encapsulated pixel data, and the first item is the Basic
                # Offset Table (PS3.5 A.4). Joining them therefore
                # prefixed the codestream with the BOT's own bytes -- four
                # zeros ahead of `ff4f ff51` for a single-frame file -- and
                # `imagecodecs` refused the result with `not a J2K or JP2
                # data stream`, so this arm had never decoded anything.
                # It failed identically on the JP2 container this project
                # wrote before #404, so it is not that container's fault
                # and predates it (#407).
                #
                # `generate_frames` is what the multi-frame arm above
                # already uses, and it is the right answer here for a
                # second reason as well as the BOT: one frame may legally
                # be split across several fragments, so "take the last
                # fragment" would decode the tail of such a frame.
                # Measured on pydicom 3.0.2: two fragments with an empty
                # BOT come back as one 64-byte frame.
                frames = list(generate_frames(pixel_bytes,
                                              number_of_frames=1))
                if not frames:
                    raise RuntimeError(
                        "encapsulated PixelData holds no frame")
                codestream = frames[0]
```

**And delete `generate_fragments` from the import at
`imagecodecs_handler.py:4`**, leaving `from pydicom.encaps import
generate_frames`. This is not tidiness. `tests/test_imagecodecs_edge_cases.py`
patches `isocenter.imagecodecs_handler.generate_fragments` in two tests, and
`unittest.mock.patch` raises `AttributeError` on a missing attribute -- so
removing the import turns **both** of those mocks red. Measured:

| tree | `tests/test_imagecodecs_edge_cases.py` |
| --- | --- |
| fix applied, import kept | `1 failed, 9 passed` (only `test_rle_lossless_handling`) |
| fix applied, import deleted | `2 failed, 5 passed` (`test_decode_error_handling` too) |

`test_decode_error_handling` is the vacuous one: with the import kept it goes
on passing because the real `generate_frames` also raises on `b"fake_pixel_data"`,
so the test proves nothing about the arm it names. **Delete the import first,
watch both fail, then rewrite them.** The rewrites:

- ~~`test_rle_lossless_handling` -- build real encapsulated bytes
  (`encapsulate([b"rle_chunk"])`), keep the `imagecodecs` mock, and add the
  assertion that makes it a #407 test:
  `assert mock_ic.rle_decode.call_args[0][0] == b"rle_chunk"` -- the codec is
  handed the fragment, not the BOT and the fragment.~~
  *[Superseded in part, #447: the mock pinned a function imagecodecs does
  not have; the test is deleted with the arm and replaced by
  `test_the_handler_does_not_claim_rle`.]*
- `test_decode_error_handling` -- same shape, with
  `mock_ic.ljpeg_decode.side_effect = ValueError("Bad data")`, still asserting
  the `RuntimeError` wrap. Now it fails for the reason it names.

### 6.4 The probe: one new test file, five tests

`tests/test_single_frame_encapsulated_decode.py`. Every test asserts on a
**decoded frame**, which is the issue's own test note and the right one: the
current failure is a `RuntimeError`, so a test pinned to the message would stay
green if the join were fixed and the frame came back wrong.

1. **`test_a_single_frame_j2k_codestream_decodes_to_the_pixels_that_were_encoded`**
   Build a 4x4 `uint16` frame with distinctive values, run it through
   `io_handlers._compress_j2k` (so the fixture is what this project actually
   writes), call `imagecodecs_handler.get_pixel_data(ds)`, assert
   `np.array_equal(out.reshape(arr.shape), arr)`.
   *Red at `124e9e5`: `RuntimeError: ... not a J2K or JP2 data stream`.*

2. **`test_the_fixture_carries_a_populated_basic_offset_table`**
   On the same `ds`: assert `parse_basic_offsets(ds.PixelData) == [0]` and that
   `list(generate_fragments(ds.PixelData))[0]` is 4 bytes of zeros. Then assert
   the handler still returns the right pixels. Without this, an empty-BOT
   fixture would make test 1 pass on unfixed code (§6.1).

3. **`test_one_frame_split_across_two_fragments_is_reassembled`**
   Hand-build `item(b"") + item(codestream[:n]) + item(codestream[n:])`;
   assert the handler decodes the whole frame. This is the test that kills a
   "take the last fragment" fix.

4. **`test_a_multi_frame_dataset_still_decodes_every_frame`**
   Two frames, `NumberOfFrames = 2`; assert shape `(2, 4, 4)` and both frames
   equal. Guards the arm this PR does not change.

5. **`test_the_imagecodecs_fallback_reads_a_frame_pydicom_cannot`** -- in
   `tests/test_single_frame_encapsulated_decode.py` as well, or in its own
   file. Build the 16-bit RGB J2K file of §3 by hand; assert
   `pytest.raises(RuntimeError)` from `pydicom.dcmread(p).pixel_array` (the
   precondition -- without it the test could pass through the ordinary
   pydicom path and never enter the fallback), then drive
   `Instance.get_pixel_data()` on a `file_path`-backed instance and assert the
   array is bit-exact. Then assert `inst.unload_pixel_data() is True` -- see
   §6.5.
   *Red at `124e9e5`: `RuntimeError: Lazy load failed ...`.*

**Registry obligation.** If the new file names `isocenter.io_handlers` (it
will, for `_compress_j2k`), add it to that `TARGETS` row. There is nothing to
add for the other two modules the file touches: `TARGETS` has exactly six
rows -- `parallel`, `crypto`, `privacy`, `remediation`, `io_handlers`,
`persistence` -- so **`isocenter/entities.py` has no row**, and neither does
`isocenter/imagecodecs_handler.py`. This PR adds neither; whether the handler
deserves one before 1.0 is Q4 in §9. Do not go looking for an `entities` row
to update -- there isn't one, and the guard only checks files that *are*
listed.

### 6.5 A comment that becomes false, and must be rewritten

`isocenter/entities.py:930`-`:941`, the third of three
`self._pixel_array_unwritten = False` clears:

> Unlike its two siblings above, this clear **SURVIVES DELETION UNTESTED**:
> reaching it needs a transfer syntax pydicom cannot decode and imagecodecs
> can, on an instance whose array has diverged, and nothing in the suite
> constructs that.

Test 5 above constructs exactly that. Measured with the #407 fix applied,
`probe_407_fallback_arm_and_edges.py`:

```
  after set_pixel_data: unwritten = True
  discard_pixel_data() -> True  unwritten still: True
  get_pixel_data OK: uint16 (4, 4, 3)  equal to the file's pixels: True
  unwritten after the read: False
  unload_pixel_data() -> True
```

and with `entities.py:941` mutated to `pass`:

```
  unwritten after the read: True
  unload_pixel_data() -> False
```

So the line is now killable, and **the comment must be rewritten in the same
PR** -- to say that #407 made the arm reachable, that
`tests/test_single_frame_encapsulated_decode.py::test_the_imagecodecs_fallback_reads_a_frame_pydicom_cannot`
is the pin, and that the sequence which reaches it is
`set_pixel_data` -> `discard_pixel_data` -> `get_pixel_data`. A comment that
says a line is untested, next to a test for it, is the same class of defect as
the rest of this milestone.

**M3 for #407** is therefore: delete `entities.py:941`. *Expected: test 5's
`unload_pixel_data() is True` assertion red.*

### 6.6 The mutations that must kill it

- **M1 -- restore the join.** Put
  `codestream = b"".join(generate_fragments(pixel_bytes))` back (and the
  import). *Expected: tests 1, 2, 3, 5 red, plus the rewritten
  `test_rle_lossless_handling`.*
- **M2 -- the last-fragment shortcut.**
  `codestream = list(generate_fragments(pixel_bytes))[-1]`.
  *Expected: test 3 red* (it returns only the second half of a two-fragment
  frame), tests 1, 2, 5 green. This mutant is why test 3 exists: it is the
  cheap "fix" the issue text itself suggests as an alternative ("taking the
  fragment rather than the join"), and it is wrong.
- **M3 -- delete `entities.py:941`** (see §6.5). *Expected: test 5's unload
  assertion red.*
- **M4 -- drop the empty-frames guard.** Replace the `if not frames: raise`
  with `codestream = frames[0]` alone. This one has **no named kill**: an
  encapsulated `PixelData` that yields zero frames raises `IndexError` instead
  of the named `RuntimeError`, both of which the outer handler wraps into the
  same `imagecodecs failed to decode ...` message. Say so rather than
  inventing a test for it -- the guard is there for the message a maintainer
  reads in the log, not for a behaviour a caller can distinguish.

### 6.7 Scope boundaries, and one edge to file

**Out of scope, deliberately:**

- **Collapsing the two arms.** The multi-frame arm at
  `imagecodecs_handler.py:130`-`:138` returns `np.array(frames)` and the
  single-frame arm returns a bare 2-D or 3-D array; those are different return
  shapes, so they are two behaviours and "one spelling per behaviour" does not
  ask for a merge. Unifying would also fold in the non-encapsulated
  fall-through, which is a third question. Two `generate_frames` call sites is
  the right answer here; a comment on the single-frame one saying why is not.
- **Registering the handler with pydicom.** `isocenter/__init__.py:80`-`:81`
  already records that this handler is reached through
  `Instance.get_pixel_data` and not through pydicom's plugin list. Unchanged.
- **`_J2K_ENCODABLE_FRAMES`'s membership.** §3. Comment only.
- **`README.md:34`.** §6.2.

**One measured edge, to file rather than fix.** A multi-frame encapsulated
dataset that declares **no** `NumberOfFrames` takes the single-frame branch,
because `getattr(ds, 'NumberOfFrames', 1)` is 1. Measured with the fix
applied:

```
=== 2. two frames, NumberOfFrames NOT declared
  NumberOfFrames present: False
  handler returned: uint8 (4, 4)  == frame 0: True  == both frames: False
```

Before the fix that file raised (for the wrong reason: `not a J2K or JP2 data
stream`). After, it quietly returns frame 0 of 2. The file is non-conformant --
(0028,0008) is Type 1C and required above one frame -- and pydicom's own
`generate_frames(buf, number_of_frames=1)` behaves the same way, so this is
not a regression from working behaviour. But it is a new quiet partial read in
a milestone named for silences, and `pydicom.encaps.parse_basic_offsets` gives
a cheap detector: a populated BOT naming more offsets than the dataset declares
frames is a contradiction the handler could refuse. **File it (§9, Q3 sibling);
do not add it here** -- it is a second behaviour change in a PR that already
has one, and it needs its own fixture, test and mutant.

---

## 7. Change list, file by file

| file | change |
| --- | --- |
| `isocenter/persistence.py` | dedup arm (`:3513`): rebuild `inst._pixel_loader` from the live instance, with the comment in §5.4 |
| `isocenter/imagecodecs_handler.py` | `:4` drop `generate_fragments` from the import; `:143` take the frame from `generate_frames`, with the comment in §6.3 |
| `isocenter/io_handlers.py` | comment only: the `_J2K_ENCODABLE_FRAMES` paragraph in §3 |
| `isocenter/entities.py` | comment only: rewrite the "SURVIVES DELETION UNTESTED" paragraph at `:930`-`:941` (§6.5) |
| `tests/test_dtype_only_replacement_survives_the_dedup.py` | **new**, five tests (§5.5) |
| `tests/test_single_frame_encapsulated_decode.py` | **new**, five tests (§6.4) |
| `tests/test_imagecodecs_edge_cases.py` | rewrite `test_rle_lossless_handling` and `test_decode_error_handling` onto real encapsulated bytes (§6.3) |
| `scripts/mutation_probe.py` | add both new test files to the `TARGETS` rows their text demands (§5.5, §6.4) |
| `CHANGELOG.md` | one **Breaking** entry and one **Fixed** entry (§8) |

**Nothing else.** In particular: no change to `docs/api/stability.md`, no
change to `README.md`, no change to `_J2K_ENCODABLE_FRAMES`'s membership, no
new `action_type`, no new export option.

**After any `io_handlers.py` or `persistence.py` edit, run
`tests/test_source_citations.py`.** Both files are cited by line number across
the tree. Measured: the only *content-pinned* citation into either
(the one in `tests/test_private_tag_empty_value_roundtrip.py:42`, which pins
line 1119) is far above both edit
sites, and the full suite with the #406 fix applied was green, so the
persistence insertion is safe. The `io_handlers.py` comment edit shifts lines
below ~3430 and was **not** in that run.

---

## 8. CHANGELOG

Two entries under `## [Unreleased]`.

**Breaking** -- #406's severe case, because a call that used to succeed now
raises, and the convention is to name the exact exception:

> **A dtype-only `set_pixel_data()` is no longer written off by the frame
> dedup, and a float instance whose pixels were replaced with integers now
> refuses the compressed export instead of writing a float file (#406).**
> `SidecarPixelLoader` snapshots BitsAllocated, PixelRepresentation, the
> geometry and the `_ISOCENTER_PIXEL_DTYPE` carrier at construction, and
> `_persist_pixels`' deduplication arm returned the loader's own frame
> without rebuilding it -- so a `set_pixel_data()` that changed only the
> *type* of the pixels, leaving the bytes bit-identical, hit the dedup and
> left the loader describing the frame it had replaced. Measured on a
> `float32` instance handed `floats.view(np.int32)`: the store, and a
> reopened session, were right; the live object handed back `float32`; and
> because the export chooses its pixel container from the array's dtype
> rather than from `attributes`, `session.export(folder)` wrote a file
> carrying `FloatPixelData` with float values, beside one `EXPORT` audit row
> reading `wrote 1 of 1` and no loss row of any kind. The loader is now
> rebuilt from the live instance when the dedup hits -- every field, not
> just the dtype, because the same window was open on Rows/Columns. **What
> breaks:** an export of such an instance at the frozen default
> `use_compression=True` now raises `isocenter.io_handlers.ExportError`
> (`wrote 0 of 1 planned instances`) with an `ERROR` audit row carrying
> `_J2K_ENCODABLE_FRAMES`' refusal -- `Compression failed: JPEG 2000
> lossless cannot carry int32 pixel data at 1 sample(s) per pixel` -- because
> the frame is now correctly seen as a 32-bit integer frame, which #404
> refuses to compress. It previously "succeeded" by writing the wrong
> pixels. `use_compression=False` writes the file the caller asked for:
> `PixelData`, BitsAllocated 32, PixelRepresentation 1, bit-exact.

**Fixed** -- #407:

> **`imagecodecs_handler` decodes single-frame encapsulated pixel data
> (#407).** The single-frame arm joined every item of the encapsulated
> `PixelData`, and the first item is the Basic Offset Table -- four zero
> bytes ahead of `ff4f ff51` -- so `imagecodecs` refused every frame with
> `not a J2K or JP2 data stream` and this arm had never decoded anything. It
> failed identically on the JP2 container written before #404, so it is
> orthogonal to that change and predates it. The frame now comes from
> `generate_frames`, which the multi-frame arm already used: it skips the
> offset table and reassembles a frame that was split across fragments,
> which "take the last fragment" would not. This makes
> `Instance.get_pixel_data()`'s imagecodecs fallback work for the population
> it exists for -- 16-bit multi-sample JPEG 2000, which Pillow cannot decode.
> ~~It does **not** move `_J2K_ENCODABLE_FRAMES`' `(2, True)` refusal:
> `ingest_worker` reads through pydicom's own decoder backend, which has no
> imagecodecs plugin, so `session.ingest()` on such a file still returns
> `ingested=0` with a `Decompression Failed` row -- measured, and now said in
> the frozenset's own comment.~~ **Superseded by #416 (v0.9.6)**, which
> gave ingest that fallback and moved the cell; see the front matter.

---

## 9. Owner questions

**Q1 -- the refusal message.** `_refuse_unencodable_j2k_frame`
(`io_handlers.py:3453`-`:3462`) says a 16-bit multi-sample J2K file "would be
written and then unreadable by this library itself". After #407 that is true
of `session.ingest()` and false of `Instance.get_pixel_data()`. This brief
rules to **leave it** (§3): the round trip a user takes after an export is an
ingest, and the remedy the sentence offers is unchanged. Tighten it to
"cannot be **ingested** by this library"? It is user-facing text on the export
path, so it is a claims call.

**Q2 -- should `_decode_pixels` fall back to `imagecodecs_handler`?**
This is what would move `(2, True)` and let this library ingest its own 16-bit
colour exports. It widens what `session.ingest()` accepts, adds a row to
`_J2K_ENCODABLE_FRAMES`, and changes what `use_compression=True` does for
16-bit RGB. Draft issue title: *`ingest_worker` reads pixels through pydicom's
decoder alone, so a file `Instance.get_pixel_data()` can decode is still
rejected at the door.* Recommend **filing it**, not doing it in this bunch --
it is a capability decision with a v1.0-shaped blast radius.

**Q3 -- two siblings, one issue or two?**
(a) `_persist_pixels`' `arr is None` arm leaves the same stale snapshot when a
descriptor is written with the pixels unloaded (§5.8, measured); (b) the
handler returns frame 0 of an encapsulated dataset whose Basic Offset Table
names more frames than the dataset declares (§6.7, measured, detectable with
`pydicom.encaps.parse_basic_offsets`). Both are "a snapshot or a count that
nobody re-checks". They share this bunch's theme and no code. One issue with
two parts, or two issues? Architect's recommendation: **two**, because they
have different fixes and different reviewers.

**Q4 -- does `isocenter/imagecodecs_handler.py` want a `TARGETS` row?**
It is 153 lines, it is on the pixel path, and after this PR it will have real
tests for the first time. #383 gave `persistence.py` a row on exactly this
reasoning. Out of scope here (adding a row means running the probe against it
to pick a budget), but worth a decision before 1.0.

---

## 10. What the reviewer should attack

1. **Run every mutation in §5.6 and §6.6.** M3 in §5.6 (defeat the dedup by
   clearing `_pixel_hash` in `set_pixel_data`) is the one that matters most:
   it turns every dtype test green and only the sidecar-growth assertion red.
   If test 1 does not go red, the growth assertion is decoration and the suite
   cannot tell a fix from a workaround.
2. **Check that #406's tests use values that differ between readings.**
   A `uint16` fixture below 32768 read as `int16` gives the same numbers, and
   the whole test collapses to `0 == 0`. The measured fixture uses
   `40000..40015`.
3. **Check that #407's fixture has a populated Basic Offset Table.** With an
   empty BOT the buggy join is accidentally correct and test 1 passes on
   unfixed code (§6.1). §6.4's test 2 exists to make that impossible; verify
   it actually asserts on `parse_basic_offsets`, not on a substring of a
   repr.
4. **Check that test 5 (§6.4) proves pydicom failed first.** Without the
   `pytest.raises` precondition the test can pass through the ordinary pydicom
   path and never touch the fallback -- the fixture-never-enters-the-arm shape,
   at a different door.
5. **Check that #406's tests 2 and 3 assert `unload_pixel_data() is True`.**
   Same shape as item 4, at #406's door. A bare call whose return value is
   discarded lets the test pass on unfixed code: `unload` refuses an unwritten
   replacement (#293), the array stays resident, and `get_pixel_data()` never
   reaches the stale loader. The assertion is the proof the reload happened.
6. **Check that #406's test 5 matches `int32` on a word boundary.**
   `"int32" in "uint32"` is `True`, and M2 makes the refusal say `uint32`, so
   a substring check leaves that mutant alive. Also check test 4's file
   readback asserts on `ds.pixel_array` values, not only on which element is
   present -- the container being right with the wrong numbers in it is the
   defect one layer down.
7. **Check the audit assertions drain first.** Any `SELECT` on `audit_log`
   without a preceding `flush_audit_queue()` is this repo's standing
   correct-by-accident shape.
8. **Check nothing on the frozen surface moved.** `docs/api/stability.md`
   should be untouched, and `tests/test_frozen_surface.py` green without
   edits.
9. **Check `tests/test_source_citations.py` and
   `tests/test_mutation_probe_targets.py` are green.** The first is at risk
   from the `io_handlers.py` comment edit; the second is at risk from the two
   new test files.
10. **Do not accept "the suite is green" as evidence for #406.** It was green
   with the fix and green without it (§5.7). Only the new tests distinguish
   the two trees.

---

## 11. Sequencing

#407 first, then #406. They do not interact in code, but #407's test 5 needs
the `entities.py` comment rewrite decided before it is written, and #406's
test 5 asserts on `_J2K_ENCODABLE_FRAMES`' refusal message -- which #407's
comment edit sits next to. Doing them the other way round means editing the
same two files twice.

Per issue: write the failing tests, watch them fail **for the reason stated**
(a `RuntimeError` for #407, a wrong dtype or a `FloatPixelData` element for
#406 -- not a fixture error), then fix, then run the mutations yourself before
handing over.

Full suite on both gate interpreters before pushing:

```bash
W=/Users/kevin/Developer/Isocenter/.claude/worktrees/<yours>
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$W /Users/kevin/Developer/Isocenter/.venv/bin/python -u -m pytest -v tests/
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$W /Users/kevin/Developer/Isocenter/.venv314t/bin/python -u -m pytest -v tests/
```

One PR, two `Fixes` lines.

---

## 12. Amendments

Corrections made **during** implementation (PR #420), in the register the
project reserves for them: what the brief predicted, what the tree actually
did, and the measurement. These are not `**Superseded in part:**` entries --
nothing later falsified a clause; the brief was simply out by a detail in six
places, and every one of them was found by running what it asked for. The
predictions above are left standing so the two can be compared.

**A1 -- §5.5 test 5 and §5.6 M2: the `uint32` comes from the dtype token, not
from a stale `PixelRepresentation`.** The brief says a `pixel_dtype`-only fix
leaves "the stale `PixelRepresentation 0`", which "makes the refusal read
`uint32`". Half right. Measured under M2, the refusal reads:

```
cannot carry uint32 pixel data at 1 sample(s) per pixel
(BitsAllocated 32, PixelRepresentation 1)
```

`PixelRepresentation` does **not** move. It is read with
`getattr(ds, 'PixelRepresentation', ...)` off the live export dataset, which
the export built from `attributes`, and it is `1` on every tree in play. What
moves is the `{arr.dtype}` token: `arr` is the frame the loader rebuilt, and
its dtype comes from `_integer_dtype(self.bits, self.pixel_representation)` --
the *loader's* stale snapshot, not the dataset's descriptor.

The brief's conclusion survives intact and its assertion is the right one:
`re.search(r"\bint32\b", ...)` is the entire kill for M2, and a bare
`"int32" in msg` leaves the mutant alive. But an assertion on
`PixelRepresentation 1` is decoration -- it passes on every mutant, and on the
two where the export does **not** raise (M1 and M4) the test dies at
`pytest.raises` and it is never reached at all. Those two are the defect
itself: the export succeeds, writes `FloatPixelData`, and reports `wrote 1 of
1`, so line 297 fails with `DID NOT RAISE ExportError` before the refusal text
is ever read. It was written
into the first cut of test 5 on the strength of this paragraph and removed in
review. A reader who trusted the original text would have strengthened the
test against the wrong variable.

**A2 -- §5.6 M3 is killed in three places, not one.** The brief predicts "test
1 red, tests 2--5 green". Measured at the time: tests 1, 4 and 5 red -- tests 4
and 5 carry their own sidecar-growth guards, so the workaround reddens them
too. Then A4 gave tests 2 and 3 the same guard, and **on the tree this log
ships in M3 reddens all five**, each on its own guard line (126, 165, 210, 248,
294). That is correct by construction rather than a loss of discrimination: M3
*is* "the arm was not entered", so the arm-entry guard is the assertion that
should speak. Under M1, M2 and M4 the guards do not fire and every test still
dies on its own reason. Stronger than designed, and it does not change M3's
purpose.

**A3 -- §5.6 M4 also kills test 5.** The brief predicts tests 2, 3 and 4.
Measured: 2, 3, 4 and 5.

**A4 -- §5.5's "prove it entered the arm" was under-delivered by the brief's
own test list.** Only tests 1, 4 and 5 were specified with a growth assertion;
tests 2 and 3 were not, and test 3's fixture (`uint8` 4x4 -> 2x8) was covered
by nothing. It does enter the arm today -- measured, sidecar 24 -> 24 bytes,
`_pixel_hash` unchanged -- so this was never a live hole. But change that
replacement's byte length and it moves silently to the write arm and passes
anyway, which is the exact shape test 1 exists to prevent. All five tests now
carry the guard on their own fixture.

**A5 -- §6.6 M1 does not redden test 3.** The brief predicts "tests 1, 2, 3, 5
red". Measured: 1, 2 and 5, plus both rewritten mocks. Test 3 stays green,
and correctly so: its hand-built `_item(b"") + _item(...) + _item(...)`
fixture has an **empty** offset table, and §6.1 already establishes that the
join is accidentally correct in that case. The two halves of the brief
disagree with each other; §6.1 is the one that is right.

**A6 -- §6.6 M4's guard is unreachable, not merely unkillable.** The brief
says the empty-frames guard "has no named kill" because an `IndexError` and
the named `RuntimeError` wrap into the same outer message. True, and it
understates the case: against pydicom 3.x there is no input that reaches the
guard at all. Measured -- `generate_frames(buf, number_of_frames=1)` yields
at least one frame for every buffer that parses (`item(b"")` -> `[b""]`,
`item(b"") + item(b"")` -> `[b""]`, `item(b"\x00\x00\x00\x00")` -> `[b""]`),
and a buffer too short to parse raises `struct.error` before the guard is
reached. So `frames` is never `[]`, and the brief's framing -- "the guard is
there for the message a maintainer reads in the log" -- describes a log line
nobody can produce. The guard is kept, because it pins pydicom's contract
rather than a behaviour, and its comment now says that in those words instead
of implying a reachable path.

**A7 -- §6.3's table is out by four.** "fix applied, import kept: `1 failed,
9 passed`" for a file holding seven tests. The neighbouring row ("import
deleted: `2 failed, 5 passed`") is exact, and is the one the sequencing
depends on. An arithmetic slip, recorded only so a reader does not go looking
for two tests that were never there.

**Not amended, deliberately.** §3's required comment paragraph contains a
literal `#(new)` for the `_decode_pixels` fallback issue; it was filed as
**#416** and the comment names that number. §5.4's advice to cite by function
name rather than line number proved right twice over: the `io_handlers.py`
comment edit shifted every line below ~3430, and `tests/test_source_citations.py`
would have graded a stale in-range citation as green.
