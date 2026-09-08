# Export Fidelity, Bunch 2: an empty list's VR, an unread resource, and the colour space the sidecar actually holds

**Date:** 2026-09-08
**Status:** Determinations MADE, with evidence. §1–§3 are the
recommendations; §0.2 lists the calls that are the owner's, each as
options with the recommendation first. No production code was changed;
one throwaway mutation was applied and reverted (`git diff -- isocenter/`
empty at the end, §9).
**Tracking:** #367 (an empty private list exports as `LO` whatever VR
the source recorded), #357 (`research_tags.json` ships and nothing
reads it), #372 (a lossy colour source's PhotometricInterpretation vs
the bytes the sidecar holds — a measurement first). Rests on #344 (the
zero-length private element, whose spec's Q1 *is* #367), #154 (recorded
private VRs), #165/#190/#195 (`_fallback_multivalue`), #183 (nested
icons; the lossy refusal this spec removes the reason for), #186 (the
pixel-geometry authority, whose "YBR_FULL survives" clause this spec
falsifies), #356 (the MIDI-B gap run; §1 of its spec binds #357).
**Supersedes in part:** `2026-08-29-pixel-geometry-authority.md` §1.1's
"New finding" paragraph, §3.8's `# YBR_FULL, YBR_ICT, RGB, MONOCHROME1
all survive` clause, the Rank-3 table row "PI stays `YBR_FULL`", and
§7 test 5 — the *fixture expectation* that a YBR_FULL source keeps its
label through export was measured wrong (§3.2); §3.8's rule itself is
correct and stands. Also `2026-09-07-blob-kind-and-nested-pixel-carriage.md`
§0 Q6's "**no lossy-compressed fixture of any family can be built in
this venv**" and "the export's other path writes Implicit VR LE"
(§4). And `2026-09-07-zero-length-private-element-export.md` Q1, which
is answered here rather than falsified. Each is marked in place.
**Base:** `main` at `283bf04` (the bunch-1 merge on top of release
0.9.3). Every `path.py:N` below is that commit. Nothing grades these
line numbers (`tests/test_source_citations.py` excludes
`docs/superpowers/`); the tests in §5 carry the content-pins instead.
**Measured with:** `/Users/kevin/Developer/Isocenter/.venv/bin/python`
(CPython 3.14.6, GIL build; pydicom 3.0.2, numpy 2.5.2, pillow 12.3.0)
in worktree
`/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-a06bc10208d1862e7`.
Every probe and every pytest ran as
`env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<worktree> <python> -u ...`
and printed `isocenter.__file__` **first**; it resolved to
`<worktree>/isocenter/__init__.py` every time. Fixtures that need an
encoder were built in two throwaway venvs under the session scratchpad
(`enc/`: pydicom 3.0.2 + pylibjpeg-openjpeg 2.5.0 + pylibjpeg-libjpeg
2.4.0 + pillow; `proj/`: `pip install -e <worktree>[tests]` + the same
plugins), **never the project `.venv`**. Every #372 number was then
re-measured in the project `.venv`, which has only Pillow, and the
numbers were identical (§3.1). The probes (`build_fixtures.py`,
`build_native.py`, `probe_meta.py`, `measure_372.py`, and the
`add_new` / parity one-liners quoted below) are scratchpad files, not
in the repo, named so numbers can be attributed; `measure_372.py` calls
`Session.ingest` and is guarded by `if __name__ == "__main__":` for the
reason the #344 spec §7.1 gives. Full suite: §9.

---

## 0. Determinations, and what is the owner's to decide

### 0.1 One line each

1. **#367 — defect, confirmed, and it is #344's arm one clause short.**
   `DicomExporter._fallback_multivalue([])` returns `('LO', [])`
   (`io_handlers.py:4553-4554`), and `_merge`'s private arm never
   consults the recorded VR for an empty *list* because
   `_value_fits_vr([], vr)` is `False` for every VR
   (`io_handlers.py:512`, list arm `bool(value) and all(...)`). So a
   private tag whose source recorded `DS` and whose graph value is
   `[]` exports as a zero-length `LO`, while `None` on the same tag
   exports as zero-length `DS` (#344). Two answers to one question.
   Fix: widen #344's predicate at `io_handlers.py:4313` from
   `if v is None:` to "None or an empty list/tuple/MultiValue",
   normalise the value to `None` there, and make `_fallback_multivalue`'s
   own empty answer `('UN', None)` — PS3.5 6.2.2's no-VR answer and the
   one `_merge` already gives (§1). `tests/test_private_tag_export.py:386`
   becomes `== ('UN', None)`. No new exception; grade unchanged.
2. **#357 — delete the file. Nothing else in `setup.py` or the
   resources directory changes.** `isocenter/resources/research_tags.json`
   is read by nothing (grep over the package: only the file itself, one
   0.7.0 CHANGELOG line, the MIDI-B spec). Its content is a second
   spelling of `session.py:242 _default_action_for_tag`'s defaults.
   **The issue's central claim is wrong and the CHANGELOG entry must
   not repeat it**: `JITTER` *is* accepted config vocabulary —
   `privacy.py:187` lists it in `_ACTION_WORDS` and `privacy.py:483`
   maps it to `SHIFT_DATE`; `configuration.py:179` documents it; the
   scaffold emits it and two tests pin that. The MIDI-B spec's
   constraint (profile documented, not shipped; no new API name before
   1.0) forbids the alternative of making it a loader-backed profile
   half. `package_data`'s glob (`setup.py:90`) needs no edit; the one
   test edit is a docstring, `tests/test_packaging_contract.py:341`
   "four" → "three"; a new test pins that every tracked resource is
   named by a string literal in the package (§2).
3. **#372 — defect, label-only, broader than the issue frames it, and
   not lossy-specific.** `ingest_worker` stores `ds.pixel_array`'s
   bytes (`io_handlers.py:1402`), and pydicom 3's `pixel_array` (default
   `as_rgb=True`) converts every 8-bit YBR family to **RGB** — `YBR_FULL`
   and `YBR_FULL_422` under **any** transfer syntax, native included;
   `YBR_ICT`/`YBR_RCT` under JPEG 2000 — **without touching**
   `ds.PhotometricInterpretation`. Ingest copies the declared label,
   the sidecar holds RGB, `resolve_photometric_interpretation`
   (`pixel_geometry.py:339`) correctly leaves a coherent-looking label
   alone at export, and the exported file declares YBR over RGB bytes.
   A conformant reader then shows the wrong colours (`YBR_FULL`:
   `(221,40,91)` read as `(169,255,65)`) or refuses the file
   (`YBR_FULL_422`: pydicom `ValueError ... a third larger than
   expected (12288 vs 8192 bytes)`). Fix at ingest, like
   PlanarConfiguration: take the decoded colour space from pydicom's
   own decoder meta (`get_decoder(ts).as_array(ds)` returns it; this
   is the call `Dataset.pixel_array` makes and then discards the meta
   of) and write `0028,0004` only when it differs (§3.4). The
   nested-icon path gets the same correction from the same helper, and
   the reason `_CARRIABLE_TRANSFER_SYNTAXES` excludes lossy syntaxes
   is gone — widening it is the owner's call (§0.2 C).
4. **Four claims in the issues and the earlier specs are overturned by
   measurement** (§4): the #372 issue's "both export paths write
   Implicit VR LE" (the default is JPEG 2000 Lossless, explicit VR);
   the #357 issue's "JITTER is a spelling no arm accepts"; the #183
   spec's "no lossy-compressed fixture of any family can be built in
   this venv" (Pillow alone, an `install_requires`, builds all of
   them); and the #186 pixel-geometry spec's / 0.9.1 CHANGELOG's
   "YBR_FULL survives" being a *fix* (the label survived; the pixels
   under it had already been converted).
5. **Existing stores are not corrected.** A 0.9.3 store holds `YBR_FULL`
   beside RGB sidecar bytes and the ingest-time fix never sees it; a
   load-time or export-time relabel cannot tell that row from a
   `set_pixel_data()` of genuine YBR bytes, which lands in the same
   sidecar with the same label. Re-ingest is the remedy and the
   CHANGELOG says so (§3.6, §0.2 B).

### 0.2 Open questions for the owner (options; recommendation first)

**A. #367 — where does the empty answer live?**
- **A1 (recommended):** in `_merge`, as one more clause on #344's
  `v is None` arm. `_fallback_multivalue([])` keeps an answer of its own
  (`('UN', None)`) so a direct caller gets PS3.5's no-VR element rather
  than a `LO` nothing recorded; `_merge` never reaches it with `[]`.
  One place decides, the other agrees with it.
- A2: give `_fallback_encoding` a `recorded` parameter and decide
  there. Rejected: a signature change on a function four test files
  call directly, and it moves #344's decision out of the arm that
  already owns it.
- A3: leave `('LO', [])` and document it. Rejected: it is the wrong
  VR on the wire for every recorded non-`LO` tag, and the #344 entry
  already promised the recorded VR for a zero-length element.

**B. #372 — what to say about stores ingested before the fix?**
- **B1 (recommended):** nothing is migrated. The CHANGELOG states that
  a store built by 0.9.3 or earlier from a YBR source still carries the
  source's label over RGB bytes, that `export()` from such a store
  writes the same file it wrote before, and that the remedy is to
  re-ingest. No schema bump: nothing in the row format changes.
- B2: a load-time correction, `has a loader and 8-bit, 3-sample, YBR
  label → RGB`. Rejected: `Instance.set_pixel_data()` of a genuine YBR
  array followed by `save()` produces an identical row, and B2 would
  corrupt it. The store cannot tell the two apart because it never
  recorded which path wrote the frame.
- B3: a one-shot `session.repair_colour_labels()`. Rejected: a new
  public name before 1.0 (#26/#379), for a population one re-ingest
  fixes.

**C. #372 — widen `_CARRIABLE_TRANSFER_SYNTAXES` to the lossy
syntaxes?** The allow-list (`io_handlers.py:290`) excludes them for
one stated reason — "carrying one risks shipping pixels through a
decoder whose colour-space behaviour nobody checked" (#183 spec
§16.6). That behaviour is now measured (§3.1) and the correction is
the same helper the top level uses.
- **C1 (recommended):** add JPEG Baseline (`1.2.840.10008.1.2.4.50`)
  and JPEG 2000 (`.4.91`), the two measured here through a nested
  item (§3.3, a `YBR_FULL` item decoded through borrowed `file_meta`
  came back RGB with meta `RGB`). Two #183 tests flip
  (`test_a_lossy_source_keeps_its_loss_row_and_carries_nothing`,
  `test_the_carriable_transfer_syntaxes_are_the_uids_pydicom_names`,
  `tests/test_nested_pixel_carriage.py:674` and `:700`), and the 0.9.3
  #183 entry's "lossy ones are refused" (`CHANGELOG.md:68`) is what the
  new entry supersedes. JPEG Extended (`.51`), JPEG-LS near-lossless
  (`.81`) and HTJ2K (`.203`) stay out until measured — an allow-list's
  whole point is that the unmeasured side is the refusing side.
- C2: correct the top level only, leave the allow-list. Then the
  comment at `io_handlers.py:270-289` has to be rewritten to a reason
  that is true, and there is none left: the nested decode goes through
  the same `pixel_array` and gets the same RGB bytes.
- C3: widen to every lossy syntax pydicom names. Rejected for the
  reason the allow-list was chosen over a deny-list.

**D. #357 — the CHANGELOG's correction to the issue text.** The entry
has to say `JITTER` is accepted vocabulary, because the issue says the
opposite and a reader who finds the issue first will "fix" the scaffold.
- **D1 (recommended):** say it in the #357 entry, cite
  `privacy.py:187` and `:483`, and leave the issue as filed (the
  CHANGELOG is the record, per CLAUDE.md).
- D2: also edit the issue body. The owner's call; the spec does not
  need it.

**E. The pre-existing coverage gap.** `tests/test_private_tag_arity_roundtrip.py`
and `tests/test_private_tag_empty_value_roundtrip.py` are in neither
`scripts/mutation_probe.py`'s `TARGETS` nor CLAUDE.md's table, and
#367's tests land in the second of them.
- **E1 (recommended):** add both under `io_handlers.py` in this PR,
  which edits both lists anyway.
- E2: file it. Costs a second PR for two list lines.

---

## 1. #367 — an empty private list and the recorded VR

### 1.1 Measured on current main

```
isocenter.__file__ = <worktree>/isocenter/__init__.py
DicomExporter._fallback_multivalue([])  -> ('LO', [])
DicomExporter._fallback_encoding([])    -> ('LO', [])
_value_fits_vr([], 'DS') / 'LO' / 'UN' / 'US' / 'PN' / 'LT'  -> False, False, False, False, False, False
```

And pydicom 3.0.2's `Dataset.add_new(tag, vr, value)` under explicit VR
(a pixel-bearing dataset written through `save_as`, read back with
`dcmread`), the three empty spellings against eleven VRs:

| value | DS | IS | US | FL | AT | SL | LO | PN | UN | UT | OB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `None` | zero-length, VR kept | same | same | same | same | same | same | same | same | same | same |
| `[]` | zero-length, VR kept | same | same | same | same | same | same | same | same | same | same |
| `()` | **TypeError** | **TypeError** | warns, zero-length | warns | warns | warns | warns | **AttributeError** | warns | warns | warns |

`None` is the only spelling that is silent everywhere, which is why the
arm normalises to it (§1.3) and why #344 chose it.

### 1.2 Why this is #344's arm and not a new one

`_merge`'s private arm (`io_handlers.py:4313-4341`) is three clauses:

```python
recorded = (vrs or {}).get(t)
if v is None:
    vr = recorded if recorded is not None else 'UN'      # #344
elif recorded is not None and _value_fits_vr(v, recorded):
    vr = recorded                                         # #154
else:
    encoded = DicomExporter._fallback_encoding(v)        # #165
```

`[]` takes the third clause because the second's guard is `False` for
an empty list under *every* VR — deliberately, since `_value_fits_vr`
recurses over the list and an empty one has nothing to check. The
#344 spec's Q1 asked exactly this ("does the empty-list case consult
the recorded VR?"); the answer is no, and the shape of the fix is the
one it suggested: the first clause's predicate is one clause short.

`_fallback_multivalue([])` is reached only from that third clause and
from direct test calls: `_fallback_encoding` has **one** production
caller, the private arm at `io_handlers.py:4342` (grep over
`isocenter/` and `isocenter/exporters/`; the call at `:4542` is
`_fallback_multivalue` recursing on a single atom, never a container),
so no even-group or nested path moves with it. **Measured, not assumed** (throwaway mutation,
reverted): deleting the `if not atoms: return 'LO', []` arm at
`:4553-4554` outright changes nothing — the join code below it takes
`all(len(atom) <= _LO_MAX for atom in [])` as `True` and returns
`('LO', [])` anyway. So "delete the arm" is not a fix, and a test that
only asserts on `_fallback_encoding([])` cannot tell the arm from its
absence. The function needs a stated empty answer, and it has to be
one `_merge` agrees with.

### 1.3 The fix

At `io_handlers.py:4313`, the predicate widens and the value is
normalised:

```python
if v is None or (isinstance(v, (list, tuple, MultiValue)) and len(v) == 0):
    vr = recorded if recorded is not None else 'UN'
    v = None     # `()` raises under DS/IS/PN in add_new (§1.1); None never does
```

`(list, tuple, MultiValue)` is the tuple `_merge` already uses at
`:4360`. The comment block above the arm gains two sentences: an empty
container is the same assertion as `None` (present, no value — PS3.5
7.4 makes no distinction on the wire), and the value is normalised
because pydicom's three empty spellings are not interchangeable.

At `io_handlers.py:4553-4554`, `_fallback_multivalue`'s arm becomes
`return 'UN', None` with the comment rewritten: `_merge` never reaches
this with an empty container (it decides before calling), so this is
the answer a *direct* caller gets, and it is PS3.5 6.2.2's — a
zero-length element whose VR was never known is `UN`, not `LO`.

Nothing else moves. `_value_fits_vr` is not widened (the #344 arm's
comment explains why: it recurses, and admitting empties there admits
`[None, 'B']`). `_fallback_encoding` keeps its signature. No exception
is added or changed: a `[]` that used to export as zero-length `LO`
exports as zero-length `<recorded VR>` or `UN`; no path that returned
now raises and no path that raised now returns. The grade is unchanged
— it was never a loss row.

### 1.4 What is not fixed here

A recorded VR that was **wrong for an empty value** (say `SQ`) is not
a case: `_record_private_vr` records what the source wrote, and a
source cannot write a zero-length `SQ` that pydicom reads back as an
attribute rather than a sequence. Under **Implicit VR** export the VR is
invisible (#344 spec §1.4), so every end-to-end test in §5.1 uses the
pixel-bearing fixture that forces the explicit-VR compressed branch.

---

## 2. #357 — `research_tags.json`

### 2.1 Measured

- `git ls-files isocenter/resources` → `ctp_rules.json`, `phi_tags.json`,
  `redaction_rules.json`, `research_tags.json`.
- `grep -rn research_tags` over the tree → the file itself,
  `CHANGELOG.md:2483` (the 0.7.0 entry that introduced four resource
  files), `docs/superpowers/specs/2026-09-06-midi-b-gap-run.md:5,:184`.
  **No Python file names it.** The three readers that exist are
  `config_manager.py:209` (`phi_tags.json`), `session.py:152`
  (`redaction_rules.json`), `session.py:169-171` (`ctp_rules.yaml`
  preferred, `ctp_rules.json` shipped).
- History: introduced in `2ba1900`, renamed in `25c2795`, never loaded
  in any commit since.
- Content: KEEP `0010,0040` / `0010,1010`; JITTER `0008,0020/0021/0022/0023`
  and `0010,0030`; REMOVE `0008,0030`. The scaffold's live defaults
  (`session.py:242 _default_action_for_tag`) are JITTER for `0008,0020`,
  KEEP for the same two, REMOVE otherwise — the file is those defaults
  plus four dates the scaffold decides by the same rule anyway.
- `JITTER`: `privacy.py:187 _ACTION_WORDS = frozenset({"REMOVE", "EMPTY",
  "SHIFT", "JITTER"})`; `privacy.py:483 elif action_code in ["SHIFT",
  "JITTER"]:` → `SHIFT_DATE`. `configuration.py:179` documents it.
  `tests/test_scaffold_features.py:34` and
  `tests/test_scaffold_profiles.py:30` pin the scaffold emitting it.
- Packaging: `setup.py:90 package_data={"isocenter": ["resources/*.json",
  "resources/*.yaml"]}` — a glob, no per-file line.
  `tests/test_packaging_contract.py` walks git-tracked non-`.py` files
  (`_data_files_in_package`) and asserts each is in the wheel and sdist;
  the only mention of a count is the docstring at `:341` ("ship four
  JSON resources"), with no assertion on the number. `publish.yml`'s
  wheel check loads `phi_tags.json` only. `docs/developer_guide.md:116`
  says `resources/*.json` generically.

### 2.2 Determination

Delete `isocenter/resources/research_tags.json`. Nothing else in the
package changes: the glob still matches the three survivors, the
packaging tests derive their list from git, and the `publish.yml` check
never named the file.

Rejected: **making it a profile's retention half.** The MIDI-B spec §1
is explicit that the profile is *documented, not shipped* and that no
new API name lands before 1.0; a loader for this file is a name
(`research_tags`, or a `profile=` value) either way. Also rejected:
**keeping it as documentation.** A JSON file in `resources/` is a
promise that something reads it, and the 0.7.0 entry made that
promise; the honest record is the deletion plus a CHANGELOG that says
nothing ever did.

### 2.3 The guard

`tests/test_packaging_contract.py` gains
`test_every_shipped_resource_is_named_by_the_package`: for every
git-tracked file under `isocenter/resources/`, its basename appears as
a string literal in some `isocenter/*.py` (an AST walk over
`ast.Constant` strings, not a regex over source text, so a commented-out
reference does not count). The direction matters: **resource → code**,
not the reverse, because `ctp_rules.yaml` is named at `session.py:169`
and deliberately does not ship. Mutations that kill it: re-add
`research_tags.json` (red: no literal names it); change the
`"redaction_rules.json"` literal at `session.py:152` to
`"redaction-rules.json"` (red — and that mutant is also the
silent-degrade class `_load_redaction_knowledge_base` has, since it
returns `[]` when the path is missing rather than raising).

---

## 3. #372 — the colour space the sidecar holds

### 3.1 Measured

Fixtures, one flat colour whose RGB and YBR triples are far apart:
RGB `(220,40,90)` ⇔ YBR_FULL `(100,123,214)`, 64×64 (JPEG 2000 refuses
16×16 — too small for the default resolution levels). Built in the
throwaway `enc/` venv (`build_fixtures.py`: lossy J2K declared
`YBR_ICT` via `ds.compress(JPEG2000, j2k_cr=[10])`, lossless J2K
declared `YBR_RCT`, lossy J2K `RGB` control, Pillow baseline JPEG
`subsampling=1` declared `YBR_FULL_422` hand-encapsulated with
`LossyImageCompression="01"`) and in the project venv without any
encoder (`build_native.py`: native `YBR_FULL` 3 bytes/px, native
`YBR_FULL_422` packed `Y0 Y1 Cb Cr` 2 bytes/px per PS3.3 C.7.6.3.1.2,
native `RGB` control). Ingested with a real `Session`, sidecar bytes
read straight from the file through the instance's loader
(offset/length/zlib), then exported both ways and read back with
pydicom as a conformant reader would (`measure_372.py`).

| fixture | source TS / declared PI | graph `0028,0004` after ingest | sidecar first triple | export default (J2K Lossless, explicit VR): PI / reader sees | export `use_compression=False` (Implicit VR LE): PI / reader sees |
| --- | --- | --- | --- | --- | --- |
| `lossy_jpeg_ybr_full_422` | JPEG Baseline / `YBR_FULL_422` | `YBR_FULL_422` | `(221,40,91)` RGB, 12288 B | `YBR_FULL_422` / **`(169,255,65)`** (wrong colours) | `YBR_FULL_422`, 12288 B / **`ValueError: ... a third larger than expected (12288 vs 8192 bytes) ... 'YBR_FULL_422' is incorrect`** |
| `native_ybr_full` | Explicit VR LE / `YBR_FULL` | `YBR_FULL` | `(221,40,91)` RGB | `YBR_FULL` / **`(169,255,65)`** | `YBR_FULL` / **`(169,255,65)`** |
| `native_ybr_full_422` | Explicit VR LE / `YBR_FULL_422` (8192 B) | `YBR_FULL_422` | `(221,40,91)` RGB, 12288 B | `YBR_FULL_422` / **wrong colours** | `YBR_FULL_422` / **`ValueError`** as above |
| `lossy_j2k_ybr_ict` | JPEG 2000 / `YBR_ICT` | `YBR_ICT` | `(220,40,90)` RGB | `YBR_ICT` / `(220,40,90)` correct; label nonconformant (ICT is a J2K-lossy-only value, PS3.3 C.7.6.3.1.2) | `YBR_ICT` / correct; label nonconformant under a native syntax |
| `lossless_j2k_ybr_rct` | JPEG 2000 Lossless / `YBR_RCT` | `YBR_RCT` | `(220,40,90)` RGB | `YBR_RCT` / correct; label allowed under this TS but false of the bytes (Isocenter re-encodes from the RGB sidecar through Pillow, whose `mct` default is 0 — the exported codestream's MCT flag was not inspected, so "no RCT" is inferred, not measured) | `YBR_RCT` / correct; label nonconformant under a native syntax |
| `lossy_j2k_rgb_control` | JPEG 2000 / `RGB` | `RGB` | `(220,40,90)` | `RGB` / correct | `RGB` / correct |
| `native_rgb_control` | Explicit VR LE / `RGB` | `RGB` | `(220,40,90)` | `RGB` / correct | `RGB` / correct |

The `(221,40,91)` vs `(220,40,90)` difference is YBR round-trip
rounding, not compression. `0028,2110` LossyImageCompression `01`
survives ingest and export on the JPEG fixture; nothing is written for
lossy provenance and nothing needs to be. `0028,0006` PlanarConfiguration
is `0` in every export (the #186 correction).

**What pydicom itself says** (`probe_meta.py`, every fixture):
`Dataset.pixel_array` never changes `ds.PhotometricInterpretation`;
`pydicom.pixels.get_decoder(ts).as_array(ds)` returns `(arr, meta)` with
`meta["photometric_interpretation"] == "RGB"` for every YBR fixture
above, and `RGB` for the two controls. With `as_rgb=False` the bytes
stay YBR but a native `YBR_FULL_422` still comes back **resampled to
`YBR_FULL`** with meta `YBR_FULL` — so "decode without converting" is
not a way to keep the declared label true either.

**Plugin-independent.** The whole table was produced twice, in the
`proj/` venv with pylibjpeg-openjpeg/libjpeg installed and in the
project `.venv` with Pillow only; every cell is identical.

**Parity checks for the fix's decode call** (one-liners, project venv):
a 2-frame native `YBR_FULL` → `as_array` shape `(2,8,8,3)`, first pixel
`(221,40,91)`, meta `RGB`, same as `pixel_array`; a sequence item
carrying `YBR_FULL` bytes with the enclosing `file_meta` borrowed (the
#183 shape) → `as_array` `(221,40,91)`, meta `RGB`, same as
`item.pixel_array`; a bare dataset with **no `file_meta`** (the
`force=True` population, #281) → `pixel_array` raises
`AttributeError: Unable to decode the pixel data as the dataset's
'file_meta' has no (0002,0010) 'Transfer Syntax UID' element` and
`get_decoder(None)` raises `TypeError: A UID must be created from a
string` — both inside `ingest_worker`'s `try` at `:1399-1410`, both a
`Decompression Failed: ...` row, different text.

### 3.2 Determination: a defect, and whose

**Defect.** The exported file is wrong in a way the source was not: a
conformant reader shows a `YBR_FULL` source in false colour and refuses
a `YBR_FULL_422` one. That is #160's shape (a nonconformant file
Isocenter wrote) and the 0.9.1 #186 entry's own standard ("a colour
icon read as garbage by a conformant reader, which is a worse outcome
than the drop").

**Population**, exact: every 8-bit, `SamplesPerPixel=3` source declared
`YBR_FULL` or `YBR_FULL_422` under **any** transfer syntax — native
Explicit/Implicit VR included, which is the ultrasound population and
is not lossy at all — plus `YBR_ICT` / `YBR_RCT` under JPEG 2000. Two
severities: the first pair is wrong colours or a reader `ValueError`;
the second pair reads correctly and is a false label under a syntax
that does not permit it. Untouched: 16-bit `YBR_FULL` (pydicom refuses
it at decode, `tests/test_ingest_failure_audit.py`, and it stays
refused); `PALETTE COLOR` and the monochromes (meta equals declared —
the developer measures this, §3.5); the float arm (`:1431`, no colour).

**Where it lives.** `ingest_worker` copies `0028,0004` through
`populate_attrs` before the decode, corrects `0028,0006` at
`:1390-1391` (also before the decode), and decodes at `:1402` with
`ds.pixel_array`, whose meta — the one place pydicom states the array's
colour space — is discarded inside pydicom (`pixels/utils.py:1465
arr, _ = decoder.as_array(...)`). Export cannot recover it: a 3-sample
array is equally `RGB` or `YBR_FULL` (the #186 spec's §3.8 says so and
is right), which is why `resolve_photometric_interpretation` correctly
leaves the label alone and why the fix is **not** there.

**Not a defect in the #186 spec's rule; a defect in its fixture
expectation.** §3.8 ("correct only an outright contradiction") is the
right export-side rule. §1.1 and §7 test 5 then assert that a
`YBR_FULL` source *should* export as `YBR_FULL`, and
`tests/test_pixel_geometry_pipeline.py:256-289` pins it without ever
checking a byte. That test has been green over RGB bytes since 0.9.1.

### 3.3 Rejected alternatives

- **Refuse at ingest** (the #183 nested-path shape). Rejects every
  native YBR ultrasound file with a `Decompression Failed` row for a
  file pydicom decodes perfectly well. The nested path refused because
  it had no measurement; it now has one.
- **Decode with `as_rgb=False`** and keep the declared label true. A
  native `YBR_FULL_422` still comes back resampled to `YBR_FULL`
  (§3.1), so the label is wrong for that population anyway; the
  redaction and OCR paths (`pixel_analysis.py`, `services.py`) read
  the sidecar as RGB; and `_compress_j2k` (`:3257`) hands Pillow an
  array it treats as RGB. Every downstream consumer already assumes
  what `as_rgb=True` gives.
- **Rewrite the label at export from the loader's presence.** §0.2 B2.
- **A colour-space table of Isocenter's own** (`YBR_* and 8-bit and 3
  samples → RGB`). A second spelling of pydicom's rule, which pydicom
  can change (it already differs between `as_rgb` settings and between
  native and encapsulated 422). The decoder's meta is the measurement;
  the table is a reading of it.

### 3.4 The fix

One helper in `io_handlers.py`, used by both decode sites:

```python
def _decode_pixels(ds):
    """The array `Dataset.pixel_array` returns, and the colour space it is in.

    `pixel_array` calls exactly this and discards the meta
    (pydicom 3.0.2 pixels/utils.py:1465). ...
    """
    ts = ds.file_meta.TransferSyntaxUID          # AttributeError/KeyError -> caller's except, as today
    arr, meta = get_decoder(ts).as_array(ds)
    return np.ascontiguousarray(arr), meta["photometric_interpretation"]
```

- `ingest_worker`, `:1402`: `arr, decoded_pi = _decode_pixels(ds)`
  inside the same `try`; after it, **after** the decode (unlike the
  `0028,0006` write, which runs before and must stay before — it reads
  nothing from the array), `if inst.attributes.get("0028,0004") !=
  decoded_pi: inst.set_attr("0028,0004", decoded_pi)`. Write only on
  difference, the `_write_str_if_changed` shape, so an RGB, monochrome
  or palette source bumps no revision.
- `_decode_nested_pixels`, `:1279`: the same call under the borrowed
  `file_meta`, and the same conditional write on the item found by
  `resolve_item_path` next to the PlanarConfiguration correction at
  `:1311-1312`.
- `_CARRIABLE_TRANSFER_SYNTAXES`, `:290`: per §0.2 C1, add `.4.50` and
  `.4.91`; the comment at `:270-289` is rewritten to say the colour
  question was measured and answered by the helper, and that the
  remaining exclusions are unmeasured, not unsafe.
- Nothing changes in `pixel_geometry.py`, `entities.py`,
  `_write_pixel_geometry` or `_compress_j2k`. `0028,2110` is left as
  the source had it.

**Exact exceptions:** none new. `Decompression Failed: <message>` rows
keep their route. For a file with no `file_meta` the row's *text*
changes: `pixel_array` raised `AttributeError: Unable to decode the
pixel data as the dataset's 'file_meta' has no (0002,0010) 'Transfer
Syntax UID' element`; the helper's `ds.file_meta.TransferSyntaxUID`
raises `AttributeError: 'FileMetaDataset' object has no attribute
'TransferSyntaxUID'` (measured; same type, same `except`, same row).
The helper reads it as an attribute rather than with a `.get()`
default, which would turn the failure into `TypeError: A UID must be
created from a string` or, worse, a decode under Explicit VR LE. The
parity test (§5.3) pins the row, not its text. Reader-side, the only messages that change are the ones
that stop: pydicom's `ValueError` on the exported 422 file, and
`(169,255,65)` becoming `(221,40,91)`.

### 3.5 What the developer must measure before merging, in order

1. `Dataset.pixel_array` runs on the `pixels` backend in this venv
   (`ds._pixel_array_opts["use_pdh"]` is `False`; `pydicom.config` has
   no `use_pdh`-style override set anywhere in `isocenter/`), so the
   helper decodes the same bytes `pixel_array` did. Confirmed by reading
   `dataset.py:1721-1727` at 3.0.2; measure, do not read.
2. `meta["photometric_interpretation"]` equals the declared value for
   `MONOCHROME1`, `MONOCHROME2`, `PALETTE COLOR` and `RGB` sources, so
   the conditional write fires for none of them (revision unchanged —
   assert `_revision` before and after).
3. A 2-frame `YBR_FULL` source (§5.2 has the row).
4. The no-`file_meta` file (#281's population) still lands in the same
   `Decompression Failed` row.

### 3.6 Existing stores

Per §0.2 B1: not migrated, and said so. The population is every
instance ingested from a YBR source by 0.7.0–0.9.3. The graph carries
the source's label, the sidecar carries RGB, and the fix runs at
ingest. No schema column changes, so no `user_version` bump.

### 3.7 MIDI-B (#356)

One sentence for that spec's §4.3, not a supersession: colour
conversion at ingest is one more reason a `pixels_retained` digest
against a curated YBR file will not match byte-for-byte — the bytes
are RGB and the label now says so. Its "unknown until the first run"
already covers this; the run will show it.

---

## 4. Claims overturned by measurement

| where | claim | measured |
| --- | --- | --- |
| #372 issue body | "both export paths write Implicit VR LE" | The default (`use_compression=True`) is JPEG 2000 Lossless, explicit VR, via Pillow (`_compress_j2k`, `:3257`); only `use_compression=False` writes Implicit VR LE raw bytes. Both are in the table (§3.1). |
| #372 issue framing | a lossy-source problem | The native `YBR_FULL` and `YBR_FULL_422` rows have no lossy step anywhere; the conversion is `as_rgb=True`, not a codec. |
| #357 issue body | `JITTER` "is a spelling no arm accepts" | `privacy.py:187`, `:483`; `configuration.py:179`; two scaffold tests. It is the accepted spelling and it is what the scaffold writes. |
| #357 issue body | a `package_data` line to delete | `setup.py:90` is a glob. Nothing in `setup.py` changes. |
| #183 spec §0 Q6 | "no lossy-compressed fixture of any family can be built in this venv" | Pillow (an `install_requires`, `setup.py:117`) builds baseline JPEG with `subsampling=1` (→ `YBR_FULL_422`) and JPEG 2000 with `irreversible=True`/`mct=1` (→ `YBR_ICT`) and reversible `mct=1` (→ `YBR_RCT`); pydicom's Pillow plugin decodes all three. The `enc/` venv was needed only for pydicom's `ds.compress()` convenience, not for any fixture the tests need. |
| #183 spec §0 Q6 | "the export's other path writes Implicit VR LE" as a narrowing | True of the raw path and irrelevant to the label: the label is wrong under both syntaxes (§3.1). |
| #186 spec §1.1, §3.8 comment, Rank-3 row, §7 test 5; 0.9.1 CHANGELOG #186 entry (`CHANGELOG.md:1663`, `:1673`) | the relabelling `YBR_FULL → RGB` was a defect and keeping `YBR_FULL` fixed it | The bytes were RGB before that line ran and after it was removed. The removed line was right about the colour space and wrong about its authority (it relabelled from `samples >= 3`, which would also relabel a genuine YBR array set in memory); the replacement kept a false label. Neither CHANGELOG entry is edited; the new entry names them. |
| #344 spec §7, and `tests/test_private_tag_export.py:378-386` docstring | "A zero-length `LO` says 'this tag was here and had no value', which is what the graph held" | It says `LO`, which is what the graph did not hold when the source recorded `DS`. |

---

## 5. Tests — what each pins, and the mutation that kills it

Every end-to-end test below ingests through `Session`, exports through
`session.export()`, and reads the file back with `pydicom.dcmread`.
Every #367 test writes pixel data into its source so export takes the
explicit-VR compressed branch (the VR is invisible under Implicit VR).
Every #372 fixture is built with Pillow and pydicom only — no plugin,
no marker, no skip.

### 5.1 #367 — in `tests/test_private_tag_empty_value_roundtrip.py`

The file already has `EMPTY_RECORDED_VR` (eleven VRs at
`0009,1005`–`0009,1010`) and module-scoped `fresh_export` /
`reloaded_export` fixtures with a pixel-bearing `_write_src`. Add a
second table, `EMPTY_LIST_RECORDED_VR`, at fresh elements: the source
writes a *valued* element under each VR (so `_record_private_vr`
records it), and `_plant` replaces the value with `[]` before export.

1. `test_an_empty_list_takes_the_recorded_vr_on_the_fresh_path[DS|IS|US|LO|PN|UT|AT]`
   — exported element present, zero-length, `VR == recorded`.
   **Killed by:** reverting the `_merge` predicate to `if v is None:`
   (every parametrisation exports `LO` — measured today).
2. `test_an_empty_list_takes_the_recorded_vr_on_the_reloaded_path[...]`
   — same through `reloaded_export` (#328's placeholder row carries
   the length-0 container). **Killed by:** the same mutation; and by
   dropping the placeholder row (the tag vanishes — #328's own red).
3. `test_an_empty_list_with_no_recorded_vr_is_un` — a private tag that
   exists only in the graph (`set_attr(tag, [])`, no source element),
   both paths: zero-length `UN`. **Killed by:** `'UN'` → `'LO'` in the
   `_merge` arm.
4. `test_an_empty_tuple_is_the_same_element` — `set_attr(tag, ())` on
   a recorded-`DS` tag: zero-length `DS`, no `DATA_LOSS` row.
   **Killed by:** deleting the `v = None` normalisation (`add_new`
   raises `TypeError` under `DS`, `_merge`'s `except` files a loss row
   — measured in §1.1).
5. `tests/test_private_tag_export.py:386` → `assert
   DicomExporter._fallback_encoding([]) == ('UN', None)`, docstring
   rewritten to say `_merge` decides before this is reached and this is
   the direct caller's answer. **Killed by:** `'UN', None` → `'LO', []`.
   (Deleting the arm outright also returns `('LO', [])`, §1.2 — so this
   assertion kills the deletion too.)
6. `tests/test_private_tag_arity_roundtrip.py:15` and `:171`: the two
   docstrings that quote the `('LO', [])` arm are rewritten (the test at
   `:163-208` asserts fresh VR == reloaded VR and stays green: both
   become `UN`, since `_write_src` records no VR for the EMPTY tag).
   Not a test change; a prose change, named so the reviewer checks it.

### 5.2 #372 — new `tests/test_colour_space_at_ingest.py`

Helpers: `_ybr_fixture(kind, frames=1)` returning a path, where `kind`
∈ {`native_ybr_full`, `native_ybr_full_422`, `jpeg_ybr_full_422`,
`j2k_ybr_ict`, `j2k_ybr_rct`, `native_rgb`, `mono2`}; the flat colour
`(220,40,90)`; the packed 422 layout from `build_native.py`; the JPEG
via `PIL.Image.save(format="JPEG", subsampling=1)` + `encapsulate`;
the J2K via `PIL.Image.save(format="JPEG2000", irreversible=True,
mct=1)` / `(irreversible=False, mct=1)`. A tolerance of ±4 per channel
covers YBR rounding and J2K quantisation at this flat colour.

1. `test_a_ybr_source_is_stored_and_exported_as_rgb[kind]` for the five
   YBR kinds: after ingest `inst.attributes["0028,0004"] == "RGB"`; the
   sidecar bytes (read through `inst._pixel_loader`'s offset/length,
   not `get_pixel_data()`, so the assertion is about what is on disk)
   start `(220±4, 40±4, 90±4)`; the exported file (default export)
   declares `RGB` and `pydicom.dcmread(...).pixel_array[0, 0]` is the
   source colour. **Killed by:** deleting the conditional `set_attr`
   in `ingest_worker` (label stays YBR; reader sees `(169,255,65)` or
   raises — measured today on every row).
2. `test_a_native_422_source_exports_raw_without_a_reader_error` —
   `native_ybr_full_422`, `use_compression=False`: the reader's
   `pixel_array` does not raise and is the source colour. **Killed by:**
   the same mutation (`ValueError ... a third larger`).
3. `test_a_two_frame_ybr_source_is_corrected_once` — `native_ybr_full`
   with `frames=2`: label `RGB` and **both** frames the source colour
   through `get_pixel_data()`. **Killed by:** a helper that decodes
   `index=0` only (the second frame is missing or garbage), or one that
   reads the label from `arr[0]`'s shape rather than the meta.
4. `test_the_label_is_written_only_when_it_changes[native_ybr_full|native_rgb|mono2|palette]`
   — under `ISOCENTER_FORCE_THREADS=1` (so the worker runs in-process
   and a monkeypatch reaches it; `docs/environment.md` documents the
   variable), `Instance.set_attr` is wrapped to record its calls: the
   YBR row sees exactly one `("0028,0004", "RGB")`, the three
   controls see none for `0028,0004`, and every row's label is what
   the source declared except the YBR one. The `palette` fixture is
   the reviewer's §7 item 3. **Killed by:** an unconditional
   `set_attr` (three controls red); a write keyed on `samples >= 3`
   rather than the meta (`palette` is 1-sample and passes, `native_rgb`
   is written and fails — and the #186 line is back).
5. `test_ybr_full_survives_load_and_export` at
   `tests/test_pixel_geometry_pipeline.py:256` **flips**: renamed
   `test_a_ybr_full_source_exports_as_rgb_over_rgb_bytes`, asserts
   `"RGB"` at `:277` and `:286`, and adds the byte check the original
   never had: the source writes YBR triple `[10, 20, 30]` at `[3, 3]`,
   so the exported file's `pixel_array[3, 3]` must be that triple's RGB
   (compute it with the same PS3.3 C.7.6.3.1.2 equations the fixture
   helper uses, ±2). Docstring rewritten to say what §3.2 says.
   **Killed by:** the same deletion as 1.
6. Nested (with C1): `test_a_lossy_icon_is_carried_and_relabelled` in
   `tests/test_nested_pixel_carriage.py` — the existing `:674` test's
   fixture (JPEG Baseline icon declared `YBR_FULL_422`), now asserting
   a `pixels:` blob kind, no `7fe0,0010` loss row, and the exported
   item's `PhotometricInterpretation == "RGB"` with its `PixelData`
   decoding to the icon's colour. **Killed by:** deleting the nested
   conditional write (item exports `YBR_FULL_422` over RGB bytes);
   removing `.4.50` from the allow-list (loss row returns). `:700`'s
   name list gains `"JPEGBaseline8Bit"` and `"JPEG2000"`.
7. `tests/test_ingest_failure_audit.py`: no change; its 16-bit
   `YBR_FULL` fixture still refuses (re-run, do not assume).

### 5.3 #372 parity — same file as §5.2

8. `test_a_file_with_no_file_meta_still_takes_the_decompression_failed_row`
   — a bare Implicit VR dataset with pixel data and no preamble, read
   by ingest with `force=True`: the file is refused with a
   `Decompression Failed` audit row and no instance. **Killed by:** a
   helper that reads the transfer syntax with a `.get()` default and
   decodes as Explicit VR LE (the file would ingest with garbage
   geometry).

### 5.4 #357

9. `tests/test_packaging_contract.py::test_every_shipped_resource_is_named_by_the_package`
   (§2.3). **Killed by:** re-adding `research_tags.json`; renaming the
   `"redaction_rules.json"` literal at `session.py:152`.
10. `:341` docstring "four" → "three" (prose).

---

## 6. Earlier specs and CHANGELOG entries this falsifies

Marked in place per CLAUDE.md, each with a front-matter
`**Superseded in part:**` line naming #372 and the clause:

- `2026-08-29-pixel-geometry-authority.md`: §1.1 "New finding" paragraph
  (`:74-80`); §3.8's `else: leave it alone # YBR_FULL, YBR_ICT, RGB,
  MONOCHROME1 all survive` comment (`:463` — the rule stands, the
  comment's implied claim that the *bytes* are YBR does not); the
  Rank-3 row `(8,8,3) | SPP=3, PI=YBR_FULL | ... | PI stays YBR_FULL`
  (`:836`); §7 test 5 (`:995-997`).
- `2026-09-07-blob-kind-and-nested-pixel-carriage.md`: §0 Q6's
  "no lossy-compressed fixture of any family can be built in this
  venv" and "Marked unmeasured deliberately" (`:158-172`); §16.6's
  "a decoder whose colour-space behaviour nobody checked" (`:1574`).
- `2026-09-07-zero-length-private-element-export.md`: Q1 gains an
  "answered by" line pointing here — not struck, since it asked rather
  than claimed.
- `CHANGELOG.md`: the 0.9.1 #186 entry (`:1663`, `:1673`) and the 0.9.3
  #183 entry (`:68`) are **not edited**; the new entries name them.

---

## 7. What the reviewer should attack

1. **§3.4's helper decodes the same thing `pixel_array` did.** The
   proof is §3.5 item 1 plus the parity rows in §3.1; a reviewer who
   finds a `_pixel_array_opts` key that `ingest_worker`'s environment
   sets (a `decoding_plugin`, a `use_pdh`) has found a second decode
   path. Grep `pydicom.config` across `isocenter/` and `tests/conftest.py`.
2. **The conditional write's revision claim** (§5.2 tests 3 and 4).
   `set_attr` bumps `_revision` on every call; if the developer writes
   unconditionally the fix is still correct and every ingested colour
   instance is dirtier than before. The tests compare against a
   control, not a constant — check the control is built the same way.
3. **`meta["photometric_interpretation"]` for `PALETTE COLOR`.**
   §3.5 item 2 asks for a measurement; if pydicom reports `RGB` there
   (it does not at 3.0.2 without `apply_color_lut`, but this is the
   one row not in §3.1's table), the write would relabel a palette
   image whose bytes are indices. The test in §5.2 item 4 must include
   a `PALETTE COLOR` fixture, not just `mono2`.
4. **The 422 packed layout in the fixture.** `build_native.py` writes
   `Y0 Y1 Cb Cr`; if the test helper writes `Y Cb Cr Y` the source is
   wrong and the assertion on the sidecar's first triple is testing
   pydicom's tolerance, not Isocenter's correction. Read the fixture
   back with pydicom before ingesting and assert `(220±4, 40±4, 90±4)`
   there too.
5. **`_merge`'s widened predicate and `_merge_sequences`.** Nested
   private empties go through the same `_merge` (`:4580` calls it per
   item), so no separate arm — but the reloaded path's placeholder row
   for a nested empty is #328's territory; confirm
   `test_private_tag_arity_roundtrip.py` covers one level down or say
   it does not.
6. **The `()` row** (§5.1 test 4). It is the only thing that kills the
   `v = None` line; if it is dropped the line is documented-equivalent
   dead code, and #344's spec accepted that shape for `_value_fits_vr`.
   Either keep the row or write the line's comment to say so.
7. **§2.3's AST walk** must look at `ast.Constant` values that are
   `str`, inside f-strings too (`ast.JoinedStr` holds `Constant`
   parts). A resource named only in an f-string is still named.
8. **C1's flipped tests** are the same file's neighbours; the reviewer
   checks the `:700` test's `assert str(getattr(uid, name)) in
   _CARRIABLE_TRANSFER_SYNTAXES` gains the two names *and* that the
   sentence in its docstring about lossy names being absent is
   rewritten, not left as a false comment over a true assertion.

---

## 8. Implementation brief — one PR, three issues, in this order

Conventional commits, one per step, `Fixes #367`, `Fixes #357`,
`Fixes #372` in the PR body. TDD per step: the test first, red for the
stated reason, then the change, then the mutation from §5 re-applied by
hand per CLAUDE.md's runbook and recorded in the CHANGELOG entry.

### Step 1 — #367

1. `tests/test_private_tag_empty_value_roundtrip.py`: §5.1 tests 1–4
   (red: `LO` exported / `TypeError` loss row).
2. `tests/test_private_tag_export.py:386` → `('UN', None)` (red).
3. `io_handlers.py:4313` predicate + `v = None`; `:4553-4554` →
   `return 'UN', None`; comments per §1.3.
4. `tests/test_private_tag_arity_roundtrip.py:15`, `:171` docstrings.
5. Mutations §5.1; `git diff -- isocenter/` shows the two hunks only.

**CHANGELOG, `## [Unreleased]` → `### Fixed`:**

> - **An empty private list exports under its recorded VR, not `LO`
>   (#367).** #344 made a zero-length private element consult the VR
>   the source recorded, and did it for `None` only: `[]` fell through
>   `_value_fits_vr` (False for an empty list under every VR, by design
>   — it recurses over the elements) into `_fallback_multivalue`, whose
>   `if not atoms: return 'LO', []` wrote a zero-length `LO` for a tag
>   the source had written as `DS`. Measured on 0.9.3: `set_attr(tag,
>   [])` on a recorded-`DS` private tag exported `(0009,10xx) LO
>   [zero length]`; `set_attr(tag, None)` on the same tag exported
>   `DS`. Two answers to one question, and #344's spec asked it as its
>   Q1. The `_merge` arm's predicate now covers `None` and an empty
>   `list`/`tuple`/`MultiValue`, and normalises the value to `None` —
>   because pydicom's three empty spellings are not interchangeable:
>   `add_new(tag, 'DS', ())` raises `TypeError` and `PN` raises
>   `AttributeError`, while `None` writes a zero-length element under
>   all eleven VRs tried. `_fallback_multivalue([])` returns `('UN',
>   None)` for a direct caller — PS3.5 6.2.2's answer for an element
>   whose VR was never known, and the one `_merge` gives — so the two
>   places agree. Deleting that arm outright was measured to change
>   nothing (the join below it returns `('LO', [])` for an empty list
>   anyway), which is why it is rewritten rather than removed.
>   **Exact exceptions:** none new; no call that returned now raises.
>   An element that was written zero-length `LO` is written zero-length
>   under the recorded VR, or `UN` where none was recorded (every
>   private element of an Implicit VR source). Under Implicit VR export
>   the change is invisible on the wire. Grade unchanged. Tests in
>   `tests/test_private_tag_empty_value_roundtrip.py` (fresh and
>   reloaded paths, seven VRs, the no-VR tag, the `()` spelling);
>   `tests/test_private_tag_export.py`'s `('LO', [])` assertion
>   flipped. `tests/test_private_tag_arity_roundtrip.py` and
>   `tests/test_private_tag_empty_value_roundtrip.py` were in neither
>   CLAUDE.md's table nor `scripts/mutation_probe.py`'s `TARGETS`
>   since they were written; both added under `io_handlers.py`.
>   Mutations: predicate reverted to `v is None` → seven red; `'UN'` →
>   `'LO'` in the arm → the no-VR test red; normalisation deleted →
>   the `()` test red; `('UN', None)` → `('LO', [])` → the direct
>   assertion red. Design record:
>   `docs/superpowers/specs/2026-09-08-export-fidelity-bunch-2.md` §1.

### Step 2 — #357

1. `tests/test_packaging_contract.py`: §2.3's test (red: no literal
   names `research_tags.json`); `:341` docstring.
2. `git rm isocenter/resources/research_tags.json`.
3. `docs/developer_guide.md:116` — unchanged (generic); confirm.
4. Mutations §5.4.

**CHANGELOG, `### Removed`** (new section under `[Unreleased]`):

> - **`isocenter/resources/research_tags.json` is deleted; nothing ever
>   read it (#357).** Introduced in 0.7.0 as one of "four JSON resource
>   files" (that entry stands, uncorrected — it was true of what
>   shipped), it was loaded by no code in any commit since: the three
>   readers are `phi_tags.json` (`config_manager.py`),
>   `redaction_rules.json` and `ctp_rules.json` (`session.py`). Its
>   content — KEEP for Patient's Sex and Age, JITTER for the four dates
>   and the birth date, REMOVE for Study Time — is the scaffold's
>   `_default_action_for_tag` written out a second time, and the
>   scaffold is the one that runs. **The issue as filed says `JITTER`
>   is a spelling no arm accepts; that is wrong.** `privacy.py`'s
>   `_ACTION_WORDS` lists it and its finding arm maps `JITTER` and
>   `SHIFT` alike to `SHIFT_DATE`; `configuration.py`'s `add_tag`
>   docstring lists it (`docs/configuration.md` does not name it, and
>   need not); the scaffold emits it and two tests pin that. No config
>   changes, no behaviour changes. `setup.py`'s `package_data` is a
>   glob and is untouched; the wheel and sdist now carry three
>   resources, and `tests/test_packaging_contract.py` gains a test that
>   every git-tracked file under `isocenter/resources/` is named by a
>   string literal in the package (the direction is resource → code,
>   because `ctp_rules.yaml` is named and deliberately not shipped).
>   Not made a profile: the MIDI-B spec (#356) keeps that profile
>   documented, not shipped, and adds no API name before 1.0.
>   Mutations: the file restored → red; the `"redaction_rules.json"`
>   literal misspelt → red. Design record: spec §2.

### Step 3 — #372

1. `tests/test_colour_space_at_ingest.py`: §5.2 tests 1–4, 8 (red:
   label YBR, reader wrong / raises).
2. `tests/test_pixel_geometry_pipeline.py:256` flipped (red).
3. `io_handlers.py`: `_decode_pixels` helper; `:1402` and `:1279` call
   it; the two conditional writes; **then** run §3.5's four
   measurements and record them in the commit message.
4. With C1: `_CARRIABLE_TRANSFER_SYNTAXES` + `.4.50`, `.4.91`; comment
   `:270-289` rewritten; `tests/test_nested_pixel_carriage.py:674` and
   `:700` flipped (§5.2 test 6).
5. Mutations §5.2.
6. The spec markings in §6 (front-matter lines and in-place strikes on
   the two earlier specs; the "answered by" line on the #344 spec).

**CHANGELOG, `### Fixed`:**

> - **A YBR colour source is stored and exported as the RGB it was
>   decoded to, and its PhotometricInterpretation now says so (#372).**
>   `ingest_worker` stores the bytes of pydicom's `pixel_array`, which
>   at pydicom 3 converts every 8-bit YBR family to RGB (`as_rgb=True`
>   is the default) and does not touch `PhotometricInterpretation`. The
>   graph copied the declared label, the sidecar held RGB, and the
>   export-side rule from #186 — correct a contradiction, leave a
>   coherent label alone — correctly left it alone. Measured on 0.9.3
>   with a flat `(220,40,90)` source: a native `YBR_FULL` file exported
>   declaring `YBR_FULL` over RGB bytes and a conformant reader showed
>   `(169,255,65)`; a `YBR_FULL_422` file (native or JPEG Baseline)
>   exported the same way, and with `use_compression=False` pydicom
>   refused the exported file outright (`ValueError: ... a third larger
>   than expected (12288 vs 8192 bytes) ... 'YBR_FULL_422' is
>   incorrect`); `YBR_ICT` and `YBR_RCT` J2K sources read back
>   correctly under a label the transfer syntax does not permit.
>   **The population is not lossy sources**, as the issue framed it: it
>   is every 8-bit, 3-sample source declared `YBR_FULL` or
>   `YBR_FULL_422` under any transfer syntax — ultrasound, native,
>   uncompressed — plus `YBR_ICT`/`YBR_RCT` under JPEG 2000. Identical
>   with Pillow alone and with the pylibjpeg plugins installed. Fixed
>   where PlanarConfiguration is fixed: at ingest, from pydicom's own
>   decoder meta (`get_decoder(ts).as_array(ds)` is the call
>   `pixel_array` makes and then discards the colour space of), written
>   only when it differs from the declared value, so RGB, monochrome
>   and palette sources bump no revision. The nested-icon path (#183)
>   takes the same helper and the same correction, and
>   `_CARRIABLE_TRANSFER_SYNTAXES` gains JPEG Baseline and JPEG 2000 —
>   the two syntaxes measured through a nested item — because the
>   reason those were refused ("a decoder whose colour-space behaviour
>   nobody checked") is now checked; the 0.9.3 #183 entry's "lossy ones
>   are refused" is superseded for those two, and the rest stay refused
>   as unmeasured, not as unsafe. **Nothing in `pixel_geometry.py`
>   changes**: `resolve_photometric_interpretation` was right, and the
>   0.9.1 #186 entry's "every non-RGB colour space was relabelled ...
>   corrected only where it contradicts itself" is superseded only in
>   what it implied — the label survived; the pixels under it had
>   already been converted, so keeping it was keeping a false one. That
>   entry, and `tests/test_pixel_geometry_pipeline.py`'s
>   `test_ybr_full_survives_load_and_export` (green over RGB bytes since
>   0.9.1, never checking one), are what this corrects; the test now
>   asserts `RGB` and the reader's colours. **Existing stores are not
>   migrated**: a store built by 0.9.3 or earlier from a YBR source
>   still carries the source's label over RGB bytes and exports the
>   same file it did, because a load-time relabel could not tell that
>   row from a `set_pixel_data()` of genuine YBR bytes; re-ingest is
>   the remedy. **Exact exceptions:** none new. Reader-side, the
>   pydicom `ValueError` on an exported 422 file stops, and the colours
>   stop being wrong. `LossyImageCompression` is left as the source
>   had it. New `tests/test_colour_space_at_ingest.py` (fixtures built
>   with Pillow and pydicom only — the #183 spec's "no lossy fixture
>   can be built in this venv" was wrong; Pillow is an
>   `install_requires`), registered under `io_handlers.py` in CLAUDE.md
>   and `TARGETS`. Mutations: the conditional write deleted → five
>   fixtures red (label YBR, reader `(169,255,65)` or `ValueError`);
>   made unconditional → the revision test red; the nested write
>   deleted → the icon test red; `.4.50` removed → the loss row
>   returns. Design record and the full measurement table: spec §3.

### Step 4 — records

- **CLAUDE.md table, `io_handlers.py` row:** add
  `test_colour_space_at_ingest.py`, `test_private_tag_arity_roundtrip.py`,
  `test_private_tag_empty_value_roundtrip.py` (alphabetical).
- **`scripts/mutation_probe.py` `TARGETS`, `io_handlers.py` list:** the
  same three, in the sorted positions (`tests/test_colour_space_at_ingest.py`
  after `test_codecs_strict.py`; the two private-tag files after
  `test_private_binary_ingest.py`).
- **Spec markings** per §6.
- **Issues to file** (not fixed here): the remaining lossy syntaxes
  (`.4.51`, `.4.81`, `.4.203`) for the allow-list once measured
  through a nested item; `_load_redaction_knowledge_base`'s
  silent `[]` on a missing resource (surfaced by §2.3's mutation);
  whether `pixels_retained` in the MIDI-B run should compare decoded
  arrays rather than bytes for colour sources (§3.7).

---

## 9. Measurement log and clean-tree record

- `_fallback_multivalue([])` / `_fallback_encoding([])` → `('LO', [])`,
  before and after the throwaway deletion of the `:4553-4554` arm
  (deletion reverted; `git diff --stat -- isocenter/` empty).
- `add_new` table §1.1: one script, eleven VRs, three spellings, read
  back with `dcmread`.
- §3.1 table: `measure_372.py` over `fixtures/` (four encoded) and
  `native/` (three native), in `proj/` (pylibjpeg present) and in the
  project `.venv` (Pillow only); cell-for-cell identical.
- Parity one-liners §3.1: 2-frame, sequence item, no-`file_meta`.
- `pydicom` source read at `dataset.py:1721-1727` and
  `pixels/utils.py:1455-1470` to confirm `pixel_array` →
  `get_decoder(ts).as_array(ds)` with the meta discarded.
- No production file was changed for keeps: `git diff -- isocenter/`
  is empty at commit time. Full suite on this base, in the worktree,
  unpiped `pytest -v` per the runbook: **1626 passed in 386.43 s**,
  0 failed, 0 errors (`isocenter.__file__` resolved to the worktree).

---

## 10. Amendments (made during implementation, 2026-09-08)

Corrections found while the code was being written, per CLAUDE.md's
distinction between an amendments log and a later supersession. The
determinations in §0–§3 stand; these are where the *mechanism* or a
*count* the spec gave was wrong when tried.

1. **§5.2 test 4's `ISOCENTER_FORCE_THREADS=1` cannot reach
   `Session.ingest()`'s worker.** `session.ingest()` passes
   `executor=self._executor` (the session's own spawn
   `ProcessPoolExecutor`) to `DicomImporter.import_files`, and
   `run_parallel` takes `_run_on_shared_executor` whenever an executor
   is given -- `executor.map`, whatever the strategy says. The variable
   decides the strategy only when no executor is passed. The test
   (`test_the_label_is_written_only_when_it_changes`) calls
   `ingest_worker(path)` directly, in-process, and wraps
   `Instance.set_attr` there. `populate_attrs` also writes through
   `set_attr`, so the assertion is a *count* of `0028,0004` writes (one
   for a control, the copy; two for the YBR row, the copy then `RGB`)
   rather than "none for a control"; it kills the same two mutants §5.2
   names. `docs/environment.md` says the variable "does not apply to
   `session.export()`"; it also does not apply to `session.ingest()`,
   for this reason. Not a code change here; noted for a docs follow-up (#390).
2. **§5.2 item 6 / §0.2 C1: the `:674` test's fixture was not "a JPEG
   Baseline icon declared `YBR_FULL_422`".** It was `_icon_item()`'s
   default -- four raw bytes, `MONOCHROME2`, one sample -- under a file
   whose transfer syntax merely *said* JPEG Baseline. With `.4.50`
   allow-listed that icon fails to decode and keeps its loss row, so
   the test would not have flipped. A real icon was built for it
   (`_jpeg_icon_item()`: Pillow JPEG, `subsampling=1`, encapsulated,
   undefined length, declared `YBR_FULL_422`), and `_write_src`'s "no
   encoder plugin exists in this environment" docstring rewritten.
3. **§5.2's J2K recipe needs `no_jp2=True`.** `Image.save(format=
   "JPEG2000", irreversible=True, mct=1)` writes a JP2 box
   (`0000000c 6a502020 ...`), not the raw codestream (`ff4fff51`) a
   DICOM fragment carries; with `no_jp2=True` it is the codestream and
   pydicom's Pillow plugin decodes it. Measured in the project venv
   before the fixture was written.
4. **§5.1 test 1's "seven red" for the predicate mutation is 15.** The
   reverted predicate sends `[]` to `_fallback_multivalue`, whose answer
   is now `('UN', None)`, so the `LO` rows go red too (they were the
   "two passed" before the fix); and the reloaded path is a second
   seven, plus the `()` test. 15 failed, 2 passed (the two
   no-recorded-VR rows, which read `UN` either way).
5. **§3.5 item 2 / §7 item 3, measured:** `get_decoder(ts).as_array(ds)`
   reports `PALETTE COLOR` for a `PALETTE COLOR` source with LUT
   descriptors and data present (pydicom 3.0.2, project venv), and
   `MONOCHROME1` / `MONOCHROME2` / `RGB` for those. The write fires for
   none; `palette` is a control in the revision test.
6. **§9's "full suite 1626 passed" is the base; this PR's counts are in
   its body.** The mutation for §5.4 ("re-add `research_tags.json`") is
   `git checkout 283bf04 -- isocenter/resources/research_tags.json`
   (staged, so `_tracked_paths_in_package` sees it) and `git rm` to
   revert -- an untracked copy is invisible to the test by design.
