# A Zero-Length Private Element Is Written, Not Dropped

**Date:** 2026-09-07
**Status:** Design approved (the output VR is the owner's decision, made
before this spec was written); ready for implementation
**Tracking:** #344. Completes the half #339 named and deliberately left
open. Touches the guard #154/#165/#190/#195 built.
**Base:** `main` at `e484bee`
**Measured with:** `/Users/kevin/Developer/Isocenter/.venv/bin/python`
(CPython 3.14.6), `pydicom 3.0.2`, `numpy` as installed. Every figure
below was taken in the worktree, with `isocenter.__file__` printed and
read first per CLAUDE.md.

---

## OPEN QUESTIONS for the owner

Four. Three are small; the second is the only one that changes what a
user sees.

**Q1. `_fallback_multivalue([]) → ('LO', [])` is the same infidelity you
rejected, one shape over.** An *empty list* under a recorded `DS` is
written as `LO`, today, and the arm is pinned by
`tests/test_private_tag_export.py:386`. `_value_fits_vr([], 'DS')` is
`False` (the `bool(value)` clause), so the recorded VR is never
consulted for it. This spec does **not** fold that in, because it is a
different population: pydicom never yields `[]` for a zero-length
element — it yields `None` (§1.3), so `[]` is reachable only from a hand
`set_attr`. Recommendation: file it, do not fold it. It is one line of
the same argument and folding it would put an untested shape in a change
whose whole value is that its population is measured.

**Q2. A `set_attr(tag, None)` on a recorded-`LO` tag now writes `LO ''`
where it used to be dropped, and no source file ever said so.** The rule
you set is "the tag's own recorded VR", and the recorded VR is a fact
about the *source element*, which was present. But the `None` here came
from a caller, an anonymisation or a remediation — not from the file. It
is still not fabrication (§4), and following the rule uniformly is what
keeps the implementation one branch rather than two. This spec follows
the rule. It goes on the record because it flips
`test_a_none_written_by_hand_onto_a_text_vr_private_tag_is_not_invented`
(`tests/test_private_tag_empty_value_roundtrip.py`), a test whose name
asserts the old direction. If you want the hand-set case to keep
dropping, say so — it needs a provenance flag the graph does not carry,
which is a bigger change than this one.

**Q3. The affected population is larger than #344 and #339's test say.**
Both name eight numeric VRs plus `UN`. Measured (§1.3), it is **eleven**
recorded VRs — `DS`, `IS`, `US`, `SS`, `UL`, `SL`, `UV`, `SV`, `FL`,
`FD`, `AT` — plus the no-VR case. `SL`, `SV` and `UV` were missed by
both. They behave identically; the only consequence is that
`EMPTY_NUMERIC` in the existing test file gains three rows. Confirm you
want them added rather than filed.

**Q4. Under the export's default transfer syntax the decision is
invisible.** `_create_ds` sets Implicit VR Little Endian, and under it a
zero-length private element is byte-identical whatever VR was chosen —
474 bytes for all nineteen VRs measured (§1.4). The recorded-VR-vs-`UN`
choice is observable only on the compressed (explicit-VR) branch. That
does not make the change pointless — the element's *presence* is the
half that matters on both branches — but it does mean the VR half of the
decision buys nothing for the most common export. Recorded here so it is
not discovered later and read as a bug.

---

## Context

`session.export()` drops a zero-length private element and files a
`DATA_LOSS` row for it:

```
WARNING: <uid>: Tag 0009,1005 not exported (data loss): no VR fits a NoneType value
```

The source file said "this tag is present and has no value". The export
says nothing at all, and the compliance report grades the run
`REVIEW_REQUIRED` for it.

#339 made the two export paths *agree* about this — fresh and reloaded
now produce the identical file and the identical audit row. It did not
make either of them right, and its own entry says so: *"Named, and
deliberately not fixed… filed as #344."*

**The decision is already made and this spec does not re-open it.** Write
the zero-length element under the tag's own **recorded VR** where one
exists, and under **`UN`** where no VR was ever recorded. §3 records what
was rejected. §5 works out the mechanism, which is the risky part,
because `_value_fits_vr` is the guard #190 and #195 rest on.

---

## 1. Reproduction and population, measured

### 1.1 The end-to-end red

One instance, Explicit VR Little Endian source, a private block of
zero-length elements, straight through `ingest()` → `export()`
(`scratchpad/probe344c.py`):

```
--- graph attributes for the 0009 block (FRESH) ---
    0009,0010 = 'ACME_HEADER'  recorded VR= 'LO'
    0009,1005 = None           recorded VR= 'DS'
    0009,1006 = None           recorded VR= 'US'
    0009,1007 = None           recorded VR= 'UL'
    0009,1008 = None           recorded VR= 'FL'
    0009,1009 = None           recorded VR= 'AT'
    0009,100a = None           recorded VR= 'IS'
    0009,100b = None           recorded VR= 'SS'
    0009,100c = None           recorded VR= 'FD'
    0009,100d = None           recorded VR= None      <- source VR was UN
    0009,1020 = ''             recorded VR= 'LO'
    0009,1021 = ''             recorded VR= 'SH'
    0009,1022 = ''             recorded VR= 'PN'

--- EXPORTED transfer syntax: 1.2.840.10008.1.2.4.90 (JPEG 2000 Lossless)
--- exported 0009 block ---
    (0009,0010) LO 'ACME_HEADER'
    (0009,1020) LO ''
    (0009,1021) SH ''
    (0009,1022) PN ''

--- DATA_LOSS rows: 9
    Tag 0009,1005 not exported (data loss): no VR fits a NoneType value
    ... (0009,1006 through 0009,100d, one row each)
```

Nine elements in the source, nine rows in the audit log, nine elements
absent from the copy. The three text-VR elements come out correctly and
are **not** part of this change.

### 1.2 The mechanism, confirmed at the function level

```
_value_fits_vr(None, vr)  is False for every VR:
   DS US UL FL AT IS SS FD UN LO SH PN UT OB  ->  all False
_fallback_encoding(None)      -> None
_fallback_encoding([None,'B']) -> None
_value_fits_vr([None,'B'],'LO') -> False
```

`None` matches no `isinstance` arm in `_value_fits_vr`, so it reaches the
closing `return False`; it matches no arm in `_fallback_encoding` either,
so `_merge` raises `ValueError("no VR fits a NoneType value")` and the
element is reported as loss and written nowhere.

### 1.3 The population is eleven recorded VRs, not eight

`scratchpad/probe344e.py` writes a zero-length private element under
every VR pydicom accepts, reads it back, and asks
`io_handlers._record_private_vr` what it would record:

| readback value | VRs | recorded VR? |
| --- | --- | --- |
| `None` | `DS IS US SS UL SL UV SV FL FD AT` | **yes, the source VR** |
| `None` | `UN` | **no** — `_record_private_vr` refuses `UN` by design |
| `None` | `OB OW OD OF OL` | not reached: `BINARY_VRS` converts them to `b""` at `io_handlers.py:863-867` before the recorder runs |
| `''` / `PersonName('')` | `LO SH PN UT ST LT UI DA TM CS AE AS DT UC UR` | yes — and these already export correctly |
| `Sequence, length 0` | `SQ` | not reached: `process_sequence` takes it first |

So the affected set is exactly:

* **eleven VRs where a recorded VR exists** — `DS`, `IS`, `US`, `SS`,
  `UL`, `SL`, `UV`, `SV`, `FL`, `FD`, `AT`; and
* **one case where none does** — a source `UN` element (explicit VR), and
  *every* private element of an Implicit VR Little Endian source, because
  `_record_private_vr` returns early on `UN`
  (`io_handlers.py:982-983`) precisely so that an implicit ingest records
  nothing.

`SL`, `SV` and `UV` are the three neither #344 nor #339's test names.
They are not in `BINARY_VRS`, are not `SQ`, `PN` or `UN`, so they take
`populate_attrs`' final `else` arm — `set_attr(tag, elem.value)` with
`elem.value is None` — and `_record_private_vr` records them, because
the value is `None` rather than `bytes` and so the bytes guard does not
fire. They are also all three in `_INTEGER_VR_RANGE`, so they are
`_value_fits_vr` territory in every other respect.

### 1.4 Under Implicit VR the choice is unobservable

Writing `add_new(tag, vr, None)` and reading the file back
(`scratchpad/probe344b.py`):

| transfer syntax | outcome |
| --- | --- |
| Explicit VR LE | every VR round-trips **under its own VR**; numeric VRs read back `None`, text VRs `''`, `UN` reads back `UN`/`None`. File sizes 476 or 480 bytes. |
| Implicit VR LE | every element reads back as `UN`, value `None`, **file size 474 bytes for all nineteen VRs tested** |

Implicit VR carries no VR on the wire and the reader has no dictionary
entry for a private tag, so it resolves everything to `UN`. The bytes are
identical whatever we chose. `DicomExporter._create_ds` sets
`ImplicitVRLittleEndian`; only the compressed branch replaces it with an
explicit-VR syntax. That is why
`tests/test_private_tag_empty_value_roundtrip.py::_write_src` puts pixel
data in its fixture and says so in a docstring — the fixture is forcing
the branch where the VR is visible at all. That is not incidental and
the new tests must keep doing it.

### 1.5 The reloaded path already carries everything the fresh one does

Measured after `ingest()` → `save(sync=True)` → `close()` → reopen
(`scratchpad/probe344d.py`): the reloaded `attributes` and
`attribute_vrs` are **identical** to the fresh ones, `None` values and
recorded VRs included.

```
RELOADED attribute_vrs: {'0009,0010': 'LO', '0009,1005': 'DS',
  '0009,1006': 'US', '0009,1007': 'UL', '0009,1008': 'FL',
  '0009,1009': 'AT', '0009,100a': 'IS', '0009,100b': 'SS',
  '0009,100c': 'FD', '0009,1020': 'LO', '0009,1021': 'SH',
  '0009,1022': 'PN'}
```

**This change therefore has no persistence half.** #339 already did that
work: it preserved the `None` through `_vertical_atom_text` /
`_vertical_atom_value` rather than skipping it, on the explicit argument
that *"if the export encoder is ever taught to emit a zero-length
element, both paths gain it at once, where a skip destroys the
information and can never be back-filled."* That is this change, and the
prediction holds: nothing in `persistence.py` needs to move. Verify it
rather than assume it — run §7's reloaded parametrisation and read the
VR the file carries, not just the tag's presence.

---

## 2. What ships

Both export paths write a **present, zero-length** element for a
`None`-valued private tag:

* under the tag's **recorded VR** when `attribute_vrs` has one;
* under **`UN`** when it does not.

No `DATA_LOSS` row is emitted for it, because nothing is lost. The
compliance grade for a run whose only losses were this population moves
`REVIEW_REQUIRED` → `PASS` (§6).

Nothing else changes. In particular the multi-valued arm is untouched:
`[None, 'B']` remains a whole-element `DATA_LOSS`, loudly, on both paths
(§5.2 gives the measured reason that is not merely conservatism).

---

## 3. What was rejected, and why

### 3.1 Rejected: fall back to `LO` for every zero-length element

This is the shape `_fallback_multivalue` already uses for the empty-list
case — `if not atoms: return 'LO', []`, whose comment reads *"a legal
element saying the tag was present with no value"*. It is one line, it
needs no change to `_value_fits_vr` at all, and it is wrong for the same
reason #154 exists: **it writes a `DS` tag out as `LO`.**

That is a different infidelity, not a smaller one. #154's whole argument
is that a private element's VR is a fact the source file gave us and the
fallback's `LO` throws it away; a zero-length `DS` written as `LO` is
that same throwing-away, on an element where we happen to have the
answer sitting in `attribute_vrs` at the call site. The owner rejected it
before this spec was written. It is recorded here so the next reader who
notices the one-line version knows it was seen and declined.

### 3.2 Rejected: skip the tag at the store, so it never reaches the export

This was #339's own proposal and #339 rejected it, for a reason that
still holds and is worth restating because it is what makes §1.5 true: a
skip makes the *file* agree between the two paths and leaves the *report*
divergent — reloaded would drop the element in silence where fresh drops
it loudly. It also destroys the information permanently, so this change
would have had nothing to work with.

### 3.3 Rejected: widen `_value_fits_vr` to admit `None` globally

This is the obvious mechanism and it is the one dangerous option on the
table. §5.2 measures what it does. Short version: it makes
`_value_fits_vr([None, 'B'], 'LO')` return `True`, `_merge` then calls
`add_new(tag, 'LO', [None, 'B'])` — which **succeeds** — and the failure
lands in `filewriter`, *past* `_merge`'s `try`, taking the whole file
with it. That is precisely the second failure mode `_value_fits_vr`'s own
docstring was written to prevent.

---

## 4. What of #60 survives, and what this supersedes

#60's ruling is usually quoted as **"absent beats fabricated"**. That is a
compression of it, and the compression is what makes this issue look like
an overturn.

What #60 actually decided, in its CHANGELOG entry (`CHANGELOG.md:2014`)
and its restatements at `:717` and `:1574`:

> *"Absent now stays absent… An unreadable date is logged rather than
> guessed at."*
> *"a date we cannot read is a date we do not have, not one we invent and
> not one we discard"*

**That survives in full, and this change is an instance of it, not an
exception to it.** #60 forbids inventing a value where the source
supplied none. A zero-length element invents nothing: it asserts exactly
what the source asserted — *this tag is present, and it has no value* —
and it does so in the one encoding DICOM provides for saying that. The
sentinel `19000101` claimed a fact the file never stated. A zero-length
`DS` claims no fact at all.

Note that #60's own second clause has been pointing this way the whole
time: *"not one we invent **and not one we discard**"*. Dropping the
element is the discard half. #60 forbade both, and this repo has been
doing the second one to this population since #118.

**What is superseded is one derived sentence in #339's CHANGELOG entry**
(`CHANGELOG.md:383`):

> *"`_split_core_and_private` is left exactly as it was. #60's ruling is
> honoured: the file still omits the tag, on both paths."*

The first sentence stays true. The second becomes false when this lands:
the file will carry the tag on both paths. The reading it rests on —
that "absent" was the honest answer for a zero-length element — was
wrong, because absence is itself a claim about the source, and a
different one from what the source made.

**Per CLAUDE.md's dated-record rule, nothing needs a `Superseded in part:`
marker for this, and the implementer must not add one.** That rule
applies to dated specs in `docs/superpowers/specs/`. #60 predates the
specs directory (v0.7.0); grepping `docs/superpowers/specs/` for it
returns nothing. Its ruling lives in `CHANGELOG.md`, which is a
historical record that is never edited in place — the new entry names
what it supersedes, and that is the whole mechanism. Two other places
quote the ruling:

* **#339's CHANGELOG entry** (`:383`, quoted above) — historical, leave
  it exactly as it is.
* **`tests/test_private_tag_empty_value_roundtrip.py`'s module
  docstring**, whose closing paragraph reads *"The file itself still
  omits the tag on both paths, which is #60's ruling: absent beats
  fabricated."* A test file is not a dated record; it must describe the
  code as it is. §7.4 specifies the rewrite.

---

## 5. Mechanism: how `None` reaches the recorded VR

### 5.1 The change, in `_merge` and nowhere else

`io_handlers.py:3592-3596`, the private arm of `_merge`, currently reads:

```python
recorded = (vrs or {}).get(t)
if recorded is not None and _value_fits_vr(v, recorded):
    vr = recorded
else:
    encoded = DicomExporter._fallback_encoding(v)
```

It becomes:

```python
recorded = (vrs or {}).get(t)
if v is None:
    # A zero-length element: the source asserted the tag's presence and
    # gave it no value, and DICOM has an encoding for exactly that. The
    # recorded VR is the source's own answer; `UN` is what an element
    # whose VR was never known is (PS3.5 6.2.2), which is every private
    # element of an Implicit VR source, because `_record_private_vr`
    # refuses to record `UN` (#344).
    #
    # Handled HERE and not by widening `_value_fits_vr`, deliberately.
    # That function recurses over a list, so admitting `None` would make
    # `[None, 'B']` "fit" LO -- `add_new` accepts it and `filewriter`
    # then raises past `_merge`'s try, failing the whole file rather
    # than the element. See the spec dated 2026-09-07 and
    # `test_a_none_among_siblings_is_the_same_loud_loss_on_both_paths`.
    vr = recorded if recorded is not None else 'UN'
elif recorded is not None and _value_fits_vr(v, recorded):
    vr = recorded
else:
    encoded = DicomExporter._fallback_encoding(v)
```

`_value_fits_vr` and `_fallback_encoding` are **not touched**. Neither is
`_fallback_multivalue`. That is the whole production diff for this
change, and it is deliberate that it is that small: every arm of
`_value_fits_vr` exists because some value shape was got wrong once, and
this change has no business reopening any of them.

An explicit `if value is None: return False` may be added at the top of
`_value_fits_vr` **as documentation only**, since that is already its
behaviour by fall-through. Prefer it: it is what a future reader will
reach for when they try to "clean up" the `None` branch in `_merge`, and
a comment there is where the trap gets explained (CLAUDE.md: *comments
explain the trap, not the code*). It changes no behaviour and no test.

### 5.2 Why widening `_value_fits_vr` is unsafe — measured

`_value_fits_vr` has a recursive list arm
(`io_handlers.py:453-460`):

```python
if isinstance(value, (list, MultiValue)):
    if vr in _VM_ONE_TEXT_VRS:
        return False
    return bool(value) and all(_value_fits_vr(a, vr) for a in value)
```

A global `if value is None: return True` therefore makes
`_value_fits_vr([None, 'B'], 'LO')` return `True`. Measured what happens
next (`scratchpad/probe344d.py`):

```
add_new(Tag(0x0009,0x1030), 'LO', [None, 'B'])   -> ok, value is [None, 'B']
ds.save_as(..., enforce_file_format=True)        -> TypeError:
    With tag (0009,1030) got exception:
    sequence item 0: expected a bytes-like object, NoneType found
  pydicom/filewriter.py:486 in write_text:  val = b"\\".join([val for val in val])
```

Read the traceback against `_value_fits_vr`'s own docstring, which names
two failure classes and says of the second:

> *"A value both of those accept and `filewriter` then refuses… raises
> **past** `_merge`'s `try`, from `write_numbers`, and fails the whole
> file rather than the element. That is precisely the trap
> `_fallback_encoding`'s own docstring names, and a recorded VR must not
> reopen it."*

This is that trap, exactly, arriving through `write_text` instead of
`write_numbers`. The cost of getting the mechanism wrong is not "one
element exports oddly"; it is **one instance fails to write** — and, on
the `write_tree` path where a whole tree is a single call, potentially
more than one.

The guards #190 and #195 rest on are safe under the narrow route for a
structural reason, not a hopeful one: **both are about the content of a
value**, and a `None` has no content. #195's guard is
`if '\\' in value and vr not in _VM_ONE_TEXT_VRS` — inside the `str` arm,
unreachable for `None`. #190's arity guards are `bool(value)` and the
list recursion — also unreachable, because the narrow route never asks
`_value_fits_vr` about a `None` at all, at any depth. A scalar `None`
never reaches the recursion (it is intercepted one level up in `_merge`);
a `None` *inside* a list never reaches the new branch (the branch tests
`v is None`, and `[None, 'B'] is not None`). The two populations do not
overlap and cannot be made to.

### 5.3 Why `UN` and not `LO` for the no-VR case

`UN` is what PS3.5 §6.2.2 says an element of unknown VR is, and it is
what `_fallback_encoding` already writes for raw bytes. Writing a
zero-length `LO` there would assert the element is text, which is a claim
we do not have. Measured: `add_new(tag, 'UN', None)` writes and
round-trips under both transfer syntaxes (§1.4), so there is no
implementation obstacle.

`UN` is an OB-family VR, which is why it takes a `None` and not a `''`.
That asymmetry is invisible in the file — a zero-length element has no
value bytes under any VR — and matters only to a reader of the graph.

---

## 6. The grade change, stated as a change

This is a grade change, in the opposite direction from #339's, and it
gets the same treatment #339's got: it goes in the entry's headline, not
in a footnote.

**#339's direction.** Before it, the reloaded export wrote `LO 'None'`
with no `DATA_LOSS` row and graded **PASS**. After it, both paths dropped
the element with a row and graded **REVIEW_REQUIRED**. #339 *added* a
grade demotion, deliberately, because a PASS over a fabricated value is
the report lying.

**#344's direction.** This population moves **`REVIEW_REQUIRED` →
`PASS`**, and the `DATA_LOSS` rows disappear entirely.

The grade is computed at `session.py:2194-2199`:

```python
validation_status=("PASS"
                   if audit_summary and not exceptions
                   and not graded_losses and not open_gaps
                   and not declined_remediations
                   and not unattested
                   else "REVIEW_REQUIRED")
```

`graded_losses` is the `DATA_LOSS` rows whose `loss_scope` is in
`GRADED_LOSS_SCOPES`. `loss_scope_for_tag` (`io_handlers.py:586-607`)
classifies by group parity, so a private tag is always
`LOSS_SCOPE_PRIVATE` — which *is* graded. Removing the rows removes the
demotion, with no other change to the grading code.

**Say plainly why a PASS is now the true answer, because a grade going up
is the one that needs the argument.** Before: nine elements in the
source, zero in the copy, nine rows saying so — the row was *correct*,
and `REVIEW_REQUIRED` was the honest grade for a copy that was missing
nine elements. After: nine elements in the source, nine in the copy,
byte-faithful, nothing to report. The `PASS` is earned by the export
getting better, not by the report getting quieter. A reviewer who
compares two runs across this change will see rows vanish, and the
CHANGELOG entry must let them tell that from a suppression: **the
population that stops filing rows is exactly the population that stops
losing elements, and the test in §7.2 asserts both halves in one place.**

**Note also**: a run whose *only* losses were this population goes to
PASS; a run with any other loss keeps its `REVIEW_REQUIRED`, because the
grade is a conjunction over all graded rows. There is no
sweep-everything-to-PASS risk here.

---

## 7. Tests

### 7.1 Baseline, before anything changes

Verified green in this worktree at `e484bee`, so "goes red" below means
red *because of the change*:

```
run.py -v tests/test_private_tag_empty_value_roundtrip.py \
          tests/test_private_tag_export.py \
          tests/test_private_tag_vr_roundtrip.py \
          tests/test_compact_rewiring_is_locked.py tests/test_compaction.py
=> 89 passed in 23.83s
```

(`run.py` is a five-line wrapper that sets `PYTHONDONTWRITEBYTECODE=1`,
puts the worktree on `sys.path`, prints `isocenter.__file__` and calls
`pytest.main`. **It needs an `if __name__ == "__main__":` guard**: without
one, `run_parallel()`'s spawned children re-import it, re-enter
`pytest.main`, and every ingest dies with `BrokenProcessPool` — which
looks exactly like a failing suite. That cost one wrong baseline reading
during this spec's preparation.)

### 7.2 Existing tests that go red first, and what each must become

All in `tests/test_private_tag_empty_value_roundtrip.py`. Write the
assertion changes **before** touching `io_handlers.py`, confirm each is
red, then implement.

| Test | Today | Must become |
| --- | --- | --- |
| `test_a_reloaded_export_does_not_invent_a_value_for_an_empty_element[…]` — parametrised over `EMPTY_NUMERIC` | asserts `Tag(0x0009, el) not in exported` | asserts the tag **is** in the export, its value is zero-length, and **its VR is the recorded one** — `DS`, `US`, `UL`, `FL`, `AT`, `IS`, `SS`, `FD` — and `UN` for the `0x100d` row. Rename it; the current name asserts the old direction. |
| `test_the_fresh_and_reloaded_exports_report_the_same_loss` | asserts both paths report the same **nine** losses | asserts both paths report **zero** losses for the block, and still asserts they are equal — the two-path agreement #339 bought must not be spent |
| `test_a_none_written_by_hand_onto_a_text_vr_private_tag_is_not_invented` | asserts `Tag(0x0009,0x1020) not in exported` after `set_attr("0009,1020", None)` | asserts it is present as `LO ''`. **This is Q2** — a direction change on a test whose name states the old direction. Rename and rewrite the docstring to say the value came from a caller, not a file, and that the rule is applied uniformly anyway. |

Per-VR coverage is what the parametrisation buys, so keep it and extend
it. The `UN` row (`0x100d`, no recorded VR) must assert `VR == 'UN'`
explicitly and not be folded into the recorded-VR rows — it is the arm
with a different mechanism and it is the arm every Implicit VR source
takes.

**Add three rows to `EMPTY_NUMERIC`** for `SL`, `SV` and `UV` (Q3), each
with a fresh element number in the `0009` block. They are red today for
the same reason as the other eight and green after, and adding them is
what makes the parametrised test's list match the measured population
rather than the issue's guess.

### 7.3 Tests that must stay green — the regression half

These are the reason the mechanism in §5.1 is narrow. Any of them going
red means the implementer widened `_value_fits_vr`.

| Test | What it protects |
| --- | --- |
| `test_a_text_vr_private_element_was_never_affected[LO/SH/PN]` | the already-correct text population. It asserts `str(value) == ''` on both paths. Untouched by this change and must stay untouched — if it moves, the change reached a population it has no business in. |
| `test_a_none_among_siblings_is_the_same_loud_loss_on_both_paths` | `[None, 'B']` is one loud `DATA_LOSS` on both paths. **This is the test that catches a global `_value_fits_vr` widen** (§5.2). Its docstring already explains the rule; add a line naming the widen as the specific mistake it now also guards. |
| `test_a_none_atom_inside_a_list_keeps_its_place` | the store keeps `[None, 'B']` verbatim. No export involvement; green throughout. |
| `test_a_zero_length_private_element_is_not_reloaded_as_the_word_none`, `test_the_at_arm_reaches_the_same_answer_as_every_other_vr` | #339's store-level fixes. Green throughout — this change has no persistence half (§1.5). |
| Whole of `tests/test_private_tag_export.py` (incl. `:386`, the `('LO', [])` empty-list arm) and `tests/test_private_tag_vr_roundtrip.py` | the #154/#165/#190/#195 guards. All 89 baseline tests must still pass. |
| `tests/test_api_coherence.py` | the two export paths still produce identical trees (§8) |

### 7.4 The module docstring

`tests/test_private_tag_empty_value_roundtrip.py`'s closing paragraph —

> *"The file itself still omits the tag on both paths, which is #60's
> ruling: absent beats fabricated."*

— becomes false and must be rewritten in place (a test file is not a
dated record). Replace it with the §4 argument in two sentences: the file
now carries the tag as a zero-length element under its recorded VR, which
is not fabrication because the source asserted the tag's presence, and
#60's rule against inventing a value it never gave is untouched. Name
#344.

### 7.5 One new test worth having

Nothing today asserts that the **fresh** path writes the element — the
existing parametrisation is on `reloaded_export` only, and the
fresh/reloaded comparison goes through the loss counts. Add a
parametrised twin on `fresh_export` asserting presence, zero length and
VR. It is cheap, it uses an existing module-scoped fixture, and without
it a regression that reintroduced the drop on the fresh path only would
be caught by loss-count equality alone — which is a weaker signal than a
test naming the element.

---

## 8. Both export paths, by construction

`session.export()` and `DicomExporter.write_tree()` both reach
`_export_instance_worker` (`io_handlers.py:1994`), which calls
`DicomExporter._merge(ds, inst.attributes, losses, vrs=getattr(inst,
'attribute_vrs', None))` at `io_handlers.py:2016-2017`. There is exactly
one instance-attribute merge in the module (plus
`_merge_sequences`' item merge at `:3854`, which passes `vrs` the same
way and gains the behaviour for nested private elements at the same
time). **The change lands on both paths in one edit and cannot land on
one.**

Two consequences worth writing down rather than discovering:

* `tests/test_api_coherence.py` will **not** go red for this and is not
  the test that proves the coverage. It compares the two paths' trees,
  so it is the guard against a future fork, not evidence about this
  change. Do not cite it as the proof.
* The **no `DATA_LOSS` row** half is identical on both paths for a
  different reason: `write_tree` never has a store handle, so it writes
  no audit row either way (`io_handlers.py:22`). It logs a warning where
  `session.export()` files a row. Both stop happening, for the same
  cause.

---

## 9. Implementation order

1. Rewrite the three assertions in §7.2 and add the `SL`/`SV`/`UV` rows.
   Run. Expect red on all three tests plus the new parametrisations.
2. Make the `_merge` change in §5.1 (and the documentation-only
   `None` guard in `_value_fits_vr`, if taken). Run the same file.
   Expect green.
3. Rewrite the module docstring (§7.4) and add the fresh-path twin
   (§7.5).
4. Run the four private-tag files plus `test_api_coherence.py`.
5. Full suite on 3.12 and 3.14t.

There is no `mutation_probe.py` `TARGETS` edit and no CLAUDE.md coverage
table edit: `io_handlers.py` is already a probe target and
`test_private_tag_empty_value_roundtrip.py` is not a new file. Check that
rather than assume it — #317 made the same check and recorded it.

## 10. CHANGELOG entry — what it must carry

Under `### Fixed`. It is not `### Breaking`: no previously-working call
raises anything new, and the observable change is an element appearing
where one was lost. Depth to match #339's entry, which this one
completes:

* the exact warning text that stops appearing
  (`no VR fits a NoneType value`), and the count on the reference
  fixture (**9 rows → 0**);
* the **grade change and its direction**, in the headline, with the note
  from §6 that the population which stops filing rows is exactly the
  population that stops losing elements;
* the measured population (eleven recorded VRs + the no-VR case), and
  that `SL`, `SV`, `UV` were missing from #339's list and #344's text;
* why the fallback-`LO` answer was rejected (§3.1) — one sentence, "it
  writes a `DS` tag out as `LO`, a different infidelity";
* why `_value_fits_vr` was **not** widened, with the measured
  `TypeError: sequence item 0: expected a bytes-like object, NoneType
  found` and the phrase "fails the whole file rather than the element";
* the sentence of #339's entry that this supersedes, quoted, with the
  §4 argument for why #60's ruling itself is untouched;
* that Implicit VR makes the VR half of the decision invisible (§1.4,
  Q4), so nobody re-derives it as a bug.
