# One Grammar for `instance_blobs.kind`, and Nested Pixel Data Carried By It

**Date:** 2026-09-07
**Status:** Design proposed. **Nothing here is approved.** §0 carries
nine OPEN QUESTIONS; Q1 is the one that decides whether this ships at
all, because the brief this spec was written from asks for work that is
**already on `main`**. Every recommendation below is marked as a
recommendation.
**Tracking:** #183 (the second half only — see §1). The `kind` grammar
is decided here for #277 and #150 option 3a as well, which is what #183
asks for. Rests on #169, #170, #193, #194, #327. Touches the ground
#125, #137, #160, #150 and #168 cover.
**Base:** `main` at `007705d`
**Measured with:**
`/Users/kevin/Developer/Isocenter/.venv/bin/python` (CPython 3.14.6),
`pydicom 3.0.2`, `numpy` as installed, macOS 25.6 / APFS / local SSD.
Every figure was taken inside the worktree with `isocenter.__file__`
printed and read first, per CLAUDE.md — it resolved to
`…/agent-a9886f0cf595f7663/isocenter/__init__.py` on every run.
**On the `scratchpad/*.py` citations below:** those probes lived in a
session-local temp directory and **are not in the repo**. They are named
so each number can be attributed, not so a later reader can re-run them.
Every figure is quoted inline for that reason; the reproduction meant to
survive is §12's test list.

---

## 0. OPEN QUESTIONS for the owner

Nine. Q1 and Q5 change what ships; Q2 changes what a user sees; Q9 is
the one added after review; the rest are scope lines.

**Q1 — the brief's premise is stale, and half the approved work is
already merged.** The instruction this spec was written from says "the
owner has approved implementing #183 **in full** (both halves)" and
describes `SidecarPixelLoader` as reconstructing dtype "from
BitsAllocated/PixelRepresentation alone". That was true when #183 was
filed and is **not true on `main`**. The float half shipped in #327
(v0.9.2) and your own comment on #183 records the split. Verified at the
code:

- `isocenter/pixel_geometry.py:75` — `PIXEL_DTYPE_ATTR =
  "_ISOCENTER_PIXEL_DTYPE"`, with a 25-line comment arguing why the
  carrier is a dtype and not an element.
- `isocenter/pixel_geometry.py:81` — `FLOAT_DTYPE_NAMES = frozenset({"float16",
  "float32", "float64"})`; `:85` — `FLOAT_DTYPE_BY_ELEMENT`.
- `isocenter/io_handlers.py:1116-1150` — `ingest_worker`'s
  `elif any(kw in ds for kw in ("FloatPixelData", "DoubleFloatPixelData"))`
  arm, which writes the array to the sidecar and stamps
  `inst.attributes[PIXEL_DTYPE_ATTR]`.
- `isocenter/io_handlers.py:2760` and `:2791` —
  `SidecarPixelLoader.pixel_dtype`, and the `if self.pixel_dtype in
  FLOAT_DTYPE_NAMES: dt = np.dtype(self.pixel_dtype)` branch that runs
  **before** the BitsAllocated/PixelRepresentation derivation.
- `isocenter/entities.py:1146-1150` — `set_pixel_data()` writing *and
  deleting* the carrier from `array.dtype`.

So this spec covers the **nested half only**. Confirm that reading
before a developer starts, because the alternative reading —
"re-implement the float half as a blob field" — would replace a shipped,
tested mechanism with a second answer to the same question, which is the
one thing CLAUDE.md's conventions forbid outright.

**Q2 — carrying icon bytes is a de-identification regression unless it
is gated, and nothing in #183 mentions it.** An Icon Image Sequence item
is a downsampled copy of the main frame. Verified: **every** pixel
consumer in the tree reads `instance.get_pixel_data()`, which is the
top-level frame and only that — `pixel_analysis.py:182` (the burned-in
identifier scan), `services.py:439` and `:772` (both redaction paths),
`io_handlers.py:2073` (the export). Nothing walks sequences for pixels.
So today an icon carrying a burned-in accession number is dropped, and
the drop is what protects the export. Carrying it re-exports a thumbnail
of exactly what redaction zeroed, with no scan and no zones applied.
**Recommendation (§9): when the export applies any redaction zone to an
instance, drop the Icon Image Sequence *item* rather than write its
bytes back, and file a `DATA_LOSS` row saying so.** Dropping the item is
conformant; writing descriptors with no Pixel Data is what #183 exists
to stop. Accept, reject, or ask for the icon to be scanned and redacted
too (which is materially larger — the zones are in the main image's
coordinate space and would have to be rescaled per icon).

**Q3 — the grammar drops the issue's `seq:` marker.** #183 sketches
`pixels:seq:0088,0200/0/7fe0,0010`. §3 spells it
`pixels:0088,0200/0/7fe0,0010` — the `:` already separates root from
path and the path's token shape is unambiguous without a literal that
every writer must remember and every parser must check. Recommendation:
drop it. If you want the sketch honoured verbatim instead, say so before
implementation, not after — the spelling is written into a `UNIQUE`
index and changing it later is a migration.

**Q4 — does `waveform` stay group 0's spelling forever?** §4 shows #277
spells multiplex group *N* as `waveform:5400,0100/N/5400,1010`. That
leaves group 0 with two spellings: the legacy bare `waveform` and the
path form. Recommendation: **keep the bare literal as group 0's only
spelling.** It costs nothing here, keeps `get_blob_refs('waveform')`
(`session.py:1070`) and the two `WHERE kind = 'waveform'` reads
(`persistence.py:1501`, `:1637`) correct with no change, and means a
store ingested before #277 needs no migration. The alternative — write
`waveform:5400,0100/0/5400,1010` for every group and keep a read-time
fallback — is #277's call to make, and the grammar admits either. This
question is recorded here so #277 inherits an answer rather than
re-deriving one.

**Q5 — does the nested *float* case get carried, or keep its loss
row?** The grammar spells `pixels:0040,0555/0/7fe0,0008` perfectly well,
and `tests/test_private_binary_ingest.py:638`
(`test_float_pixel_data_inside_a_sequence_item_is_reported`) currently
pins the loss row for it. Recommendation: **keep the loss row; do not
carry it.** That test's own docstring says the shape is "unreachable
from a conformant file" — the float pixel elements are top-level Image
Pixel Module members with no macro that nests them — so carrying it
would be untested-in-anger machinery whose only exercise is a synthetic
fixture. The grammar proves it *could* be carried, which is what #183
asks for; carrying it is not the same request.

**Q6 — encapsulated icons: carry, or report?** Measured (§5.3): with an
RLE-encapsulated source, the nested icon's element value is 90 bytes of
encapsulated fragments, and pydicom decodes it correctly **only** when
the enclosing dataset's `file_meta` is borrowed onto the sequence item.
The export writes Implicit VR Little Endian with raw bytes (measured:
`1.2.840.10008.1.2`, 16-byte top-level payload from a 104-byte
encapsulated source), so verbatim carriage of fragments would be
nonconformant. §5.3 recommends decoding at ingest, which handles both
native and encapsulated sources with one code path. The residual is
lossy-JPEG icons, whose decoded Photometric Interpretation changes
(YBR → RGB) — recommendation: refuse those at ingest and let them keep
today's `DATA_LOSS` row, because rewriting an icon's Photometric
Interpretation is a correctness claim this spec has no measurement for.

Two things Q6 needs before it is answerable, both established after the
first draft:

- *How is the case detected?* An icon shares the file's transfer
  syntax, so the trigger is `ds.file_meta.TransferSyntaxUID` being in
  the lossy-JPEG family (`JPEGBaseline8Bit`, `JPEGExtended12Bit`,
  `JPEG2000`, `JPEGLSNearLossless`, and `pydicom.uid.JPEGLossyCompressedPixelTransferSyntaxes`
  is the maintained list). Without a stated trigger the refusal is a
  recommendation with no implementation.
- *Does the top level already have this problem?* **Yes, apparently, and
  it is unmeasured.** `ingest_worker` never mentions `0028,0004`,
  `PhotometricInterpretation` or `resolve_photometric_interpretation`
  (grep over `inspect.getsource`, `scratchpad/probe_review.log`) — the
  only `0028,xxxx` it touches is `0028,0006` PlanarConfiguration. So
  whatever pydicom's decoder returns for a lossy YBR source, ingest
  stores it and the declared PI rides through unchanged, at the **top
  level** as much as inside an icon. I could not measure the decoded
  result: this venv's pydicom has no JPEG *encoder*
  (`ImportError: cannot import name 'JPEGBaseline8BitEncoder'`), so I
  could not build the fixture. **Marked unmeasured deliberately rather
  than asserted.** If the top level does silently mis-declare PI, then
  by "one spelling per behaviour" the nested path must do the same thing
  the top level does — and the right fix is a separate issue about the
  top level, not a nested-only refusal that makes the two paths differ.

**Q7 — one PR or two?** §11 recommends **one**. The two halves of the
nested work (store the bytes; write them back) have no independently
observable value: carrying bytes nothing writes back changes nothing a
user sees, and a writeback with nothing to write is dead code. The
float/nested split the brief asks about was already answered by history
— #327 shipped float alone precisely because it *did* have independent
value.

**Q8 — should `record_blob_ref` gate its `kind` too?** Today
`persist_blob` validates (`persistence.py:2212`) and `record_blob_ref`
does not — measured in §2.1: an arbitrary string goes into the table
through the second door. This spec has to widen the first gate anyway;
recommendation is to put the same validation on the second, so there is
one answer to "is this a legal kind". Measured cost: one `re.fullmatch`
per blob row, against 0.015 ms per row for the row write itself (§7.2).
Mark it in or out.

**Q9 — how hard should the shifted-index guard be?** (Added after
review; see §8.2.) The blob key is a *position*, recorded at ingest and
resolved at export, and a sibling removed in between makes a stale path
resolve to the **wrong item** rather than to nothing. Verified that no
site in the tree can do this today — the only two item-level deletions
(`persistence.py:974`, `io_handlers.py:1226`) are tail truncations
keeping item 0, and remediation removes whole sequences rather than
single items — so this is latent, not live. §8.2 recommends the cheap
form: compare the resolved item's declared geometry against the blob's
length before writing, and file a `DATA_LOSS` row on mismatch. The
stronger form stores the icon's Rows/Columns on the blob row and
compares those, catching equal-length mismatches too, at the cost of two
columns on `instance_blobs` and a schema migration. Cheap form
recommended; the owner should say if the stronger one is wanted before
the table is widened, because widening it later is the expensive
direction.

---

## Context

`instance_blobs` holds the sidecar's index: one row per binary payload,
keyed `UNIQUE(instance_uid, kind)`, where `kind` has always been one of
two literals. #183 asks for a third and fourth shape — pixel data nested
inside a sequence item, and (at the time it was filed) float pixel data
— and is emphatic that the spelling must be settled **once**, because
#277 and #150 option 3a both want to extend the same column and three
independent spellings of a composite `kind` is worse than the single
string it replaces.

That is the whole reason this spec exists before a developer does. The
carriage mechanics are ordinary; the string is a schema decision written
into a `UNIQUE` index, and changing it after rows exist is a migration.

Two things about the ground it sits on, both verified rather than
inherited:

- #169/#170/#193/#194 closed the *silence*. A nested (7fe0,0010) is
  reported today: measured end to end (§1.2), one `DATA_LOSS` row,
  scope `STANDARD`, and the compliance report names it. What is left is
  the bytes.
- The float half of #183 shipped in #327 and is **not** in scope. See
  Q1.

---

## 1. The brief's premise, checked against the code

### 1.1 What #183 got right, and how it was confirmed

| #183's claim | Verdict | How confirmed |
| --- | --- | --- |
| `instance_blobs.kind` is unconstrained `TEXT NOT NULL` | **TRUE** | DDL read back from a live store: `kind TEXT NOT NULL`, `UNIQUE(instance_uid, kind)`, plus `idx_blobs_uid_kind ON instance_blobs(instance_uid, kind)`. `persistence.py:561-571`, `:616`. |
| The sole gate is one literal tuple in `persist_blob` | **TRUE for `persist_blob`; the *sole* claim is FALSE** | `persist_blob(kind='pixels:seq:0088,0200/0/7fe0,0010')` → `ValueError: Unknown blob kind: …`. `record_blob_ref` with the same string → `OK`, row stored. `persistence.py:2212` vs `:2229`. |
| A composite kind is accepted with no DDL change | **TRUE** | Four rows coexisted in one store: `('pixels',0,12)`, `('waveform',12,12)`, `('pixels:seq:0088,0200/0/7fe0,0010',200,50)`, `('pixels:seq:0088,0200/0/0088,0200/1/7fe0,0010',300,50)`. |
| `compaction`'s `_read_blob_index` is kind-agnostic | **TRUE** | Its `SELECT` has no `kind` predicate (`persistence.py:3492-3506`). All four rows above appeared in `live_rows`. |
| The #125 compaction-corruption objection therefore does not apply | **TRUE, but for a narrower reason than stated** | See §1.3 — compaction consults `kind` twice, and both are *narrowing* guards that do the safe thing for an unknown kind. |
| The nested icon is dropped, descriptors survive | **TRUE** | §1.2. |
| "The export re-merge is the real cost, not the storage" | **FALSE** — inverted | §8. The re-merge prototype is 13 lines; the store side is seven sites. |

Probe: `scratchpad/probe_kind.py`.

### 1.2 What the nested icon does today, end to end

Fixture: an IOD-complete CT Image Storage instance, 4×4 uint8 top-level
Pixel Data, one `IconImageSequence` item at 2×2 with 4 bytes of its own
(7fe0,0010). Full pipeline `ingest → save → audit → anonymize →
export(dicom, use_compression=False) → generate_report`
(`scratchpad/probe_icon.py`):

```
  files: 1
  top-level PixelData present: True
  icon item keys: ['(0028,0002)', '(0028,0004)', '(0028,0006)',
                   '(0028,0010)', '(0028,0011)', '(0028,0100)',
                   '(0028,0101)', '(0028,0102)', '(0028,0103)']
  icon PixelData present: False

=== DATA_LOSS rows ===
  DATA_LOSS [STANDARD] Standard tag 7fe0,0010 (OW) was not ingested;
                       unrouted pixel elements are not held in the
                       object graph, so it is not in the exported file.

=== report grade ===
   | **Validation Status** | **PASS** |
```

Nine `0028,xxxx` descriptors, not the eight #183 names — the count is a
property of the fixture, not of the code; this one declares
PlanarConfiguration too. The shape is exactly as filed: full geometry,
no bytes, Pixel Data being Type 1 in the Icon Image Macro
(PS3.3 C.7.6.1.1.6). The `PASS` is correct under the parity rule and is
#150's question, not this one.

**`IODValidator` is not the check and will not become one.** It knows
`Common` and `CTImage` for exactly one SOP Class
(`isocenter/validation.py:13-27`), so the Type 1 icon violation passes
validation today and will pass after this change. Do not add the Icon
Image Macro to it as a ride-along; that is a separate decision about
what the validator is for.

### 1.3 Where compaction *does* read `kind`, and why it is already safe

`_read_blob_index` is kind-agnostic, as #183 says. Compaction as a whole
is not — it consults `kind` twice, and #183's "compaction does not
enumerate kinds" is too strong. Both sites are **narrowing** guards that
already do the right thing for an unknown kind, which is why the
conclusion survives:

- `persistence.py:3564` — `if row['kind'] == 'pixels':` before publishing
  into `uid_map`. `uid_map` is keyed by UID alone and is what
  `session.compact()` hands to `_rewire_sidecar_loaders`, so a nested
  blob entering it would point the top-level `_pixel_loader` at an icon.
  The `==` (not a prefix test) is what keeps that correct.
- `persistence.py:3617` — `WHERE id = ? AND kind = 'pixels'` when
  back-patching the legacy `instances.pixel_offset` mirror. A composite
  kind must not touch that mirror, and does not.

Measured (`scratchpad/probe_kind.py`), with four kinds in one store:

```
  compact_sidecar uid_map: {'1.2.3.4': (0, 12)}          <- pixels only
  post-compaction rows: [('pixels',0,12), ('waveform',12,12),
                         ('pixels:seq:…/7fe0,0010', 24, 0), …]
  instances.pixel_offset/length: (0, 12)                 <- untouched
```

So a new kind gets its offsets rewired and does **not** get an in-memory
loader repointed. That is a missing feature (§7.4), not corruption.

**Noticed, not introduced, not fixed here:** the `(24, 0)` above is my
probe's artefact — those rows carried offsets past EOF — but the
behaviour it exposes is real. `_rewrite_live_frames`
(`persistence.py:3547-3552`) logs `Compaction Warning: Unexpected EOF for
instance ID %s`, then writes `length = len(data)`, so a row whose offset
has gone bad is silently rewritten to length 0 and the compaction reports
success. The frame hash catches it at the next read. Pre-existing on
`main`; worth a line in whatever issue tracks compaction next.

### 1.4 `_read_blob_index`'s siblings: every site that reads `kind`

Ten, and the developer needs all ten. Grep: `grep -rn "kind" isocenter/`.

| Site | Reads | Effect of a path-form kind |
| --- | --- | --- |
| `persistence.py:2212` | `persist_blob`'s literal tuple | **Must widen** (§6.1) |
| `persistence.py:2229` | `record_blob_ref` — no gate at all | Q8 |
| `persistence.py:879` | legacy back-fill inserts `'pixels'` | Untouched — pre-blob-table stores have no nested rows |
| `persistence.py:2427` | `persist_pixel_data` records `'pixels'` | Untouched |
| `persistence.py:2861` | `save_all`'s `blob_rows` appends `'pixels'` | **Extended** (§7.2) |
| `persistence.py:1501`, `:1637` | `WHERE kind = 'waveform'` hydration pre-fetch, `load_all` / `load_patient` | Untouched; a sibling is added beside each (§7.3) |
| `persistence.py:3564` | `== 'pixels'` for `uid_map` | Untouched — §1.3 |
| `persistence.py:3617` | `AND kind = 'pixels'` legacy mirror | Untouched — §1.3 |
| `io_handlers.py:1654` | ingest records `'waveform'` | Untouched |
| `session.py:1070` | `get_blob_refs('waveform')` | Untouched; a sibling is added (§7.4) |

The two accessors are exact-literal by construction —
`get_blob_ref(uid, kind)` and `get_blob_refs(kind)` both bind `kind` as
a parameter to `=`. Measured: `get_blob_refs('pixels')` returned only
the bare row, `get_blob_refs('pixels:seq:0088,0200/0/7fe0,0010')`
returned only that one. Neither needs changing; the nested reads use a
prefix query (§7.3).

### 1.5 The float half's second-order point is moot by construction

#183 says that once provenance exists, "the export writeback should
prefer it over `arr.dtype`, since `dtype.kind == 'f'` cannot distinguish
'the source was Float Pixel Data' from 'someone handed `set_pixel_data`
a float array'."

**Verified: the two cannot disagree.** `set_pixel_data()`
(`entities.py:1146-1150`) writes `PIXEL_DTYPE_ATTR` from
`array.dtype.name` when the dtype is floating-point and **deletes** it
otherwise — the comment there says why the delete matters. So the
carrier is a function of the resident array's dtype, and the export's
dispatch (`io_handlers.py:2151` `arr.dtype.kind == 'f'`, then
`arr.itemsize in (4, 8)`) reaches the same element either way. The two
populations #183 wants to distinguish also want the *same* answer:
float32 goes under (7fe0,0008) whether it came from a Parametric Map or
from a caller.

**Recommendation: change nothing.** The test that would catch this
premise flipping is
`tests/test_float_pixel_data_export.py` — the module already covers the
`set_pixel_data`-replaces-a-float-with-an-int direction, which is the
only way the two could come apart. Say so in the CHANGELOG rather than
adding a branch that can only ever agree with the one beside it.

---

## 2. What is actually being decided

One string, for three real consumers and one hypothetical:

1. **Nested pixel data** (#183, this spec) — needs a sequence path and
   an item ordinal, at arbitrary depth.
2. **Multi-rate waveforms** (#277, milestone v1.1.0, **not implemented
   here**) — needs one blob per multiplex group.
3. **#150 option 3a** — verified from #150's own comment thread: 3a
   *is* "carry all groups through ingest, storage and DICOM export", and
   its cost table names `UNIQUE(instance_uid, kind)`,
   `get_blob_refs('waveform')` and the two `WHERE kind = 'waveform'`
   reads as the sites it must move. **3a and #277 are the same consumer,
   not two.** The brief's "four consumers" are three, and #183's
   "decided once for both halves and for #150 option 3a" is satisfied by
   satisfying #277.
4. **Nested float pixel data** — spellable, recommended not carried
   (Q5).

---

## 3. The grammar

### 3.1 Definition

```
kind   := root | root ":" path
root   := "pixels" | "waveform"
path   := step ( "/" step )* "/" tag
step   := tag "/" index
tag    := [0-9a-f]{4} "," [0-9a-f]{4}
index  := 0 | [1-9][0-9]*
```

In words, and this is the sentence to remember: **a blob's kind is its
root, and — when the payload sits inside a sequence — a colon, then the
`iter_item_tree` path to the enclosing item written as
`tag/index` steps separated by `/`, then a final `/` and the tag of the
element the bytes came out of.**

The path segment is a serialization of exactly the tuple
`iter_item_tree` already yields (`entities.py:417-432`): a tuple of
`(sequence_tag, index)` steps where `sequence_tag` is the lowercase-hex
`"gggg,eeee"` spelling used everywhere in the graph. That is deliberate
and is the main reason to prefer this shape over any other: the codebase
already has one answer to "where in the instance is this", used by
`PhiFinding.entity_path`, `resolve_item_path` and
`_rehydrate_findings`. A blob path that is a different shape from a
finding path would be a second answer to the same question, which is
what CLAUDE.md's conventions exist to prevent.

The **terminal tag** is not decoration. It is what tells the export
writeback which element to create: `7fe0,0010` → Pixel Data,
`5400,1010` → Waveform Data, `7fe0,0008` → Float Pixel Data. Without
it, a re-merge would have to infer the element from the root, and
`waveform` → (5400,1010) is only true until it is not.

### 3.2 Delimiters and escaping: there is none, and none is possible

The brief asks what happens because DICOM tags contain commas and
sequence paths contain slashes. The answer is that the token alphabets
are closed and disjoint from the delimiters:

- a `tag` is drawn from `[0-9a-f,]` — it contains a comma and cannot
  contain `/` or `:`;
- an `index` is drawn from `[0-9]` — it can contain neither;
- `:` occurs exactly once, and `/` only between tokens.

So no delimiter can appear inside a token, no escaping mechanism exists,
and **none may be added**. A kind that does not match the grammar is not
escaped, it is refused: `ValueError` at the gate (§6.1). That is the
same discipline `FLOAT_DTYPE_NAMES` applies to the dtype carrier — a
string that came back out of the store is data, and the reader
allow-lists rather than interprets.

The tag/index alternation is positional and unambiguous even though
`0088` is also a run of digits: after the root, `rest.split("/")` yields
an **odd** number of tokens ≥ 3, positions 0, 2, 4, … are tags (they
contain a comma) and positions 1, 3, 5, … are indices (they do not).
A path with an even token count is malformed by construction.

### 3.3 Future extension without ambiguity

If a later change ever wants a non-path qualifier after the root — a
per-frame split, say — it can take one because no keyword is a legal
`tag`: `pixels:frame/3` cannot be confused with a path, since `frame`
has no comma. This is recorded so that the next person does not think
the grammar is closed and invent a fourth spelling beside it.

### 3.4 Length, against the `UNIQUE` index

Measured (`scratchpad/probe_kind.py`, `probe_batch.py`):

- A realistic depth-1 icon kind, `pixels:0088,0200/0/7fe0,0010`, is
  **28 characters**.
- The longest kind produced by the 200-icon-per-instance probe
  (`Referenced Image Sequence` → `Icon Image Sequence`, depth 2) was
  **42 characters**.
- A synthetic depth-40 kind of **500 characters** stored and read back
  through `record_blob_ref` with no error.

SQLite imposes no key-length limit on a b-tree index; `SQLITE_MAX_LENGTH`
(1 GB by default) bounds the value, not the index entry. **There is no
practical length limit and the grammar states none.** A depth cap would
be a second bound on the same thing —`populate_attrs`/`process_sequence`
already recurse without one, and if a bound is ever wanted it belongs
there, at the parse, not here at the key.

### 3.5 The parser

One function, in `persistence.py` beside the gate it feeds, returning
the `iter_item_tree` shape so callers do not each re-derive it:

```python
_BLOB_TAG = r"[0-9a-f]{4},[0-9a-f]{4}"
_BLOB_KIND_RE = re.compile(
    rf"^(pixels|waveform)(:({_BLOB_TAG}/(0|[1-9][0-9]*)/)+{_BLOB_TAG})?$")

def parse_blob_kind(kind: str) -> Tuple[str, tuple, Optional[str]]:
    """('pixels'|'waveform', path, terminal_tag).

    `path` is the tuple `iter_item_tree` yields — `(("0088,0200", 0), …)`
    — and is `()` for a root blob, whose `terminal_tag` is None.
    """
```

`serialize_blob_kind(root, path, terminal_tag)` is its inverse and is
the only thing any writer may call. Two functions, one grammar; nothing
constructs a kind by f-string at a call site, which is precisely the
"invented at the call site" failure #183 names.

---

## 4. The four consumers, spelled — the proof the grammar has room

| Consumer | Payload | Kind |
| --- | --- | --- |
| **#183, this spec** — Icon Image Sequence item 0's Pixel Data | icon frame, raw | `pixels:0088,0200/0/7fe0,0010` |
| **#183, arbitrary depth** — an icon on Referenced Image Sequence item 3 | icon frame, raw | `pixels:0008,1140/3/0088,0200/0/7fe0,0010` |
| **#277 / #150 3a** — Waveform Sequence multiplex group 2 | group 2's samples | `waveform:5400,0100/2/5400,1010` |
| **#277 / #150 3a** — multiplex group 0 | group 0's samples | `waveform` (legacy literal; see Q4) |
| **Q5, spellable, recommended not carried** — nested (7fe0,0008) | float frame | `pixels:0040,0555/0/7fe0,0008` |
| Root pixels | top-level frame | `pixels` |

Read the third row carefully, because it is the whole proof. #277 needs
"per-multiplex-group blobs", and a multiplex group **is** a sequence
item: Waveform Sequence is (5400,0100), each item is one multiplex
group, and Waveform Data (5400,1010) lives inside it. So #277 needs no
new grammar, no new concept, and no second spelling — it needs the same
path form this spec introduces, applied to a different root. The two
`WHERE kind = 'waveform'` hydration reads become `= 'waveform'` plus a
`LIKE 'waveform:%'` sibling, exactly as §7.3 does for pixels.

#150's own 3a table asserts that "suffixed kinds would be invisible to
compaction, which is precisely the failure `io_handlers.py:452-456`
warns about. Three call sites must move together or `compact()` reclaims
the extra groups." **That is measured wrong** — §1.3 shows compaction
rewires an unknown kind's offsets correctly and only declines to publish
it into `uid_map`. The three call sites do have to move, but for the
in-memory-loader reason, not the reclamation one. Worth correcting on
#150 when #277 is picked up.

---

## 5. Ingest: discovery is free, decoding is not

### 5.1 The walk already exists

`populate_attrs` recurses into every sequence item via `process_sequence`
(`io_handlers.py:1006-1025`), and already *sees* nested group-`7fe0`
elements — the `if elem.tag.group == 0x7fe0:` block at
`io_handlers.py:843-856` is where the nested (7fe0,0010) gets its
`DATA_LOSS` row. `tests/test_private_binary_ingest.py:578`
(`test_pixel_data_inside_a_sequence_item_is_reported`) is the executable
proof that the walk reaches it.

Measured (`scratchpad/probe_scale.py`), one instance with N
`ReferencedImageSequence` items each carrying a 64×64 icon:

```
n_items=   0  file=   4.9 KiB  populate_attrs= 0.02 ms  second_full_walk= 0.01 ms  elements=  24  nested 7fe0,0010 reported=  0
n_items=  50  file= 216.4 KiB  populate_attrs= 0.61 ms  second_full_walk= 0.15 ms  elements= 624  nested 7fe0,0010 reported= 50
n_items= 200  file= 850.9 KiB  populate_attrs= 2.33 ms  second_full_walk= 0.58 ms  elements=2424  nested 7fe0,0010 reported=200
```

Every nested icon is found today. **Do not add a discovery pass**: a
second full recursive walk of the same dataset costs 0.58 ms against
`populate_attrs`'s 2.33 ms at 200 items — 25% of the ingest parse, for
information the first walk already has. The change is threading a path
and an accumulator, the way `dropped` and `unscanned` already ride.

### 5.2 The signature change

`populate_attrs(ds, item, dropped=None, is_root=True, unscanned=None)`
gains `nested=None, path=()`; `process_sequence(tag, elem, parent_item,
dropped=None, unscanned=None)` gains `nested=None, path=()` and passes
`path + ((tag, index),)` per item. Both are internal, two call sites
deep, and the same shape as the `is_root` thread #169 added.

`populate_attrs` appends to `nested` a
`(path, tag_str, vr, elem)` tuple for each nested candidate — the
pydicom element itself, because decoding needs the enclosing dataset and
happens in the same worker microseconds later. Nothing pydicom-shaped
crosses a process boundary.

### 5.3 Decoding: measured, and the reason it cannot be verbatim

`ingest_worker` calls `populate_attrs` at `io_handlers.py:1085`, before
the pixel-extraction block at `:1097`. The decode goes immediately after
that call, in the same worker.

**pydicom cannot decode a sequence item's pixel data on its own.**
Measured (`scratchpad/probe_icon.py`), on a native
ExplicitVRLittleEndian source:

```
  icon.pixel_array -> AttributeError Unable to decode the pixel data as
      the dataset's 'file_meta' has no (0002,0010) 'Transfer Syntax UID'
  pydicom.pixels.pixel_array(icon) -> AttributeError (same)
```

**Borrowing the enclosing dataset's `file_meta` decodes it correctly**,
including through encapsulation. Measured
(`scratchpad/probe_encaps.py`), on an RLE-encapsulated source whose icon
is also RLE-encapsulated:

```
file TransferSyntaxUID: 1.2.840.10008.1.2.5
nested icon raw len: 90  first 16: feff00e00400000000000000feff00e0
nested icon undefined length: True
icon.pixel_array with borrowed file_meta: [0, 1, 2, 3]
```

So the recipe is one line: `item.file_meta = ds.file_meta` then
`item.pixel_array`, `np.ascontiguousarray(...).tobytes()`.

**Verbatim carriage of the element value is wrong and must not be
implemented.** Same probe, the export end:

```
EXPORT TransferSyntaxUID: 1.2.840.10008.1.2
EXPORT top-level PixelData len: 16      (from a 104-byte encapsulated source)
EXPORT icon keys: [ …eight 0028,xxxx… ]  (no Pixel Data)
```

The export writes Implicit VR Little Endian with the top-level frame
decoded to raw bytes. Writing 90 bytes of encapsulated fragments into
that file would produce an icon no reader can decode, under a transfer
syntax that says there are no fragments. Decode at ingest, store raw
interleaved bytes, write raw — which is exactly what the top-level path
does, so there is one rule for both depths.

The residual is Q6: a lossy-JPEG icon decodes to RGB from a declared
`YBR_FULL_422`, and the exported item's Photometric Interpretation would
have to be rewritten to match. Recommendation there is to refuse those
at ingest.

### 5.4 Routing and reporting must come from one place

This is the trap, and it is #194's shape at a third site. If
`_is_routed` is taught to return `True` for a nested (7fe0,0010), then
an icon that **fails** to decode is reported as routed and its loss row
vanishes — a silent drop, which is the defect #169 closed.

The float arm sets the precedent to follow (`io_handlers.py:1152-1170`):
on a decode failure it leaves `p_bytes = None` and the instance "behaves
in every respect as it did before".

**Rule: `_is_routed` does not change.** `populate_attrs` collects
candidates into `nested` and does **not** suppress the `dropped` append;
`ingest_worker` decodes each candidate and, on success, removes its
entry from `dropped` before `meta['dropped_private_binary'] = dropped`
at `:1086`. One list, one decision, made by the code that actually knows
whether the bytes were carried. State that in a comment, because the
tidier-looking alternative — a static `True` in `_is_routed` — is what a
later reader will reach for.

*Two spellings of the removal, and which to pick.* The rule above
appends every candidate to both `nested` and `dropped` and has
`ingest_worker` `remove()` the successes. That reconciles two lists by
count, and it works only because a `(tag, value)` entry for one icon is
indistinguishable from another's — correct, but fragile in a way a
reader cannot see. The alternative keeps the decision in one place:
`populate_attrs` grows an optional `nested` parameter and, **when it is
not None**, routes a nested (7fe0,0010) into `nested` *instead of*
`dropped`; `ingest_worker` then appends the failures to `dropped`
itself, where it is the code that knows. When `nested is None` — the
direct `populate_attrs` callers in the tests — behaviour is exactly
today's. **Recommended: the second.** Same one-decision property, no
two-list reconciliation, and the `nested is None` default keeps every
existing caller untouched. Mentioned as a developer's call because both
satisfy the rule; only the second makes the rule visible in the code.

Successfully decoded blobs ride out as
`meta['nested_pixels'] = [(path, terminal_tag, raw_bytes, sha256), …]`,
for the reason `waveform_groups` and `dropped_private_binary` do: the
worker may be a subprocess with no store handle, and the return tuple's
arity is unpacked at every call site (`io_handlers.py:1406`). #150's 3a
note claims "the bytes cannot ride in `meta` cheaply" — they cost the
same pickle either way; the slot is not what makes them expensive.

### 5.5 `import_files` writes the frames

The waveform block at `io_handlers.py:1637-1655` is the template: append
each frame through `sidecar_manager.write_frame(...)`, build the loader,
and record the reference. Its comment about calling `record_blob_ref`
without `conn=` — "this loop runs outside any open SqliteStore
transaction" — applies unchanged.

---

## 6. The gate

### 6.1 `persist_blob`

```python
if kind not in ("pixels", "waveform"):
    raise ValueError("Unknown blob kind: {!r}".format(kind))
```

becomes a `parse_blob_kind(kind)` call whose `ValueError` carries the
grammar in its message. Measured today
(`scratchpad/probe_kind.py`): `'pixels'` and `'waveform'` pass;
`'pixels:seq:0088,0200/0/7fe0,0010'`, `'pixels#0088,0200/0'` and
`'pixels/nested'` all raise. After the change the first stays raising
(the `seq:` marker is not in the grammar — Q3), the last two still
raise, and `'pixels:0088,0200/0/7fe0,0010'` passes.

### 6.2 `record_blob_ref`

Ungated today — see Q8. Recommendation: the same `parse_blob_kind` call.

---

## 7. The store side — seven sites

This is the larger half, against #183's claim. Each site, with what it
gains.

### 7.1 Where the loaders live

**Recommendation: a dict on `Instance`, not a field on `DicomItem`.**

```python
# Transient: nested sidecar payloads, keyed by their parsed blob kind.
_nested_pixel_loaders: Dict[Tuple[tuple, str], SidecarPixelLoader]
```

Reasons, in order of weight:

1. `Instance` already holds `_pixel_loader`, `_pixel_hash` and
   `_waveform_loader`. One place answers "what binary does this instance
   carry", and the save walk, the compaction rewire and the export
   transport all iterate one dict.
2. `DicomItem` is `@dataclass(slots=True, eq=False)`
   (`entities.py:251`), so a loader on the item is a class-shape change
   that `clone_sequences` (`entities.py:451`) and
   `_make_lightweight_copy` (`session.py:3818`) would both have to learn
   about — and the PHI-scan copies must *not* carry heavy loaders.
3. The key is the parsed kind, so nothing has to re-derive a path.

**Transport to the export worker needs nothing.** Neither `Instance` nor
`DicomItem` defines `__getstate__`/`__reduce__` (grepped: no hits in
`entities.py` or `io_handlers.py`), so `_pixel_loader` already pickles
with the instance, and `ExportContext.instance` is the live object on
both paths (`session.py:3589` `instance=instance`;
`io_handlers.py:3198-3199` `instance=inst`). The `sidecar_path` /
`pixel_offset` / `pixel_length` / `pixel_alg` fields on `ExportContext`
(`io_handlers.py:1736-1740`) are a zero-copy hint set only by
`_generate_export_contexts`, not a requirement; the session path does
not set them at all. **Recommendation: add no `ExportContext` field.**

### 7.2 `save_all`'s `blob_rows`, and why batching is not optional

`_prepare_instance_rows` (`persistence.py:2860-2863`) appends one
`('pixels', offset, length, hash, alg)` row per instance. It gains a
loop over `inst._nested_pixel_loaders` emitting one row each, keyed
`inst.sop_instance_uid` — which is what makes the change survive
`regenerate_uid()` (§7.5).

The batching matters, measured, and a developer who reaches for
`persist_blob` per icon will not notice. 200 nested blobs of 4 KiB each
(`scratchpad/probe_scale.py`, `probe_batch.py`):

```
via persist_blob, one connection each : 322.8 ms total (1.614 ms each)
frames appended, then rows in one txn :  11.9 ms total
    frames  8.88 ms (0.044 ms each)
    rows    3.03 ms (0.015 ms each)
```

**27× .** The nested path must follow `save_all`'s existing shape —
frames appended in the prepass, rows written inside the one transaction
with `conn=` — not `persist_blob` in a loop.

Nested frames are written **once, at ingest**. Nothing in the pipeline
mutates an icon: remediation edits `attributes`, redaction touches the
top-level array, anonymize touches neither. So `save_all` re-emits the
*row* (cheap, and required for UID changes) and never re-appends the
frame.

### 7.3 Hydration

`load_all` (`persistence.py:1497-1504`) and `load_patient`
(`persistence.py:1633-1640`) each pre-fetch waveform refs in one query,
"rather than per instance to keep hydration a fixed number of queries".
Add one sibling to each:

```sql
SELECT instance_uid, kind, offset, length, compress_alg, hash
FROM instance_blobs WHERE kind LIKE 'pixels:%'
```

grouped by `instance_uid`. Measured (`scratchpad/probe_plan.py`) against
a store of 20,200 blob rows, 200 of them nested:

```
existing waveform pre-fetch:   0 rows, 0.37 ms   plan: SCAN instance_blobs
proposed nested pre-fetch:   200 rows, 0.60 ms   plan: SCAN instance_blobs
```

Both are full scans — `idx_blobs_uid_kind` is `(instance_uid, kind)` and
cannot serve a `kind`-only predicate either way. **Recommendation: add
no index.** The new query is the same shape and the same order of cost
as one already accepted, and an index whose only reader is a
once-per-session prefetch is not worth the write amplification on every
blob row.

The prefix cannot collide with the legacy literals. Measured
(`scratchpad/probe_batch.py`), after adding bare `pixels` and `waveform`
rows to a store of 200 nested ones: `LIKE 'pixels:%' -> 200`,
`= 'pixels' -> 1`.

One caveat on the operator. SQLite's `LIKE` is ASCII case-insensitive by
default, so `LIKE 'pixels:%'` would also match `PIXELS:…`. Under Q8
answered *yes* that is unreachable — the gate rejects uppercase, so no
such row can exist, and `LIKE` is fine. Under Q8 answered *no*, an
ungated `record_blob_ref` caller could write one, and the correct
spelling becomes `GLOB 'pixels:*'`, which is case-sensitive. Decide the
operator with Q8, not independently of it.

`_create_pixel_loader` (`persistence.py:885-888`), the helper
`_wire_waveform_loader` mirrors on the waveform side, is the model for building each nested
`SidecarPixelLoader`. Its geometry comes from the **nested item's own**
`0028,xxxx` attributes, which hydration has already put on the
`DicomItem` — resolve with `resolve_item_path(instance, path)`, and
treat a `None` return as "this blob's item is gone", never as the
instance (`entities.py:434-449` says why in terms this spec inherits
wholesale).

### 7.4 Compaction rewire

`session.compact()` (`session.py:1064-1070`) reads
`get_blob_refs('waveform')` for exactly this reason: "compact_sidecar's
uid_map is pixels-only by design … a loader left on a pre-compaction
offset reads the wrong bytes or runs off the end of the file." Nested
loaders are in the same position, and `_rewire_sidecar_loaders`
(`session.py:1085`) gains a third map keyed `(uid, kind)`.

The `_pixel_swap_lock` discipline that method documents — taken per
instance, around offset and length together, a leaf with nothing
acquired inside it — applies unchanged and must be honoured for the
nested rebinds too.

### 7.5 UID changes, which is the one that bites silently

`regenerate_uid()` (`entities.py:565-605`) replaces
`sop_instance_uid` on every redaction, and `instance_blobs` is keyed by
UID. `tests/test_redaction_identity.py:248-296`
(`test_the_store_holds_one_instance_and_one_pixel_blob`) exists because
this went wrong once already, for the top-level blob:

```
assert blobs == [(new_uid, "pixels")], (
    "… a row under any other UID is an orphan only `compact()` notices")
```

Because §7.2 re-emits nested rows from `inst.sop_instance_uid` on every
save, they follow the instance for free. The **bytes** do not move — the
new row points at the same offset — so no re-append is needed and
`_read_blob_index` sees the frame as live. Rows left under the retired
UID are removed by `_delete_instances` (`persistence.py:87-105`), which
deletes `instance_blobs` for a UID with no `kind` predicate and so needs
no change.

**That test must be extended, not left alone.** As written it asserts
the blob list is *exactly* `[(new_uid, "pixels")]`, so a redacted
instance carrying an icon will turn it red — correctly, and the fix is
to assert the nested row is present under the new UID and absent under
the old one. Named in §12.

### 7.6 `_delete_instances`, `compact_sidecar`'s orphan sweep

No change; both are kind-agnostic (§1.3, §7.5).

### 7.7 Legacy stores

**No migration, and none is needed.** A store ingested before this change
simply has no rows matching `LIKE 'pixels:%'`, so the new prefetch
returns nothing and hydration behaves exactly as it does today. Its
ingest-time `DATA_LOSS` row for the nested (7fe0,0010) stays **true** —
those bytes really were not carried — which is the property #168's and
#237's version-stamped-attestation work is about: an old record must not
be silently reinterpreted under a new capability. The row says what was
true when it was written and continues to.

A user who wants the icons from an old store re-ingests; that is already
the documented answer for every ingest-side capability gain, and it
needs no attestation column because nothing here claims anything about
the *past*. Contrast the redaction attestation case, where the store
records a claim about what was done — there, a version stamp is
load-bearing. Here it would be a stamp with no reader.

---

## 8. The export re-merge, re-costed — #183 has this inverted

#183 says: "The export re-merge is the real cost, not the storage."
**Measured, it is the cheapest part of the change.**

`_merge_sequences` (`io_handlers.py:3877-3903`) is a 26-line static
method that walks `{tag: DicomSequence}` and appends a `Dataset` per
item. It has no notion of a path, and it does not need one:
**the writeback is a post-pass, not a rewrite.** Prototype, run against
a real exported file (`scratchpad/probe_icon.py`):

```python
def resolve_ds_item(ds, path):
    """path is a tuple of ('gggg,eeee', index) steps."""
    cur = ds
    for tag_str, idx in path:
        g, el = (int(x, 16) for x in tag_str.split(','))
        try:
            seq = cur[(g, el)].value
        except KeyError:
            return None
        if idx >= len(seq):
            return None
        cur = seq[idx]
    return cur
```

```
  resolved item: True
  after re-merge, icon PixelData present: True bytes: b'\x01\x02\x03\x04'
```

Thirteen lines of resolver, plus a loop in `_export_instance_worker`
after `_merge_sequences(ds, inst.sequences, losses)` at
`io_handlers.py:2035` that, for each nested loader, resolves its path
against `ds` and writes the element. Against §7's seven store-side
sites, three of which are in two functions each: **the store side is the
order of magnitude, not the re-merge.**

The resolver is a `ds`-side twin of `resolve_item_path`
(`entities.py:434`), and it must inherit that function's rule verbatim:
a `None` return means "this item is gone" and the blob is skipped with a
loss row — never "write it onto the root". Writing an icon's pixels onto
the instance would fabricate a top-level element that was never in the
file, which is #57's defect exactly.

Two constraints on the write:

- **VR.** The export writes Implicit VR Little Endian (measured, §5.3),
  so no VR reaches the file — but pydicom needs one to encode.
  `add_new(0x7FE00010, vr, data)` with `vr = 'OW' if BitsAllocated > 8
  else 'OB'`, per PS3.5. Recommendation: carry the **source** element's
  VR on the blob is *not* needed — derive it, one rule, from the item's
  own BitsAllocated, which hydration has already restored.
- **Compression.** `use_compression=True` J2K-compresses the top-level
  frame only. The nested writeback is unconditional raw and does not
  consult `ctx.compression`; state that in a comment, since "the icon
  wasn't compressed too" will read as an oversight.

### 8.1 Both export paths, by construction

`tests/test_api_coherence.py` pins `session.export()` and
`DicomExporter.write_tree()` to identical trees, compared by full
relative path. Verified that the post-pass covers both without being
written twice: `write_tree` (`io_handlers.py:3312`) builds contexts with
`_generate_export_contexts` and dispatches
`run_parallel(_export_instance_worker, export_tasks, …)` at
`io_handlers.py:3378-3386` — the **same worker** `session._export_dicom`
uses. A post-pass inside `_export_instance_worker` is therefore on both
paths by construction, which is the property CLAUDE.md's "one spelling
per behaviour" asks for and the reason not to put it in
`_merge_sequences` (which `export_batch` callers could reach separately).

`write_tree` carries the loaders because `_generate_export_contexts`
passes the live `inst` (`io_handlers.py:3198-3199`), and the fixture
generators in `scripts/` build graphs with no loaders at all — so their
nested dict is empty and the post-pass is a no-op for them.

### 8.2 The key is positional, and the graph mutates in between — the shift guard

**The invariant this section defends: position is the only identity a
sequence item has.** The path is recorded at *ingest* and resolved at
*export*, and everything that happens in between — hydration, audit,
remediation, redaction — can change the sequence's contents. §5.4's
`resolve → None → DATA_LOSS row` rule handles an item that was
**removed**. It does not handle an item whose **index shifted** because
an earlier sibling was removed: that path resolves, to the *wrong item*,
and the icon is written into it silently. Silent wrong bytes is this
repo's worst failure class, and it is the same hazard CLAUDE.md names
for `entity_path` and `_live_target`.

Enumerated every item-level mutation in the tree
(`grep -n '\.items\.pop\|\.items\.remove\|del .*\.items\['` over
`isocenter/`) — there are exactly two, and **neither can shift a
surviving item's index today**:

- `persistence.py:974` `del seq.items[1:]` (`_prune_hollow_multiplex_items`)
- `io_handlers.py:1226` `del wf_seq.items[1:]` (the #160 ingest prune)

Both are tail truncations of Waveform Sequence keeping item 0, so no
survivor moves. Remediation's `seq_removals` (`privacy.py:417`) raises
`REMOVE_TAG` on a *sequence tag* — it deletes whole sequences, never one
item out of several — so it destroys paths (handled) rather than
shifting them.

So the hazard is latent, not live. **Recommendation: add the guard
anyway**, because the check is two lines and the alternative is that the
first person to write `items.pop(i)` anywhere in this codebase silently
corrupts exports with no test able to see it. Before `add_new`, the
post-pass compares the resolved item's own descriptors against the blob:

```python
expected = (item.Rows * item.Columns * item.SamplesPerPixel
            * (item.BitsAllocated // 8)
            * int(getattr(item, 'NumberOfFrames', 1) or 1))
if expected != len(decoded):
    # The path resolved to an item whose geometry is not the one the
    # bytes were taken from -- an index shifted under us. Refuse the
    # write; a DATA_LOSS row is the honest outcome and a wrong icon is
    # not. Position is the only identity a sequence item has, which is
    # why this check exists rather than a trusted lookup.
    losses.append(...); continue
```

It is not a proof of identity — two icons of equal geometry are
indistinguishable by it — but it converts the *detectable* half of the
failure from silent-wrong-bytes into a reported loss, which is the
difference that matters. Q9 (below, added after review) asks the owner
whether the stronger form is wanted: store the icon's Rows/Columns
alongside the blob and compare those, which catches equal-length
mismatches too at the cost of widening `instance_blobs`.

---

## 9. The de-identification gate (Q2)

Verified above: nothing scans or redacts an icon. Every consumer reads
`get_pixel_data()`. So the drop is currently what protects the export,
and carrying the bytes removes that protection without saying so.

**Recommendation.** In `_export_instance_worker`, the nested writeback
is skipped — and the enclosing `IconImageSequence` **item removed from
`ds`** — when the instance was redacted. A `DATA_LOSS` row is appended
saying the icon was dropped because the frame it derives from was
redacted.

**"Was redacted" must be two conditions, not one.** The obvious gate,
`ctx.redaction_zones`, is *not sufficient*, and this was checked rather
than assumed. `ctx.redaction_zones` comes from `_redaction_zones_for`
(`session.py:3604-3610`), which looks the zones up **at export time**,
from the **current** configuration, keyed on
`series.equipment.device_serial_number`. `RedactionService` does not
consult that: it redacts whatever `rois` its caller passed
(`services.py:433-465` and `:758-794`). So the zones list is empty at
export while the pixels are redacted whenever any of these holds — the
config rule was edited or the serial number changed between `redact()`
and `export()`; `RedactionService` was driven directly with ad-hoc
`rois`; the series has no `equipment` (the lookup returns `[]` on the
first branch). In each case the top-level frame ships zeroed and the
icon ships intact: a thumbnail of exactly what redaction removed.

The instance itself carries the answer. `services.py:464` and `:794`
both write `inst.attributes["_ISOCENTER_REDACTION_HASH"] = config_hash`
on every path that actually modified pixels, and `:433`/`:758` read it
back as the skip attestation. So the gate is:

```python
redacted = bool(ctx.redaction_zones) or (
    "_ISOCENTER_REDACTION_HASH" in ctx.instance.attributes)
```

The attestation is the load-bearing half; `ctx.redaction_zones` stays as
the belt for a redaction that is configured but has not run yet in this
session. Note the attestation is over the *configuration*, not the
pixels (#237) — which is fine here, because the question being asked is
only "did redaction touch this instance", not "is that redaction
current".

Three reasons for the item removal rather than a bare skip:

1. Leaving descriptors with no bytes is the Type 1 violation this whole
   spec exists to fix; reintroducing it on the redaction path would be
   #160's shape at a fourth site.
2. #160 settled the identical question for discarded multiplex groups
   and chose exactly this — drop the item, keep the audit row.
3. Icon Image Sequence is Type 3 wherever the macro is included, so an
   absent sequence is conformant and an under-populated item is not.

The scope for that row is `LOSS_SCOPE_STANDARD` by the parity rule
(`7fe0` is even), consistent with every other row this family files.
Whether a redaction-driven icon drop deserves `SIGNAL` is #150's
question and must not be decided here as a ride-along — the emitter
comment at `io_handlers.py:577-593` is explicit about that.

This gate is also why §7.1's dict is keyed by parsed kind: the export
needs to ask "does this blob sit under an Icon Image Sequence" without
re-parsing a string.

---

## 10. What conformant output looks like

For the Icon Image Macro (PS3.3 C.7.6.1.1.6), an exported item must
carry:

- its `0028,xxxx` descriptors — Rows, Columns, SamplesPerPixel,
  PhotometricInterpretation, BitsAllocated, BitsStored, HighBit,
  PixelRepresentation, and PlanarConfiguration when SamplesPerPixel > 1
  — which it already does today (§1.2), and
- **(7fe0,0010) Pixel Data, Type 1**, holding the icon's frame under the
  file's transfer syntax.

After this change, that is the output: nine descriptors and 4 bytes for
the §1.2 fixture, decoded raw and written raw under Implicit VR LE, VR
derived as `OW`/`OB` from BitsAllocated (§8).

Where the bytes cannot be carried — the encapsulated-lossy case (Q6), a
decode failure (§5.4), or the redaction gate (§9) — the conformant
output is **no Icon Image Sequence item at all**, plus the `DATA_LOSS`
row. Never descriptors without data. That is the single rule this spec
adds to the exporter and it is worth writing as a comment where the
post-pass lives.

---

## 11. Scope and PR shape

**Recommendation: one PR, nested only.**

- The float half is merged (Q1). There is nothing to sequence against.
- The nested half's two sub-halves — carry the bytes, write them back —
  have no independent observable value in either order. Bytes carried
  and never written back change nothing a user sees and would ship a
  store growing by every icon in the cohort for no benefit; a writeback
  with an empty loader dict is unreachable code. Splitting them would
  produce one PR whose tests can only assert about SQL rows and one
  whose tests cannot run without the first.
- The evidence the brief asks for — is the re-merge an order of
  magnitude larger than the storage? — comes out the other way (§8), so
  the split the brief anticipates has no cost argument behind it either.

**#277 is not in this PR.** The grammar is decided for it (§4) and
nothing here forecloses it, but implementing multi-rate carriage means
`_extract_waveform`, `SidecarWaveformLoader`'s single-blob assumption,
both exporters, the Murmur bridge and the stress benchmark — #150's own
3a table costs it at six production files — and it sits on v1.1.0.

---

## 12. Tests

### 12.1 Baseline first

Before any edit, and recorded in the PR:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$W /Users/kevin/Developer/Isocenter/.venv/bin/python -u -m pytest -v \
  tests/test_private_binary_ingest.py tests/test_blob_storage.py \
  tests/test_float_pixel_data_export.py tests/test_redaction_identity.py \
  tests/test_api_coherence.py tests/test_save_all_contract.py
```

with `isocenter.__file__` printed and read first. `pytest -v`, no pipe,
per CLAUDE.md.

### 12.2 Existing tests that go red, and what each must become

| Test | Why it flips | What it must assert instead |
| --- | --- | --- |
| `tests/test_private_binary_ingest.py:578` `test_pixel_data_inside_a_sequence_item_is_reported` | The bytes are carried, so the row must **stop** being filed. Reporting a loss that did not happen is #194's defect at a third site — the owner's #183 comment asks for this assertion by name. | Renamed to say the nested pixel data is *carried*; asserts zero `DATA_LOSS` rows for `7fe0,0010`, and that a blob row exists under kind `pixels:0088,0200/0/7fe0,0010`. |
| `tests/test_private_binary_ingest.py:615` `test_the_top_level_pixel_data_of_that_same_file_is_still_not_reported` | **Stays green as written — do not budget time to change it.** Measured (`scratchpad/probe_review.py`): its icon carries only `Rows`, `Columns` and 4 bytes, so borrowing `file_meta` and calling `.pixel_array` on that item raises `AttributeError: Missing required element: (0028,0100) 'Bits Allocated'`. By §5.4 a decode failure leaves the entry in `dropped`, so its `len(rows) == 1` still holds. Contrast the `:578` fixture, whose icon has the full macro and decodes to `[1, 2, 3, 4]`. | Unchanged, but re-purposed: it becomes the §12.4.6 undecodable-icon tripwire, and its docstring should say so. Enriching the fixture to full Icon Image Macro descriptors is what would flip it — do that only if the intent is to test the top-level boundary against a *carried* icon, and then it becomes `len(rows) == 0`. |
| `tests/test_private_binary_ingest.py:638` `test_float_pixel_data_inside_a_sequence_item_is_reported` | **Must stay green** under Q5's recommendation. Named here because a developer widening `_is_routed` by depth alone will break it, and it is the tripwire that catches exactly that. | Unchanged. Add a line to its docstring saying the grammar spells this and the spec declined to carry it. |
| `tests/test_redaction_identity.py:248` `test_the_store_holds_one_instance_and_one_pixel_blob` | Asserts the blob list is exactly `[(new_uid, "pixels")]`. Its fixture carries no icon today, so it may stay green — but the moment a nested blob is added to that fixture it goes red, and it is the only test that pins UID-follows-blob. | Extended with a second case: an instance with an icon, redacted, asserting the nested row is present under the **new** UID and absent under the old one (§7.5). |

### 12.3 Tests that must stay green — the regression half

- `tests/test_api_coherence.py` — the two export paths still produce
  identical trees (§8.1). This is the check that the post-pass did not
  land on one path only.
- `tests/test_blob_storage.py`, `tests/test_save_all_contract.py:182`
  (`test_instance_blobs_mirrors_every_stored_frame`) — `instance_blobs`
  must not lag `instances`; the nested rows join that invariant.
- `tests/test_float_pixel_data_export.py` — untouched by this change,
  and the proof of it.
- `tests/test_waveform_ingest.py` — `get_blob_refs('waveform')` and the
  two `WHERE kind = 'waveform'` reads still see exactly what they saw.
- `tests/test_packaging_contract.py` — no new import.

### 12.4 New tests worth having

1. **The grammar round-trips.** `serialize_blob_kind(parse_blob_kind(k))
   == k` for each of §4's six spellings, and `parse_blob_kind` raises on
   an even token count, a `seq:` marker, an unknown root, an uppercase
   tag, and a negative or zero-padded index. This is the one test that
   makes the grammar a contract rather than a convention.
2. **Depth ≥ 2 round-trips end to end.** Ingest → save → close → reopen
   → export, with the icon on
   `ReferencedImageSequence[3]/IconImageSequence[0]`, asserting the
   exported bytes equal the source's. Depth 1 alone would leave the path
   loop unexercised — a rule that is only right at the depth it was
   tested is what #169 started from.
3. **The source file goes away.** The same shape as
   `_ingest_save_close_and_move` in
   `tests/test_float_pixel_data_export.py`: ingest, save, close, delete
   the source, reopen, export, assert the icon's bytes survive. This is
   the whole point of carrying them in the store rather than re-reading.
4. **Compaction preserves a nested blob.** Write, compact, read back
   through the rewired loader — the failure mode §7.4 exists to prevent
   is silent wrong bytes, which only a byte-comparison after a
   compaction catches.
5. **The redaction gate (§9).** An instance with an icon and a
   configured redaction zone exports with **no** Icon Image Sequence
   item and one `DATA_LOSS` row naming the reason. This is the test that
   makes Q2's answer executable rather than a paragraph.
6. **An undecodable nested icon still files its loss row** (§5.4) — the
   #194-shape tripwire.
   `tests/test_private_binary_ingest.py:615` already *is* this test:
   its bare-descriptor icon cannot decode (measured, §12.2), so it stays
   green and should be re-documented rather than rewritten.
7. **A shifted index refuses rather than writes** (§8.2). Build an
   instance with two icons, remove the first item from the sequence
   after ingest and before export, and assert the export files a
   `DATA_LOSS` row instead of writing icon 1's bytes into icon 0's item.
   The failure this catches is silent-wrong-bytes and nothing else in
   the suite can see it.
8. **The redaction gate fires without configured zones** (§9). Redact an
   instance, then clear the configuration rule (or change the series'
   device serial) so `_redaction_zones_for` returns `[]`, and export.
   The icon must still be dropped, on the strength of
   `_ISOCENTER_REDACTION_HASH` alone. A test gated only on
   `ctx.redaction_zones` passes while the hole is open, which is why
   this one is specified separately from test 5.

### 12.5 Mutation probe

`scripts/mutation_probe.py`'s `TARGETS` maps modules to tests and is the
maintained version of CLAUDE.md's table. `io_handlers.py` and
`persistence.py` both gain behaviour here; add
`tests/test_private_binary_ingest.py` to `persistence.py`'s list if the
new grammar tests land beside it, and run the probe on the two touched
modules before the PR — this change adds branches whose wrong answer is
silent.

---

## 13. Implementation order

1. `parse_blob_kind` / `serialize_blob_kind` plus their tests (§12.4.1).
   Nothing else compiles against a grammar that is not yet a function.
2. `persist_blob`'s gate, and `record_blob_ref`'s if Q8 says yes.
3. `populate_attrs` / `process_sequence` path threading and the `nested`
   accumulator. No behaviour change yet — the candidates are collected
   and discarded.
4. `ingest_worker` decode + `meta['nested_pixels']`, and the
   `dropped` removal of §5.4. `import_files` writes frames and rows.
   §12.2's first two tests flip here.
5. Hydration prefetch and loader wiring (§7.3). §12.4.3 goes green.
6. `save_all` blob rows (§7.2) and the redaction UID case (§7.5).
7. Compaction rewire (§7.4). §12.4.4 goes green.
8. The export post-pass (§8) and the redaction gate (§9). §12.4.2, .5
   go green.
9. Full suite, then `pylint isocenter`.

---

## 14. CHANGELOG entry — what it must carry

CLAUDE.md: the CHANGELOG is the project's primary design record and
breaking entries state the exact exception a previously-working call now
raises. Four things this one owes a reader:

1. **The grammar, in full**, with §4's table. It is a schema decision and
   the CHANGELOG is where schema decisions live; a reader adding a fifth
   consumer must not have to find this spec.
2. **The `persist_blob` exception change.** `persist_blob(inst,
   'pixels:seq:0088,0200/0/7fe0,0010', …)` raised `ValueError: Unknown
   blob kind` before and after — the sketch in #183 is not the grammar
   (Q3) — while `persist_blob(inst, 'pixels:0088,0200/0/7fe0,0010', …)`
   now succeeds. Say both, or the next reader will assume the issue's
   spelling works.
3. **That the nested `DATA_LOSS` row stops being filed**, and that this
   is the point rather than a regression — with the counter-case (a
   decode failure, an encapsulated-lossy icon, a redacted instance) still
   filing one.
4. **The redaction gate**, stated as a deliberate loss: an instance with
   redaction zones exports without its icon, on purpose, because nothing
   scans or redacts an icon and the alternative is exporting a thumbnail
   of what was just removed.

And it should correct #150's 3a note in passing (§4): compaction rewires
an unknown kind's offsets correctly; the three waveform call sites move
for the loader reason, not the reclamation one.

---

## 15. Filed rather than fixed

Three things found while measuring, none of them this change's:

1. **`_rewrite_live_frames` turns a bad offset into a zero-length row
   with a WARNING** (§1.3). The data is definitively gone and the
   compaction reports success; only the frame hash catches it, later,
   somewhere else.
2. **`IODValidator` covers one SOP Class and two modules**
   (`validation.py:13-27`), so it certifies nothing about the Icon Image
   Macro before or after this change. It reads like a conformance gate
   and is not one.
3. **#150's option 3a costing is wrong about compaction** (§4). Worth a
   comment on that issue so #277 does not inherit the error.
