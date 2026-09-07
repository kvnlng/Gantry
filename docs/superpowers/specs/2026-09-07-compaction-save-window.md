# Compaction's Residual Save Window: A Costed Tradeoff

**Date:** 2026-09-07
**Status:** Decision OPEN. This spec presents two arms with measured
costs and ends with a recommendation (§8) for the owner to accept or
reject. **Nothing here is approved.**
**Tracking:** #320. Rests on #295, #294, #287, #274, #250, PR #317.
Weighed against #26 (the v1.0.0 API freeze).
**Base:** `main` at `e484bee`
**Measured with:** `/Users/kevin/Developer/Isocenter/.venv/bin/python`
(CPython 3.14.6) on macOS 25.6 / APFS / local SSD. All timings are
warm-page-cache, buffered I/O, **no `fsync`** — §4.3 says what that means
for reading them.
**On the `scratchpad/*.py` and `*.log` citations below:** those probes
were written in a session-local temp directory and **do not exist in the
repo**. They are named so the numbers can be attributed, not so a later
reader can run them. The reproduction that is meant to survive is §9's,
which is specified as a repo test; §4's timings are quoted inline
precisely because their scripts are gone.

---

## OPEN QUESTIONS for the owner

**Q1 — the arm.** §8 recommends Arm A plus three amendments. Accept,
reject, or take Arm B.

**Q2 — Arm B is bigger than #320 says, and the issue's costing is
therefore wrong.** #320 costs "a coarse save-wide lock held across the
whole `save_all`". Measured, `save_all` is **one of five** places in
production code that append a sidecar frame and commit its row
separately (§3.4). Read that carefully, because the short version is
easy to misread in Arm B's favour and easy to misread against it:

- Each of the five reaches the **same** window. Phases A–D corrupt
  through `persist_pixel_data`; **phase E corrupts through `save_all`**
  and is measured (§2, phase E). So a lock over `save_all` **does** close
  the `save_all`-driven variant of every ordering — that half of #320's
  design is sound and I am not disputing it.
- What it does **not** close is the same four orderings driven by the
  other four sites — which is how my first four phases reached them, and
  which includes the redaction path (`persist_pixel_data`) and both
  ingest paths.

So the correct sentence is not "Arm B closes nothing"; it is **"Arm B as
#320 prices it closes one of the five ways in"**. If you take Arm B, you
are taking a **sidecar-generation lock over five call sites**, not one
lock around one method. Confirm you want that scope before it is built.

**Q3 — a claim in `compact()`'s docstring and in #295's CHANGELOG entry
is narrower than its wording suggests.** Both read as though the refusal
covers *a save* in flight at entry. `has_pending_saves()` reads only the
persistence manager's in-flight set and queue; `Session.save(sync=True)`
runs `save_all` on the **caller's** thread (`session.py:810-815`) and
never enters either, and a redaction's `persist_pixel_data` is not a save
at all. **Measured `has_pending_saves() == False` in all five corrupting
orderings** (§2) — including phase E, where a concurrent
`save(sync=True)` writes a frame and commits its row entirely inside the
window and the guard still reads `False`. Nothing ever claimed the guard
saw a second writer, and its own docstring admits it is point-in-time;
the correction wanted is to the two places whose prose over-reads it, and
it is a docs/comment fix, not a behaviour change.

**Q4 — a defect found while auditing, not folded in.** The lock order
documented in three places — `_pixel_swap_lock` before `sidecar._lock`,
"never reversed" (`persistence.py:671`, `persistence.py:2960-2961`,
`session.py:1068`) — names a lock **nothing ever acquires**.
`SidecarManager._lock` is constructed, excluded from `__getstate__`,
recreated in `__setstate__`, and never taken: `write_frame` and
`read_frame` use `fcntl.flock` on the file descriptor instead
(`sidecar.py:47-63`). The invariant is vacuously true. File it.

---

## Context

#295 closed two things and #317 said plainly that it closed neither of
the rest:

* **Closed:** the torn rebind (`offset` and `length` are now assigned
  under `_pixel_swap_lock`, per instance).
* **Closed:** a compaction *entered* while the persistence manager has
  work queued or in flight — `compact()` raises `RuntimeError`.
* **Open:** a write that starts *after* the check and runs concurrently
  with the rewrite.

#320 is that statement as an issue, and it carries a design worked
through and rejected in part by #317. **The owner asked for a costed
tradeoff, not a pick.** §2 reproduces the race deterministically, §3
re-audits every premise #317's deadlock argument rests on, §4 measures
what compaction costs, §5 and §6 cost the two arms, §7 weighs #26, and
§8 recommends.

---

## 1. Verifying what #320 inherits

#320 tells the implementer not to re-derive the design in it. Every claim
in it was checked against `e484bee` anyway, per CLAUDE.md's *verify, do
not trust*. Results:

| #320's claim | Verdict |
| --- | --- |
| A `_compaction_lock` scoped to the pixel prepass would be **incorrect**, because a writer can release the lock and then commit its rows, and a compaction slipping into that window reclaims live frames | **Holds, and is worse than stated.** The release-then-commit shape is not confined to the prepass: `persist_pixel_data` has it too (`_pixel_swap_lock` released at `persistence.py:2404`, `record_blob_ref` at `:2426`), and so does ingest (§3.4). Reproduced as §2's phase D. |
| Correctness needs the lock held across the **whole** `save_all` | **Necessary but not sufficient.** §3.4: `save_all` is one of five sites. |
| Deadlock-safe as audited — three premises | Two hold exactly, one holds in substance and is wrong as literally written. §3.1–3.3. |
| It puts a new lock **above** the documented `_audit_write_lock` → `_memory_lock` pair | **Holds.** §5.2. |
| `compact()` leads with `save(sync=True)`, so the same thread would take it twice unless the acquisition is placed after that save | **Holds.** `session.py:810-815` then `session.py:1002-1013`. The placement #320 prescribes is still the correct one. |
| (from #295, inherited) the failure mode is "reads the wrong bytes or runs off the end of the file" | **Wrong for the residual race, and this is the finding that moves the recommendation.** Measured (§2), the outcome is a hard `RuntimeError` on the next read, not silent wrong pixels. |

---

## 2. The race, reproduced deterministically

`compact_sidecar()` has four phases and the window is different in each,
so it is not one race. Five orderings follow: A–D vary *where* in the
compaction the concurrent write lands, and E varies *which writer* does
it — which is the axis Arm B's scope turns on (Q2). Each ordering below is forced with a pair of
`threading.Event`s from a monkeypatched phase method — the style of
`tests/test_export_flushes_before_it_sweeps.py` — so nothing depends on
timing. A helper thread makes a pixel change and calls
`store.persist_pixel_data(...)` inside the parked window; the main
thread runs `session.compact()`.

Scripts: `scratchpad/race320.py` (phases A, B, C),
`scratchpad/race320d.py` (phase D), `scratchpad/race320e.py` (phase E).
Logs: `race320c.log`, `race320d.log`. Session-local; see the front
matter.

### Phase A — the append lands during `_rewrite_live_frames`

```
   original_first_px: 247
   has_pending_saves_during: False
   intruder: ok
   row_after_append: (16428, 27)      <- into the pre-compaction inode
   compact: ok
   sidecar_size_after: 8214
   row_final: (0, 4107)               <- overwritten by _apply_new_offsets
   readback: RuntimeError: Pixel Loader failed for 1.2.3.0:
       Integrity Error: Pixel data hash mismatch for 1.2.3.0.
       Expected 8027abbc..., got 12a4fceb... Loader(offset=0, length=4107, alg=zlib)
```

The new frame went into the file that `_swap_in_compacted_sidecar` then
renamed to `.compact.bak` and `os.remove`d. `_apply_new_offsets` rewrote
the instance's row to the compacted position of its **pre-change** frame.
The hash — written by `persist_pixel_data` — is the new frame's. Store
and sidecar now disagree, and the disagreement is detected on the next
read.

### Phase B — the append lands between the two `os.replace` calls

```
   has_pending_saves_during: False
   ERROR: Failed to persist pixel swap for 1.2.3.0:
       [Errno 2] No such file or directory: .../race_pixels.bin
   intruder: FileNotFoundError: [Errno 2] No such file or directory
   compact: ok
```

`_swap_in_compacted_sidecar` renames the sidecar away before renaming the
temp file in, and `write_frame` opens **by path, per call**
(`sidecar.py:54`). For that instant the path names nothing. The writer
raises out of `persist_pixel_data`, which logs ERROR and re-raises — so
on the redaction path this surfaces as a failed redaction. Loud, and the
compaction reports success.

### Phase C — the append lands after the swap, before `_apply_new_offsets`

```
   has_pending_saves_during: False
   intruder: ok
   row_after_append: (8214, 27)       <- correct, in the NEW file
   compact: ok
   sidecar_size_after: 8241
   row_final: (0, 4107)               <- overwritten anyway
   readback: RuntimeError: ... Pixel data hash mismatch ... Loader(offset=0, length=4107)
```

The worst-looking of the four: the writer did everything right, wrote a
correct row into the compacted file, and `_apply_new_offsets` overwrote
it from a map computed before the write existed. The fresh frame is
orphaned at offset 8214.

### Phase D — a blob row `_read_blob_index` never saw

A row **INSERTed** after `_read_blob_index` (a concurrent ingest, or a
first `persist_pixel_data` for an instance created during the window) has
an id that is not in `updates`, so `_apply_new_offsets` leaves it alone
— pointing into the pre-compaction layout of a file that is now smaller:

```
   size_before: 98370
   row_at_append: (98370, 39)
   compact: ok
   size_after: 32790
   row_final: (98370, 39)
   points_past_eof: True
   readback: RuntimeError: Pixel Loader failed for 1.2.3.NEW:
       Integrity Error: Failed to read/decompress frame for 1.2.3.NEW:
       Incomplete read from sidecar. Expected 39, got 0.
```

### Phase E — the intruder is `save(sync=True)`, not `persist_pixel_data`

Phases A–D all write through `persist_pixel_data`, which is **not** a
site #320's Arm B covers, so on their own they would leave the reader
thinking Arm B closes nothing. It does not: the same window is reachable
through `save_all`. Here the helper thread calls `set_pixel_data()` and
then `session.save(sync=True)` inside the parked `_rewrite_live_frames`,
so the frame is appended and its row committed by `save_all`:

```
   size_before: 98370
   intruder: ok
   row_after_save: (98370, 39)        <- into the pre-compaction inode
   pending_after_intruder_save: False
   compact: ok
   size_after: 32790
   row_final: (0, 16395)              <- overwritten by _apply_new_offsets
   points_past_eof: False
   readback: RuntimeError: Pixel Loader failed for 1.2.3.0:
       Integrity Error: Pixel data hash mismatch for 1.2.3.0.
       Expected cf606a7d..., got 0f83dd3e... Loader(offset=0, length=16395, alg=zlib)
```

Same window, same outcome, different way in. This is the ordering Arm B
as #320 prices it **would** close, and it is why Q2 says "one of five",
not "none".

### What this establishes

1. **The race is real and reproducible on demand**, in five distinct
   orderings, and a test can be written for it (§9).
2. **`has_pending_saves()` returns `False` in every one of them** — Q3,
   and phase E measures the half that was otherwise only inspection:
   a `save(sync=True)` that runs start to finish inside the window
   leaves the guard reading `False`, because it runs `save_all` on the
   caller's thread and never enters the manager's queue or in-flight
   set. `persist_pixel_data` is not a save at all. The #295 refusal
   cannot fire for the population that reaches the window.
3. **The outcome is loud data loss, not silent wrong data.** Four of the
   five phases end in an exception the caller cannot miss; the fifth
   (B) raises at the writer. Nothing here reads plausible-looking wrong
   pixels back into an export. The `_pixel_hash` written alongside every
   frame is what makes it loud, and it is loud after a reopen too,
   because `record_blob_ref` stores the hash and `_apply_new_offsets`
   rewrites only `offset` and `length`.

   **This holds for waveforms too, and it was worth checking rather than
   assuming.** `compact()` rewires waveform loaders as well as pixel ones
   (`wave_updates` from `get_blob_refs('waveform')`), and `persist_blob`
   (`persistence.py:2222`) is site #3 in §3.4's table — so the waveform
   half reaches the same window. `SidecarWaveformLoader.read_raw()`
   verifies a sha256 over the frame and raises
   `ValueError: Waveform integrity check failed: expected …, got …`
   (`io_handlers.py:2922-2928`), and the hash is armed from the blob
   row (`persistence.py:915,920`), not from a field a rewrite touches.
   Different exception type from the pixel path's
   `RuntimeError: Integrity Error`, same property: detected, not
   swallowed. Had this check been absent, the residual race would have
   been *silent wrong samples* for waveforms and the recommendation in
   §8 would not stand as written.

Point 3 is a correction to what this issue inherits from #295 ("reads the
wrong bytes or runs off the end of the file"). #295 was describing the
**torn rebind**, which was silent and which #295 closed. The residual is
a different failure with a different severity, and costing it as though
it were the torn rebind over-rates it.

**What is genuinely lost.** One instance's newly written pixels, plus an
orphaned frame in the sidecar that the next compaction reclaims. The
instance's row points at its previous frame and every read of it raises
until something rewrites it. That is data loss; it is just not
undetectable data loss.

---

## 3. #317's deadlock audit, re-checked premise by premise

#317's audit was: *"no pixel writer holds `_memory_lock` while touching
pixel state — all three `persist_pixel_data` call sites are in
`services.py`, which contains no `_get_connection` block, and
`_prepare_pixel_frames` runs before the connection opens."* Checked
against `e484bee` rather than taken on trust.

### 3.1 "`services.py` contains no `_get_connection` block" — TRUE

```
grep -n "_get_connection" isocenter/services.py   ->   (no matches)
```

### 3.2 "all three `persist_pixel_data` call sites are in `services.py`" — TRUE for production, FALSE as literally written

In `isocenter/`, the call sites are exactly three, all in `services.py`:
`:475`, `:578`, `:831`. Repo-wide, ten test files call
`store.persist_pixel_data(...)` directly, including
`tests/test_compact_rewiring_is_locked.py:44` and
`tests/test_compaction.py:36`. Those callers hold no lock, so the
deadlock argument is unaffected — but the sentence as written is not
true of the tree and should be scoped to `isocenter/` when it is
restated.

### 3.3 "no pixel writer holds `_memory_lock` while touching pixel state" — TRUE, and for a stronger reason than the audit gives

`_memory_lock` exists only on a `:memory:` store and is acquired at
**exactly one place**: `persistence.py:738`, inside `_get_connection`.
So the premise reduces to "no pixel writer is inside a `_get_connection`
block while holding `_pixel_swap_lock`". Both writers satisfy it by
construction:

* `persist_pixel_data` (`:2370-2415`) takes `_pixel_swap_lock`, reads,
  hashes, `write_frame`s, rebinds the loader, and **releases** before
  `record_blob_ref` at `:2426` — with a comment saying exactly why
  (*"never hold a thread lock across a sqlite write that can wait out the
  busy timeout"*).
* `_persist_pixels` (`:2980`) runs inside `_prepare_pixel_frames`, which
  `save_all` calls **before** `with self._get_connection()`
  (`persistence.py:2479-2482`). No connection exists yet.

One further site the audit does not name and which is fine:
`Session._apply_redaction_outcomes` (`session.py:2860-2864`) takes
`_pixel_swap_lock` around a loader rebind and does no sqlite work and no
`persist_pixel_data` call inside it.

**Verdict: #317's audit holds today.** It holds for the specific lock
`_pixel_swap_lock`, against the specific pair `_audit_write_lock` →
`_memory_lock`. It does **not** establish anything about the lock Arm B
would add, because that lock does not exist yet; §5.2 does that work
separately.

### 3.4 The premise #317's audit does not contain, and which decides Arm B's scope

Every sidecar-write-then-commit-row sequence in production code:

| # | Site | Writes the frame | Commits the row |
| --- | --- | --- | --- |
| 1 | ingest, pixels | `io_handlers.py:1442` (`sidecar_manager.write_frame`) | later, in the store walk |
| 2 | ingest, waveform | `io_handlers.py:1622` | `io_handlers.py:1636` (`record_blob_ref`) |
| 3 | `SqliteStore.persist_blob` (waveform) | `persistence.py:2222` | `persistence.py:2224` |
| 4 | `SqliteStore.persist_pixel_data` (redaction swap) | `persistence.py:2404`, inside `_pixel_swap_lock` | `persistence.py:2426`, **outside** it |
| 5 | `SqliteStore._persist_pixels` (save prepass) | `persistence.py:3039` | inside `save_all`'s transaction |

#320's Arm B covers **only #5**. Every phase in §2 corrupts through #4;
phase D's real-world twin is #1 or #2. This is the single most important
correction in this spec: **a lock around `save_all` does not close the
race #320 is about.** A correct Arm B is a *sidecar-generation lock*
taken by all five, and by `compact_sidecar`.

---

## 4. What compaction actually costs

`scratchpad/bench320.py` and `bench320b.py`. Incompressible frames
(`default_rng(7)`, uint8), 512 KB each, half orphaned before compacting
so there is real work. Phase timings by monkeypatching the four private
methods. Logs: `bench320-400x200MB.log`, `bench320-4000x2GB.log`.

### 4.1 Compaction

| corpus | sidecar | `compact_sidecar()` total | `_read_blob_index` | `_rewrite_live_frames` | swap | `_apply_new_offsets` |
| --- | --- | --- | --- | --- | --- | --- |
| 400 inst / 210 MB | 209.8 → 104.9 MB | **0.027 s** | 0.002 s | 0.022 s (81%) | 0.000 s | 0.002 s |
| 4000 inst / 2.10 GB | 2.10 → 1.05 GB | **0.298 s** | 0.004 s | 0.215 s | 0.000 s | 0.005 s |

The unprotected window — rewrite + swap + database update, i.e. the span
Arm B's lock would cover on the compaction side — is **0.024 s** and
**0.221 s** respectively. Both work out at ~4.9 GB/s over the *live*
bytes, so the shape is `live_bytes ÷ storage_throughput` and nothing
else: `_read_blob_index` and `_apply_new_offsets` are flat in the corpus
size at a few milliseconds.

### 4.2 What a save costs, for the "serialized during compaction" half

| save | duration |
| --- | --- |
| `save_all`, 400 dirty instances, arrays resident | **0.072 s** |
| `save_all`, 400 dirty instances, non-resident | **0.008 s** |

Both are smaller than the compaction they would queue behind. So on this
hardware Arm B's *throughput* cost is close to nil: a save that arrives
during a 2 GB compaction waits ~0.22 s for work that itself takes ~0.07 s.

### 4.3 Read these numbers with their scope attached

Three caveats, because quoting 0.298 s for a 2 GB compaction without them
would be the kind of claim CLAUDE.md warns about.

1. **`_rewrite_live_frames` does no `fsync`** (`persistence.py:3540-3567`
   — plain buffered `open(..., "rb")` / `open(..., "wb")`), and the page
   cache was warm from writing the corpus moments earlier. 4.9 GB/s is a
   local-SSD-with-warm-cache figure, not a storage figure.
2. **It scales linearly with live bytes**, which the two points confirm
   (105 MB → 0.022 s; 1.05 GB → 0.215 s, the same rate). The honest model
   for other hardware is `live_bytes ÷ throughput`. On network storage at
   100 MB/s, a 1 GB live set is **~10 s**; at 50 MB/s, **~21 s**. That is
   the same storage-throughput argument #287 made for the SQLite write
   lock, arriving at a different lock.
3. **`_SHUTDOWN_JOIN_TIMEOUT_S = 30.0`** (`persistence_manager.py:29`).
   §6.3 is why that constant matters to Arm B.

---

## 5. Arm A — document and accept the window

Keep #295's refusal and the per-instance `_pixel_swap_lock`. Write the
residual down as known and accepted.

### 5.1 What it costs

The race stays, in all five orderings of §2. Quantified:

* **Width**: the whole of `_rewrite_live_frames` + swap +
  `_apply_new_offsets` — **0.024 s** at 200 MB and **0.221 s** at 2 GB
  measured, `live_bytes ÷ throughput` in general (§4.3).
* **Reachability**: only from a caller already violating `compact()`'s
  documented PRECONDITION — a second thread saving or redacting during a
  compaction. A single-threaded caller cannot reach it at all.
* **Blast radius**: one instance per overlapping write. One orphaned
  frame, reclaimed by the next compaction.
* **Detectability**: **loud**. `RuntimeError: Integrity Error` on the next
  read of that instance, before and after a reopen (§2). Phase B raises
  at the writer.
* **Detectability, honestly**: the error arrives at a *distance* — at
  export or verify time, not at compaction time — and `compact()` reports
  success. The user sees a working compaction and a broken instance
  later, with nothing tying the two together. That is the real cost of
  Arm A and it is not zero.

### 5.2 What Arm A does not cost

No new lock, so no new position in the documented order, and nothing for
#26's freeze to inherit (§7). No change to the persistence manager's
liveness. No change to `__getstate__`.

---

## 6. Arm B — a coarse lock, correctly scoped

### 6.1 What it actually has to be

Not "a lock across `save_all`". A **sidecar-generation lock**, held by
all five writers of §3.4 across their write-frame-*and*-commit-row span,
and by `compact()` across `compact_sidecar()` plus
`_rewire_sidecar_loaders`, acquired **after** `compact()`'s leading
`save(sync=True)` so the same thread does not take it twice.

Site 1 (ingest's pixel frame at `io_handlers.py:1442`) is the awkward
one: it runs in `ingest_worker`, which may be in a **spawned
subprocess**. A `threading.Lock` on the store does not cross a process
boundary — `__setstate__` gives the child a *new* lock
(`persistence.py:708-726`). So either the ingest sites are excluded and
Arm B is documented as not covering a concurrent ingest, or the lock has
to be an `fcntl` file lock like `write_frame`'s own. Neither is bad;
neither is what #320 describes. This needs settling before implementation
and is the substance of Q2.

### 6.2 Position in the lock order

New order: `_save_lock` → { `_pixel_swap_lock` → sidecar } →
`_get_connection` → `_memory_lock`. The documented pair
`_audit_write_lock` → `_memory_lock` is unchanged and no cycle is
created: nothing takes `_save_lock` while holding `_audit_write_lock` or
`_memory_lock` (the audit-writer path is `_drain_and_write` →
`_audit_write_lock` → `_get_connection`, and it never saves). So #317's
audit **extends** cleanly — but the extension is a fresh argument about a
lock that does not exist yet, not a corollary of the audit it did.

The real cost here is not deadlock risk. It is that **the top of the
order becomes a lock, and #26 freezes the shape of that argument** (§7).

### 6.3 The liveness cost #320 does not name

The lock is held for the compaction. The persistence manager's worker
blocks on it mid-`save_all`. If a `close()` lands in that window,
`shutdown()` joins with `_SHUTDOWN_JOIN_TIMEOUT_S = 30.0`
(`persistence_manager.py:29, :679`), and #314 treats a timed-out join as
a wedged worker: `_drain_recoverable_saves()` then runs a reconciliation
`save_all` on the closing thread — which blocks on the same lock. **A
compaction longer than 30 s misfires the whole #313/#314/#315
machinery**, turning a healthy compaction into the shape those three
issues exist to detect.

Measured, that needs a live set of ~1.5 GB at 50 MB/s, or ~3 GB at
100 MB/s — network storage, not this laptop, where 2 GB compacts in
0.298 s (§4.1). It is not hypothetical for the 100 GB+ datasets the
memory design targets. It is exactly the shape #287 fixed for the SQLite
write lock: a lock whose hold time is set by storage throughput, sitting
under a fixed timeout.

### 6.4 Mechanical costs

* The lock must be added to `SqliteStore.__getstate__`'s
  `keys_to_remove` and recreated in `__setstate__`. Miss it and **every
  pickle of a store raises `TypeError: cannot pickle '_thread.lock'
  object`** — the trap `persistence.py:686-698` documents for #218.
  `tests/test_save_redact_race.py:241`
  (`test_the_store_still_pickles_with_its_pixel_swap_lock`) is the test
  that would go red, and it needs a twin for the new lock.
* Reentrancy: `compact()` calls `save(sync=True)` and then acquires; if
  any other path ever acquires and then saves, a plain `Lock` deadlocks
  outright. Either audit that none does and keep a plain `Lock` (a plain
  lock is preferable — it fails loudly rather than hiding a nesting), or
  use `RLock` and accept that it hides one.

### 6.5 What Arm B buys

**Correctly scoped** (§6.1), all five orderings of §2 become impossible
rather than unlikely, for writers in the same process. **As #320 prices
it** — one lock around `save_all` — only phase E's route closes; phases
A–D reach the window through `persist_pixel_data` and are untouched. It does not close a second *process*
writing to the same store unless the lock is `fcntl`-based (§6.1), and
`SidecarManager` already uses `fcntl` for exactly that reason.

---

## 7. #26, weighed honestly

#26 is the v1.0.0 API freeze on `DicomSession`. The brief's framing is
that a lock ordering added now is one the freeze inherits, so "cheap to
add later" is probably false. That is **half right and worth splitting**.

**Not frozen:** `_save_lock` would be a private attribute of
`SqliteStore`, which #26 does not name. Adding or removing it later
breaks no documented interface.

**Effectively frozen:** the **lock-order invariant**. This repo treats
`_audit_write_lock` → `_memory_lock` as a documented contract that new
code is audited against — #218, #274, #280, #295, #313–#316 all argue in
its terms. Putting a third lock above it makes every future concurrency
argument a three-level one, and that is inherited by everything after
1.0 whether or not the attribute is public.

**Also effectively frozen:** the **latency contract**. Saves that used to
overlap the start of a compaction would block. That is observable, and
after 1.0 a change in that direction is a behaviour change users can
have built on.

**But the freeze cuts the other way too**, and this is what the brief's
framing misses: the correctly-scoped Arm B is **five call sites plus a
cross-process question** (§6.1), not the one-method change #320 costs.
Landing that under milestone pressure, in the same milestone as four
other bunches, is how a lock ordering gets frozen with a premise nobody
re-checked. #317's own audit is the cautionary example: sound, and one of
its three premises was already not true of the tree as written (§3.2).

---

## 8. RECOMMENDATION

*This is a recommendation, for the owner to accept or reject.*

**Take Arm A, with three amendments — and file the correctly-scoped
Arm B as a post-1.0 item rather than building it now.**

The three sentences with the numbers behind them:

> The window is **0.024 s at 200 MB and 0.221 s at 2 GB** on measured
> hardware (`live_bytes ÷ throughput` in general), it is reachable only
> by violating a precondition `compact()` has always documented, and —
> the finding that moves this — its outcome is **not** the silent wrong
> pixels inherited from #295 but a hard `RuntimeError: Integrity Error`
> on the next read of one instance, in four of five forced orderings,
> surviving a reopen, and for waveforms as well as pixels. Arm B as #320
> costs it (one lock around `save_all`) closes **one of the five ways in**
> — measured, phase E — and leaves the other four open, because
> `save_all` is one of **five** separate write-frame-then-commit-row
> sites (§3.4) and the redaction and ingest routes reach the same window
> without it; the version that does work is
> a sidecar-generation lock over all five plus a cross-process question
> at the two ingest sites, which is a materially larger change than the
> issue prices. Against that, Arm B's own measured benefit is a
> throughput cost of ~nil (a 0.072 s save queueing behind a 0.221 s
> compaction) but a liveness cost that is not: the lock's hold time is
> storage-bound and sits under a **fixed 30.0 s** shutdown join, so on
> network storage a ~1.5 GB live set turns a healthy compaction into the
> wedged-worker shape #313/#314/#315 exist to detect.

The three amendments, all cheap, all inside Arm A:

1. **Correct the over-read claim (Q3).** `compact()`'s docstring and the
   comment at the refusal read as though the enforced half covers *a
   save* in flight, and only one kind of save is in view.
   `has_pending_saves()` sees neither a concurrent `save(sync=True)` from
   another thread nor a redaction's `persist_pixel_data` — measured
   `False` in all five orderings, phase E included. Say what the guard
   actually covers: a save queued on the persistence manager. This is a comment fix and it
   is the difference between an accepted risk and a wrong claim.
2. **Land §9's test as a characterization test.** A race nobody can
   reproduce is a race nobody can verify fixed, and this one reproduces
   deterministically. It also pins the failure *mode* — the integrity
   error rather than silent bytes — which is the load-bearing half of
   this recommendation and the thing that would quietly stop being true
   if `_pixel_hash` were ever dropped from the loader.
3. **File the sidecar-generation lock**, with §3.4's table and §6.1's
   cross-process question in the issue body, so the next person costs the
   real change rather than the one #320 describes.

**The case against this recommendation, stated fairly**, because the
owner should see it: the measured throughput cost of Arm B is
approximately zero, #26 does mean the ordering gets cheaper to add now
than later, and "loud data loss at a distance, with the compaction
reporting success" is a low bar for a de-identification tool. If you
weigh the distance between the corruption and its symptom more heavily
than I do — and it is a defensible weighting, since it is the same
distance #153 and #250 were both about — Arm B is the right call, and
§6.1's scope question is then the first thing to settle.

---

## 9. The test, whichever arm is taken

Named `tests/test_compaction_races_a_concurrent_write.py`. It is a
characterization test under Arm A and the red-then-green test under
Arm B; the assertions differ, the scaffolding does not.

**Style**: `tests/test_export_flushes_before_it_sweeps.py` — a helper
thread on a bounded daemon, ordering forced by a pair of
`threading.Event`s, the helper's exception kept rather than swallowed.

**Scaffolding.** Build a small graph by hand (four to six instances,
64×64 or 128×128 `uint8`), `persist_pixel_data` each,
`save(sync=True)`, then drop half from the graph and save again so there
are orphans to reclaim. Monkeypatch one of `SqliteStore`'s four phase
methods to `set()` a `parked` event and `wait()` on `released`. Run
`session.compact()` on the main thread; the helper waits for `parked`,
does `set_pixel_data(new)` + `persist_pixel_data`, then sets `released`.

**Two traps that cost real time when this was built:**

* **The new content must be produced inside the window.** `compact()`
  leads with `save(sync=True)`, so a `set_pixel_data` done *before*
  `compact()` is captured by that save and the "intruder" appends a
  duplicate of a frame the index already knows about. The first version
  of this reproduction read green for exactly that reason.
* **Read back through the loader, not from memory.**
  `persist_pixel_data` does not unload, so `get_pixel_data()` returns the
  resident array and measures nothing. Call `discard_pixel_data()` first
  (not `unload_pixel_data()`, which refuses a diverged array — #293).

**Assertions, per phase.** All five are worth having; each has a distinct
mechanism, and E is the one that distinguishes the two Arm B scopes.

| Phase | Patch | Assert (Arm A, characterization) | Assert (Arm B) |
| --- | --- | --- | --- |
| A | `_rewrite_live_frames` | the row is the compaction map's value, not the intruder's; the read raises `Integrity Error`/hash mismatch | the read returns the new pixels |
| B | `_swap_in_compacted_sidecar`, parking between the two `os.replace` calls | the writer raises `FileNotFoundError` | the writer succeeds |
| C | `_apply_new_offsets` | the intruder's correct row is overwritten; the read raises | the row survives |
| D | `_rewrite_live_frames`, intruder persists an instance whose blob row did not exist at `_read_blob_index` | `offset + length > os.path.getsize(sidecar_path)`; the read raises `Incomplete read from sidecar` | the row is inside the file and reads back |
| E | `_rewrite_live_frames`, intruder does `set_pixel_data` + **`session.save(sync=True)`** rather than `persist_pixel_data` | the row is the compaction map's value; the read raises `Integrity Error`/hash mismatch | the read returns the new pixels — **only if the lock is held through `_rewire_sidecar_loaders`**, see below |

**Assert `has_pending_saves() is False` inside every window.** That is
the assertion that pins Q3 and stops someone reading the #295 refusal as
covering this. Phase E is the one that makes it a measurement rather
than an inspection: a `save(sync=True)` runs to completion inside the
window and the guard still reads `False`.

**Phase E's Arm B column is reasoned, not measured, and the reasoning
has a release point in it.** Arm B cannot be run without the production
change, so this is an argument and it is stated so the implementer can
check it rather than inherit it. `session.compact()` is
`save(sync=True)` → guard → `compact_sidecar()` →
`_rewire_sidecar_loaders(updates, wave_updates)`. **If the lock were
released at the end of `compact_sidecar()`**, phase E's intruder would
run in the gap before the rewiring: its `save_all` appends to the *new*
sidecar (correct), commits a correct row, and leaves the in-memory
loader on the new offset — and `_rewire_sidecar_loaders` then overwrites
that loader from a map computed before the write existed. Database row
right, resident loader wrong, and the readback after
`discard_pixel_data()` raises the same hash mismatch; only a reopen
would read correctly. That is a *different* corruption from phase E's,
not an absence of one, and it is why §6.1 specifies the hold as
`compact_sidecar()` **plus** `_rewire_sidecar_loaders`. An implementer
who scopes the lock to `compact_sidecar()` alone will see this test stay
red and conclude Arm B does not work. It does; the scope was wrong.

**Phase E's helper needs its own `set_pixel_data` inside the window for
the same reason as the others**, and one more: `session.save(sync=True)`
from the helper is a second `save_all` overlapping the compaction's
already-completed one, so the frame must be new to that second save or
there is nothing for it to append.

**A waveform variant is worth one test, not four.** Build an instance
with a Waveform Sequence, `persist_blob(inst, 'waveform', samples)`
inside phase A's window, and assert the read raises
`ValueError: Waveform integrity check failed` (Arm A) / returns the new
samples (Arm B). It is a different loader with a different exception
type, and §2's "loud, not silent" claim is only as good as its weakest
data path.

Do **not** write these against `session.compact()`'s front door without
the phase patches: the front door's leading `save(sync=True)` makes the
window unreachable from a queued save, which is the same reason #317's
own #295 test forces `has_pending_saves()` true instead of queueing one.

---

## 10. Filed rather than fixed

* **Q4 — `SidecarManager._lock` is never acquired.** Three comments
  document a lock order over it. Either take it in `write_frame` and
  `read_frame` (making the documented order real) or delete it and the
  three comments. Do not leave a documented invariant with no code
  behind it: that is precisely the "prose that was true once" this repo
  keeps finding.
* **`compact_sidecar` rebinds `self.sidecar = SidecarManager(...)`**
  (`persistence.py:3463`). A concurrent writer that already read
  `self.sidecar` writes through the previous manager object. Harmless
  today only because `write_frame` opens by path per call and the Python
  lock is unused — i.e. it is harmless *because of* the defect above.
  Fixing one without the other introduces a real bug.
* **`persist_pixel_data`'s `except`/`raise e` (`persistence.py:2436`)**
  re-raises with `raise e` rather than a bare `raise`, truncating the
  traceback at that frame. Cosmetic; noticed while reading phase B's
  output.
