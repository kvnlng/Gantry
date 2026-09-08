# The Sidecar Concurrency Contract: a gate, a pass-lock, and what 1.0 promises

**Date:** 2026-09-08
**Status:** Determinations MADE, with evidence. The design in §8 is the
recommendation; §0.2 lists the calls that are the owner's, each as
options with the recommendation first. No production code was changed;
two throwaway mutations were applied and reverted (`git diff` clean at
the end, §14).
**Tracking:** #368 (the whole concurrency contract; the v1.0.0 tag waits
on it per the owner's comment of 2026-09-08,
https://github.com/kvnlng/Isocenter/issues/368#issuecomment-5584761918),
#376 (three packaging/hygiene items), #373 (the loader's padding
tolerance). Rests on the 2026-09-07 spec
(`2026-09-07-sidecar-write-compaction-serialization.md`, "Decision
OPEN"), on the owner's ruling of 2026-09-07 that `redact()` concurrent
with `compact()` MUST be safe in 1.0, on #320, #366, #295, #280, #250,
#183, #314. Weighed against #26 / #379 (the v1.0.0 API freeze).
**Supersedes in part:** the 2026-09-07 spec's Status line and §11 ("Do
not build the gate until Q1 and Q2 are answered" — Q1 was answered by
the owner the same day), §12.3's "The scaffolding does not change"
(**false, measured**, §10), and §13's "Reasoned, not reproduced" for
§7.2 (now reproduced, §5). Those clauses are marked in place in that
file; nothing else in it was found wrong.
**Base:** `main` at `696a208` (release 0.9.3). Every `path.py:N` below
is that commit. Nothing grades these line numbers
(`tests/test_source_citations.py` excludes `docs/superpowers/`); the
tests in §9 carry the content-pins instead.
**Measured with:** `/Users/kevin/Developer/Isocenter/.venv/bin/python`
(CPython 3.14.6, GIL build) on macOS 25.6 / APFS / local SSD, warm page
cache, in worktree
`/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-aa6c35271afe3ca34`.
Every probe and every pytest ran as
`env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<worktree> .venv/bin/python -u ...`
and printed `isocenter.__file__` **first**; it resolved to
`<worktree>/isocenter/__init__.py` every time. Probes that spawn workers
are real files with `if __name__ == "__main__":`. The probes
(`count_sites.py`, `probe_flock.py`, `probe_s7.py`,
`probe_double_persist.py`, `probe_373.py`, `probe_front_door.py`,
`probe_memory_child.py`, `probe_scaffold_deadlock.py`) live in a
session-local scratchpad and are **not in the repo**; they are named so
numbers can be attributed. The reproductions meant to survive are §9's
tests. The full suite ran twice: once under the throwaway #373 bound
(**1603 passed in 407 s**, §7) and once clean (§14).

---

## 0. Determinations, and what is the owner's to decide

### 0.1 One line each

1. **Q3, gate placement.** Six `write_frame` sites on current main, by
   grep and by AST — `io_handlers.py:1754`, `:1875`, `:1981`,
   `persistence.py:2335`, `:2561`, `:3227` — plus `compact()`. Placement
   as the 2026-09-07 spec §6: at each site, spanning append **and** row
   commit, always outside `_pixel_swap_lock`, never inside `write_frame`.
   Confirmed, not changed (§1).
2. **Q5, sqlite amplification.** Sqlite cannot leave the gate (phases D
   and E prove the gate must span append→commit). Bounded by a module
   constant `_SIDECAR_GATE_TIMEOUT_S = 180.0` sitting in the #280
   inequality `120 < 180 < 240 < 300`; expiry raises `RuntimeError`
   naming the lock file and the constant; it reaches redaction as a
   `RedactionError` with an ERROR audit row, ingest as an ERROR audit
   row, a background save as a logged failure with the instances still
   dirty, and `compact()` as a raise. The population that can wait out
   a long hold shrinks to saves from other threads, because redaction
   workers never wait behind a compaction at all under §8 (§2).
3. **#26 consequence.** Lock order is an internal invariant, not frozen
   surface: every name is private and lives in implementation files.
   Pinned by a recording-wrapper test plus one documentation sentence.
   What #379 must record: `compact()` **raising** during an open pass and
   `redact()`/`ingest()` **waiting** behind a running compaction are
   observable behaviours of frozen public methods (§3).
4. **§7, orphan predicate.** Reproduced on current main at the store
   layer *and through the front door* (`session.compact()` mid-`redact()`
   on the processes path returned success and deleted all three redacted
   rows). Fix: **not** a predicate change; a second stable lock file
   `<sidecar>.pass.lock` held `LOCK_SH` by `Session.redact()` and
   `Session.ingest()` for the whole pass, taken `LOCK_EX|LOCK_NB` by
   `compact()` under the gate, which refuses with `RuntimeError` while
   any pass is open. What it breaks: `compact()` during a pass raises
   instead of silently corrupting; a pass starting during a compaction
   waits (bounded) (§4).
5. **§7.2, loader-offset span.** Reproduced (DB row moved to 8214, the
   parent bound the child's loader at 12321, read raised). Closed by the
   same pass-lock: no compaction can run between a worker's commit and
   the parent's apply. **New finding:** one `execute_redaction_task`
   appends **two** frames; the `finally` persist at `services.py:576-578`
   is redundant on every path it runs on and is deleted in this PR (§5).
6. **#376 / #373.** Drop `OS Independent`, add `Operating System ::
   POSIX`, move `import fcntl` to module scope, one bidirectional
   packaging test. `*.lock` in `.gitignore` before any lock file exists.
   `:memory:` teardown unlinks its temp sidecar and lock files behind an
   ownership flag that pickling drops. #373: bound in elements
   `target_size <= arr.size <= target_size + 1`, both directions raise
   `RuntimeError("Integrity Error: ...")`, the 1-D return goes; no
   fixture depends on the wide tolerance (1603 passed under the bound);
   independent of #368's hash (§6, §7).

### 0.2 Open questions for the owner (options; recommendation first)

**A. What "safe" means for `compact()` during a `redact()`.**
- **A1 (recommended): refuse loudly.** `compact()` raises
  `RuntimeError` while a pass is open; a pass starting during a
  compaction waits. No schema change, crash-released by the kernel,
  closes §7, §7.2 and the ingest variant at once. It is *safe*; it is
  not *interleaving*.
- A2: interleave. Pending-UID table so the predicate keeps
  worker-committed rows, plus the parent re-reading offsets under the
  gate in `_apply_redaction_outcomes`. Costs a schema column, a sqlite
  read per applied mutation, a crash-leak (rows pending forever), and
  still needs the gate. Rejected in §4.3 unless interleaving is a
  product requirement.

**B. The gate deadline.**
- **B1 (recommended): `_SIDECAR_GATE_TIMEOUT_S = 180.0`**, a module
  constant like `_SQLITE_BUSY_TIMEOUT_S`, no env var, pinned by a
  packaging-contract test to `120 < it < 240` (and `< 300`). Tolerates a
  compaction of ~800 MB live on 100 MB/s storage behind a stuck sqlite
  writer; a healthy compaction on this SSD holds the gate 0.217 s/GB.
- B2: a larger value (e.g. 230 s, the most the inequality allows) to
  tolerate longer compactions on network storage. Buys 50 s; costs
  nothing structural. Either is fine; 180 is the round number.
- B3: no deadline (blocking `flock`). Rejected: a stuck holder becomes a
  silent stall, which is exactly #250's shape.

**C. A background save that expires at the gate.** Today a failed
`save_all` on the worker thread is `logger.error("Background save
failed: ...")` and the instances stay dirty; no audit row.
- **C1 (recommended): leave it**, and say so in the docstring. It is the
  same channel every other background-save failure uses; the next
  `save()` retries; `close()`'s unsaved-instances warning names them.
- C2: add an ERROR audit row for a gate expiry on the save path. One
  more place the audit log means something new; file it rather than
  build it here.

**D. Ingest under the pass-lock.**
- **D1 (recommended): `ingest()` holds `LOCK_SH` too.** One `flock`
  per ingest call; ingest ∥ `compact()` was already precondition-
  violating and now refuses loudly instead of reclaiming freshly-ingested
  rows (sites 1–3 write blob rows before any `instances` row exists).
- D2: declare ingest ∥ `compact()` unsupported and leave it to the
  precondition. Cheaper by one flock; leaves the same predicate hole
  open on the path #368's own body named first.

**E. A pre-existing failure found on the way (not this bunch): `:memory:`
stores cannot redact on the processes path.** Measured: on a GIL build
with `ISOCENTER_FORCE_PROCESSES=1`, `redact()` on a `:memory:` session
raises `RedactionError ... OperationalError: no such table:
instance_blobs`, because `__setstate__` (`persistence.py:719`) hands the
child `_memory_conn = None` and `_get_connection` then opens a *fresh,
empty* `:memory:` database. Processes are the default on 3.12, the
`python_requires` floor. Options: file it as a 1.0 item (recommended —
it is a front-door failure on the floor version; `:memory:` should
either force threads or refuse), or document `:memory:` as
threads-only. Not in this PR; named here because the reviewer will
trip over it if a §9 test is written against `:memory:`.

---

## 1. Q3 — gate placement on current main

### 1.1 Six sites, counted twice

`grep -n "write_frame(" isocenter/` and an `ast` walk
(`count_sites.py`, counting `Call` nodes whose `func.attr ==
"write_frame"` under `isocenter/`) agree on six call sites, all on the
`SidecarManager` instance; `read_frame` is not a writer and takes no
lock. `fcntl` is imported exactly once, inside `write_frame`
(`sidecar.py:46`).

| # | Site | Process | Append | Row commit | Under `_pixel_swap_lock`? |
| --- | --- | --- | --- | --- | --- |
| 1 | ingest pixel frame, `io_handlers.py:1754` (aggregation loop, parent) | parent | :1754 | per-result, same iteration | no |
| 2 | ingest nested pixel frame, `io_handlers.py:1875` | parent | :1875 | :1891 | no |
| 3 | ingest waveform frame, `io_handlers.py:1981` | parent | :1981 | :1995 | no |
| 4 | `persist_blob`, `persistence.py:2335` | caller (worker on the processes path for waveforms) | :2335 | `record_blob_ref` :2337 | no |
| 5 | `persist_pixel_data`, `persistence.py:2561` | caller (redaction **worker**, spawned) | :2561 (inside swap lock :2527) | `record_blob_ref` :2583 (outside) | append yes, commit no |
| 6 | `_persist_pixels` in `save_all`, `persistence.py:3227` | worker thread or `save(sync=True)` caller | :3227 (inside swap lock :3168) | :2924, inside `save_all`'s transaction | append yes, commit no |

The 2026-09-07 spec's correction — the ingest worker (`io_handlers.py:
1401-1402`) produces *bytes*, and the parent's aggregation loop is what
appends — holds on this base. Sites 1–3 are parent-only; sites 4 and 5
run in whatever process calls them, which for `execute_redaction_task`
is a **spawned** child on GIL builds; site 6 runs on the persistence
worker thread or the caller's thread.

### 1.2 Placement, confirmed

Exactly as the 2026-09-07 spec §6, restated so this file stands alone:

- **`SqliteStore._sidecar_gate`** = a `threading.Lock` (in-process
  fairness) followed by `fcntl.flock(fd, LOCK_EX)` on a fresh fd opened
  on the stable path `<sidecar>.lock` (cross-process, inode-stable
  across `os.replace`). Released in reverse: close the fd, release the
  lock. One fd per acquisition, opened *after* the thread lock is held,
  so two threads in one process never hold two fds on the file at once
  (the self-deadlock the spec measured).
- **Acquired at the six sites, spanning append and row commit.** Sites
  1–3: around each result's write-then-record pair inside the loop
  iteration (not the whole loop — a 10k-file ingest must not hold the
  gate for its duration). Site 4: around `persist_blob`'s body. Site 5:
  around `persist_pixel_data`'s body, **outside** `_pixel_swap_lock`.
  Site 6: around `save_all` from `_prepare_pixel_frames` through commit,
  which is the whole transaction — phase E (`test_compaction_races_a_
  concurrent_write.py:454-487`) is a row committed after
  `_apply_new_offsets` from a frame appended before `_read_blob_index`,
  and only a gate that spans the commit closes it.
- **Acquired by `Session.compact()`** after `save(sync=True)` and after
  the `has_pending_saves()` refusal, held through `compact_sidecar()`,
  `get_blob_refs('waveform')`, `get_nested_pixel_refs()` and
  `_rewire_sidecar_loaders()`. Released after the rewire (the spec's
  §6.6: release earlier and the rewire overwrites a correct loader from
  a stale map).
- **Never inside `write_frame`.** `SidecarManager` is stateless by
  #366's design and is called directly by tests and fixture generators
  (`tests/test_pixel_geometry_pipeline.py:963 _one_frame_loader`); a
  "gate is held" assertion there breaks every one of them, and the
  spec's cycle argument (`_persist_pixels` calls `write_frame` under
  `_pixel_swap_lock`; `_rewire_sidecar_loaders` takes `_pixel_swap_lock`
  under the gate) stands. The seventh-site detector is §9.5's AST count
  instead.

### 1.3 Lock order (internal invariant, §3)

```
_sidecar_gate  →  _pixel_swap_lock                      (compact's rewire; sites 5, 6)
_sidecar_gate  →  sqlite  (_memory_lock / file busy-wait)   (every site's row commit)
_audit_write_lock → sqlite                                (unchanged, separate chain)
_sidecar_gate  →  pass-lock, LOCK_NB only                 (compact's refusal, §4)
pass-lock LOCK_SH, taken holding nothing                  (redact/ingest, parent thread)
write_frame's inode flock: leaf                           (unchanged)
```

The gate is the one lock deliberately held across a sqlite write. That
is the exception to the "nothing above `_pixel_swap_lock` waits on
sqlite" reasoning #287 established, and it is the subject of §2.

### 1.4 Rejected

- **Gate inside `write_frame`** (the original brief's placement): two
  fds on one file in one thread self-deadlock; cycle against the rewire.
  Measured by the 2026-09-07 spec (`probe_reentrancy.py`), re-measured
  here (`probe_flock.py` arm *b*).
- **`flock` on the sidecar itself**: inode-bound; a blocked writer wakes
  after the swap and appends into the unlinked inode (spec §2, reproduced
  there; the stable-path lock file is the fix).
- **Gate around the whole ingest loop**: holds the gate for a full
  ingest; a background save behind it would expire at 180 s on any real
  dataset.

---

## 2. Q5 — sqlite amplification

### 2.1 Can sqlite work move outside the gate? No, and here is why.

The hazard the gate closes is *frame appended before `_read_blob_index`,
row committed after `_apply_new_offsets`* (phase D, and phase E for site
6). A row committed outside the gate can land in exactly that window,
and its offset then points into the pre-compaction layout of a smaller
file (`Incomplete read from sidecar`, the #320 characterization). So the
gate spans the commit by construction. "Structurally absent" is
therefore not available for the row commit; what *is* structurally
absent is the population that could wait behind a **long** hold:

- Under §8, a redaction worker never waits behind a compaction: either
  the pass opened first (and `compact()` refuses at once) or the
  compaction opened first (and `redact()` waits at the pass-lock *before
  dispatching any worker*, holding nothing). The spec's "every frame
  writer in every process for 120 s" chain needs a compaction holding
  the gate while workers queue, and §8 makes that ordering impossible.
- What remains is: a save (background or `sync=True`) queued behind a
  compaction, and any writer queued behind a writer stuck in sqlite.

### 2.2 Where the amplification actually is

Every site already writes a sqlite row. A writer stuck behind an
external sqlite lock stalls `_SQLITE_BUSY_TIMEOUT_S = 120.0`
(`persistence.py:175`, applied at `:761`) today, gate or no gate. The
gate changes *serial* stalls into *queued* stalls: waiter 1 gets the gate
after the holder's 120 s error, stalls itself, and waiter 2 behind it
expires at the gate deadline. So the per-caller worst case under the
gate is `min(_SIDECAR_GATE_TIMEOUT_S, 120 s + one frame write)`, and the
error a waiter gets names the gate rather than the database. That is the
amplification: a bounded wait, a differently-named error, on a system
that was already stuck.

### 2.3 The deadline

`_SIDECAR_GATE_TIMEOUT_S = 180.0` in `persistence.py` beside
`_SQLITE_BUSY_TIMEOUT_S`. Acquisition polls `LOCK_EX|LOCK_NB` (the
in-process `threading.Lock` takes `acquire(timeout=remaining)` first)
with a short sleep between attempts (10 ms; the uncontended path takes
the lock on the first `LOCK_NB` and pays 11.6 µs, measured), and raises
on expiry. Why polling: `flock` has no timeout and `signal.alarm` does
not reach non-main threads; a 10 ms poll adds at most 10 ms to a
contended acquisition against a 26 ms hold at 200 MB.

The inequality, with what each bound protects:

```
_SQLITE_BUSY_TIMEOUT_S = 120  <  _SIDECAR_GATE_TIMEOUT_S = 180  <  _WORKER_FAULTHANDLER_TIMEOUT_S = 240  <  faulthandler_timeout = 300  <  Run Tests 1200
```

- **> 120 + a frame write.** A waiter behind a holder legitimately
  waiting out sqlite must not give up first: it would raise a gate error
  that misnames the fault (the database is what is stuck) and, on the
  save path, leave the instances dirty when the holder was seconds from
  succeeding. The 2026-09-07 spec's "below 30 s" (`:1033`, `:574-576`)
  fails this test and is rejected; its reason — `_SHUTDOWN_JOIN_TIMEOUT_S
  = 30.0` — is handled below rather than by shortening the gate.
- **< 240 and < 300.** A stuck gate must error inside both faulthandler
  windows so the dump shows a thread *waiting at the gate* with a stack,
  and the error, not the job cap, ends the test (#280, #250).
- **No env var.** A user-tunable deadline is how `timeout=900.0` went
  unquestioned; tests monkeypatch the constant, and the packaging test
  (§9.7) pins the name into the acquire helper's source with
  `inspect.getsource`, the #280 shape.

### 2.4 What raises, to whom, and whether the audit log sees it

The gate error is `RuntimeError(f"Sidecar gate {lock_path} not acquired
within _SIDECAR_GATE_TIMEOUT_S={deadline:g} s; a compaction or another
writer is holding it")`. One class, one spelling, so every channel below
carries the same text.

| Waiter | Where it surfaces | Audit-observable? |
| --- | --- | --- |
| Redaction worker (site 5, spawned child) | `execute_redaction_task`'s `except` → `RedactionOutcome(ok=False)` → parent `_apply_redaction_rules` raises `RedactionError` after recording | **Yes**, ERROR row (existing path, `test_worker_loss_is_reported.py`) |
| Ingest (sites 1–3, parent loop) | per-result `except Exception` → `_record_failure` (`io_handlers.py:2047`) | **Yes**, ERROR row (`test_ingest_failure_audit.py`) |
| Waveform `persist_blob` from a worker (site 4) | same as the calling task | Yes, via the task |
| Background save (site 6, worker thread) | `persistence_manager` logs `Background save failed: ...`; instances stay dirty; next save retries; `close()` warns with their UIDs | **No** (open question C) |
| `save(sync=True)` (site 6, caller thread) | raises to the caller | No (as today for any save error) |
| `compact()` | raises to the caller before any rewrite (gate is taken before `compact_sidecar()`) | No (as today) |

### 2.5 The liveness trade, stated

A `close()` whose persistence worker is queued behind a compaction longer
than `_SHUTDOWN_JOIN_TIMEOUT_S = 30.0` (`persistence_manager.py:29`) will
have #314's wedged-worker machinery misfire on a healthy compaction.
That is 3 GB live on this SSD or ~300 MB on 100 MB/s network storage.
Today the same ordering corrupts the save instead. Loud-and-late beats
silent-and-wrong; the docstring says so, and the structural fix — a
two-phase compaction that copies live frames *without* the gate and
holds it only for the O(delta) tail — is a follow-up filed under #368's
liveness label, not this PR.

### 2.6 Rejected

- **Sqlite commit outside the gate with a generation check** (commit the
  row, then re-read the sidecar generation and retry if it moved): a
  second answer to "which offsets are current", the same shape the
  `text_index` was (#84), and it does not cover site 6's transaction.
- **A deadline below 30 s** (spec §11): pre-empts a legitimate sqlite
  wait; see 2.3.
- **Env-var deadline**: see 2.3.

---

## 3. #26 — is the lock order frozen surface?

**Internal invariant.** #26's status comment freezes the facade; #379
defines the frozen names (`__all__`'s five, `Session`'s 28 methods,
persistence renders unfiltered). `_sidecar_gate`, `_pixel_swap_lock`,
`_audit_write_lock`, `_memory_lock`, the two lock files and their order
are all private, live in implementation files, and no public method's
signature or return changes. They can be re-ordered after 1.0 without a
major version, provided the observable behaviour below holds.

**Where it is written.** Both: the recording-wrapper test (§9.4) is what
enforces it; one paragraph in CLAUDE.md's "Hybrid storage" section and
`compact()`'s docstring is what tells the next reader why. Not the
public docs: `docs/api/session.md` documents behaviour, not locks.

**What #379 needs to know.** Two behaviours of frozen public methods are
new and observable, and their docstrings must state them before the tag:

1. `Session.compact()` raises `RuntimeError` while a `redact()` or
   `ingest()` pass is open on the same store, from any thread or process
   of this session.
2. `Session.redact()` and `Session.ingest()` block (bounded by
   `_SIDECAR_GATE_TIMEOUT_S`) while a `compact()` is rewriting, and then
   proceed.

Neither is a signature change. Both are contract. #379's list should
carry them as such; the lock names should not appear in it.

---

## 4. §7 — the orphan predicate

### 4.1 Reproduced on current main

**Store layer** (`probe_s7.py`): three persisted instances; one
`regenerate_uid()` + `set_pixel_data` + `persist_pixel_data` (the
worker's exact sequence). `instances` does not know the new UID; the
blob row exists under it. `compact_sidecar()` returned normally, the
sidecar went **12347 → 12321** bytes, the row under the new UID was
gone, and `get_pixel_data()` raised `RuntimeError` (integrity). The
predicate at `persistence.py:3697-3700` (`EXISTS (SELECT 1 FROM
instances i WHERE i.sop_instance_uid = b.instance_uid)`) is what deleted
it.

**Front door** (`probe_front_door.py`, `ISOCENTER_FORCE_PROCESSES=1`):
a file-backed session with three instances under one serial; rules for
that serial; `_apply_redaction_outcomes` patched to materialise every
outcome (so every worker had committed its row under a regenerated UID)
and park; `session.compact()` on a helper thread while parked.
`compact()` **returned success**, the rows at offsets **120/143/166**
were deleted, the parent then bound the three child loaders, and all
three `get_pixel_data()` calls raised. `instances knows the new uids?
[False, False, False]` while parked — the exposure is the whole pass, as
the 2026-09-07 spec §7.1 said.

### 4.2 Why a predicate change is the wrong fix

The predicate is *correct*: a row no `instances` row references is, by
the store's own definition, unreferenced. What is wrong is that during a
pass the graph carries references the store has not been told about
yet. Any fix that teaches the predicate about them (a pending table, a
generation column) makes the store carry a second answer to "what is
live" — and leaves §7.2 open, because the row surviving does not stop
its offset moving under the parent's resident loader.

### 4.3 The design space, costed

| Option | Closes §7 | Closes §7.2 | Closes ingest variant | Schema | Crash behaviour | Cross-process | 100 GB scaling |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Pending-UID table (worker inserts, parent deletes on apply) | yes | **no** | no | +table | rows leak forever; predicate then never reclaims them | yes | one insert per instance per pass |
| Generation/epoch column on `instance_blobs` | yes | **no** | partial | +column | same leak | yes | same |
| Worker files under the **original** UID; parent renames row on apply | yes | **no** | no | none | none | yes | **broken**: a concurrent parent `save_all` re-emits `(orig_uid, P_old)` from `_persist_pixels`' dedup and the `ON CONFLICT(instance_uid, kind)` upsert (`persistence.py:2342`) overwrites the worker's row; the redacted frame is then reclaimed |
| In-process "pass open" flag on the store | yes | yes | yes | none | leaks on crash of the session (harmless: same process) | **no**: `compact()` from another process of the same session does not exist, but a spawned worker calling `persist_blob` cannot see it either — and neither can a test's helper process | free |
| **Pass-lock file, `LOCK_SH` per pass, `LOCK_EX\|LOCK_NB` by `compact()`** | **yes** | **yes** | **yes** | none | kernel-released on any death (measured: SIGKILL of an SH holder frees it) | yes | one `flock` per pass, not per instance |

**Chosen: the pass-lock.** Measured facts it rests on (`probe_flock.py`,
arms *f*–*j*): two `LOCK_SH` holders coexist (two threads, two fds);
`LOCK_EX|LOCK_NB` is refused (`BlockingIOError`) while any SH holder
exists, in-process or in a spawned child; a `LOCK_SH` waiter behind an
EX holder blocks and wakes when EX is released (0.205 s hold → 0.205 s
wait); SIGKILL of an SH holder frees it; cost 12.7 µs median per
acquisition. `LOCK_SH|LOCK_NB` polling gives the pass a bounded wait
with the same constant as the gate.

**Why this closes §7.2 too.** §7.2's span is *child commit → parent
apply*. With `redact()` holding SH from before dispatch until after
`_apply_redaction_outcomes` returns, no `compact()` can acquire EX inside
that span, so no offset moves under a loader the parent has not yet
bound. The `_pixel_swap_lock` rebind in `_apply_redaction_outcomes`
(`session.py:2938`) is unchanged.

**Why the ingest variant closes for free.** Sites 1–3 commit blob rows
before the `instances` row is committed (later in the same loop
iteration). With `ingest()` holding SH, `compact()` refuses for the
whole ingest.

### 4.4 What it breaks

- `compact()` called from another thread while `redact()` or `ingest()`
  is running now **raises** `RuntimeError("compact() refused: a
  redact() or ingest() pass is open on <sidecar>.pass.lock; wait for it
  to return")` instead of returning success and corrupting. The refusal
  sits *after* the leading `save(sync=True)` and the
  `has_pending_saves()` check and *before* `compact_sidecar()`, for the
  same reason the #295 refusal sits there: refusing after the rewrite
  is worse than not checking.
- `redact()` or `ingest()` called while a `compact()` is rewriting now
  **waits** up to `_SIDECAR_GATE_TIMEOUT_S` before dispatching anything,
  then proceeds. Where it waits: `redact()` drains the persistence
  manager on entry, and that drain's `save_all` is site 6, so behind a
  running compaction the wait usually lands at the *gate* inside the
  drain before the SH poll is ever reached; a `redact()` with nothing
  to drain waits at the pass-lock. Same bound, same outcome: on expiry
  it raises the gate error before any worker starts and before any UID
  is regenerated (nothing to undo).
- Direct callers of `store.compact_sidecar()` or of
  `RedactionService.execute_redaction_task` are **not** covered: the
  pass is a `Session` concept. §9.2's store-layer reproduction therefore
  stays green *as a characterization of the predicate*, and only the
  front-door test flips.
- **State scope** (the sentence the leading-save argument needs): the
  chain "no pass open ⇒ every mutation applied ⇒ `compact()`'s leading
  `save(sync=True)` wrote every `instances` row ⇒ the EXISTS predicate is
  sound" holds for **this session's graph**. Two live `Session`s on one
  store are out of scope, and always were: one persistence manager, one
  audit thread, one graph. The lock files are cross-process because this
  session's *workers* are spawned, not because two sessions are
  supported.

### 4.5 Cost

One `flock` per `redact()`/`ingest()` call (12.7 µs) and one
`LOCK_EX|LOCK_NB` attempt per `compact()`. Nothing per instance; nothing
resident; nothing in the child. On the free-threaded path the same
primitive works unchanged (SH holders are counted per fd, and the
threads path opens one fd per pass). The spawn pin
(`parallel.py:380-430`, `test_parallel_contract.py:135,176`) is
load-bearing for a *second* reason now: a forked child would inherit
the pass-lock's fd and the gate's fd, and a fork-inherited `flock` is
shared, not duplicated — the child could release the parent's lock.

---

## 5. §7.2 — the loader-offset span, and the double persist

### 5.1 Reproduced

`probe_s7.py` second arm: a deep-copied child regenerates, persists (row
under the new UID, loader at **12321**); an `instances` row is inserted
by hand under the new UID to stand in for a fixed predicate, and
`live[0]`'s `instances` row is deleted so the rewrite has to slide
everything after it. `compact_sidecar()` moved the new UID's row to
**8214**; `_apply_redaction_outcomes` then bound the child's loader
(still 12321) onto the parent's instance; `get_pixel_data()` raised. The
2026-09-07 spec §13 called this "reasoned, not reproduced"; it is now
reproduced, and the first attempt at reproducing it *passed* until an
orphan was placed before the frame — a fixed-predicate world with no
other orphans hides §7.2, which is worth knowing when reading a green
test.

### 5.2 The double persist (new, in scope)

`probe_double_persist.py` wrapped `SidecarManager.write_frame` and ran
one `execute_redaction_task` on a 64×64 8-bit instance: **two**
`write_frame` calls, `[(28, 36), (64, 36)]`, the sidecar grew **72**
bytes for one 36-byte frame; the mutation dict's loader points at
**28** and the blob row (and the instance's own loader after the
`finally`) at **64**. The second write is `services.py:576-578`,
guarded by `modified and not failed`. On every path where that guard is
true, `persist_pixel_data` at `:475` has already run (if `:475` raised,
`failed` is `True` and the guard is false), so the second call is
redundant everywhere it executes. Its costs: sidecar growth doubled per
redaction; the parent binds a loader whose offset disagrees with the
committed row until the next save's `_persist_pixels` dedup re-emits it;
and one more frame for `compact()` to reclaim. The front-door probe
showed the same three unreferenced frames at 51/74/97.

**Delete the `finally` persist in `execute_redaction_task`.** Keep the
serial arm's (`process_machine_rules`, `services.py:827-831`), which is
that path's only one. The `:475` call's comment ("cannot move into the
finally") stays true and becomes the only call. Test and mutation in
§9.8.

---

## 6. #376 — three items

### 6.1 `OS Independent` vs `import fcntl`

`setup.py:44` claims `Operating System :: OS Independent`; `sidecar.py:46`
imports `fcntl` inside `write_frame`, so a Windows install succeeds and
fails at the *first sidecar write* with `ModuleNotFoundError: No module
named 'fcntl'`. The Windows path is not taken: two lock files, `flock`
semantics (SH/EX, inode-stability across `os.replace`), and the spawn
pin's reasoning are all POSIX, and there is no CI for Windows to back a
claim. **Drop the classifier; add `Operating System :: POSIX`; move
`import fcntl` to `sidecar.py`'s module scope** so `import isocenter`
fails at import on Windows, where the packaging claim is checked, rather
than at the first write. (`sidecar` is imported by `persistence`, which
is imported by `session`, so the failure is at `import isocenter`.)

**Packaging-contract test, bidirectional** (§9.9): no classifier
containing `OS Independent`; a classifier starting `Operating System ::
POSIX`; and an `ast` walk of `isocenter/` finds a module-scope `import
fcntl`. If `fcntl` ever leaves, the test says the POSIX claim is now the
unbacked one, which is the direction a Windows port would take.

### 6.2 `*.lock` in `.gitignore`, first

The tree's `.gitignore` has `*.bin`, `*.db`, `*.log` and no `*.lock`.
Tests write into the repo root (CLAUDE.md: "leave them alone rather than
adding cleanup"), so the first test run after the gate lands would leave
`<name>_pixels.bin.lock` and `<name>_pixels.bin.pass.lock` untracked in
the root. Add `*.lock` beside `*.bin` **as the first commit of the PR**,
before any code creates one. It also covers the template's commented
`Pipfile.lock`/`poetry.lock`/`pdm.lock` patterns, harmlessly — none of
those tools is used here.

### 6.3 The leaked `:memory:` sidecar

`persistence.py:631-636` creates a `NamedTemporaryFile(delete=False)`
per `:memory:` store and nothing unlinks it (measured: the file exists
after `close()`, `probe_memory_child.py`). With the gate it would leak
two more files. **`stop()` unlinks the sidecar and both lock files when
the store is `:memory:` and owns them.** Ownership is a flag set in
`__init__`'s `:memory:` branch, dropped in `__getstate__` and set
`False` in `__setstate__`: `tests/test_save_redact_race.py` calls
`clone.stop()` on pickled clones, and a clone unlinking the parent's
sidecar mid-run is the trap. File-backed stores unlink nothing — the
sidecar is data, and the lock files are stable paths other processes of
this session may be polling. Zero files for file-backed, three for
`:memory:`.

---

## 7. #373 — the loader's padding tolerance

### 7.1 Measured today (`probe_373.py`, current main)

| Frame | Result on main |
| --- | --- |
| 8-bit 2×2, 4 B (exact) | `(2, 2)` |
| 8-bit 2×2, 5 B (one pad) | `(2, 2)` — the population `test_pixel_geometry_pipeline.py:1024` keeps |
| 8-bit 2×2, 6 B | `(2, 2)`, **silent truncation** |
| 8-bit 2×2, 16 B (4× oversize) | `(2, 2)`, **silent truncation** |
| 8-bit 2×2, 3 B (short) | returned **1-D `(3,)`** |
| 16-bit 2×2, 8 B | `(2, 2)` |
| 16-bit 2×2, 9 B (one *byte*) | bare `ValueError: buffer size must be a multiple of element size` from `np.frombuffer`, before the fallback |
| 16-bit 2×2, 10 B (one element) | `(2, 2)`, **silent truncation** |
| 16-bit 2×2, 16 B (2×) | `(2, 2)`, **silent truncation** |
| 16-bit 2×2, 6 B (short) | 1-D `(3,)` |

The fallback is `io_handlers.py:3497-3507`: `try: reshape; except
ValueError: if arr.size >= target_size: arr = arr[:target_size] ...
else: return arr`. Ingest never produces a pad — the worker uses
`np.ascontiguousarray(ds.pixel_array).tobytes()` (`:1401-1402`) — so the
one-pad-byte population is a frame written by something other than
ingest (the fixture at `:1024` writes raw bytes through
`_one_frame_loader`), and it is the *only* surplus with a DICOM reason:
OB values are padded to even length, which for 8-bit data with an odd
sample count is exactly one byte. A 16-bit frame cannot carry a one-byte
pad from DICOM (its byte length is already even), so the 9 B row is not
a real population; it is a loud error in the wrong channel.

### 7.2 The bound

In elements, on `arr` after `np.frombuffer`:

```
if not (target_size <= arr.size <= target_size + 1):
    raise RuntimeError(
        f"Integrity Error: frame for {uid} holds {arr.size} samples; "
        f"geometry {target_shape} needs {target_size} (one trailing pad "
        f"byte is tolerated, nothing else)")
arr = arr[:target_size].reshape(target_shape)
```

Both directions raise; the 1-D return goes. The `try/except ValueError`
around `reshape` goes with it — after the bound, `reshape` cannot fail.
`RuntimeError("Integrity Error: ...")` is the loader's existing
spelling for a frame that is not what its row says; it reaches callers
as `RuntimeError("Pixel Loader failed for <uid>: ...")` via
`entities.py:789-790`, and the export worker's `Pixel Loader failed`
channel (`io_handlers.py:3486`). For the 16-bit 9 B row: check `len(raw)
% itemsize` before `np.frombuffer` and raise the same `Integrity Error`
naming bytes, so the one wrong-channel `ValueError` joins the rest.

### 7.3 Nothing depends on the wide tolerance

The bound was applied as a **throwaway mutation** (three lines before
the reshape, reverted with `git checkout -- isocenter/io_handlers.py`,
`git diff --stat` empty) and the full suite run under it: **1603 passed
in 407.17 s**, exit 0. No fixture, including the pad test at `:1024`,
depends on truncation or on the 1-D return.

### 7.4 Overlap with #368: none

#368's story is "the hash beside the frame makes a moved or overwritten
frame loud": a byte-level property, and every #320 ordering ends in
`Integrity Error` from the hash. #373 is *the right bytes with the wrong
geometry*: the hash passes and the reshape is what lies. A frame that
survives compaction has identical bytes, so compaction cannot produce a
#373 failure, and a #373 failure cannot be produced by any ordering the
gate closes. The two guards are independent and the CHANGELOG should not
present one as covering the other.

---

## 8. The design as a whole

Two primitives, both `fcntl.flock` on stable paths beside the sidecar,
both crash-released, both pickled away and recreated on the far side:

1. **The gate, `<sidecar>.lock`** — mutual exclusion between *frame
   writers* and *the compaction rewrite*. Six sites + `compact()`.
   Closes #320's residual (all five phases plus waveform) and #368's
   original scope. Bounded by `_SIDECAR_GATE_TIMEOUT_S`.
2. **The pass-lock, `<sidecar>.pass.lock`** — `compact()` may not run
   while the session's graph carries references the store has not been
   told about. `redact()` and `ingest()` hold SH for the pass;
   `compact()` takes EX|NB under the gate and refuses. Closes §7, §7.2
   and the ingest variant. Bounded by the same constant on the SH side;
   never waited on under the gate (NB only), so no cycle.

Plus the double-persist deletion (§5.2), which is not concurrency but is
the only reason a redaction's committed row and the parent's loader
disagree after this PR.

**What remains open after this PR, named:** two sessions on one store
(out of scope); direct `compact_sidecar()`/`RedactionService` callers
(not the facade); the 30 s shutdown-join misfire behind a >30 s
compaction (liveness follow-up: two-phase compaction); `:memory:` on the
processes path (§0.2 E, pre-existing, not this bunch); open question C.

---

## 9. Tests — what each pins, and the mutation that kills it

Each test is named with the module row it registers under (CLAUDE.md
table and `scripts/mutation_probe.py` `TARGETS` must agree; the
persistence/session/sidecar files have no row of their own, so the #320
precedent — register under `io_handlers.py` — is followed, and a
persistence row is filed as a follow-up).

### 9.1 `tests/test_compaction_races_a_concurrent_write.py` — rewritten, not extended

**The scaffolding cannot stay** (§10). Under a gate, `_park_phase` parks
`compact_sidecar()` inside `_rewrite_live_frames` *with the gate held*;
the `_Intruder` then calls `persist_pixel_data` (site 5, gated) and
blocks; `released` is set only in the intruder's `finally`, which is
never reached. Measured with a simulated gate
(`probe_scaffold_deadlock.py`): the intruder sat on the gate for the
full 3.00 s window and the parked compaction's `released.wait` returned
`False`. Phases A, C, D, E and the waveform variant have that shape; B
parks on `os.replace` with the same result.

**Arm B, two assertions per phase:**
- *Blocked-then-landed*: the park is released by a timer thread 0.3 s
  after `parked` (not by the intruder). The intruder records
  `time.monotonic()` after its write returns; `compact()` records its
  return; assert the write returned **after** compaction returned, the
  row's offset is inside the post-compaction file, and readback is OK.
  **Mutation:** remove the gate at the phase's site → the write lands
  during the park → readback raises `Integrity Error` (today's
  characterization) → red.
- *Deadline names the lock*: with `_SIDECAR_GATE_TIMEOUT_S`
  monkeypatched to 0.5 s and the park held longer, the intruder's write
  raises `RuntimeError` whose text names `<sidecar>.lock` and the
  constant. **Mutation:** blocking `LOCK_EX` instead of the bounded loop
  → the intruder never raises → the test's own `_WAIT` fires → red.
- `has_pending_saves() is False` inside the window stays, because the
  #320 claim about the #295 guard is unchanged.
- Docstring: "five" → six, "post-1.0" → closed by #368, and the
  characterization framing goes.

### 9.2 `tests/test_compact_refuses_during_a_pass.py` (new, front door)

Three tests, executor-independent (the refusal is a kernel fact, so the
2026-09-07 spec's "skip on free-threaded" is not needed):

- **`compact()` during `redact()` raises and reclaims nothing.** Patch
  `_apply_redaction_outcomes` to materialise its outcomes and park; run
  `session.compact()` on a helper thread while parked; assert
  `RuntimeError` naming `.pass.lock`; assert every blob row present
  before the park is present after; release; assert every redacted
  instance reads back and carries its new UID. **Mutation:** remove the
  `LOCK_EX|LOCK_NB` attempt in `compact()` → compaction succeeds, rows at
  the pre-park offsets are deleted, readback raises → red.
- **`redact()` during `compact()` waits, then proceeds.** Park
  compaction inside `_rewrite_live_frames`; start `redact()` on a helper
  thread; assert no worker has been dispatched (wrap
  `execute_redaction_task` with a counter) while parked; release; assert
  `redact()` returns the expected count and readback is OK.
  **Mutation:** drop the `LOCK_SH` in `redact()` → workers dispatch
  during the park → their rows are reclaimed → readback raises → red.
- **`compact()` during `ingest()` raises.** Same shape as the first with
  the aggregation loop parked after its first result. **Mutation:** drop
  the `LOCK_SH` in `ingest()` → red. (Owner's option D.)

### 9.3 `tests/test_compaction_reclaims_a_row_instances_does_not_carry.py` (new, store layer)

The 2026-09-07 spec's §12.2: `regenerate_uid` + `persist_pixel_data` +
`compact_sidecar()` at the store layer; assert the row is gone and
readback raises. **Green before and after**: it characterizes the
predicate, and its docstring says the facade's pass-lock is what stops
the front door reaching it. **Mutation it kills:** a predicate rewritten
to keep unreferenced rows (the rejected pending-table shape) → the row
survives → red, which is the point: the predicate is *meant* to reclaim
this.

### 9.4 `tests/test_sidecar_gate_order.py` (new)

Recording wrappers on `_sidecar_gate.acquire/release` and
`_pixel_swap_lock.__enter__/__exit__` (per-thread stacks), run through
`save(sync=True)`, `persist_pixel_data`, `persist_blob`, a small ingest,
and `compact()`. Assert: the gate is never acquired by a thread holding
`_pixel_swap_lock`; the pass-lock EX attempt happens with the gate held
and is `LOCK_NB`; `compact()` acquires the gate after `save(sync=True)`
returns (the wrapper on `save_all` records order). **Mutation:** move
site 6's gate inside `_persist_pixels`' swap lock → red on the first
assertion. **Mutation:** take the gate before `save(sync=True)` in
`compact()` → red on the third (and the leading save would deadlock on
site 6 in a real run).

### 9.5 Seventh-site detector, in `tests/test_sidecar_gate_order.py`

Rule-3 shape (`test_source_citations.py`): an `ast` walk of `isocenter/`
collects every `Call` whose `func.attr == "write_frame"`, as
`(file, line)`; assert the set equals the set the gate-order test
instruments (six). **Mutation:** add a seventh `write_frame` call
anywhere in `isocenter/` → red before anyone asks whether it is gated.

### 9.6 `tests/test_sidecar_gate_crosses_processes.py` (new)

A real spawned child (module-scope worker, `if __name__` guard not
needed in a test module but the worker must pickle) holds the gate's
`flock` on `<sidecar>.lock` and signals; the parent, with
`_SIDECAR_GATE_TIMEOUT_S` monkeypatched to 0.5 s, calls
`persist_pixel_data` and must raise the gate error; the child releases;
the parent's write succeeds. **Mutation:** replace the `flock` half with
the `threading.Lock` alone → the parent succeeds while the child holds
→ red. Also the pickle twin of `test_save_redact_race.py:241`:
`pickle.dumps(store)` round-trips with `_sidecar_gate` present, the
clone's gate is a fresh object, and the clone's ownership flag is
`False`. **Mutation:** remove `_sidecar_gate` from `__getstate__`'s
`keys_to_remove` → `TypeError: cannot pickle '_thread.lock' object` →
red.

### 9.7 `tests/test_packaging_contract.py` — two additions

- `test_the_sidecar_gate_deadline_sits_inside_the_timeout_family`:
  `persistence._SQLITE_BUSY_TIMEOUT_S < persistence._SIDECAR_GATE_TIMEOUT_S
  < parallel._WORKER_FAULTHANDLER_TIMEOUT_S` and `< faulthandler
  threshold`, using `_faulthandler_threshold_and_step_seconds()`; and
  `inspect.getsource` of the acquire helper contains
  `_SIDECAR_GATE_TIMEOUT_S`. **Mutation:** 60.0 → red (below sqlite);
  400.0 → red (outside the window); a re-inlined literal → red.
- `test_the_platform_classifier_matches_the_fcntl_import` (§6.1).
  **Mutation:** restore `OS Independent` → red; delete the POSIX line →
  red; delete `import fcntl` → red (the other direction).

### 9.8 `tests/test_services.py` — one addition

`test_one_redaction_task_appends_exactly_one_frame`: wrap
`SidecarManager.write_frame` with a counter around one
`execute_redaction_task`; assert one call, sidecar growth of one frame,
and `outcome.mutation["pixel_loader"].offset` equals the committed
row's offset. **Mutation:** restore the `finally` persist → two calls,
row offset ≠ mutation offset → red.

### 9.9 `tests/test_pixel_geometry_pipeline.py` — #373 additions

Beside `:1024` (which stays and is the one-pad population):
- surplus of two 8-bit samples raises `RuntimeError` matching
  `Integrity Error` and naming both sizes. **Mutation:** `<=` → `>=`
  (restoring truncation) → red.
- short frame raises the same. **Mutation:** restore the 1-D return →
  red.
- 16-bit frame with an odd byte length raises `Integrity Error`, not
  `ValueError`. **Mutation:** drop the pre-`frombuffer` check → bare
  `ValueError` → red.
- the pad test's mutation: `+ 1` → `+ 0` → the existing `:1024` test
  goes red, which is what keeps the population.

### 9.10 `tests/test_persistence.py` or new — `:memory:` cleanup (§6.3)

`SqliteStore(":memory:")`, record the three paths, `stop()`, assert
none exists. A pickled clone's `stop()` leaves the parent's files in
place. **Mutation:** drop the ownership flag → the clone unlinks the
parent's sidecar → red. A file-backed store's `stop()` leaves its
sidecar and lock files. **Mutation:** unlink unconditionally → red.

### 9.11 `.gitignore`

One assertion in the packaging contract: `*.lock` is a pattern in
`.gitignore`. Low value on its own; its job is to fail *before* the
first lock file is committed by accident.

---

## 10. What the 2026-09-07 spec got wrong, and what it got right

- **§12.3 "The scaffolding does not change" — false, measured** (§9.1).
  Marked in place.
- **Status "Decision OPEN" and §11 "Do not build the gate until Q1 and
  Q2 are answered" — superseded** by the owner's 2026-09-07 ruling
  (build) and this spec's Q5 answer. Not false; the decision was made.
- **§13 "§7.2 — Reasoned, not reproduced" — now reproduced** (§5.1). A
  confirming note, not a strike.
- **§11's "`LOCK_NB` with a deadline below 30 s" and "an assertion in
  `write_frame`"** — rejected here with reasons (§2.3, §1.2). These were
  recommendations, not claims; nothing to strike.
- Everything measured there and re-measured here holds: six sites, the
  inode failure, the self-deadlock, the cycle, the costs, §7, the ingest
  variant's shape, the `read_frame` correction, the `raise e` sites,
  the `:memory:` leak.

---

## 11. What the reviewer should attack

1. **The pass-lock's SH scope.** It must be held from before the first
   `regenerate_uid()` can run (before `run_parallel` dispatch) until
   after `_apply_redaction_outcomes` returns, in a `finally`. A
   `RedactionError` raised from `_apply_redaction_rules` must release
   it. `redact_by_machine` goes through `redact()` (`session.py:3093`)
   and must not open a second pass. Check `ingest()`'s `import_files`
   path the same way.
2. **The refusal's position in `compact()`.** After the leading
   `save(sync=True)` and the `has_pending_saves()` check, before
   `compact_sidecar()`, with the gate held, `LOCK_NB` only. A blocking
   EX here deadlocks against a pass whose workers are waiting on the
   gate.
3. **Site 6's span.** The gate must cover `_prepare_pixel_frames` *and*
   the transaction's commit. A gate that releases between them
   reopens phase E exactly.
4. **Sites 1–3's span.** Per iteration, write-then-record, not the
   whole loop; and the `except Exception` arm must run *after* the gate
   is released, or a failing result holds the gate while
   `_record_failure` writes an audit row through the audit thread.
5. **The polling loop's fd hygiene.** One fd per acquisition, opened
   after the thread lock, closed on every exit including exception; a
   leaked fd holds the flock for the life of the process.
6. **`__getstate__`/`__setstate__`.** `_sidecar_gate`, the ownership
   flag, and nothing else new; `__setstate__` recreates the gate. The
   child must open its *own* fd on the same path.
7. **The `finally` deletion in `execute_redaction_task`.** Confirm by
   reading `failed`'s assignments that no path reaches the guard with
   `:475` unexecuted. The `RedactionOutcome` for a redaction that failed
   *after* `:475` (a later zone raising) must still be `ok=False` with
   no hash and the frame it appended is an orphan the next compaction
   reclaims — unchanged from today.
8. **The 9.1 rewrite.** The timer-released park must not race the
   intruder's *start*: the intruder must be blocked on the gate before
   the timer fires, or the "landed after" assertion is vacuous. Pin it
   by having the intruder set a `waiting` event immediately before its
   write and the timer wait on that event first.
9. **#373's message.** It must name the UID, both sizes and the shape;
   an export failing on one frame in ten thousand is diagnosed from that
   line alone.
10. **The `OS Independent` test's AST walk.** It must accept `import
    fcntl` at module scope only; a guarded `try: import fcntl` would
    make the POSIX claim true and the walk's answer false.
11. **The CHANGELOG correction.** The 0.9.3 #320 entry is not edited;
    the new entry says it was wrong on both counts and cites the
    comment URL. Check the owner's actual words against the entry.

---

## 12. Implementation brief — one PR, three issues, in this order

Branch from `main` at `696a208`. One PR; `Fixes #376`, `Fixes #373`,
`Fixes #368` in the body. Every step: failing test first, then the
change, then the mutation run by hand per CLAUDE.md's runbook
(`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$W`, print `isocenter.__file__`,
`pytest -v` unpiped). Run the mapped tests after each step and the full
suite before pushing.

### Step 0 — hygiene (#376, item 2)

- `.gitignore`: add `*.lock` under the `# SQLite` block beside `*.bin`.
- Commit alone: `chore: ignore sidecar lock files before any exist (#376)`.

### Step 1 — packaging claim (#376, item 1)

1. Test: `test_the_platform_classifier_matches_the_fcntl_import` (§9.7).
   Red on `OS Independent`.
2. Change: `setup.py` — delete `"Operating System :: OS Independent"`,
   add `"Operating System :: POSIX"` with a comment naming `fcntl.flock`
   and the two lock files as the reason. `sidecar.py` — `import fcntl`
   at module scope, comment: fail at import where the classifier is
   checked, not at the first write. This puts `fcntl` in view of
   `test_every_unguarded_third_party_import_is_declared_in_setup_py`,
   which classifies by `sys.stdlib_module_names`
   (`tests/test_packaging_contract.py:126`); `fcntl` is in that set on
   every platform, so the move does not trip it.
3. Mutations: restore the classifier; delete the import. Both red.

### Step 2 — `:memory:` cleanup (#376, item 3)

1. Test: §9.10. Red: the sidecar survives `stop()`.
2. Change: `SqliteStore.__init__` sets `self._owns_temp_sidecar = True`
   in the `:memory:` branch (`False` otherwise); `__getstate__` drops it;
   `__setstate__` sets `False`; `stop()` unlinks the sidecar and the two
   lock paths (`os.remove` in a `try/except FileNotFoundError`) when the
   flag is set. The lock paths are computed by two small helpers
   (`_gate_path()`, `_pass_lock_path()`) added here and used by Step 5.
3. Mutations: drop the flag; unlink unconditionally. Both red.

### Step 3 — #373

1. Tests: §9.9's three new tests. Red: truncation, 1-D return, bare
   `ValueError`.
2. Change: `io_handlers.py` `SidecarPixelLoader.__call__`: byte-length
   check before `np.frombuffer`; the element bound; `arr[:target_size]
   .reshape(target_shape)` unconditionally; delete the `try/except
   ValueError` and the 1-D return. Comment: why one pad byte is the
   whole tolerated population (OB even-length padding, 8-bit odd count)
   and why the guard is in elements not bytes.
3. Mutations: `<=`→`>=`; restore the 1-D return; drop the byte check;
   `+ 1`→`+ 0` (kills via `:1024`). All red.
4. Run `tests/test_pixel_geometry_pipeline.py`, then the full suite
   (measured clean under this bound: 1603 passed).

### Step 4 — the double persist (#368, prerequisite)

1. Test: §9.8. Red: two calls.
2. Change: delete `services.py:576-578`'s persist and the paragraph of
   comment that justifies it (`:568-575`); leave `:474-475`'s comment,
   amended to say it is now the only call. Leave
   `process_machine_rules`' call.
3. Mutation: restore the call. Red.

### Step 5 — the gate (#368, scope 1)

1. Tests: §9.1's Arm B rewrite (red: the intruder's write lands during
   the park), §9.4, §9.5, §9.6, §9.7's deadline test.
2. Change, `persistence.py`: `_SIDECAR_GATE_TIMEOUT_S = 180.0` beside
   `_SQLITE_BUSY_TIMEOUT_S` with the inequality in its comment;
   `_sidecar_gate` (`threading.Lock`) created in `__init__` and
   `__setstate__`, dropped in `__getstate__`; a context manager
   `_hold_sidecar_gate()` implementing thread-lock-then-polled-flock with
   the deadline and the one-fd rule; sites 4, 5, 6 wrapped per §1.2;
   `compact_sidecar()` **not** wrapped (the session holds the gate
   across it). `io_handlers.py`: sites 1–3 wrapped per iteration.
   `session.py` `compact()`: gate taken after the refusal, held through
   the rewire. Comments at each site explain the trap (span the commit;
   outside the swap lock; not in `write_frame`).
3. Mutations: per §9.1/§9.4/§9.5/§9.6/§9.7. Each red.
4. Docstrings: `compact()` — "five" → six, "post-1.0" gone, the
   "still convention" paragraph replaced by the gate and the two
   observable behaviours (§3); `test_compaction_races_a_concurrent_write.py`
   header likewise.

### Step 6 — the pass-lock (#368, scope 2)

1. Tests: §9.2 (three, red: compaction succeeds and reclaims), §9.3
   (green, characterization).
2. Change, `session.py`: a context manager `_hold_pass()` (SH, polled
   NB with the same constant) around `_apply_redaction_rules`'s dispatch
   and apply in `redact()`, and around `import_files` in `ingest()`;
   `compact()` takes `LOCK_EX|LOCK_NB` on `_pass_lock_path()` under the
   gate and raises the refusal on `BlockingIOError`, releasing the gate
   in the same `finally`. `redact_by_machine` unchanged (it calls
   `redact()`).
3. Mutations: per §9.2. Each red.
4. Docstrings: `redact()`, `ingest()`, `compact()` state the two
   behaviours of §3.

### Step 7 — records

**CHANGELOG.md**, new `## [Unreleased]` above `## [0.9.3]`:

- *Fixed (#368):* what the gate closes (phases A–E and waveform, the
  #320 residual) and what the pass-lock closes (§7, §7.2, ingest), with
  the reproduction numbers (12347→12321; 120/143/166 deleted through
  the front door; 8214 vs 12321). **Exact exceptions:** `compact()`
  during a pass now raises `RuntimeError("compact() refused: a redact()
  or ingest() pass is open on ...")` where it returned success and
  corrupted; a writer that cannot take the gate in
  `_SIDECAR_GATE_TIMEOUT_S = 180 s` raises `RuntimeError("Sidecar gate
  ... not acquired within ...")`, which reaches `redact()` as
  `RedactionError` with an ERROR audit row, `ingest()` as an ERROR audit
  row, a background save as a logged `Background save failed` with the
  instances still dirty, `save(sync=True)` and `compact()` as the raise.
  Why the old behaviour was wrong: it reported success and the failure
  arrived at export time with nothing tying the two together (#153's
  distance). The deadline's place in the #280 inequality. The
  liveness trade (§2.5) and the follow-up filed.
- *Fixed (#368):* the double persist — one frame per redaction, the
  committed row and the parent's loader agree.
- *Correction to the 0.9.3 entry for #320, left standing where it is:*
  it says #368 is "post-1.0 and deliberately not milestoned". The owner
  ruled otherwise on 2026-09-07 (concurrent `redact()`/`compact()` must
  be safe in 1.0 — recorded in #368's re-scoped body,
  https://github.com/kvnlng/Isocenter/issues/368) and on 2026-09-08 that
  the v1.0.0 tag waits on #368, in the comment that itself says the
  0.9.3 entry is now wrong on both counts
  (https://github.com/kvnlng/Isocenter/issues/368#issuecomment-5584761918);
  the entry is wrong on both counts and is not edited, per the dated-
  record rule. The same entry's "one of five" is six (#183 added the
  nested-pixel site in the same release).
- *Fixed (#373):* the exact exception (`RuntimeError: Integrity Error:
  frame for <uid> holds N samples; geometry (r, c) needs M ...`) where
  the loader truncated silently or returned a 1-D array; the one-pad-
  byte population kept and why; independence from #368's hash.
- *Changed (#376):* classifier swap and what a Windows `pip install`
  now sees (a package that does not claim to run there; `import
  isocenter` raises `ModuleNotFoundError: No module named 'fcntl'` at
  import rather than at the first write); `*.lock` ignored; the
  `:memory:` temp files unlinked on `stop()`.

**CLAUDE.md:**
- Table row `io_handlers.py`: add `test_compact_refuses_during_a_pass.py`,
  `test_compaction_reclaims_a_row_instances_does_not_carry.py`,
  `test_sidecar_gate_order.py`, `test_sidecar_gate_crosses_processes.py`
  (the others already listed). `scripts/mutation_probe.py` `TARGETS`'
  `io_handlers.py` entry gets the same four names — the two lists must
  agree.
- "Hybrid storage": one paragraph naming the two lock files, the order
  `_sidecar_gate → _pixel_swap_lock → sqlite`, that the gate is the one
  lock held across a sqlite write, and that `compact()` refuses during
  a pass.
- "Tests write `*.db`, `*_pixels.bin`, ..." add `*.lock`.
- Any line citing `sidecar.py` for `fcntl` (Rule 2 pins) must be
  re-checked after the import moves: `tests/test_source_citations.py`
  grades them.

**Docs:** `docs/api/session.md:21` (`compact`) and the `redact`/`ingest`
entries carry the two behaviours; `docs/configuration.md:214`'s compact
sentence likewise. `docs/environment.md` unchanged (no new env var —
deliberately).

**Old spec:** the `**Superseded in part:**` line and three in-place
marks (already applied in this branch, §14).

**Issues to file from this PR, not fix:** two-phase compaction
(liveness, §2.5); open question C if C2; §0.2 E (`:memory:` on
processes); a `persistence.py` row in the coverage table and
`TARGETS`; the `raise e` sites (2026-09-07 §13) if #366 did not take
them.

---

## 13. Measurement log (numbers cited above, in one place)

| Fact | Value | Probe |
| --- | --- | --- |
| `write_frame` call sites in `isocenter/` | 6 (grep = AST) | `count_sites.py` |
| `fcntl` imports | 1, `sidecar.py:46` | grep |
| flock acquire, uncontended | 12.7 µs median (11.6 µs `LOCK_NB`) | `probe_flock.py` |
| SH + SH coexist; EX\|NB refused while SH held (thread or spawned child); SH waits behind EX and wakes; SIGKILL frees SH | all hold; 0.205 s hold → 0.205 s wait | `probe_flock.py` f–j |
| §7 store layer | 12347 → 12321 bytes; row gone; read raises | `probe_s7.py` |
| §7 front door (processes) | `compact()` success; rows 120/143/166 deleted; 3/3 reads raise | `probe_front_door.py` |
| §7.2 | row → 8214, loader stays 12321, read raises | `probe_s7.py` |
| Frames per `execute_redaction_task` | 2: `[(28,36),(64,36)]`, +72 B | `probe_double_persist.py` |
| #320 scaffolding under a gate | intruder blocked 3.00 s; `released.wait` → False | `probe_scaffold_deadlock.py` |
| #373 table | §7.1 | `probe_373.py` |
| Suite under the #373 bound | 1603 passed, 407.17 s | `suite_373_mutation.log` |
| `:memory:` + processes `redact()` | `RedactionError` / `no such table: instance_blobs`; sidecar survives `close()` | `probe_memory_child.py` |
| Timeout family | 120 / 30 / 240 / 300 / 1200 s | `persistence.py:175`, `persistence_manager.py:29`, `parallel.py:34`, `pytest.ini`, `tests.yml` |

## 14. Clean-tree and suite record

- Diff against `696a208` limited to `isocenter/`: empty; no untracked
  files under `isocenter/`. The one throwaway mutation (the #373 bound
  in `io_handlers.py`) is reverted; the simulated gate in
  `probe_scaffold_deadlock.py` was a monkeypatch, never an edit.
- Clean full suite on this worktree, `pytest -v -p no:cacheprovider`
  under the runbook invocation, `isocenter.__file__` in the worktree:
  **1603 passed in 411.38 s**, exit 0, 0 failed.
- Under the throwaway #373 bound, same invocation: 1603 passed in
  407.17 s, exit 0 (§7.3).

## 15. Amendments (during implementation, 2026-09-08)

Corrections made while building §12, recorded here rather than by
editing the sections above. None changes a §0 determination or an
owner's decision.

1. **§9.1's second mutation is not killable where §9.1 puts it.** "A
   blocking `LOCK_EX` instead of the bounded loop → the intruder never
   raises → red" assumed the flock is the only wait. Under §1.2's own
   design the gate is a `threading.Lock` *then* the flock, and the
   deadline test's intruder and compaction are two threads of one
   process, so the thread lock's `acquire(timeout=)` expires first and
   the flock is never reached: measured, the mutation **survived** the
   deadline test (1 passed in 0.61 s). It is observable only when the
   holder is another process, so it is killed by §9.6's test instead,
   reworked to run the parent's write on a helper thread with a bounded
   join (red in 10.1 s under the mutation, as a failure rather than a
   stall). The deadline test's docstring says which half it pins.
2. **§9.5 keys sites on `(file, enclosing function)` as a multiset, not
   `(file, line)`.** Line numbers rot on every unrelated edit above a
   site; function names stay green through refactors and go red on a
   seventh call wherever it lands. Strictly stronger, not a determination
   change.
3. **Site 5's `write_frame` now sits in `SqliteStore._swap_pixels_under_gate`**,
   the body of `persist_pixel_data`, which takes the gate and calls it.
   §1.1's table names `persist_pixel_data`; the public method and its
   behaviour are unchanged, the split is what keeps the gate outside the
   existing `try` without re-indenting seventy lines. §9.5's set names
   the helper.
4. **§12 Step 6's `_hold_pass()` lives on the store, not the session.**
   The flock primitives -- `_hold_pass_lock` (SH, polled) and
   `_refuse_while_pass_open` (EX|NB, held through the block) -- are
   `SqliteStore` methods beside `_hold_sidecar_gate`, because they need
   `sidecar_path` and the constant and share `_flock_within` with the
   gate. `Session.redact()`, `Session.ingest()` and `Session.compact()`
   are the only callers, so §4.4's "the pass is a `Session` concept"
   still holds: `compact_sidecar()` and `RedactionService` take neither.
5. **§9.2's third test parks at the first `Equipment.from_parts`**,
   inside the first result's iteration -- after its frames are appended
   (gate released) and before its `instances` row can exist -- rather
   than "after its first result". Between iterations every frame is
   already linked into the graph and `compact()`'s leading save would
   write its row, so there is no unreferenced frame to measure; at
   `from_parts` there is, and on 0.9.3 the compaction there dropped it.
6. **§9.10's test file** is `tests/test_memory_store_unlinks_its_temp_files.py`,
   new rather than in `test_persistence.py`.
7. **§2.4's table, one row sharpened.** The gate error reaches
   `persist_pixel_data`'s existing `except: log; raise`, so a redaction
   worker's expiry is logged as `Failed to persist pixel swap for <uid>:
   Sidecar gate ...` before it becomes the `RedactionOutcome(ok=False)`
   the row describes.
