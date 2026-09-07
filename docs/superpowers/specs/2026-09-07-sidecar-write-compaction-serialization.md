# Serializing Sidecar Writers Against Compaction: One Question, Not Two

**Date:** 2026-09-07
**Status:** Decision OPEN. This spec costs one design, tries hard to break
it, and ends with a recommendation (§11) for the owner to accept or
reject. **Nothing here is approved and no production code was changed.**
**Tracking:** #366 and #368, treated as one question at the owner's
instruction. Rests on #320, #295, #294, #287, #274, #250, #183, #220,
PR #317. Weighed against #26 (the v1.0.0 API freeze).
**Base:** `main` at `ae705bb`. Every line number below is that commit.
**Measured with:** `/Users/kevin/Developer/Isocenter/.venv/bin/python`
(CPython 3.14.6) on macOS 25.6 / APFS / local SSD, warm page cache.
Every probe ran as `env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<worktree>
.venv/bin/python -u <script>`. **`isocenter.__file__` was printed and
read under that exact invocation and resolves to the worktree**
(`.../agent-a83156e2d6005c478/isocenter/__init__.py`, CPython 3.14.6) —
though in candour it was checked *after* the measurements rather than
before, because the first attempt at the check was refused by the
sandbox and re-running it was not done until the spec was drafted. The
invocation is identical, so the measurements are about this tree; the
ordering is stated because CLAUDE.md's runbook asks for the print
first and this run did not do it first.
**On the probe scripts:** `probe_inode.py`, `probe_lockfile.py`,
`probe_reentrancy.py`, `probe_redaction_span.py`, `bench_gate.py`,
`bench_gate_d2.py`, with logs `bench_gate_run1.log`,
`bench_gate_d_run1.log`, `bench_gate_d2_run1.log`. They were written in
a session-local scratchpad and **do not exist in the repo**; they are
named so numbers can be attributed, not so a later reader can run them.
The reproductions meant to survive are §12's, specified as repo tests.

---

## OPEN QUESTIONS for the owner

**Q1 — the question 1.0 actually turns on, and it is not the one the
brief asks.** Is **`redact()` concurrent with `compact()` a supported
use in 1.0, or documented-unsupported?** Everything else follows from
that answer. The brief and #368 both frame the cross-process exposure as
*ingest* — "site 1 runs in `ingest_worker`, which may be in a spawned
subprocess". **Measured, that is wrong in both directions** (§4). All
three ingest `write_frame` calls run in the **parent's** aggregation
loop; `ingest_worker` returns bytes and writes no frames. The one
production sidecar writer that genuinely runs in a spawned subprocess is
`persist_pixel_data`, reached from `services.py:475/578/831` inside
`execute_redaction_task`, which `session.py:2773` dispatches through
`run_parallel` — **processes on every GIL build, i.e. 3.12, the CI
floor and the `python_requires` floor**. So the reachable population is
not exotic ingest; it is the core redaction operation on the default
build. That should decide Q1, and Q1 should decide everything below.

**Q2 — a defect wider than the one #320/#368 are about, found while
costing this, and which no lock closes.** A redaction worker calls
`regenerate_uid()` and *then* `persist_pixel_data()`
(`services.py:462` → `:475`), so its `record_blob_ref` files the blob
row under the **new** UID. The `instances` table learns that UID only
when the parent's `_apply_redaction_outcomes` applies the mutation and
the caller subsequently saves — and `redact()` deliberately does not
save ("call `.save()` afterwards to persist it"). For that whole span
the row is **orphan-shaped** to `_read_blob_index`, whose liveness
predicate is `EXISTS (SELECT 1 FROM instances WHERE sop_instance_uid =
b.instance_uid)` (`persistence.py:3667-3684`). Reproduced
deterministically (§7): a compaction observing that state **deletes the
redacted frame's row and reclaims its bytes**, and the instance then
raises `RuntimeError: Integrity Error` on every read. Per mutation the
window is the worker's `record_blob_ref` → the parent's
`_apply_redaction_outcomes` for that mutation — `compact()`'s leading
`save(sync=True)` rescues everything **already applied**, and §7.1 says
so rather than over-claiming an unbounded tail — but the **unapplied**
population is continuously non-empty for the whole pass, so the exposure
interval is a redaction pass rather than the 0.024–0.221 s rewrite
window #320 prices. **The gate does not close it**, because the failure
is in the orphan *predicate*, not in atomicity. File it. Decide whether
it blocks Q1.

**Q3 — the gate's mechanism, if Q1 says "supported".** §6 recommends a
**store-level gate**: a plain `threading.Lock` whose holder also flocks
a stable `<sidecar_path>.lock` file, acquired at six writer sites and by
`compact()`, always *outside* `_pixel_swap_lock`. The alternative the
brief describes — the flock inside `write_frame` — is **measurably
unsafe**: probe (b) shows two open file descriptions on one lock file
block each other *within one thread*, and `_persist_pixels` calls
`write_frame` inside `_pixel_swap_lock` (`persistence.py:3157` →
`:3216`), so that placement creates `_pixel_swap_lock → gate` at the
writer against `gate → _pixel_swap_lock` at `_rewire_sidecar_loaders`.
That is a cycle. Accept §6.2's placement or say which other one.

**Q4 — #366's binary is false and I am recommending a third answer.**
Not "make `_lock` real" and not "delete it and say `flock` is the whole
story", but: **delete `SidecarManager._lock`** (it never was the append
lock), **keep `write_frame`'s inode flock** (it is the append lock, and
it is 0.178% of a frame write), and **add the gate as a third, separate
thing** at the store. Three names for three questions. Confirm you want
three rather than two.

**Q5 — the sqlite amplification, which is new and which I cannot make
go away.** Under the gate, `record_blob_ref` and `_apply_new_offsets`
run *inside* it and open sqlite connections. A gate-holder that waits
out `_SQLITE_BUSY_TIMEOUT_S = 120.0` (`persistence.py:175`) holds every
other writer, in every process, for those 120 s — **four times** the
fixed `_SHUTDOWN_JOIN_TIMEOUT_S = 30.0` (`persistence_manager.py:29`).
Bounded, not a deadlock, but it is #250's shape arriving at a new lock,
and #280 lowered 900 → 120 precisely to keep such a stall inside one
faulthandler window. §6.5 offers a bounded (`LOCK_NB` + deadline)
acquisition as the mitigation. Accept it, or accept the amplification.

---

## 1. Every inherited claim, checked

CLAUDE.md's *verify, do not trust*. The brief's own claims are in this
table alongside the issues'.

| Claim | Source | Verdict |
| --- | --- | --- |
| `fcntl.flock` appears exactly once in the package, in `write_frame` | brief | **Holds.** `grep -rn flock isocenter/` returns `sidecar.py:55`, `sidecar.py:63`, and one prose mention at `persistence.py:3051`. `import fcntl` appears once, `sidecar.py:48`. |
| `write_frame` **and `read_frame`** use `fcntl.flock` | #366, and the compaction spec's Q4 | **False.** `read_frame` (`sidecar.py:67-113`) opens `rb` and flocks nothing. Only `write_frame` locks. Minor, and exactly the "prose that was true once" shape #366 is itself complaining about. |
| `SidecarManager._lock` is constructed, excluded from `__getstate__`, recreated in `__setstate__`, and never acquired | #366, brief | **Holds exactly.** `sidecar.py:18`, `:124-125`, `:131`. No `acquire`, no `with self._lock` anywhere in the package. |
| Three comments document a lock order over it | #366, brief | **Holds, with drift.** The lines have moved since #366 was filed: `persistence.py:671` → **`:673`**, `persistence.py:2960-2961` → **`:3137-3138`**, `session.py:1068` → **`:1127`**. All three still say `_pixel_swap_lock` before `sidecar._lock`. |
| Writers already serialize against one another, cross-process, correctly | brief | **Holds for the append itself.** `write_frame` flocks `LOCK_EX` on the fd it appends through, so `seek(0,2)` → `tell()` → `write` is atomic against another `write_frame`. It does **not** serialize the append against the row commit that follows it, which is #368's whole point. |
| `compact_sidecar` never takes it | brief | **Holds.** `persistence.py:3587-3646` acquires nothing. Its only mutual exclusion is `session.compact()`'s `has_pending_saves()` refusal (`session.py:1052-1059`), which #320 measured as `False` in every corrupting ordering. |
| It never contends for the writers' lock *by construction*, because it writes `.compact.tmp` and swaps | brief | **Holds, and is the sharper statement.** `_rewrite_live_frames` (`:3703`) writes `temp_path`; `_swap_in_compacted_sidecar` (`:3748`) `os.replace`s. The compactor touches the writers' inode only to *read* it. |
| `compact_sidecar` rebinds `self.sidecar` at `persistence.py:3463` | #366 | **Holds, at `:3640`.** Line drift again. |
| ...and that is harmless today only because `_lock` is unused | #366, brief | **Holds, and the rebind is worse than harmless — it is inert.** `SidecarManager(self.sidecar_path)` constructs an object whose only state is `filepath` (identical), a fresh unused `_lock`, and an `_ensure_file()` that finds the file already there. The old and new objects are indistinguishable. See §8.2: the fix is a deletion, not a lock. |
| `save_all` is one of **five** write-frame-then-commit-row sites | #320's spec §3.4, #368 | **Stale: there are six.** #183 landed `io_handlers.py:1875` (nested pixel payloads) in this very base commit. §4 has the corrected table. |
| Site 1 (ingest pixels) runs in `ingest_worker`, which may be in a spawned subprocess | #368, #320's spec §6.1 | **False, and it is the load-bearing error.** All three ingest `write_frame` calls are in `import_files`' aggregation loop in the **parent** (`io_handlers.py:1700` `for meta, inst, p_bytes, ... in results:`), commented "Main Thread Sequential Write". `ingest_worker` returns bytes. §4. |
| A cross-process lock is nonetheless required | #368's conclusion | **Holds — for a site #368 does not name.** `persist_pixel_data` runs in a spawned child on every GIL build (§4, row 4). #368 is right for the wrong reason. |
| `compact()` calls `save(sync=True)` first, so the lock must be acquired after it | #320, #368, brief | **Holds.** `session.py:1043` then the gate would sit at `:1061`. §6.3 says exactly where and why. |
| The hold is `live_bytes / throughput`; 0.024 s at 200 MB, 0.221 s at 2 GB | #320's spec §4 | **Reproduced independently.** §5: 0.026 s for a 400-instance / 200 MB corpus (3.76 GB/s over live bytes) and 0.217 s to rewrite 1.00 GB live (4.60 GB/s). |
| The hold must include `_rewire_sidecar_loaders` | #368 | **Holds as reasoning, and it is nearly free.** Measured at **0.0006 s for 4000 loaders** (§5.3) — 0.2% of a 2 GB compaction. #368 presents this as a cost; it is not one. It is also now a *three*-map rewiring (`nested_updates`, #183), which #368 predates. |
| A `threading.Lock` cannot cross a process boundary; `__setstate__` gives the child a new one | #368 | **Holds.** `persistence.py:704-727`. |
| Prefer a plain `Lock` over an `RLock` | #368 | **Holds under §6.2's placement, and only under it.** Under the brief's placement a plain lock is impossible. §6.3. |
| `_pixel_swap_lock` is never held across a sqlite write | `persistence.py:674-675`, #317's audit | **Holds.** `persist_pixel_data` releases at `:2569` before `record_blob_ref` at `:2575`; `_persist_pixels` runs inside `_prepare_pixel_frames`, before `save_all` opens its connection (#287). |
| `services.py` contains no `_get_connection` block | #317's audit, restated in #320's spec §3.1 | **Holds** (`grep` returns nothing) — **and it is no longer the reassurance it reads as.** `services.py:475` calls `store_backend.persist_pixel_data`, which calls `record_blob_ref`, which opens its own connection (`persistence.py:2575`). The sqlite work is one frame down, not absent. |
| All process pools are spawn-pinned | inferred, needed by §6 | **Holds, and it is load-bearing for the gate.** `parallel.py:402-412` and `session.py:610-619` both pin `get_context("spawn")` (#220, #250). A *forked* child would inherit the gate's open file descriptor and therefore its flock, silently, and two processes would believe they held it. §9.4. |

---

## 2. The inode/rename failure mode, reproduced

The brief's central premise is that compaction cannot simply flock the
sidecar file itself, because **`flock` binds the inode, not the path**,
and the swap replaces the inode. It asked for a probe because if the
premise is wrong the whole design changes.

**It is not wrong.** `probe_inode.py` models `write_frame` and
`_swap_in_compacted_sidecar` exactly — writer does `open(path,'r+b')`
then `flock(LOCK_EX)`; compactor holds `LOCK_EX` on its own fd for the
same path, writes `.compact.tmp`, `os.replace`s twice, unlocks, removes
the backup:

```
compactor: holds LOCK_EX, inode 68375279
compactor: swap done. path inode now 68375295
compactor: backup removed

writer inode at open : 68375279
writer inode at write: 68375279
writer offset committed to the DB: 14
path inode after all : 68375295
bytes now at path    : b'COMPACTED'

WRITER WROTE INTO THE LIVE FILE? False
=> flock followed the INODE
```

The writer blocked correctly, woke correctly, and appended into an inode
with no name — the file `os.remove(backup_path)` (`persistence.py:3638`)
then destroyed. It returned offset 14 into a live file that is 9 bytes
long, so the row it commits dangles past EOF: `IOError: Incomplete read
from sidecar`, which is #320's phase D arriving by a second route.

**So flocking the sidecar itself is not merely insufficient; it is a
trap that makes the race *look* closed.** Every writer serialises, every
writer blocks, and the data is still lost. That is a worse failure than
the one we have, because the lock is evidence the reader will trust.

### 2.1 The stable lock file fixes it — and only in one of the two ways
### you can write it

`probe_lockfile.py` runs the same swap against a `<path>.lock` that is
created once and never renamed, in two arms that differ only in
statement order:

```
[open-then-lock]                     [lock-then-open]
   writer offset committed: 14          writer offset committed: 9
   live file size after   : 9           live file size after   : 18
   wrote into live inode  : False       wrote into live inode  : True
   row within the file    : False       row within the file    : True
   VERDICT: LOST/DANGLING               VERDICT: SAFE
```

**A writer that opens the sidecar before taking the gate is exactly as
broken as it was with no gate at all.** It holds an fd on the old inode
across the swap and appends into it.

Today `write_frame` reads:

```python
with open(self.filepath, 'r+b') as f:      # sidecar.py:54
    fcntl.flock(f, fcntl.LOCK_EX)          # sidecar.py:55
```

An implementer who "adds the gate" by inserting a second flock *inside*
that `with` block writes the `open-then-lock` arm. The gate must wrap
the `open`, not sit beside it. §6.2 places the acquisition in the
callers, above `write_frame` entirely, which makes this correct by
construction rather than by care — that is one of the reasons §6.2 is
preferred over the brief's placement, and it is the reason worth keeping
if the other arguments ever stop applying.

---

## 3. What `flock` composes with, measured

`probe_reentrancy.py`, all five arms on this platform:

```
(a) same fd, second LOCK_EX returned in 0.0000s -> no block
(b) two fds, same thread: BLOCKED (BlockingIOError) -> a blocking call here SELF-DEADLOCKS
(c) contender still blocked after 0.25s: True
(c) contender waited 0.261s -> threads of one process DO exclude
(d) lock-file inode unchanged across the sidecar swap: True
(e) child holds it, parent blocked -> cross-process exclusion works
(e) after SIGKILL the lock was free in 0.0000s -> no stale lock survives a crash
(e) the lock FILE itself still exists: True size 0
```

Five facts the design rests on, each with a consequence:

1. **(a) `flock` on one open file description is idempotent** — a
   re-acquisition converts rather than blocks. So a *single cached fd*
   would let two threads of one process both believe they hold the
   gate. A cached fd therefore needs an in-process lock beside it; §6.1
   uses a fresh fd per acquisition and a plain `Lock` instead.
2. **(b) Two fds in one thread deadlock.** This is the fact that kills
   the brief's placement. It is also why "make `SidecarManager._lock`
   real" cannot be done by promoting the flock: the flock has no
   reentrancy to promote.
3. **(c) Threads of one process do exclude** when each opens its own fd.
   So the flock alone is a complete mutual-exclusion primitive across
   both axes — which is what makes the "subsume or complement" question
   answerable (§10.1).
4. **(d) The lock file's inode is untouched by the sidecar's swap.**
   That is the whole point, and it is now measured rather than assumed.
5. **(e) A crash releases the lock.** The kernel drops it when the last
   fd closes, including on `SIGKILL`. The residue is a **zero-byte file
   that carries no state**, so there is no stale-lock recovery problem
   and nothing to time out. Compare a PID-file or an `O_EXCL` lock,
   both of which would need one. §9.2.

---

## 4. Where frames are written, and in which process

Corrected against `ae705bb`. **Six sites, not five**, and the process
column is the correction that matters.

| # | Site | Frame | Row commit | Runs in |
| --- | --- | --- | --- | --- |
| 1 | ingest, pixels | `io_handlers.py:1754` | `save_all`'s walk, later | **parent** — `import_files`' aggregation loop |
| 2 | ingest, **nested** pixels (#183, new) | `io_handlers.py:1875` | `io_handlers.py:1891` | **parent** — same loop |
| 3 | ingest, waveform | `io_handlers.py:1981` | `io_handlers.py:1995` | **parent** — same loop |
| 4 | `persist_pixel_data` (redaction swap) | `persistence.py:2553`, inside `_pixel_swap_lock` | `persistence.py:2575`, **outside** it | **parent thread** from `services.py` when threads; **spawned child** from `services.py:475/578/831` when processes |
| 5 | `persist_blob` (waveform) | `persistence.py:2327` | `persistence.py:2329` | caller's thread, parent |
| 6 | `_persist_pixels` (save prepass) | `persistence.py:3216` | `persistence.py:2916`, inside `save_all`'s transaction | parent, persistence-manager thread or caller |

**Sites 1–3 are in the parent.** `import_files` builds `results =
run_parallel(ingest_worker, ..., return_generator=True)`
(`io_handlers.py:1655`) and then iterates it at `:1700`; the workers
return `(meta, inst, p_bytes, p_hash, p_alg, w_bytes, w_hash, err)` and
write nothing. The comment at `:1753` says so: *"Persist Pixels to
Sidecar (Main Thread Sequential Write)"*. #368's cross-process question
is attached to the three sites that do not have one.

**Site 4 is the one that does.** `Session._redact_parallel` dispatches
`service.execute_redaction_task` through `run_parallel`
(`session.py:2773`) with the store pickled into the child (#220).
`run_parallel` picks a `ProcessPoolExecutor` on any GIL build, threads
only on free-threaded (`parallel.py`). `execute_redaction_task` calls
`self.store_backend.persist_pixel_data(inst)` at `services.py:475`, and
the two `finally`-arm variants at `:578` and `:831`. That is
`write_frame` **and** `record_blob_ref` — an append and a sqlite commit
— inside a spawned subprocess, on 3.12, which is both the
`python_requires` floor and half the PR gate. `parallel.py:407` already
records the consequence in a comment: CI once stalled *"900 seconds in a
forked worker's `persist_pixel_data`"* (#250).

So the correct sentence, and it should replace #368's:

> A `threading.Lock` on the store cannot serialise the **redaction**
> path, because on every GIL build that path writes its frames from a
> spawned child. Ingest never needed one.

---

## 5. What the gate costs, measured

### 5.1 Per-write overhead

`bench_gate.py` §A. 512 KB incompressible payload, `zlib`, the real
`SidecarManager.write_frame` (which `fsync`s):

| | median |
| --- | --- |
| `write_frame(512 KB, zlib, fsync)` | **0.0067 s** |
| the same, wrapped in one lock-file cycle | **0.0066 s** |
| `os.open` + `flock(LOCK_EX)` + `LOCK_UN` + `close` alone | **11.9 µs** (p95 **12.2 µs**) |

**0.178% of one frame write.** Uncontended throughput cost is nil, and
that is not a rounding argument — it is two orders of magnitude below
the `fsync` already in the path. A gate acquired once per *save* rather
than once per frame is cheaper still.

### 5.2 The hold

`bench_gate.py` §B, 400 instances × 512 KB = 200.1 MB, half orphaned:

```
sidecar before: 200.1 MB
compact_sidecar(): 0.026 s, 200 live uids
sidecar after : 100.0 MB
=> 3.76 GB/s over live bytes
```

`bench_gate_d.py` §E, the copy loop at scale — a 2.00 GB source, every
other 512 KB row live:

```
rewrote 1.00 GB live in 0.217 s -> 4.60 GB/s
```

Both reproduce #320's spec §4 independently (0.024 s / 0.221 s, ~4.9
GB/s). The model is `live_bytes ÷ throughput` and nothing else.

### 5.3 The two spans #368 adds, and what they cost

`_rewire_sidecar_loaders` over **4000** in-memory instances:
**0.0006 s** (min of 3). Over the 200-instance corpus: 0.0000 s.

`compact_sidecar()` **plus** `_rewire_sidecar_loaders` measured together
on the 200 MB corpus: **0.0593 s** (`bench_gate_d2.py`; the spread
against §5.2's 0.026 s is page-cache state, not the rewiring).

**#368 presents the rewiring as an extension of the hold that has to be
argued for. It costs 0.2% of a 2 GB compaction.** The argument for
including it (§6.4) is correctness; there is no cost side to weigh.

### 5.4 Cross-process contention, measured

`bench_gate_d2.py`. Parent takes the gate, *then* starts a child that
blocks in `flock`, then compacts the 200 MB corpus and releases:

```
sidecar: 200.1 MB, 200 live of 400
parent held the gate 0.0593 s for compact_sidecar + _rewire
child (a spawned redaction worker's write_frame) blocked: 0.3607 s
```

The child's 0.3607 s is the parent's deliberate 0.3 s stagger plus the
0.0593 s hold: it waited exactly the hold, to the millisecond. **A
spawned redaction worker is blocked for `live_bytes ÷ throughput`, and
the mechanism is confirmed rather than assumed.**

### 5.5 The liveness number the owner has to look at

| storage | 1.0 GB live | 1.5 GB live | 3.0 GB live |
| --- | --- | --- | --- |
| this laptop (4.6 GB/s) | 0.22 s | 0.33 s | 0.65 s |
| 500 MB/s | 2.0 s | 3.1 s | 6.1 s |
| 100 MB/s network | 10.2 s | 15.4 s | **30.7 s** |
| 50 MB/s network | 20.5 s | **30.7 s** | **61.4 s** |

Bold crosses `_SHUTDOWN_JOIN_TIMEOUT_S = 30.0`
(`persistence_manager.py:29`, joined at `:679`). #368's costing of this
is correct and I am confirming it rather than restating it: a `close()`
landing inside such a hold joins with that fixed timeout, #314 reads a
timed-out join as a wedged worker, `_drain_recoverable_saves()` then
runs a reconciliation `save_all` on the closing thread — which blocks on
the same gate. **A compaction longer than 30 s misfires the whole
#313/#314/#315 machinery**, converting a healthy compaction into the
exact shape those three issues exist to detect. It needs ~1.5 GB live at
50 MB/s or ~3 GB at 100 MB/s: network storage, not a laptop, and not
hypothetical for the 100 GB+ datasets the memory design targets.

**And the amplification is worse than that, because of §6.5's sqlite
chain: 120 s, not 30 s, is the real ceiling on a gate hold.**

---

## 6. The design, and the three attacks against it

### 6.1 The primitive

A **store-level gate**, one per `SqliteStore`:

* `self._sidecar_gate = threading.Lock()` — the in-process half.
  Constructed in `__init__` beside `_pixel_swap_lock`, added to
  `__getstate__`'s `keys_to_remove` (`persistence.py:684-701`) and
  recreated in `__setstate__` (`:704-727`). **Miss either and every
  pickle of a store raises `TypeError: cannot pickle '_thread.lock'
  object`** — the trap that comment already documents for #218.
* `<self.sidecar_path>.lock` — the cross-process half. Created by
  `os.open(path, os.O_RDWR | os.O_CREAT)`, **never** `open(path,'wb')`
  (which truncates, harmlessly today and confusingly forever), **never**
  renamed, **never** removed for a file-backed store.
* Acquire: take the `Lock`, then `os.open` a **fresh** fd and
  `flock(LOCK_EX)` on it. Release: `LOCK_UN`, `close`, release the
  `Lock`. Fresh fd per acquisition, because §3 fact (a) makes a cached
  one unable to exclude two threads of one process — and the `Lock`
  makes that safe, because it guarantees only one fd of this process is
  ever in the flock at a time, so §3 fact (b)'s self-deadlock is
  converted into a plain, loud, in-process `Lock` deadlock.

The `Lock` is plain, not an `RLock`, per #368's preference — and under
§6.2's placement that preference is *achievable*, which under the
brief's placement it is not (§6.3).

**Why not on `SidecarManager`?** Because `SidecarManager` is constructed
ad hoc: `io_handlers.py:3382` and `:3553` build one per loader read.
A gate on an object with that lifetime would be a gate per read. The
store owns exactly one sidecar for its lifetime, and the gate belongs
next to `_pixel_swap_lock` and the three comments that will now describe
it. `SidecarManager` stays a dumb file wrapper.

### 6.2 Where it is acquired — every site, exactly

**The rule is one sentence: the gate is always acquired *outside*
`_pixel_swap_lock`, and never inside `write_frame`.**

| Site | Acquire at | Hold across |
| --- | --- | --- |
| 1–3 ingest | `import_files`' aggregation loop (`io_handlers.py:1700`), once around the whole loop or once per result | the `write_frame`s and their `record_blob_ref`s |
| 4 `persist_pixel_data` | top of the method, `persistence.py:2510`, **above** `with self._pixel_swap_lock` at `:2519` | the swap block **and** `record_blob_ref` at `:2575` |
| 5 `persist_blob` | top of the method, `persistence.py:2299` | `write_frame` (`:2327`) and `record_blob_ref` (`:2329`) |
| 6 `save_all` | `persistence.py:2628`, **immediately before** `self._prepare_pixel_frames(...)` | the prepass **and** the transaction that commits the rows (`record_blob_ref` at `:2916`) |
| compaction | `session.py:1061`, **after** `save(sync=True)` at `:1044` and after the refusal at `:1053` | `compact_sidecar()` (`:1063`), `get_blob_refs` (`:1071`), `get_nested_pixel_refs` (`:1078`), **and** `_rewire_sidecar_loaders` (`:1086`) |

Site 6 is the one that turns "a lock around `save_all`" into something
larger than #320 prices, and it is unavoidable: the frame is appended in
the prepass and the row committed in the transaction, so a gate that
covers only one of them covers neither.

`write_frame` keeps its own inode flock unchanged. It is a *different*
lock answering a *different* question (§8.1), it composes (two files, no
self-deadlock), and it costs 11.9 µs.

**One assertion is worth more than any of the above.** Put in
`write_frame`, or in a private `_assert_gated()` the six sites and
`write_frame` share: *the gate is held by this thread*. A seventh write
site added after 1.0 then fails loudly on its first test run instead of
racing a compaction on a customer's cluster. Without it, this design's
correctness is a property of six call sites nobody is obliged to
re-read. `tests/test_source_citations.py` is this repo's precedent for
making prose fail; this is the same move in code.

### 6.3 Attack 1 — reentrancy

The brief asks exactly where the acquisition goes and why. Three
nestings to rule out, and one of them is fatal to the placement the
brief proposes.

**(i) `compact()`'s leading save.** `compact()` is `save(sync=True)`
(`session.py:1043`) → refusal (`:1053`) → `compact_sidecar()` (`:1063`)
→ rewiring (`:1086`). `save(sync=True)` runs `save_all`, which under
site 6 takes the gate. So the gate must be acquired **after** the save
and, by preference, after the refusal — `session.py:1061`. #320 and
#368 both prescribe this and both are right. If it were acquired before
the save: with a plain `Lock`, an immediate self-deadlock on the same
thread; with a `LOCK_NB` flock, a spurious "another writer holds the
gate" against yourself.

**(ii) `write_frame` under `_pixel_swap_lock` — the one that kills the
brief's design.** `_persist_pixels` takes `_pixel_swap_lock` at
`persistence.py:3157` and calls `write_frame` at `:3216`, *inside* it.
`persist_pixel_data` does the same (`:2519` → `:2553`). So if the gate
lives in `write_frame`:

* the writer's order is `_pixel_swap_lock → gate`;
* `_rewire_sidecar_loaders` takes `_pixel_swap_lock` per instance
  (`session.py:1132`) while the compactor holds the gate, so the
  compactor's order is `gate → _pixel_swap_lock`;
* those two orders are a cycle, and it is reachable by exactly the
  concurrency this whole design exists to make safe.

Probe (b) shows the same thing with no second thread at all: if a site
took the gate and then reached `write_frame`, the second `os.open` +
`flock` on the same lock file blocks against the first
(`BlockingIOError` under `LOCK_NB`, an unkillable wait under a blocking
call). **`flock` has no reentrancy to promote.** The way out is `RLock`
+ flock-at-depth-0 — which works, and which #368 argues against for a
good reason: a reentrant lock hides the next ordering mistake instead of
raising it.

§6.2's placement avoids all of it. The gate is above `_pixel_swap_lock`
at every site, nothing nests, and the plain `Lock` #368 wants is
achievable.

**(iii) Anything under the gate that saves.** Nothing does:
`compact_sidecar` and `_rewire_sidecar_loaders` call no save;
`record_blob_ref` writes one row. That is the audit that lets the plain
`Lock` stand, and it is the audit the assertion in §6.2 keeps true.

### 6.4 Attack 2 — liveness

Measured in §5.4 and §5.5, so only the parts that are not just numbers:

* **The hold is now applied cross-process**, which is new. §5.4 shows a
  child blocked for exactly the hold. On a redaction of a large cohort
  every worker blocks, not one.
* **The rewiring adds 0.0006 s at 4000 instances**, so #368's insistence
  that the hold span it is free. Include it. The reason to include it is
  #368's own and it is correct: a writer running between the end of
  `compact_sidecar()` and `_rewire_sidecar_loaders` appends to the *new*
  file, commits a *correct* row, leaves a *correct* resident loader —
  and the rewiring then overwrites that loader from a map computed
  before the write existed. Database right, memory wrong, hash mismatch
  on the next read, corrected only by a reopen. A different corruption,
  not the absence of one. **Note that the span is now three maps, not
  two** (`nested_updates`, #183, `session.py:1078`); a gate scoped to
  the two #368 knew about would leave the icon path in the gap.
* **The 30 s cliff is real and I am not softening it.** ~1.5 GB live at
  50 MB/s. Anything past it turns a healthy compaction into #314's
  wedged-worker shape. There is no fix inside this design: the hold is
  storage-bound by construction. The mitigations are (a) don't do this
  before Q1 is answered, (b) §6.5's bounded acquisition, so a *waiter*
  fails loudly rather than being counted as wedged, or (c) make
  `_SHUTDOWN_JOIN_TIMEOUT_S` a function of the corpus, which is a
  different issue and a worse one.

### 6.5 Attack 3 — deadlock against sqlite

The question the brief asks: can any path hold the gate while waiting on
a sqlite write lock held by a thread that is itself waiting to write a
frame?

**Waiting on a frame write is the only edge that could close the cycle,
and no sqlite-lock holder ever waits on one.** #287 is what makes this
true and it is worth naming, because the property is recent: `save_all`
appends every frame in `_prepare_pixel_frames` **before**
`with self._get_connection()` (`persistence.py:2628` precedes
`:2632`-ish), so the transaction does row upserts and nothing else — the
docstring at `:3050-3053` says *"no compression, no sidecar append, no
`flock` wait"*. `record_blob_ref` called with `conn=` joins an existing
transaction and writes one row. `_read_blob_index` and
`_apply_new_offsets` are pure sqlite. So:

```
gate  ->  _pixel_swap_lock          (leaf: two assignments, no sqlite)
gate  ->  _get_connection -> _memory_lock
_audit_write_lock -> _get_connection -> _memory_lock     (unchanged, never saves)
write_frame's inode flock: a leaf under everything
```

Every edge points the same way. **No cycle. The gate is a new top, and
`_audit_write_lock → _memory_lock` is untouched** — nothing takes the
gate while holding either, because the audit writer's only path is
`_drain_and_write → _audit_write_lock → _get_connection` and it never
saves. This extends #317's audit; it is not a corollary of it, and it is
a fresh argument about a lock that does not exist, exactly as #320's
spec §6.2 warned.

**What is not a deadlock but is the real cost, and it is Q5.**
`_SQLITE_BUSY_TIMEOUT_S = 120.0` (`persistence.py:175`). A gate-holder
inside `record_blob_ref` or `_apply_new_offsets` can wait the full 120 s
on a stuck sqlite writer — and while it waits it holds the gate, so
**every** other frame writer, in every process, waits 120 s too. That is
four times the 30 s shutdown join, so a single stuck sqlite writer now
misfires #313/#314/#315 by a comfortable margin where before it only
stalled one thread. It is #250's shape — a long silent stall — arriving
at a lock #280's timeout reduction cannot see.

**Recommended mitigation (a recommendation, not a finding).** Acquire
the flock with `LOCK_NB` in a bounded retry loop and raise on timeout,
rather than blocking. The deadline should be shorter than
`_SHUTDOWN_JOIN_TIMEOUT_S`, so a gate that cannot be had surfaces as an
error *before* the join that would misread it as a wedged worker. That
is #280's own philosophy — surface a stuck writer loudly rather than
stall silently — applied to the new lock at the moment it is introduced,
rather than after an issue is filed about it. It costs one error path
and it is the difference between a gate that degrades loudly and one
that degrades into #250.

### 6.6 What the gate closes, precisely

Against `tests/test_compaction_races_a_concurrent_write.py`'s five
phases:

| Phase | Mechanism | Closed by the gate? |
| --- | --- | --- |
| A — append during `_rewrite_live_frames` | writer's frame lands in the pre-swap inode | **Yes.** The writer's whole span is excluded from the rewrite. |
| B — append between the two `os.replace`s | `write_frame` opens a path that names nothing | **Yes**, and this is the phase §2.1 makes conditional: only if the sidecar is opened *after* the gate is taken. |
| C — append after the swap, before `_apply_new_offsets` | correct row overwritten by a stale map | **Yes.** |
| D — a blob row `_read_blob_index` never saw | INSERT after the index read, offset left pointing into the old layout | **Yes — but only because §6.2 puts the gate around the whole of `compact_sidecar()`, `_read_blob_index` included.** A gate scoped to the rewrite alone leaves D open. |
| E — the intruder is `save(sync=True)` | same window through `save_all` | **Yes**, via site 6, and only if the hold spans `_rewire_sidecar_loaders` (§6.4). |

And the waveform variant, through site 5, by the same argument.

**What it does not close is §7, and §7 is bigger than all five.**

---

## 7. The defect the gate does not close

`probe_redaction_span.py`. Three instances persisted and saved; then the
exact sequence a redaction worker runs — `regenerate_uid()`
(`services.py:462`), redact the array, `persist_pixel_data`
(`services.py:475`) — and then a `compact_sidecar()`, serialised
deliberately, because **this is not a race: it is a state a compaction
can observe for the whole length of a redaction pass.**

```
blob rows before: [('1.2.3.0','pixels',0,16395), ('1.2.3.1','pixels',16395,16395), ('1.2.3.2','pixels',32790,16395)]

worker: old uid 1.2.3.1 -> new uid 1.2.826.0.1.3680043.8.498.17888655...
worker: its blob row: [('1.2.826.0.1.3680043.8.498.17888655...','pixels',49185,39)]
worker: instances table knows the new uid? False

compaction: uid_map keys: ['1.2.3.0','1.2.3.1','1.2.3.2']
compaction: sidecar 49224 -> 49185
compaction: blob rows after: [('1.2.3.0',...), ('1.2.3.1',...), ('1.2.3.2',...)]

REDACTED FRAME'S ROW SURVIVED? False
readback: RuntimeError Pixel Loader failed for 1.2.826...: Integrity Error:
          Failed to read/decompress frame ...
```

**The compaction deleted the redacted frame's blob row and reclaimed its
bytes.** Not because it raced the rewrite — it did not; the rewrite had
not started when the row was written. Because `_read_blob_index`'s
liveness predicate (`persistence.py:3667-3684`) is *"an `instances` row
exists with this UID"*, and the worker's regenerated UID does not reach
`instances` until the parent's `_apply_redaction_outcomes` applies the
mutation (`session.py:2827`, which the docstring at `:2835-2841` says
must apply the new identity) **and** the caller then saves — and
`redact()` deliberately does not save: its docstring says *"call
`.save()` afterwards to persist it."*

### 7.1 The window, stated exactly

**Per mutation, the window is: the worker's `record_blob_ref` → the
parent's `_apply_redaction_outcomes` for *that* mutation.** It is not
"plus however long the caller waits to save", and getting that wrong
would be the same over-read this spec criticises elsewhere. Once the
parent has applied a mutation, the instance carries the new UID and is
`mark_modified()`, so `compact()`'s own leading `save(sync=True)`
(`session.py:1043`) writes its `instances` row, the blob row becomes
live, and the frame survives. **The front door rescues everything
already applied.**

What it does not rescue is the **unapplied** population — the mutations
still in the worker pool or queued in the generator `session.py:2773`
returns, which `_apply_redaction_outcomes` consumes incrementally. At
any instant that is on the order of `max_workers` plus whatever is
buffered, and it is *continuously* non-empty for the whole length of the
pass. So the exposure *interval* is a redaction pass and the blast
radius at any moment is a handful of instances, which is still two to
four orders of magnitude wider than the 0.024–0.221 s window #320
accepts, and it destroys the redacted output specifically.

**What the probe reproduced, and what it did not.** It drove
`store.compact_sidecar()` directly, so it establishes the *state* at the
store layer: given a blob row under a UID `instances` does not carry,
compaction deletes the row and reclaims the bytes. Reaching that state
through the public `session.compact()` additionally requires the
**processes** path and a mutation the parent has not yet applied —
because otherwise the leading save closes it. That is the population Q1
is about, and it is 3.12's default. §12.2 says how to drive the front
door for it.

**The threads path (3.14t) is narrow, and the contrast is the point.**
There the worker *is* the parent's object, so `regenerate_uid()`,
`persist_pixel_data()` and `mark_modified()` all land on the parent
instance immediately and the leading save rescues it — except where
`_prepare_pixel_frames` froze the dirty set before the worker dirtied
the instance (`persistence.py:3043`, whose docstring states that
freezing). Narrow, and only on the free-threaded build. **The exposure
is a property of the default build**, which is exactly why Q1 reads the
way it does.

Four things follow, and each of them matters to Q1 and Q2:

1. **No lock closes it.** The failure is in the orphan predicate, not in
   atomicity. Making the child's write-frame-and-commit-row atomic
   against compaction changes nothing: the row is orphan-shaped *after*
   it commits, correctly formed, for the whole span.
2. **A gate cannot be stretched over it.** `redact()` would have to hold
   the gate from dispatch through `_apply_redaction_outcomes` — and the
   children need the gate to write their frames, so they would block on
   the parent's flock forever. That is a real deadlock, and it is the
   reason this cannot simply be folded into §6.
3. **Ingest has the same shape, and a far narrower window.** Sites 1–3
   `record_blob_ref` before any `instances` row exists — but the linkage
   that puts `inst` into the graph follows a few statements later in the
   same loop iteration, after which `compact()`'s leading save writes
   the row. So the exposed population is at most **the one result being
   iterated**, for a window of a few lines, not "every frame ingested so
   far". Not reproduced; the mechanism is the same predicate on the same
   line. Say it because it shows this is a property of
   `record_blob_ref`'s ordering against `instances`, not of redaction —
   and say the width, because the redaction version is the one that
   matters and conflating them helps neither.
4. **The documented serial pipeline is safe, and that is the whole of
   why this has never been seen.** `compact()` leads with
   `save(sync=True)` (`session.py:1043`), which writes the `instances`
   rows for every applied mutation, so `ingest → save → redact → save →
   compact` never observes the state. It is reachable only by a
   concurrent `compact()` — the same precondition-violating population
   as #320, with an exposure interval of a whole redaction pass instead
   of 0.024–0.221 s.

Candidate fixes, none costed here and all of them a separate issue:
a monotonic generation or `created_at` column on `instance_blobs` that
compaction refuses to reclaim below; `record_blob_ref` inserting a stub
`instances` row; or the worker filing under the pre-redaction UID and
the parent re-keying. The first is the smallest and the only one that
does not change what `instances` means.

### 7.2 A second unclosed span on the same path, which the orphan fix
### does not reach either

Worth separating from §7 because it survives *both* the gate and a fixed
orphan predicate, and an implementer who fixes those two will believe
the redaction path is done.

`_apply_redaction_outcomes` binds `mutation['pixel_loader']`
(`session.py:2925`, bound at `:2946`) — a loader object carrying the offset the
**child** computed. If a compaction runs between the child's
`record_blob_ref` and the parent's application of that mutation, it
rewrites the row to X′ and `_rewire_sidecar_loaders` rebinds whatever
loader the parent instance currently holds (the *pre*-redaction one).
The parent then applies the mutation and binds X. Database X′, resident
loader X, hash mismatch on the next read after `discard_pixel_data()`,
correct only after a reopen.

The gate cannot reach this: the interval it spans is a generator yield
in the parent, and §7 point 2's deadlock argument applies unchanged.
Two candidate answers, neither costed: `_apply_redaction_outcomes`
re-reads `(offset, length)` from `instance_blobs` when it binds rather
than trusting the mutation's, or `compact()` refuses while a redaction
pass is open — **a flag, not a lock**, because a lock is the thing that
deadlocks the children. This is the third item the "build §6 and §7
together" path in §11 has to settle, and it is why that path is still
not a complete answer.

---

## 8. #366's two halves under this recommendation

### 8.1 `SidecarManager._lock` and the three comments

#366 offers two ways out and says either is fine. **I recommend a third,
and the reason is that both of #366's are built on the same conflation:
that `_lock` and the flock are two spellings of one thing.** They are
not. There are three questions, and today two of them have no answer:

| Question | Answer today | Answer recommended |
| --- | --- | --- |
| Do two appends interleave, or get the same offset? | `write_frame`'s inode flock (`sidecar.py:55`). Correct. | unchanged |
| Do two threads of one process contend for a `SidecarManager` object? | `_lock`. Never acquired. **The object has no mutable state** — `filepath` is set once and never written — so there is nothing for it to protect. | **delete `_lock`, `__getstate__`'s branch, and `__setstate__`** |
| Do appends and a *rewrite* see the same generation of the file? | nothing | the gate (§6) |

So: **delete `SidecarManager._lock` and rewrite the three comments** at
`persistence.py:673`, `persistence.py:3137-3138` and `session.py:1127`
to name what is actually ordered. Under §6 that is
`_sidecar_gate → _pixel_swap_lock → sqlite`, with `write_frame`'s inode
flock a leaf beneath all three — which **inverts** the order the three
comments currently assert (`_pixel_swap_lock` before the sidecar lock).
That inversion is safe because the lock they name does not exist; but it
must be written down as an inversion, not slipped in, or the next reader
audits new code against a rule that changed underneath them.

**If Q1 says the gate is not built, delete `_lock` and the comments
anyway.** That half of #366 stands on its own, costs nothing, and
removes a documented invariant with no code behind it — which is the
failure mode #366 was filed about. It should land before 1.0.

### 8.2 The `self.sidecar` rebind

`persistence.py:3640`. #366 says a concurrent writer holding the
previous manager object writes through the wrong one, and that this
becomes a live bug the moment `_lock` is real.

**Verified, and the conclusion is stronger than the issue's: the rebind
is inert and should be deleted, not protected.** `SidecarManager.__init__`
sets `self.filepath` (the same string), creates an unused `_lock`, and
calls `_ensure_file()`, which finds the file present. The new object is
indistinguishable from the old one in every observable way. Under §6 the
gate lives on the store, not the manager, so the rebind cannot even
become the hazard #366 describes.

Delete `persistence.py:3640`, with a comment saying why the line looked
necessary: it reads as "re-open the compacted file", and the file is
never held open between calls, so there is nothing to re-open. That
comment is the deliverable — the deletion without it invites the line
back.

**#366's premise that the two halves cannot be fixed independently is
therefore false under this recommendation**, and it is false in the safe
direction: the coupling it identifies exists only on the branch where
`_lock` is *taken*, which §8.1 rejects. Both halves become one-line
deletions plus prose. Say so in the issue.

### 8.3 The third, cosmetic half

`persistence.py:2586`: `raise e` rather than a bare `raise`, truncating
the traceback at that frame. Confirmed. Two siblings share the defect
and #366 does not name them: `persistence.py:750` and
`persistence.py:2024`. If this is folded in, fold in all three.

---

## 9. The things that have to be stated rather than investigated

### 9.1 Reads are not gated, deliberately

`read_frame` takes no lock today and would take none under this design.
A reader that opens before the swap reads the old inode correctly to
completion (it is not unlinked until `os.remove(backup_path)`, and even
then its data survives while the fd is open). A reader that opens after
the swap but before `_rewire_sidecar_loaders` reads at a pre-compaction
offset and gets `Integrity Error: Pixel data hash mismatch` or
`Incomplete read from sidecar` — loud, transient, and correct on retry
after the rewiring. Gating reads would put the whole compaction hold in
front of every `get_pixel_data()`, which is the memory design's hot
path. **Not gating them is a decision, and this paragraph is it.**

### 9.2 A lock file left behind by a crash

Nothing to clean up. Probe (e): the kernel releases the flock when the
last fd closes, `SIGKILL` included, and the residue is a **zero-byte
file holding no state**. There is no stale-lock detection, no PID file,
no timeout, and no recovery path — which is the main practical argument
for `flock` over an `O_EXCL` sentinel, and it should be written into the
comment beside the lock file's creation so nobody adds a reaper.

Two housekeeping consequences:

* **`.gitignore` needs `*.lock`.** Tests write `*_pixels.bin` into the
  repo root and it is covered by `*.bin`; `foo_pixels.bin.lock` is
  **not** matched by any existing pattern. Without the entry the first
  test run after this lands dirties the working tree.
* **`:memory:` stores leak one.** `persistence.py:635-636` gives an
  in-memory store a `NamedTemporaryFile(suffix="_pixels.bin",
  delete=False)` sidecar; its `.lock` twin is created beside it and
  nothing unlinks either. Pre-existing for the sidecar, new for the
  lock. Either unlink both in `close()` or accept two files instead of
  one, but decide rather than discover.

### 9.3 Windows

**State the existing constraint; do not invent a new one.**
`sidecar.py:48` does `import fcntl` inside `write_frame`. `fcntl` is
POSIX-only, so on Windows **`write_frame` already raises
`ModuleNotFoundError` on its first call** — no sidecar frame can be
written at all, which means no ingest with pixels, no redaction swap and
no save with dirty pixel data. There is no `msvcrt` fallback anywhere in
the package. The gate adds a second `fcntl` user and changes nothing
about this.

What it does do is make an already-unbacked claim harder to ignore:
`setup.py:44` carries `"Operating System :: OS Independent"`. That is
the same shape as the `python_requires=">=3.9"` the classifier block's
own comment (`setup.py:45-46`) says was deleted for being an unbacked
promise. **File it**, and file it as the classifier's problem rather
than as a porting task: what a Windows port would take is not
established here and is deliberately not guessed at, because §2's whole
argument is that assuming a locking primitive's semantics is how this
gets got wrong. `os.replace` over a file another handle has open is
itself a different question on Windows, and neither it nor a
`msvcrt.locking` arm was investigated.

### 9.4 The spawn pin is load-bearing for the gate

`parallel.py:402-412` and `session.py:610-619` both pin
`multiprocessing.get_context("spawn")` for #220 and #250's reasons. That
pin is now also what makes the gate correct: a **forked** child inherits
the parent's open file descriptors, and an inherited fd carries its
flock — so a fork taken while the parent held the gate would give the
child a lock it never acquired and cannot account for, and two processes
would both believe they held it. Add this to the reasons already listed
in those comments. `tests/test_parallel_contract.py` already pins the
argument; it should pin this reason too, or the reason is prose again.

---

## 10. Subsume or complement

### 10.1 Does the stable lock file subsume #368's five-site `threading.Lock`?

**It subsumes the mechanism and keeps the scope, and #368 should be
re-scoped rather than closed.** Three separate answers, because the
question has three parts:

* **Mechanically, one gate with two halves, not two gates.** Probe (c)
  shows `flock` on per-acquisition fds already excludes threads of one
  process, so in principle the flock alone would do both jobs. In
  practice it must not be used alone: probe (b) shows it has no
  reentrancy and fails in the kernel rather than in Python. The
  `threading.Lock` in §6.1 is there to make the in-process half fail
  *loudly and locally*, and to guarantee only one fd of this process is
  ever in the flock. So #368's `threading.Lock` survives — as the
  in-process half of one gate, not as a second mechanism with its own
  ordering argument.
* **Scope-wise, #368 is right and this spec extends it.** Its central
  correction — that the span is write-frame-**and**-commit-row at every
  site, and that the compaction hold must include the loader rewiring —
  is confirmed. Its site list is stale by one (#183's nested pixels) and
  its rewiring hold, which it presents as a cost, measures 0.0006 s.
* **Its cross-process premise is wrong and its conclusion is right.**
  Ingest does not need a cross-process lock; redaction does. §4.

**Can `flock` span a write-frame-then-commit-row sequence that includes
a sqlite transaction?** Yes, mechanically — it is an advisory lock on an
unrelated file and knows nothing about sqlite. The question is whether
it *should*, and §6.5 is the answer: it can, at the price of the 120 s
amplification, which is why §6.5 recommends a bounded acquisition. So
**both halves are required, and the reason is not "one covers threads
and one covers processes" but "one gives reentrancy failures a Python
traceback and the other crosses the process boundary."** Say that in
#368's body; it is the sentence the next implementer needs.

Concretely, #368 should be edited to: six sites not five; the ingest
sites are in the parent and never needed this; `persist_pixel_data` is
the spawned-subprocess site and it is on the default build; the rewiring
hold costs 0.0006 s; the gate is `Lock` + stable-lock-file flock, above
`_pixel_swap_lock`, never inside `write_frame`; and §7 is a prerequisite
question, not a detail.

### 10.2 #26 and 1.0

**Not frozen.** `_sidecar_gate` is a private attribute of `SqliteStore`.
`<sidecar>.lock` is an implementation file. Neither is named by #26.

**Effectively frozen, and this is the real weight.** The lock *order*.
This repo audits new concurrency against a documented order — #218,
#274, #280, #295, #313–#316 all argue in its terms — and §6.5 puts a
third level above `_audit_write_lock → _memory_lock`. Every concurrency
argument after 1.0 becomes a three-level one. Also frozen: the **latency
contract**. A save or a redaction worker that used to overlap the start
of a compaction now blocks for `live_bytes ÷ throughput`, cross-process.
That is observable, and after 1.0 changing it back is a behaviour change
someone can have built on.

**The brief's "cheap to add later is probably false" is half right, and
§7 is what splits it.** Yes, the ordering is cheaper to introduce before
the freeze than after. But the *reachable population* has just changed
under both #320 and #368: it is not exotic ingest, it is `redact()`
concurrent with `compact()` on the default build — and on that path the
gate closes the 0.221 s rewrite window and leaves §7's redaction-pass
window wide open. **Landing the gate before 1.0 would freeze a
three-level lock order in exchange for closing the smaller half of a
two-part problem**, which is precisely the "a lock ordering frozen with
a premise nobody re-checked" that #368 filed itself post-1.0 to avoid.

---

## 11. RECOMMENDATION

*A recommendation, for the owner to accept or reject.*

**Land #366 now as two deletions and three rewritten comments. Do not
build the gate until Q1 and Q2 are answered; answer Q2 first, because it
changes what the gate is worth.**

The three sentences with the numbers behind them:

> The brief's stable-lock-file design is **correct and cheap**: the
> inode-versus-path failure it is built on **reproduced** (a writer that
> blocked on the sidecar's own flock woke after the swap and appended
> into an unlinked inode, committing offset 14 into a 9-byte live file),
> a stable `<sidecar>.lock` fixes it **only** when the sidecar is opened
> *after* the gate is taken, and the gate costs **11.9 µs** per
> acquisition — **0.178%** of a 6.7 ms `write_frame` — while the hold is
> **0.026 s** at 200 MB and **0.217 s** per 1 GB live, with
> `_rewire_sidecar_loaders` adding **0.0006 s at 4000 instances**, so
> #368's insisted-upon rewiring span is free and a spawned worker
> blocked behind a 200 MB compaction waited exactly **0.0593 s**
> (measured, not modelled). Against that: the brief's placement — the
> flock inside `write_frame` — is **unsafe**, because two fds on one
> lock file deadlock within a single thread and `_persist_pixels` calls
> `write_frame` inside `_pixel_swap_lock`, so it creates a real cycle
> against `_rewire_sidecar_loaders`; the gate must instead be acquired
> at six sites (not five — #183 added one in this base commit) always
> *outside* `_pixel_swap_lock`, which puts a third level above the
> documented `_audit_write_lock → _memory_lock` pair that #26 freezes,
> and which lets one stuck sqlite writer hold **every** frame writer in
> **every** process for `_SQLITE_BUSY_TIMEOUT_S = 120 s` — four times
> the fixed **30.0 s** shutdown join that #313/#314/#315 read as a
> wedged worker. And the finding that moves the recommendation: a
> **concurrent `compact()` deletes an in-flight redaction's blob row and
> reclaims its bytes outright** — reproduced deterministically at the
> store layer — because `_read_blob_index` calls a row live only while
> an `instances` row carries its UID, and a redaction worker files under
> a **regenerated** UID that the parent puts in `instances` only when it
> applies that worker's mutation; `compact()`'s leading `save(sync=True)`
> rescues every mutation already applied, but the unapplied set is
> continuously non-empty for the whole pass on the processes path, so
> the exposure interval is a redaction pass rather than 0.221 s, it is
> on the same concurrent-`compact()` path, and **no lock closes it** —
> nor does fixing the orphan predicate alone, because §7.2 is a third
> unclosed span on the same path. A gate shipped now would freeze a
> three-level lock order in exchange for the smaller half of the
> problem.

**What to land now, before 1.0** — all of it deletion and prose, none of
it new concurrency:

1. **Delete `SidecarManager._lock`**, its `__getstate__` branch and its
   `__setstate__` line (`sidecar.py:18`, `:124-125`, `:131`). The object
   has no mutable state for it to protect.
2. **Rewrite the three comments** (`persistence.py:673`,
   `persistence.py:3137-3138`, `session.py:1127`) to say that
   `write_frame`'s `fcntl.flock` on the fd is the whole of the append
   discipline, that `_pixel_swap_lock` is above it, and — the part
   #366's second option omits — that **nothing serialises an append
   against a compaction**, with a pointer to #368.
3. **Delete `persistence.py:3640`'s `self.sidecar = SidecarManager(...)`**
   with a comment saying why it looked necessary and is not.
4. **Correct `read_frame`'s description in #366 and in the compaction
   spec's Q4**: `read_frame` does not flock.
5. **File §7** as its own issue, with the probe transcript. It is wider
   than #320, it is on the default build, and it is the reason to answer
   Q1 before building anything.
6. **Re-scope #368** per §10.1. Do not close it; its scope correction is
   right and its cross-process conclusion is right for the wrong reason.
7. **File the `OS Independent` classifier** (§9.3) and the three
   `raise e` sites (§8.3). Neither is this work; both were found by it.

**What to build, if and only if Q1 says concurrent `redact()`/`compact()`
is supported in 1.0:** §6 as specified — plain `Lock` + stable lock
file, six sites plus compaction, always outside `_pixel_swap_lock`,
`LOCK_NB` with a deadline below 30 s, an assertion in `write_frame` —
**plus** §7's orphan predicate **and** §7.2's loader-offset span, in the
same piece of work. All three or none: each of the three leaves the
redaction path broken on its own, and the gate is the *smallest* of
them.

**The case against this recommendation, stated fairly.** The gate is
genuinely cheap (0.178% per write, 0.06 s hold at 200 MB), the freeze
genuinely makes the ordering cheaper now than later, and §7 being open
is an argument for fixing §7, not for leaving the 0.221 s window as
well. If you weigh "a de-identification tool should not have a
documented-but-unenforced single-writer precondition at 1.0" more
heavily than I do — and that is a defensible weighting, since the
precondition is enforced by a guard that measurably never fires — then
the answer is to build §6 **and** §7 together as one pre-1.0 piece of
work, and Q3 and Q5 become the first things to settle.

---

## 12. Tests, whichever way this goes

Style throughout: `tests/test_compaction_races_a_concurrent_write.py`,
which already exists and which these extend rather than replace — a
helper thread on a bounded daemon, ordering forced by a pair of
`threading.Event`s from a monkeypatched phase method, the helper's
exception kept rather than swallowed, and the readback done through the
loader after `discard_pixel_data()` (not `unload_pixel_data()`, which
refuses a diverged array, #293).

### 12.1 The deterministic reproduction of the writer-during-compaction loss

**`tests/test_flock_does_not_survive_the_sidecar_swap.py`** — new, and
the one this design most needs, because it pins the *reason* the gate is
shaped the way it is rather than the fact that it works.

No session, no store: `SidecarManager` and `os.replace` alone, so it
cannot rot with the pipeline.

**The event ordering is the whole test and it is easy to get subtly
wrong**, which cost real time when the probe was built. Three
`threading.Event`s, in this order and no other:

* Main thread takes `flock(LOCK_EX)` on its own fd **first**, then sets
  `holder_ready`. If the writer can reach its `flock` before this, it
  acquires immediately, never blocks, and the test measures nothing
  while reading green.
* Writer thread waits on `holder_ready`, then `open(path,'r+b')`, then
  sets `opened` — the signal must sit **between** the `open` and the
  `flock`, because "the writer is holding an fd on the old inode" is the
  state the swap has to land on top of — then `flock(LOCK_EX)`, which
  blocks.
* Main thread waits on `opened`, writes `.compact.tmp`, `os.replace`
  twice, unlocks, `os.remove`s the backup. The writer wakes and does
  `seek(0,2)`, `tell()`, `write`.
* Assert `os.fstat(writer_fd).st_ino != os.stat(path).st_ino` — the
  writer wrote into an inode that is no longer the sidecar.
* Assert `offset + length > os.path.getsize(path)` — the row it would
  commit dangles past EOF.

Then a second test with the same scaffolding and a `<path>.lock`,
**parametrised over `open`-then-`lock` and `lock`-then-`open`**,
asserting that the first is still lost and the second lands inside the
file. That parametrisation is the test: without it, someone "tidies"
`write_frame` by moving the lock acquisition beside the existing flock
and the suite stays green while §2.1's arm comes back.

**Under Arm A (no gate) both tests pass as characterization**, exactly
as `test_compaction_races_a_concurrent_write.py` does. They are the
recorded reason a future gate must be a stable lock file, and they cost
nothing to keep.

### 12.2 §7's reproduction

**`tests/test_compaction_reclaims_an_in_flight_redaction.py`** — new,
and it wants **two** tests, because §7.1 splits the finding into a state
and a way of reaching it.

**Test one: the state, at the store layer, with no threads at all.**
Serial: `persist_pixel_data` + `save(sync=True)` three instances; then
`regenerate_uid()`, `set_pixel_data(zeros)`, `store.persist_pixel_data`
— the worker's exact sequence — then **`store.compact_sidecar()`
directly, not `session.compact()`**, and the test's docstring must say
why: the front door leads with `save(sync=True)`, which would write the
`instances` row and close the window, so calling it here would be the
"green for the wrong reason" trap §320's spec names. This test pins the
predicate, and it will not rot.

* Assert the new UID is absent from `instances` at the moment of the
  compaction (that is the precondition, and stating it is what stops the
  test being read as a race).
* Assert the new UID's row is **gone** from `instance_blobs`.
* Assert the readback raises `RuntimeError` with `Integrity Error`.

**Test two: the front door, which is what Q1 turns on.** Drive
`session.compact()` on a helper thread while a redaction is mid-pass,
with `_apply_redaction_outcomes` parked by a pair of `threading.Event`s
so at least one mutation is committed by the worker and unapplied by the
parent — the state §7.1 says the leading save cannot rescue. Force
processes (`ISOCENTER_FORCE_PROCESSES`, per `docs/environment.md`), or
the threads path applies the mutation to the parent's own object and the
window closes. Assert the same three things. **Skip this test on a
free-threaded build rather than letting it pass vacuously**; a skip that
names the reason is the honest form of "narrow on 3.14t".

A waveform twin through `persist_blob` is worth one more test, for the
same reason #320's spec gives: the "loud, not silent" claim is only as
good as its weakest data path, and the waveform loader raises a
different exception type (`ValueError: Waveform integrity check
failed`).

### 12.3 What goes red if the gate is built

* `tests/test_compaction_races_a_concurrent_write.py` — all five phases
  plus the waveform variant flip from the characterization column to the
  Arm B column of #320's spec §9 table. The scaffolding does not change.
  Phase D flips **only** if the gate spans `_read_blob_index`; phase E
  **only** if it spans `_rewire_sidecar_loaders` (§6.6).
* `tests/test_save_redact_race.py:241`
  (`test_the_store_still_pickles_with_its_pixel_swap_lock`) needs a twin
  for `_sidecar_gate`, or the first `ProcessPoolExecutor` dispatch after
  the gate lands raises `TypeError: cannot pickle '_thread.lock' object`
  — the #218 trap `persistence.py:684-701` documents.
* `tests/test_compact_rewiring_is_locked.py` asserts the current
  `_pixel_swap_lock` discipline in `_rewire_sidecar_loaders`; under §6.2
  that loop runs with the gate held above it, and the test should be
  extended to assert the *order*, not just the acquisition — that is the
  assertion that would have caught the brief's cycle.
* A new `tests/test_sidecar_gate_order.py` pinning: nothing acquires the
  gate while holding `_pixel_swap_lock`; `write_frame` asserts the gate
  is held; `compact()` acquires after `save(sync=True)`. Written as
  instrumentation on the real locks (a recording wrapper, as
  `test_save_all_contract.py:261-267` wraps `write_frame`), not as a
  grep over the source.
* `tests/test_parallel_contract.py` — extend the spawn pin's stated
  reasons with §9.4's.

---

## 13. Filed rather than fixed

* **§7** — a concurrent compaction reclaims an in-flight redaction's
  frame. Wider than #320, unclosed by any lock, on the default build.
  The one to file first, with §7.1's exact window rather than the loose
  version.
* **§7.2** — the loader-offset span between the child's commit and the
  parent's `_apply_redaction_outcomes`, which survives both the gate and
  a fixed orphan predicate. Reasoned, not reproduced. File it with §7 or
  beside it; do not let it be read as part of §7, because the fix is
  different.
* **§7 point 3** — the same shape on the ingest path, by the same
  predicate, in a window of a few statements. Reasoned, not reproduced.
* **`setup.py:44`'s `OS Independent`** against `sidecar.py:48`'s
  unconditional `import fcntl` (§9.3).
* **`persistence.py:750`, `:2024`, `:2586`** — three `raise e` where a
  bare `raise` belongs. #366 names only the third.
* **`.gitignore` has no `*.lock`** (§9.2), and a `:memory:` store leaks
  its `NamedTemporaryFile` sidecar today and would leak a lock file too.
* **The compaction spec's Q4 and #366 both say `read_frame` flocks.** It
  does not. Correct the issue; leave the dated spec alone and mark the
  clause per CLAUDE.md's rule if it is ever edited.
