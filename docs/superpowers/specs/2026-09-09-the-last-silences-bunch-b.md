# The Last Silences, bunch B: the strategy `redact()` names, and the lever it cannot honour

**Date:** 2026-09-09
**Milestone:** v0.9.5 -- The Last Silences
**Issues:** #384 (`redact()` names no strategy), #400 (`ISOCENTER_FORCE_PROCESSES=1` on a `:memory:` store)
**Base:** `main` at `f544989` (bunch A: #392, #386, #388, #404)
**Status:** design brief for a TDD developer. **§14 Amendments is the current
design.** All four §10 questions were ruled on 2026-09-09, after this body was
written and by the same owner, and **Q1 was ruled against this brief's
recommendation** -- the ruling itself changed once the measurement in §4.1
disproved its premise. Where §14 disagrees with anything above it, §14 wins;
the body is left standing because a design record whose value is that it says
what was thought at the time cannot be quietly rewritten to match what was
decided afterwards.

Both issues are the session reporting a strategy it is not using. #384 is a
sentence with the fact removed from it; #400 is an operator's lever discarded
without a word. They share one seam -- the `_Strategy` that `parallel.py`
resolves and `session.py` never sees -- and the fix is to make that object
travel, so that the line a user reads and the refusal a user hits are both
*readings of the decision* rather than second guesses at it.

---

## 1. How this was measured

Nothing below is inherited from the issue text. Both defects were reproduced
from scratch on both gate interpreters, and where the issues disagree with the
code, the code is what is reported -- §4.1 is the case where they do.

- **Floor:** `/Users/kevin/Developer/Isocenter/.venv/bin/python` -- 3.12.14, `.[dev]`.
- **Free-threaded gate:** `/Users/kevin/Developer/Isocenter/.venv314t/bin/python` -- 3.14.7 free-threading build, `.[dev]`.
- **Worktree:** `/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-ae2825b99ea90773c`.

Every invocation carried `PYTHONDONTWRITEBYTECODE=1` and
`PYTHONPATH=<worktree>`, and every probe prints `isocenter.__file__` as its
first line. Measured, on both interpreters:

```
isocenter.__file__ = /Users/kevin/Developer/Isocenter/.claude/worktrees/agent-ae2825b99ea90773c/isocenter/__init__.py
```

so the editable install did not resolve to the main checkout.

The free-threaded gate reports, after `import isocenter` -- which pulls numpy,
pydicom and imagecodecs:

```
python = 3.14.7 free-threading build (main, Aug 31 2026, 10:41:21) [Clang 23.1.0 ]
gil_enabled = False
```

and the floor reports `gil_enabled = no attr`: 3.12.14 has no
`sys._is_gil_enabled` at all, which is what the `hasattr` guard at
`parallel.py:337` is for. Both facts matter to §7. A 3.14t test that does not
assert `sys._is_gil_enabled() is False` **inside the test process, after the
imports** is not testing the free-threaded path, because an extension without
free-threaded support re-enables the GIL silently and the green run then proves
nothing about the gate.

Three probes, committed beside this brief in
`docs/superpowers/specs/2026-09-09-the-last-silences-bunch-b/` for bunch A's
reason -- a document whose whole claim is measured evidence cannot point at a
directory that exists for nobody but its author. They are kept off the docs site
by `mkdocs.yml`'s `exclude_docs` and cannot be collected by pytest, because
`pytest.ini` sets `testpaths = tests`.

- `probe_384_decision_table.py` -- calls the shipped `_resolve_strategy` over the
  full lever matrix (§2).
- `probe_384_400_redact_speech.py` -- runs the whole `redact()` pass on seven
  store-and-lever arms, capturing stdout verbatim, the `_Strategy` actually
  resolved, and which of the three dispatch paths ran (§3.2, §4.1).
- `probe_400_refusal_blast_radius.py` -- a pytest plugin that wraps
  `DicomSession.redact` with the §4.4 precondition and runs the whole suite, so
  the blast radius of the refusal is measured rather than guessed (§8). Loaded
  with `-p probe_400_refusal_blast_radius` and its directory on `PYTHONPATH`; no
  production file was edited to obtain it.
- `probe_400_ruling_blast_radius.py` -- the same plugin rewritten for the ruled
  design of §14 (warn on one lever, refuse on the other) and re-run on both
  interpreters. §14.10 has the result, and it is the number that counts.
- `probe_400_audit_under_processes.py` -- confirms `audit()` on a `:memory:`
  store survives `ISOCENTER_FORCE_PROCESSES=1`, which one test in §14.4 depends
  on.

---

## 2. What actually decides the strategy

`run_parallel()` never asks the question twice: `_resolve_strategy` calls
`_use_threads(force_threads, maxtasksperchild)` once and stores the answer on a
frozen `_Strategy`. `_use_threads` (`parallel.py:285`) is the whole of the
precedence. Measured by calling it through `_resolve_strategy` for all sixteen
combinations of the three environment levers and the `force_threads` argument,
on both interpreters:

**3.12.14** (`gil_enabled = no attr`)

| FORCE_THREADS | FORCE_PROCESSES | MAX_TASKS | `force_threads=` | `use_threads` | dispatch |
| --- | --- | --- | --- | --- | --- |
| - | - | - | False | False | ProcessPoolExecutor(spawn) |
| - | - | - | True | True | ThreadPoolExecutor |
| - | - | 25 | False | False | recycling multiprocessing.Pool |
| - | - | 25 | True | False | recycling multiprocessing.Pool |
| - | 1 | - | False | False | ProcessPoolExecutor(spawn) |
| - | 1 | - | True | **True** | ThreadPoolExecutor |
| - | 1 | 25 | False | False | recycling multiprocessing.Pool |
| - | 1 | 25 | True | False | recycling multiprocessing.Pool |
| 1 | - | - | False | True | ThreadPoolExecutor |
| 1 | - | - | True | True | ThreadPoolExecutor |
| 1 | - | 25 | False | False | recycling multiprocessing.Pool |
| 1 | - | 25 | True | False | recycling multiprocessing.Pool |
| 1 | 1 | - | False | True | ThreadPoolExecutor |
| 1 | 1 | - | True | True | ThreadPoolExecutor |
| 1 | 1 | 25 | False | False | recycling multiprocessing.Pool |
| 1 | 1 | 25 | True | False | recycling multiprocessing.Pool |

**3.14.7t** (`gil_enabled = False`) is identical except in the two rows where no
lever is set and no argument is passed:

| FORCE_THREADS | FORCE_PROCESSES | MAX_TASKS | `force_threads=` | `use_threads` | dispatch |
| --- | --- | --- | --- | --- | --- |
| - | - | - | False | **True** | ThreadPoolExecutor |
| - | 1 | - | False | False | ProcessPoolExecutor(spawn) |

Read out of the table, the order is:

1. **`maxtasksperchild`** -- from the argument, else `ISOCENTER_MAX_TASKS_PER_CHILD`
   through `_env_int(..., minimum=1)`. Non-`None` means processes, always, because
   only `multiprocessing.Pool` implements recycling. This is the only arm that
   warns, and only when threads were actually asked for (#185).
2. **`force_threads=True` (the argument) or `ISOCENTER_FORCE_THREADS=1`** -- threads.
   The argument and the variable are indistinguishable here: one `or`.
3. **`ISOCENTER_FORCE_PROCESSES=1`** -- processes.
4. **The free-threaded default** -- `hasattr(sys, "_is_gil_enabled") and not
   sys._is_gil_enabled()`. Threads on 3.14t, processes on 3.12 (no attribute at
   all), processes on a GIL-enabled 3.14.

`_redaction_worker_count()` (`session.py:517`) settles the *count* separately --
half the CPUs capped at eight, `ISOCENTER_MAX_WORKERS` overriding -- and
`redact()` passes it to `run_parallel` as `max_workers=`, so `_resolve_strategy`
leaves it alone. Measured `max_workers=7` on this box in every arm below.

**The consequence that both issues turn on:** `redact()` passes
`force_threads=(store_backend.db_path == ":memory:")`. That argument sits at
rank 2, so on a `:memory:` store it beats `ISOCENTER_FORCE_PROCESSES` (rank 3)
and loses to `ISOCENTER_MAX_TASKS_PER_CHILD` (rank 1).

---

## 3. #384 -- the line that names no strategy

### 3.1 The issue's complaint is already half-fixed, and the half that is left is the milestone's

#384 was filed against `Executing using {max_workers} workers (Process
Isolation)...`, a parenthetical that was false on the threads path. That
parenthetical is **gone on `main`**, removed in `f0d3bab` as part of #381's
`:memory:` work:

```
-        print(f"Executing using {max_workers} workers (Process Isolation)...")
+        # No "(Process Isolation)": the pool is threads on a free-threaded
+        # build and on every `:memory:` store (#381), so the parenthetical
+        # was a claim this line could not keep.
+        print(f"Executing using {max_workers} workers...")
```

So the issue's second remedy -- "or drop the parenthetical" -- was taken, and its
first -- "have the message name the strategy `run_parallel` resolved" -- was not.
A developer reading only the issue would look for a false line, not find it, and
close it. **That is the wrong reading.** The milestone is *The Last Silences*, and
what `f0d3bab` did was convert a lie into a silence: `session.py:2901` today is
the only line the pass prints about how it will run, and it is the same line for
every strategy the pass can take.

### 3.2 The residual defect, measured

`probe_b.py` runs the whole pass -- three 16x16 CT instances on one machine
serial, one rule, one zone -- across seven arms, and records the `_Strategy` the
`"Redacting Pixels"` dispatch resolved together with the dispatch path that
actually ran. On **3.12.14**:

| arm | store | levers | `use_threads` | path that ran | outcome | printed |
| --- | --- | --- | --- | --- | --- | --- |
| A | `:memory:` | none | True | ThreadPoolExecutor | returned 3 | `Executing using 7 workers...` |
| B | `:memory:` | `FORCE_PROCESSES=1` | True | ThreadPoolExecutor | returned 3 | `Executing using 7 workers...` |
| C | `:memory:` | `MAX_TASKS_PER_CHILD=1` | False | recycling Pool | **RedactionError** | `Executing using 7 workers...` |
| D | `:memory:` | `FORCE_THREADS=1` + `FORCE_PROCESSES=1` | True | ThreadPoolExecutor | returned 3 | `Executing using 7 workers...` |
| E | file | none | False | ProcessPoolExecutor | returned 3 | `Executing using 7 workers...` |
| F | file | `FORCE_PROCESSES=1` | False | ProcessPoolExecutor | returned 3 | `Executing using 7 workers...` |
| G | file | `FORCE_THREADS=1` | True | ThreadPoolExecutor | returned 3 | `Executing using 7 workers...` |

On **3.14.7t** the same seven arms give the same seven printed lines, and arm E
differs in substance: `use_threads = True`, `ThreadPoolExecutor`. That is the
divergence #384 was filed from -- the same session, the same words, two
different strategies, decided by which interpreter is running.

Three dispatch paths, two interpreters, seven configurations, **one sentence**.
The number of workers is the only fact the line carries, and it is the fact a
reader least needs: the thing that changes what the worker *is* -- a thread
mutating the live `Instance`, versus a spawned process mutating a pickled copy
whose result has to be applied back by `_apply_redaction_outcomes` -- is
unstated.

### 3.3 Design: resolve once, carry the answer, print the answer

The fix must not re-derive the choice at the print site. A second reading of the
environment at `session.py:2901` would be a second implementation of §2's four
ranks, and a second implementation is a second thing that can disagree -- which
is the defect class this whole milestone is about. So:

1. `redact()` resolves the strategy itself, **once**, by calling
   `parallel._resolve_strategy` with exactly the settings it would otherwise have
   passed to `run_parallel`.
2. It prints from that object, and logs from that object.
3. It hands that object to `run_parallel` as `strategy=`, and `run_parallel`
   dispatches on it instead of resolving a second time.

The printed object *is* the dispatched object. There is no arrangement of the
environment under which the line and the pool can disagree, because there is one
`_Strategy` and both read it.

**The new line.** Two exact spellings, chosen so a test can assert a whole line
rather than a substring:

```
Executing using 7 workers (threads)...
Executing using 7 workers (processes)...
```

`(threads)` when `strategy.use_threads`, `(processes)` otherwise -- including the
recycling pool, which is processes. The vocabulary is `docs/environment.md`'s
("threads"/"processes"), not the retired "(Process Isolation)": that phrase named
an implementation property rather than the choice, and reintroducing it would
make the recycling arm ambiguous.

The line says **what**, not **why**. `(threads: the store is in memory)` is
tempting and is exactly the re-derivation this design exists to prevent -- the
strategy object does not carry a reason, and inventing one at the print site
would put the four ranks back in a second place. The *why* is what the refusal in
§4 exists to say, and it says it only when it matters.

The `INFO` log line beside the print gains the same fact from the same object:

```
Starting granular redaction (3 tasks, workers=7, strategy=threads)...
```

### 3.4 Rejected alternatives

- **Re-read the environment at the print site.** A second implementation of §2.
  Rejected on the constraint that names this bunch.
- **Call `_use_threads(force_threads, None)` again just before the print.** Not a
  second *implementation* -- it is the same function -- but a second *invocation*
  with independently supplied arguments. It agrees today and stops agreeing the
  day someone adds `maxtasksperchild=` to `redact()`'s dispatch. The whole point
  of carrying the object is that there is nothing to keep in sync.
- **`run_parallel(..., on_strategy=callback)`,** with `redact()` printing from a
  callback fired inside `run_parallel` after resolution and before dispatch. This
  resolves once and was the first design. It is rejected because of where the
  callback runs: inside `redact()`'s `try`, whose handler is
  `get_logger().exception("Redaction failed. Images already processed are still
  redacted in memory; the rest are untouched."); raise`. For the print alone that
  is harmless, but §4's refusal has to be raised from the same knowledge, and a
  refusal raised there makes the session print a sentence about work it never
  started. Measured in probe arm C, which is the existing path that already
  triggers that handler:
  `ERROR: Redaction failed. Images already processed are still redacted in memory; the rest are untouched.`
  A fix for a milestone about false sentences must not add one.
- **Have `run_parallel` log the strategy on every call.** Fixes the silence
  everywhere, including `ingest()`'s ignored `ISOCENTER_FORCE_THREADS` (#390), and
  needs no new parameter. Rejected as out of scope for this bunch, not as wrong:
  it is a new INFO line on every parallel pass in the library and belongs in its
  own issue with its own noise budget. Named in §12.

---

## 4. #400 -- the lever a `:memory:` store cannot honour

### 4.1 The issue body is backwards, the correction is right, and the ruling's mechanism clause inherited the error

#400's Evidence paragraph says that with `ISOCENTER_FORCE_PROCESSES=1`,
`_use_threads(force_threads=True, ...)` "returns `False` because the environment
variable wins", the workers spawn, and the outcome is the #381 failure or a clean
run depending on whether a worker touches the store. The owner's own correction
comment of 2026-09-08 says that paragraph is backwards. **The correction is what
the code does.** Measured here independently, on both interpreters.

Probe arm B -- `Session(":memory:")`, `ISOCENTER_FORCE_PROCESSES=1`, ingest, one
rule, `redact()`:

```
--- B  :memory:  FORCE_PROCESSES=1
    strategy.use_threads: True   max_workers=7   maxtasksperchild=None
    executor path      : ThreadPoolExecutor
    outcome            : ('returned', 3)
    zone zeroed / outside kept: True / True
    stdout, verbatim:
      | Queued 3 redaction tasks across 1 rules.
      | Executing using 7 workers...
      | Redaction complete: 3 of 3 images updated. Remember to call .save() to persist.
```

Threads ran. The pass was correct. Nothing was logged about the variable. The
`no such table: instance_blobs` failure cannot occur on this path, on either
interpreter.

The lever that *does* produce the crash is `ISOCENTER_MAX_TASKS_PER_CHILD`, and
it produces it deterministically, not by luck -- probe arm C, identical on 3.12
and 3.14t:

```
--- C  :memory:  MAX_TASKS_PER_CHILD=1
    strategy.use_threads: False   max_workers=7   maxtasksperchild=1
    executor path      : recycling multiprocessing.Pool
    outcome            : ('raised', 'RedactionError: Redaction failed for 3 of 3
                          instances; ... First: 1.2.826.0.1.0: ... OperationalError:
                          no such table: instance_blobs. ...')
    stdout, verbatim:
      | Queued 3 redaction tasks across 1 rules.
      | Executing using 7 workers...
      | WARNING: force_threads=True was set, but worker recycling (maxtasksperchild=1)
      |          was also asked for and only multiprocessing.Pool implements it, so
      |          this run uses processes. ...
      | ERROR: 1.2.826.0.1.0: Redaction failed for ...: no such table: instance_blobs
      | ERROR: 1.2.826.0.1.1: ...
      | ERROR: 1.2.826.0.1.2: ...
```

Three of three, every time. `execute_redaction_task` ends in
`store_backend.persist_pixel_data(inst)` unconditionally, so "whether a worker
happens to touch the store" is not a variable: every worker touches it.

**This falsifies two sentences the ruling comment relies on**, both inherited
from the Evidence paragraph the correction had already withdrawn:

- "the outcome under `ISOCENTER_FORCE_PROCESSES=1` on a `:memory:` store depends
  on whether a worker happens to touch the store" -- it does not. That arm never
  reaches a worker process at all, and the arm that does fails deterministically.
- "state the exact exception a previously-'working' call now raises, and that the
  previous success was luck about whether a worker read the store" -- the previous
  success was not luck. It was correct, every time, and the defect was never the
  result. **A CHANGELOG entry written to that sentence would be a false release
  note**, and §9 does not write it.

What the ruling's *remedy* clauses say is unaffected by any of this, and they are
consistent with the measurement:

- "`redact()` on a `:memory:` store raises ... with an exception that names both
  the store and the variable -- the caller set the variable, so the caller can
  unset it."
- "`docs/environment.md`'s `ISOCENTER_FORCE_PROCESSES` row gains the exception as
  its one documented limit -- the documented precedence order (environment
  variable wins) is otherwise unchanged."

The second clause is only meaningful if `ISOCENTER_FORCE_PROCESSES=1` on a
`:memory:` store raises. This brief therefore takes the remedy clauses as
controlling and the mechanism clause as residue of the withdrawn evidence.
**Owner question Q1 in §10 confirms that reading rather than assuming it.**

### 4.2 The defect, stated correctly

An operator sets `ISOCENTER_FORCE_PROCESSES=1` -- in a shell profile, a CI job, a
deployment manifest -- and runs a pipeline against `Session(":memory:")`. Every
other parallel pass in that process obeys the variable: `audit()`,
`scan_pixel_content()` and `export()` run in processes, and `ingest()` ignores it
either way (#390). `redact()` alone runs in threads, and says nothing. The
operator's model of what their process is doing is wrong at exactly one step, and
there is no output anywhere that would tell them.

That is #185's shape and not #185's answer, which §4.3 is about.

### 4.3 Why refuse and not warn, and why #185 is the inverse

The owner's ruling is **refuse, not warn**, and the reason generalises cleanly
once the mechanism is stated correctly.

In **#185**, `session.export()` passes `maxtasksperchild=25` and an operator sets
`ISOCENTER_FORCE_THREADS=1`. Recycling wins, and it is *right* that it wins:
export decodes and compresses through C libraries that leak steadily, and
reclaiming a worker every 25 tasks is the behaviour the export path was measured
into. The operator's request is dropped, but **processes are the correct choice
for that call**. There is a correct run happening and something true about it the
operator would otherwise not know, which is the definition of an annotation. A
warning is exactly right there.

In **#400**, processes are never correct for a `:memory:` store. There is no
correct-run-with-a-caveat to annotate: `SqliteStore.__setstate__` hands a spawned
child `_memory_conn = None`, `_get_connection` then opens a fresh, empty
in-memory database with no `instance_blobs` table, and every redaction worker
writes to the store. The configuration the operator asked for **has no correct
execution at all**. A warning in front of a run that then succeeds in threads is a
line nobody reads in front of a result that was never in doubt; a warning in
front of the recycling arm is noise before a stack trace. Neither buys the reader
anything they can act on, and both preserve the silent-clean-run path, which *is*
the defect.

The sharper way to say the inversion, and the one to give a reviewer:

> In #185 the lever is **honoured**, and the warning explains what was dropped
> from a choice that was right. In #400 the lever **cannot be honoured** -- there
> is no run in which obeying it is correct -- and the only honest response is to
> decline the configuration. One is a silence about what was done; the other is a
> silence about what was ignored. A silence about what was done is worth a line.
> A silence about what can never be done is worth a refusal.

### 4.4 The refusal: where, which type, what text

**Keyed on the request, not on the outcome.** This is the part that is easy to
get wrong. A refusal keyed on "the resolved strategy is processes" would fire on
`Session(":memory:")` with **no levers set at all** on 3.12, because §2 rank 4
makes processes the default there and the `force_threads=True` argument is the
only reason it works. `:memory:` must keep working out of the box on the floor
interpreter. So the question the refusal asks is *did the operator ask for
processes*, and the answer must come from the same code that ranks the levers,
not from a second reading of the environment. §5 is how.

**Where.** In `redact()`, **after** the `if not self.configuration.rules` guard
**and after** the `persistence_manager.flush()` drain, and **before** the
`RedactionService` construction, the `try` and the pass-lock. Four reasons, in
order of weight:

1. It is outside `redact()`'s `except Exception` handler, so the refusal is not
   preceded by `ERROR: Redaction failed. Images already processed are still
   redacted in memory; the rest are untouched.` -- a sentence about work that
   never started. Probe arm C shows that handler firing today. This is why the
   design is §3.3 and not the callback of §3.4.
2. It is **after** the drain, deliberately, so that
   `docs/api/stability.md`'s frozen Behaviours clause -- "`audit()` and
   `redact()` drain the persistence manager on entry" -- stays true verbatim of
   every call, including a refused one. Placing the refusal above the drain would
   narrow a second frozen clause for no gain: the drain mutates nothing a caller
   can observe, it is idempotent, and a caller who fixes the environment and
   retries wants it to have happened. One frozen clause is narrowed by this
   change (§10, Q3), not two.
3. "Has done nothing when it does" is then true in the sense that matters, and
   in the same sense `compact()`'s refusal is (`docs/api/stability.md`,
   Compaction and passes, item 1): no pass-lock taken, no task prepared, no SOP
   Instance UID regenerated, no pixel touched, no audit row, no attestation, no
   console line beyond the refusal itself. The drain is the one thing that has
   happened, and it is the thing the frozen clause promises happens.
4. Because it precedes task preparation, it does not depend on whether any image
   matched. A configuration that cannot run is refused whether or not it had work
   to do, which is one behaviour rather than two.

`redact_by_machine()` swaps in a one-rule configuration and calls `self.redact()`
inside a `try/finally` that restores the original rules, so it inherits the
refusal and its `finally` still runs. Its docstring gains the same `Raises:`
clause.

**Which type: `RuntimeError`.** Plain, not a subclass, and deliberately neither
of the two neighbours.

- **Not `RedactionError`.** It is frozen (`docs/api/stability.md`, Exceptions),
  takes `(failures, attempted)`, means *the pass ran and these instances failed*,
  and is raised at the end of a pass so a caller that catches it still holds a
  correct graph and a `REVIEW_REQUIRED` report. Here nothing was attempted and
  there is no graph to describe. Worse, `RedactionError` **is** a `RuntimeError`,
  so a caller writing `except RedactionError` around `redact()` -- the documented
  way to catch a partial pass -- would swallow a configuration refusal and read
  "your environment cannot run this" as "some images failed". §7's test asserts
  `not isinstance(exc, RedactionError)` for exactly this reason.
- **Not `ValueError`.** Nothing about the arguments the caller passed is invalid;
  `show_progress` and `force` are fine. What is invalid is the pairing of a store
  with an environment.
- **`RuntimeError` is this project's type for "this configuration has no correct
  execution".** `compact()` raises it while a pass is open and when the pass-lock
  wait expires; `redact()` already raises it on that expiry; bunch A's
  `require_package_resource` raises it for a missing shipped resource, on this
  very issue's reasoning -- its docstring says "**A refusal rather than a
  warning**, on #400's reasoning". Matching it is the point. A third refusal
  vocabulary is what the constraint forbids.
- **No `_RedactionRefusal(RuntimeError)` subclass.** Bunch A introduced
  `_J2kFrameRefusal` only because `_compress_j2k`'s outer handler would otherwise
  have wrapped its sentence into `Compression failed: ...`. The equivalent handler
  here is `redact()`'s `except Exception`, and the refusal is raised before it.
  Placement removes the need for the type; a subclass would be machinery with
  nothing to do.

**The text.** House style, from bunch A's two refusals: name the concrete thing
being refused with its values, say why it cannot work *here*, say what continuing
would have done, and end with the remedy as something to type. One shared body,
one lever-specific tail -- the shape of bunch A's
`require_package_resource(consequence=...)`, where the caller's own words for the
consequence travel with the call.

The `ISOCENTER_FORCE_PROCESSES` message:

```
redact() cannot run on a ":memory:" store with ISOCENTER_FORCE_PROCESSES=1.
A redaction worker writes its redacted frame back to the store, and a spawned
process is handed _memory_conn=None by SqliteStore.__setstate__ -- it opens a
fresh, empty in-memory database with no instance_blobs table -- so processes are
never correct for this store. This call would have run in threads and discarded
ISOCENTER_FORCE_PROCESSES without a word. Unset ISOCENTER_FORCE_PROCESSES for
this session, or use a file-backed store -- Session("session.db") -- where
processes are the default.
```

The `ISOCENTER_MAX_TASKS_PER_CHILD` message, in scope only if **Q2** (§10) is
answered yes:

```
redact() cannot run on a ":memory:" store with ISOCENTER_MAX_TASKS_PER_CHILD set.
A redaction worker writes its redacted frame back to the store, and a spawned
process is handed _memory_conn=None by SqliteStore.__setstate__ -- it opens a
fresh, empty in-memory database with no instance_blobs table -- so processes are
never correct for this store. Only multiprocessing.Pool implements worker
recycling, so this call would have run in processes and every task would have
failed with "no such table: instance_blobs". Unset ISOCENTER_MAX_TASKS_PER_CHILD
for this session, or use a file-backed store -- Session("session.db") -- where
processes are the default.
```

Each message names, in order: the method, the **store type**, the **variable**,
**why processes cannot work for this store**, what continuing would have done,
and **two remedies**. The consequence sentence is selected on
`strategy.use_threads` -- `True` means the request was discarded, `False` means it
was obeyed and will fail -- which is a reading of the resolved decision, not a
second derivation of it.

### 4.5 Rejected alternatives

- **Warn instead of refuse.** The owner's ruling, and §4.3's reasoning. Recorded
  because a reviewer arriving from the issue title -- which still reads "warn like
  the recycling branch does" -- will ask.
- **Refuse whenever the resolved strategy is processes.** Refuses
  `Session(":memory:")` with no levers on 3.12. Rejected on measurement, not
  taste: §2 rank 4 is a default, not a request.
- **Stop passing `force_threads=True` and let the levers decide, refusing when
  they choose processes.** The same failure, and it would additionally undo #381.
- **Read `ISOCENTER_FORCE_PROCESSES` directly in `session.py`.** The cheapest
  patch, and wrong twice. It re-encodes the rank-2-beats-rank-3 ordering in a
  second file, so probe arm D -- `FORCE_THREADS=1` **and** `FORCE_PROCESSES=1`,
  where the operator's effective request is threads and nothing is being denied --
  would refuse. And for the recycling lever a session-side `_env_int` call would
  warn a second time about a malformed value `_resolve_strategy` has already
  reported. §5 is the alternative.
- **Refuse inside `parallel.py`.** `parallel.py` knows nothing about stores and
  must not learn: `run_parallel` is also the entry point for scanning,
  verification, zone discovery and export, none of which care about `db_path`. The
  store rule lives in `session.py`; only the lever *attribution* comes from
  `parallel.py`.

---

## 5. The shared seam: one decision, two readers

Both fixes need something `session.py` cannot see today. #384 needs *which
strategy was chosen*; #400 needs *which lever asked for processes*. Both are
facts about the one decision `_use_threads` makes, and the design rule is that
neither may be recomputed anywhere else.

`_use_threads` returns a bool, so the attribution has nowhere to go. It also
cannot be computed by a sibling function afterwards: any such function would have
to re-encode the ranking to know that `ISOCENTER_FORCE_THREADS` supersedes
`ISOCENTER_FORCE_PROCESSES`, and computing it *from* the resolved `use_threads`
is impossible -- on `redact()`'s `:memory:` path `use_threads` is always `True`,
so an attribution keyed on it is always `None` and the refusal never fires.

**The change:** the precedence function returns both facts at once.

```python
class _Choice(NamedTuple):
    """What the threads-or-processes ranking decided, and who asked."""
    use_threads: bool
    processes_requested_by: Optional[str]


def _resolve_execution_choice(force_threads, maxtasksperchild,
                              recycling_lever) -> _Choice:
    ...
```

- `use_threads` is exactly today's answer, computed by exactly today's four
  ranks, with today's #185 warning in today's arm.
- `processes_requested_by` is the name of the lever that **asked** for processes,
  whether or not it got them, and `None` when nobody asked. It is populated on
  the way through the ranks, so the `force_threads` short-circuit cannot erase it:
  - rank 1 fires: `recycling_lever` -- `"ISOCENTER_MAX_TASKS_PER_CHILD"` when the
    value came from the environment, `"the maxtasksperchild argument"` when a
    caller passed it. `_resolve_strategy` is the only place that knows which, so
    it passes the spelling in; this mirrors the #185 warning, which already
    chooses between those two spellings for the threads side.
  - `ISOCENTER_FORCE_THREADS=1` is set: `None`. The operator's own threads lever
    supersedes their processes lever by the documented order, so their effective
    request is threads and nothing has been denied.
  - `ISOCENTER_FORCE_PROCESSES=1`: `"ISOCENTER_FORCE_PROCESSES"`, whether threads
    then win by the `force_threads` argument or not.
  - nothing set: `None`. Rank 4 is a default, and a default is not a request --
    this single line is what keeps `Session(":memory:")` working on 3.12.
- `_Strategy` gains `processes_requested_by: Optional[str]`, set from the same
  `_Choice`. `_resolve_strategy` calls `_resolve_execution_choice` **once**, as it
  calls `_use_threads` once today.

`_use_threads` is **deleted**, not kept as a bool-returning wrapper. The project's
own rule is that pre-1.0 duplicate spellings are deleted rather than aliased
(`CLAUDE.md`, Conventions; `tests/test_api_coherence.py`), and a wrapper that
unwraps `.use_threads` is a second spelling of the same question. `parallel.py` is
tier 3 wholesale (`docs/api/stability.md`, Private: "`parallel.py` (`run_parallel`
included -- the environment registry is the contract, the function is not)"), so
this is not an API change. The call sites to migrate are listed in §6 and §8.

Then:

- **#384** reads `strategy.use_threads` at the print site.
- **#400** reads `strategy.processes_requested_by` at the refusal site, and
  `strategy.use_threads` to choose the consequence sentence.

One resolution, one object, two readers. A reviewer asking "where could the
printed line and the pool disagree?" has one place to look and finds it is the
same field.

---

## 6. Change list, file by file

Line numbers are `main` at `f544989`. `tests/test_source_citations.py` cites no
line in `session.py` or `parallel.py`, so the insertions below move nothing that
a test pins by number -- verified by grep before designing.

### 6.1 `isocenter/parallel.py`

| Where | Change |
| --- | --- |
| after `_Strategy` (line 128-154) | Add `class _Choice(NamedTuple)` with `use_threads: bool` and `processes_requested_by: Optional[str]`. Docstring says what "requested" means and that `None` covers both "nobody asked" and "the operator's own threads lever supersedes it". |
| `_Strategy` fields (129-142) | Add `processes_requested_by: Optional[str]`. Frozen dataclass; the field is positional after `use_threads` or keyword everywhere -- `_resolve_strategy` is the only constructor. |
| `_use_threads` (285-338) | **Rename to `_resolve_execution_choice`, third parameter `recycling_lever`, return `_Choice`.** Body keeps all four ranks and the #185 warning verbatim; each rank additionally names its attribution. The docstring keeps the "this is where that order lives" paragraph and gains the "who asked" half. |
| `_resolve_strategy` (220-282) | Where it reads `ISOCENTER_MAX_TASKS_PER_CHILD` (259-267), record which spelling the value came from. Call `_resolve_execution_choice(force_threads, maxtasksperchild, recycling_lever)` once and unpack both fields into `_Strategy`. |
| `run_parallel` (424-504) | New keyword `strategy: Optional[_Strategy] = None`, documented as: when given, it is used as-is and every resolution keyword (`max_workers`, `chunksize`, `maxtasksperchild`, `disable_gc`, `force_threads`, `show_progress`, `progress`, `desc`, `total`) is ignored, because the caller has already resolved them. Body becomes `if strategy is None: strategy = _resolve_strategy(...)`. Dispatch, `yield_exceptions` and `return_generator` are untouched. |
| `run_parallel` docstring (440-448) | The sentence naming `_use_threads` as where the order lives is renamed with the function. |

### 6.2 `isocenter/session.py`

| Where | Change |
| --- | --- |
| import (line 34) | Add `_resolve_strategy` to `from .parallel import ...`. **Note the binding:** with this import style a test that patches `parallel._resolve_strategy` does **not** reach `session.py`; it must patch `session_module._resolve_strategy`. `tests/test_memory_store_redaction_strategy.py` already patches `session_module.run_parallel` for the same reason. §7 states each test's patch target explicitly. |
| new module-level helper | `_refuse_processes_on_a_memory_store(db_path, strategy)` -- returns `None` or raises. Module scope beside `_redaction_worker_count` (517), so it is unit-testable without a session and so `redact()` stays readable. Holds §4.4's message and nothing else; it reads two fields of `strategy` and one string, and decides nothing. |
| `redact()` (2747), after the rules guard (2831-2835) and after the drain (2843-2844), before `RedactionService(...)` at 2846 | `is_memory = self.store_backend.db_path == ":memory:"` -- one spelling, used twice. Then `max_workers = _redaction_worker_count()` (moved up from 2895), then `strategy = _resolve_strategy(max_workers=..., chunksize=1, maxtasksperchild=None, disable_gc=False, force_threads=is_memory, show_progress=show_progress, desc="Redacting Pixels", total=None)`, then `_refuse_processes_on_a_memory_store(self.store_backend.db_path, strategy)`. After the drain, for §4.4 reason 2. |
| `redact()` (2861) | Pass `strategy` through: `self._apply_redaction_rules(service, strategy, show_progress, force)`. |
| `redact()` docstring `Raises:` (2810-2815) | A second `RuntimeError` clause: the refusal, what it names, and that it is raised before the drain and before the pass-lock so nothing has happened. Keep the existing pass-lock-timeout clause. |
| `_apply_redaction_rules` (2868) | Signature becomes `(self, service, strategy, force=False)`. Delete the local `max_workers = _redaction_worker_count()` (2895) and read `strategy.max_workers`; **delete `show_progress` too** -- it now lives on `strategy` and two spellings of one setting is the thing the conventions forbid. No test calls this method directly (five files mention it in prose only), so the signature change is internal. |
| the print (2896-2901) | `print(f"Executing using {strategy.max_workers} workers ({'threads' if strategy.use_threads else 'processes'})...")`, with the comment rewritten from "no parenthetical, because we cannot keep the claim" to "the parenthetical is read off the resolved strategy, which is the object the pool is built from". |
| the log (2902-2905) | Add `strategy=threads`/`strategy=processes` from the same field. |
| the dispatch (2957-2966) | `run_parallel(service.execute_redaction_task, tasks, strategy=strategy, return_generator=True, yield_exceptions=True)`. The `max_workers=`, `chunksize=`, `force_threads=`, `desc=` and `progress=` keywords go, because they are now inside `strategy`. The long comment above it keeps its `:memory:` paragraph, with "asked for per call" restated as "resolved per call": the `force_threads` argument now sits on the `_resolve_strategy` call a few lines up in `redact()`. |
| `redact_by_machine()` (3256) docstring | A `RuntimeError` clause noting it inherits the refusal from `redact()` and that the `finally` restores the configuration first. |

---

### 6.3 Two guards that watch these two files, and what each needs

**`tests/test_source_citations.py`.** It sweeps every `.py` and `.md` in the
tree except `CHANGELOG.md` and `docs/superpowers/`, so this brief's own
`parallel.py:285` and `session.py:2901` citations are **not** graded and cannot
be reddened by the developer's insertions. Two live citations do name lines in
the files this change edits, both Rule 1 (in-range) rather than Rule 2 (content
pins), so neither turns red -- and both must still be corrected by hand, because
Rule 1 is exactly the check that cannot see this:

- `tests/test_audit_read_barrier.py:233` cites "`_use_threads`
  (`parallel.py:133`)". `_use_threads` is at `parallel.py:285`; line 133 is
  inside `_Strategy`'s field list. **The citation is already wrong on
  `f544989`** -- see §12 -- and this change renames the function it names, so it
  is rewritten to `_resolve_execution_choice` with the line it actually lands on.
- `tests/test_packaging_contract.py:534` cites `session.py:152`, which is
  `_load_redaction_knowledge_base`. Nothing in §6.2 inserts above it **unless the
  import at line 34 gains a wrapped line**; if it does, every number below shifts
  by one and this citation must move with it. Check it after the import edit, not
  before.

**`tests/test_mutation_probe_targets.py`.** It requires that any test file whose
text contains `isocenter.parallel` be listed under `"isocenter/parallel.py"` in
`scripts/mutation_probe.py`'s `TARGETS`; extras are allowed, omissions fail.
`tests/test_memory_store_redaction_strategy.py` deliberately avoids the dotted
spelling to stay off that list (its docstring says so). **The two new files in §7
should be added to `TARGETS` rather than avoiding the spelling**: they cover
`parallel.py`'s decision directly, extras cost nothing to the guard, and a probe
that does not run them would over-report survivors for exactly the mutation
§7.3's choice table exists to kill. The entry is
`TARGETS["isocenter/parallel.py"]` at `scripts/mutation_probe.py:94-98`, whose
budget of 80 should be left alone. `CLAUDE.md`'s module-to-tests table is
untracked since `c925829` and is not a second place to update.

---

## 7. TDD test plan

The milestone's standard: **every test asserts the speech.** A test that proves
threads ran and not that the session said so is the wrong test here. Each entry
below names the single production edit that reddens it, and says how the
assertion is anchored against the correct-by-accident shapes.

### 7.0 The anchoring rules, once

**Never assert a substring of the strategy banner.** Measured, probe arm C: a
single line of real `redact()` output contains the words `threads`, `processes`,
`ISOCENTER_FORCE_THREADS` and `ISOCENTER_MAX_TASKS_PER_CHILD` together --

```
WARNING: force_threads=True was set, but worker recycling (maxtasksperchild=1) was
also asked for and only multiprocessing.Pool implements it, so this run uses
processes. ... unset ISOCENTER_MAX_TASKS_PER_CHILD to get threads; ...
```

so `assert "threads" in capsys.readouterr().out` passes on a run that printed
`(processes)`, and `assert "processes" in ...` passes on a run that printed
`(threads)`. Every banner assertion in this plan is therefore whole-line
membership -- `expected_line in captured.out.splitlines()` -- plus the negative
`assert not any("(processes)" in line for line in lines)` where the positive
claim is threads, and the mirror where it is not.

**The expected line is a literal, not a computation.** Each of these tests sets
`ISOCENTER_MAX_WORKERS=3` with `monkeypatch.setenv`, so the expectation is the
literal string `Executing using 3 workers (threads)...` rather than an f-string
built from `_redaction_worker_count()`. Building the expectation out of the
production helper would keep the test green if that helper broke, and would make
the assertion depend on the CPU count of whoever runs it.

**The free-threaded gate skips on the build and asserts on the runtime.**
`pytest.mark.skipif(sysconfig.get_config_var("Py_GIL_DISABLED") != 1)` selects
free-threaded *builds*, which an extension cannot change. Measured, so the
predicate is not assumed: `Py_GIL_DISABLED` is `1` on the 3.14.7t venv and
`None` on the 3.12.14 venv, and `!= 1` is therefore the right comparison for
both -- `not sysconfig.get_config_var("Py_GIL_DISABLED")` would work too, but
the explicit `!= 1` will not be read as a truthiness accident. Inside the test,
`assert sys._is_gil_enabled() is False` is a hard assertion, after `import
isocenter` has pulled numpy, pydicom and imagecodecs. The two together mean a
build that re-enables the GIL turns the test **red** rather than skipping it,
which is the whole point of having 3.14t on the gate. Skipping on
`sys._is_gil_enabled()` would do the opposite, and is the shape to refuse in
review.

**Audit assertions flush first and carry a positive control.**
`store_backend.get_audit_summary()` calls `flush_audit_queue()` before its
`SELECT`, so it is a barrier rather than a race -- but a count that is `0` both
because nothing was written and because the query found nothing is a `0 == 0`
that grades nothing. The "no row was written" assertion below is preceded, in the
same test and the same session, by a successful pass whose row is asserted
present.

### 7.1 #384 -- new file `tests/test_redaction_names_its_strategy.py`

Fixture: the `_populate` shape from `tests/test_memory_store_redaction_strategy.py`
-- three 16x16 CT instances on one serial, every pixel `FILL = 1000`, one rule,
one 10x10 zone. Non-zero pixels and an image larger than the zone, for that
file's own reasons. An autouse fixture sets `ISOCENTER_MAX_WORKERS=3` and deletes
all three strategy levers; each test sets back only what it needs.

| Test | Asserts | The single production edit that reddens it |
| --- | --- | --- |
| `test_a_memory_store_says_it_is_running_in_threads` | the whole line `Executing using 3 workers (threads)...` is in `out.splitlines()`; no line contains `(processes)`; `redact() == 3` | the strategy conditional replaced by the bare `Executing using ... workers...` of `f544989` |
| `test_a_file_store_says_it_is_running_in_processes` | file store, `ISOCENTER_FORCE_PROCESSES=1` (no refusal: not `:memory:`); the whole `(processes)` line; no line contains `(threads)` | the conditional inverted, or hardcoded to `(threads)` |
| `test_the_recycling_pool_is_reported_as_processes` | file store, `ISOCENTER_MAX_TASKS_PER_CHILD=2`; the whole `(processes)` line | a banner keyed on the force levers rather than on `strategy.use_threads`; the recycling arm sets neither |
| `test_the_banner_is_the_strategy_the_pool_was_built_from` | file store, **no** levers; `monkeypatch.setattr(parallel, "_resolve_execution_choice", ...)` returning threads; assert the banner says `(threads)` **and** a spy on `parallel._run_on_new_executor` recorded `strategy.use_threads is True` | the banner re-deriving the choice at the print site from the environment or from `sys._is_gil_enabled()`. The patched resolver and the real environment disagree, and only a banner that reads the resolved object survives |
| `test_the_log_line_names_the_strategy_too` | `caplog` at INFO; the whole formatted record `Starting granular redaction (3 tasks, workers=3, strategy=threads)...` is among the records' messages | `strategy=` dropped from the log line |
| `test_a_free_threaded_build_runs_a_file_store_in_threads_and_says_so` | `skipif` on `Py_GIL_DISABLED`; `assert sys._is_gil_enabled() is False`; file store, no levers; the whole `(threads)` line, plus the `_run_on_new_executor` spy | the banner hardcoded to `(processes)`; and, separately, losing `parallel.py`'s `hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()` rank, which turns this arm into processes and reddens the banner honestly |

`parallel._resolve_execution_choice` is the right patch target for the fourth
row, because `_resolve_strategy` looks it up as a module global at call time, so
patching the `parallel` module reaches it under either import style. The banner's
own input, `_resolve_strategy`, is imported **into** `session.py` by name, so a
test that wanted to patch *that* must patch `session_module._resolve_strategy`.
Stated because the mistake is invisible: patching the wrong module produces a
green test that exercised the unpatched code.

### 7.2 #400 -- new file `tests/test_memory_store_refuses_processes.py`

Same fixture. Each test sets exactly one lever unless it is the both-levers case.

| Test | Asserts | The single production edit that reddens it |
| --- | --- | --- |
| `test_force_processes_on_a_memory_store_refuses` | `pytest.raises(RuntimeError)`, then four separate assertions on `str(exc)`: it contains `":memory:"`, `ISOCENTER_FORCE_PROCESSES`, `instance_blobs`, and `Unset ISOCENTER_FORCE_PROCESSES` | the refusal deleted |
| `test_the_refusal_is_not_a_redaction_error` | `type(exc) is RuntimeError` **and** `not isinstance(exc, RedactionError)` | the refusal raised as `RedactionError`. Its own test so it cannot sit behind an assertion that fails first, and both halves are needed: `RedactionError` **is** a `RuntimeError`, so `isinstance(exc, RuntimeError)` alone passes for the wrong type |
| `test_the_refusal_says_the_request_was_about_to_be_discarded` | `str(exc)` contains `would have run in threads` and does **not** contain `would have run in processes` | the consequence sentence hardcoded to one arm instead of read from `strategy.use_threads` |
| `test_the_refusal_names_only_the_lever_that_was_set` (**Q2 only**; it is parametrised over the two levers and there is only one lever to parametrise over if Q2 is no) | the message names the lever that is set and does **not** name the other | the message hardcoding `ISOCENTER_FORCE_PROCESSES`, which passes the first row of this table and fails here |
| `test_the_refusal_has_done_nothing` | one `:memory:` session: (1) no lever, `redact() == 3`, and `get_audit_summary()["REDACTION"]` is captured as `before` and asserted **non-zero** -- the positive control, whose value is whatever `record_redaction_pass` writes for one rule-pass rather than a number pinned in prose here; (2) set the lever, `pytest.raises`; (3) `get_audit_summary()["REDACTION"] == before`, every instance's pixels and SOP UID are what step 1 left, a `session_module.run_parallel` spy recorded no `Redacting Pixels` dispatch in step 2, and `caplog` holds no record beginning `Redaction failed.` | the refusal moved below task preparation, below the drain, or inside the `try`. The spy and the count stay green for a late refusal; the `caplog` clause is what catches the in-`try` placement, because `redact()`'s handler logs that sentence for any exception raised there |
| `test_a_memory_store_with_no_lever_still_redacts_and_says_threads` | no levers; `redact() == 3`; the whole `(threads)` banner line; every zone zeroed and every pixel outside it still `FILL` | attribution that treats rank 4 (the default) as a request. This is the test that stops the refusal breaking `Session(":memory:")` on 3.12, where processes are the default |
| `test_a_file_store_with_force_processes_still_redacts` | file store, `ISOCENTER_FORCE_PROCESSES=1`; `redact() == 3`; the whole `(processes)` banner | the `db_path == ":memory:"` half of the refusal condition dropped |
| `test_force_threads_beside_force_processes_does_not_refuse` | `:memory:`, **both** force levers; `redact() == 3`; the whole `(threads)` banner | attribution that reports `ISOCENTER_FORCE_PROCESSES` without the `ISOCENTER_FORCE_THREADS` supersession, which is exactly the session-reads-the-variable design rejected in §4.5 |
| `test_max_tasks_per_child_on_a_memory_store_refuses` (**Q2 only**) | `pytest.raises(RuntimeError)`; the message names `ISOCENTER_MAX_TASKS_PER_CHILD` and contains `would have run in processes` and `no such table: instance_blobs`; no `RedactionError` | attribution that covers only `ISOCENTER_FORCE_PROCESSES` |
| `test_the_refusal_reaches_redact_by_machine` | `redact_by_machine(SERIAL, ZONE)` raises the same `RuntimeError`, and `session.configuration.rules` is the original list afterwards | the refusal placed inside `_apply_redaction_rules` and reached only from `redact()`; it also pins that the `finally` still runs |
| `test_the_refusal_fires_even_when_no_image_matches` | `:memory:`, lever set, a rule whose serial matches nothing; `pytest.raises(RuntimeError)` rather than the `No matching images found` return of `0` | the refusal placed after task preparation |

Both files run on both gate interpreters unchanged: every arm selects its
strategy through a lever or through the `:memory:` store, never through the
interpreter's default. The one exception is
`test_a_free_threaded_build_runs_a_file_store_in_threads_and_says_so` in §7.1,
which is *about* the default and is gated on the build.

### 7.3 Unit level -- `tests/test_parallel_contract.py`

| Test | Asserts | The single production edit that reddens it |
| --- | --- | --- |
| `test_the_choice_records_which_lever_asked_for_processes` | a table over the lever matrix asserting the whole `(use_threads, processes_requested_by)` pair per row: nothing set on 3.12 gives `(False, None)`; `FORCE_PROCESSES=1` with `force_threads=True` gives `(True, "ISOCENTER_FORCE_PROCESSES")`; both force levers give `(True, None)`; `maxtasksperchild` from the environment gives `(False, "ISOCENTER_MAX_TASKS_PER_CHILD")` and from the argument gives `(False, "the maxtasksperchild argument")` | the attribution computed after the `force_threads` short-circuit, which turns row two's second element into `None`. That is the single mutation the whole refusal rests on, and this is the test that kills it |
| `test_a_pre_resolved_strategy_is_used_as_given` | `run_parallel(f, items, strategy=s)` with `s.use_threads` true while the environment says `ISOCENTER_FORCE_PROCESSES=1`; the worker's `os.getpid()` equals the parent's | `run_parallel` re-resolving when `strategy` is supplied |
| the existing `_use_threads` assertions at lines 187-195, 562-565, 589, 765, 781, 809 and 873 | migrate to `_resolve_execution_choice(...).use_threads`, unchanged in meaning | -- |

---

## 8. Blast radius, measured

The refusal was simulated suite-wide before it was designed into anything.
`refusal_plugin.py` wraps `DicomSession.redact` with §4.4's precondition -- both
levers -- and raises; the whole suite then runs under `-p refusal_plugin`.

**3.12.14:**

```
===== PROBE: refusal fired 2 times =====
  ISOCENTER_FORCE_PROCESSES  <-  tests/test_memory_store_redaction_strategy.py::test_a_memory_store_redacts_through_the_front_door_under_processes (call)
  ISOCENTER_FORCE_PROCESSES  <-  tests/test_memory_store_redaction_strategy.py::test_the_memory_store_asks_for_threads_per_call_and_a_file_store_does_not (call)

FAILED tests/test_frozen_surface.py::test_the_two_pass_behaviours_are_stated_as_contract
FAILED tests/test_memory_store_redaction_strategy.py::test_a_memory_store_redacts_through_the_front_door_under_processes
FAILED tests/test_memory_store_redaction_strategy.py::test_the_memory_store_asks_for_threads_per_call_and_a_file_store_does_not
============ 3 failed, 1768 passed, 1 skipped in 280.36s (0:04:40) =============
```

**3.14.7t:** identical -- the same two firings, the same three failures,
`3 failed, 1768 passed, 1 skipped in 268.21s`.

The `test_frozen_surface.py` failure is an artefact of the probe, not of the
design: that test asserts `"RuntimeError" in inspect.getdoc(DicomSession.redact)`
and the plugin's wrapper has no docstring. The real `redact()` docstring already
names `RuntimeError` for the pass-lock timeout and will name it twice after §6.2.
Worth knowing anyway: **`tests/test_frozen_surface.py::test_the_two_pass_behaviours_are_stated_as_contract`
is a live guard on the `Raises:` clause this change edits.**

`ISOCENTER_MAX_TASKS_PER_CHILD` fired **zero** times across 1771 tests on both
interpreters, so folding it in (**Q2**) costs the suite nothing.

**The measured number is an upper bound, not the exact figure.** The plugin's
condition refuses on `ISOCENTER_FORCE_PROCESSES=1` unconditionally, where §4.4's
design does not refuse when `ISOCENTER_FORCE_THREADS=1` supersedes it (probe arm
D). The plugin's condition is therefore a strict superset of the designed one,
and the designed refusal can only fire on fewer tests than the two named above --
which is what makes "two, and both in one file" safe to plan against.

### 8.1 `tests/test_memory_store_redaction_strategy.py` must be rebuilt, and coverage is genuinely lost

That file's autouse fixture sets `ISOCENTER_FORCE_PROCESSES=1` and deletes the
other two levers, and its module docstring says why:

> **Every test here sets `ISOCENTER_FORCE_PROCESSES=1`** and deletes the other
> two levers, so the processes path is selected structurally on the free-threaded
> build too. Without that, 3.14t's default of threads would decide the test before
> the fix is consulted, and the suite would be green there for the wrong reason.

**The refusal deletes that technique.** After this change there is no way to make
`redact()` on a `:memory:` store take the processes path -- that is the point of
the refusal -- so no test can select it structurally, on any interpreter. Say
this plainly rather than quietly rewriting the fixture:

- `test_a_memory_store_redacts_through_the_front_door_under_processes` is
  **renamed** (its name states a configuration that no longer exists) to
  `test_a_memory_store_redacts_through_the_front_door`, and the fixture becomes
  per-test rather than autouse, with the lever removed from this arm. On **3.12**
  it keeps its full force: processes are the default there, so `force_threads=True`
  is still the only reason the pass succeeds, and deleting it still reddens the
  test. On **3.14t** it is now weaker -- threads are the default, so the argument
  is not load-bearing there.
- `test_the_memory_store_asks_for_threads_per_call_and_a_file_store_does_not`
  keeps both halves, but the spy changes: after §6.2 the dispatch passes
  `strategy=` rather than `force_threads=`, so the spy asserts
  `kwargs["strategy"].use_threads` instead of `kwargs.get("force_threads")`. Its
  file-store half keeps `ISOCENTER_FORCE_PROCESSES=1`, which is why the fixture
  must be per-test. Its documented killing mutation changes with it: not "the
  `force_threads=` keyword deleted from the `run_parallel(` call" but "the
  `force_threads=` argument deleted from the `_resolve_strategy(` call in
  `redact()`". The mutation still exists and is still one edit; it has moved
  fifty lines.
- **What replaces the lost 3.14t coverage:** §7.2's
  `test_force_processes_on_a_memory_store_refuses` and
  `test_max_tasks_per_child_on_a_memory_store_refuses` run on both interpreters
  and assert the refusal, and §7.3's choice table asserts the attribution that
  makes it fire. The #381 property that used to be proved by "processes forced,
  and it still worked" is now proved by "processes requested, and it was refused"
  -- which is a stronger statement about the same fact, since it no longer depends
  on the argument beating the variable at run time.
- The module docstring is rewritten to say all of the above. It is a *test*
  docstring, not a dated spec, so it is edited in place; the dated specs in §9.3
  are the ones that get `Superseded in part:` markers instead.

---

## 9. Documentation and CHANGELOG

### 9.1 `docs/environment.md`

`tests/test_documented_env_vars.py` binds every `_env_is`/`_env_int`/`os.environ`
read with a literal name to a **row** in this file, in the read-but-undocumented
direction. No variable is added or removed here, and the reads move but do not
change in number, so the guard stays satisfied by construction. Three rows change
their prose:

- **`ISOCENTER_FORCE_PROCESSES`.** Today: "**Except `redact()` on a `:memory:`
  store**, which runs in threads on every interpreter, this variable
  notwithstanding". That sentence becomes the exception the ruling asked for:
  `redact()` on a `:memory:` store **raises `RuntimeError`** when this variable is
  set, naming the store, the variable and the remedy, before any work is done --
  because there is no execution in which the variable can be obeyed. The three
  levers' documented order is **unchanged** and stays stated in this row.
- **`ISOCENTER_MAX_TASKS_PER_CHILD`.** Today: "with this set, that call runs in
  processes and fails with `RedactionError` naming `no such table:
  instance_blobs`. Unset it, or use a file-backed store." Under **Q2** that
  sentence is now false and becomes the `RuntimeError` refusal. Under a "no" to
  Q2 it stays exactly as it is, and §7.2's Q2 row is not written.
- **`ISOCENTER_DB_PATH`.** Its `:memory:` sentence "`redact()` runs in threads on
  every interpreter (#381)" gains "or refuses -- see the two rows below".
- Not part of this bunch but noted in passing, because the file is open: the
  `ISOCENTER_MAX_WORKERS` row's stale `CPU_COUNT * 1.5` default was already fixed
  in 0.9.4 and reads correctly today.

### 9.2 `docs/api/stability.md`

Two edits, **both owner calls** (§10, Q3):

- **Behaviours.** "`redact()` on a `:memory:` store runs in threads on every
  interpreter (#381)" is a frozen clause and becomes "runs in threads on every
  interpreter, or refuses". That is a *narrowing* of a frozen promise: a
  combination that used to return now raises.
- **Exceptions.** A line beside `compact()`'s: "`redact()` raises `RuntimeError`
  on a `:memory:` store when the environment asks for processes, before any work
  is done."

`tests/test_frozen_surface.py` parses the **parameters** table row for row, and
no signature changes here, so it stays green. `redact()`'s parameters are
`show_progress=True, force=False` before and after. **A signature change would be
an owner call and this design does not make one** -- making a previously-silent
call raise is not a signature change. What a caller who relied on the old
behaviour now sees is in §9.4.

### 9.3 Dated specs that this falsifies

`CLAUDE.md`'s rule: a later change that falsifies a clause of a dated spec adds a
`**Superseded in part:**` line to that spec's front matter naming the issue and
the clause, and marks the clause in place, striking the original rather than
erasing it. Two qualify, and the front-matter line is not optional:

- `docs/superpowers/specs/2026-09-08-frozen-surface-and-strategy-bunch-3.md`,
  lines 340-343, which quote the print line verbatim as
  `Executing using {max_workers} workers...` and record the parenthetical's
  removal as the answer. #384 supersedes the clause: the parenthetical returns,
  carrying the resolved strategy. Line 1370 quotes the same statement at
  `session.py:2833` and moves with it.
- `docs/superpowers/specs/2026-09-08-sidecar-concurrency-contract.md`, line 1219,
  which states `redact()` prints `Executing using N workers (Process
  Isolation)...`. Already stale at `f544989`; #384 is the issue that retires it.

### 9.4 CHANGELOG entry shape

Two entries, in `## [Unreleased]`.

**Under `### Breaking`, for #400.** The project's rule is that a breaking entry
states the exact exception a previously-working call now raises and why the old
behaviour was wrong. The shape, with the trap §4.1 measured:

> **`redact()` on a `":memory:"` store now refuses an environment that asks for
> processes, instead of ignoring it (#400).** `Session(":memory:")` plus
> `ISOCENTER_FORCE_PROCESSES=1` used to return normally, and the redaction was
> correct: `redact()` passes `force_threads=True` for a `:memory:` store, that
> argument outranks the variable, and the pass ran in threads on every
> interpreter. **What was wrong was not the result but the silence.** The
> operator's lever was discarded with nothing logged, so a process in which
> `audit()`, `scan_pixel_content()` and `export()` all obeyed the variable had one
> step that did not, and no output said so. That call now raises `RuntimeError`
> naming the store, naming `ISOCENTER_FORCE_PROCESSES`, saying that a spawned
> worker is handed `_memory_conn=None` and finds no `instance_blobs` table, and
> giving two remedies -- unset the variable, or use a file-backed store. It is
> raised before the persistence drain and before the pass-lock, so nothing has
> happened when it arrives: no task prepared, no SOP Instance UID regenerated, no
> audit row, no attestation. **A caller who relied on the old behaviour** -- a
> `:memory:` pipeline running under a profile that sets the variable -- now sees a
> `RuntimeError` from `redact()` where they previously saw a correct redaction,
> and must unset the variable for that session. `except RedactionError` does
> **not** catch it, deliberately: nothing was attempted, so the exception that
> means "these instances failed" would be the wrong report. `except RuntimeError`
> does, since `RedactionError` is one. **A warning was considered and rejected**
> (#185 is the inverse case): there, recycling wins and processes are *correct*
> for `export()`, so the warning annotates a good run; here processes are never
> correct for a `:memory:` store, so there is nothing to annotate and a warning
> would have preserved the silent path that is the defect.

Add, only under **Q2**: that `ISOCENTER_MAX_TASKS_PER_CHILD` on a `:memory:`
store now raises the same `RuntimeError` before the pass instead of the
`RedactionError` naming `no such table: instance_blobs` that 0.9.4 documented,
and that the old failure was deterministic -- three of three instances, measured
on both interpreters -- rather than the intermittent thing the issue described.

**Under `### Changed`, for #384.** Console lines are tier 2, so the entry names
both spellings:

> **`redact()` names the execution strategy it resolved (#384).** The line was
> `Executing using N workers...` in 0.9.4 and
> `Executing using N workers (Process Isolation)...` before that; it is now
> `Executing using N workers (threads)...` or `Executing using N workers
> (processes)...`, and the parenthetical is read off the `_Strategy` the pool is
> built from rather than derived a second time at the print site. The 0.9.4
> spelling was not false, it was empty: measured across seven store-and-lever
> combinations on 3.12.14 and 3.14.7t, three dispatch paths produced one identical
> sentence, including the case the issue was filed from -- a file-backed store with
> no levers set, which runs in processes on 3.12 and in threads on 3.14t. The
> `INFO` log line beside it gains the same field. Under threads the worker mutates
> the live `Instance`; under processes it mutates a pickled copy whose result is
> applied back by the parent, so this is the difference that decides what a worker
> is, and it was the one fact the line did not carry.

---

## 10. Owner questions

Answer before §7's tests are written. None of them is the developer's to decide.

**Q1 -- the ruling's mechanism clause.** The 2026-09-09 ruling on #400 says
"`redact()` on a `:memory:` store raises when the lever has overridden its
request for threads". Measured (§4.1), `ISOCENTER_FORCE_PROCESSES` never
overrides it -- threads win, always, on both interpreters. Read literally, the
ruling would apply only to `ISOCENTER_MAX_TASKS_PER_CHILD`, and
`ISOCENTER_FORCE_PROCESSES=1` would go on being ignored in silence, which is the
defect the correction comment says the issue should ask to fix. This brief reads
the clause as residue of the withdrawn Evidence paragraph and takes the ruling's
remedy clauses -- name the store and the variable, give the
`ISOCENTER_FORCE_PROCESSES` row "the exception as its one documented limit" -- as
controlling, so that **`ISOCENTER_FORCE_PROCESSES=1` on a `:memory:` store
raises even though threads would have won**. Confirm.

**Q2 -- does `ISOCENTER_MAX_TASKS_PER_CHILD` refuse too?** The ruling does not
mention it, and it is the lever that actually produces the crash: measured, three
of three instances fail with `no such table: instance_blobs` on both
interpreters. Folding it in **changes what a released version claimed** --
0.9.4's `docs/environment.md` documents that arm as failing with `RedactionError`
naming `no such table: instance_blobs`, and it would now raise `RuntimeError`
before the pass instead. It costs the suite nothing (zero firings across 1771
tests on both interpreters, §8). **Recommendation: yes**, because it is the only
way to state the invariant plainly -- *`redact()` on a `:memory:` store runs in
threads or refuses; it never produces `no such table: instance_blobs`* -- and a
refusal that covers one of two levers leaves the milestone's own defect shape in
the tree. If the answer is no, drop the second message in §4.4, the two Q2 rows
in §7.2, and the second `docs/environment.md` edit in §9.1.

**Q2 carries one problem of its own, and it is a false sentence.** Under §3.3's
design `redact()` resolves the strategy first, and `_resolve_execution_choice`
emits the #185 warning while resolving. Measured, probe arm C: on a `:memory:`
store with `ISOCENTER_MAX_TASKS_PER_CHILD` set, that warning reads
`force_threads=True was set, but worker recycling (maxtasksperchild=1) was also
asked for ... so this run uses processes.` Under Q2=yes, the refusal fires
immediately after it -- so a milestone about false sentences would ship a line
saying "this run uses processes" one line above a refusal of a run that never
starts. Two ways out, and the owner should pick:

- **(a) Accept the pair and document it.** One extra line of output, already
  accurate about the *strategy* if not about the run, on a path that now raises
  anyway. No further code movement. The #185 warning tests
  (`tests/test_parallel_contract.py:765-873`) are untouched.
- **(b) Move the warning from resolution to dispatch.** `_resolve_execution_choice`
  stops calling the logger and `_Choice` carries the lever spelling it would have
  named; `run_parallel` emits the warning where the strategy is actually *used*.
  A strategy that is resolved and then refused never warns, because it was never
  run. This is the better rule -- report at the point of use -- and it is a real
  cost: the #185 warning assertions at `tests/test_parallel_contract.py:765-873`
  move from `_use_threads(...)` to a dispatch-level assertion, and the warning
  stops firing for any caller that resolves a strategy without running it (there
  is exactly one such caller, `redact()`, and only on the refusing path).

**Recommendation: (b) if Q2 is yes, (a) is moot if Q2 is no** -- with only
`ISOCENTER_FORCE_PROCESSES` in scope, the #185 warning never fires on the
refusing path, because that arm sets no `maxtasksperchild`. So Q2 and this
sub-question stand or fall together, which is why they are one question.

**Q3 -- the frozen clause.** `docs/api/stability.md` currently promises, under
**Behaviours**, "`redact()` on a `:memory:` store runs in threads on every
interpreter (#381)". The refusal narrows it to "runs in threads on every
interpreter, or refuses". Narrowing a frozen behaviour is an owner call, not the
developer's, even pre-1.0. It is **one** clause, not two: the sibling clause
"`audit()` and `redact()` drain the persistence manager on entry" stays true
verbatim, because §4.4 places the refusal after the drain for exactly that
reason. This is **not** a signature change -- `redact()` keeps
`show_progress=True, force=False` and `tests/test_frozen_surface.py`'s parameter
table is untouched -- and making a previously-silent call raise is not a
signature change either. It is named here because it is the frozen surface, and
the constraint is to name it and stop.

**Q4 -- the exact banner wording.** `(threads)` and `(processes)` are proposed
over the retired `(Process Isolation)` and over a longer form. Console `print`
lines are tier 2, so this is changeable later with a CHANGELOG entry, but the
tests in §7 assert whole lines and will encode whichever spelling is chosen. Say
now if a different one is wanted.

---

## 11. What the reviewer should attack

Ordered by how much would have to be undone if the attack lands.

1. **"You refuse a configuration that works."** This is the strongest objection
   and it is correct on its face: measured, `:memory:` plus
   `ISOCENTER_FORCE_PROCESSES=1` returns 3, zeroes every zone, and leaves every
   pixel outside them intact, on both interpreters. The defence is §4.3 and the
   owner's ruling, not the measurement: the run is right and the *report* is
   absent, and a lever that can never be obeyed for this store is not a tuning
   knob with a caveat, it is a configuration error. If the reviewer prefers a
   warning here, the thing to argue with is Q1, not the code.
2. **"Q1 is you overruling the owner."** Read the ruling's mechanism clause
   literally and this brief refuses a case the ruling does not describe. §4.1
   argues the clause inherited a withdrawn measurement and the remedy clauses are
   what survive. That is an interpretation, it is flagged as Q1 rather than
   buried, and it is the single point on which the whole of §4 turns.
3. **"`processes_requested_by` is a second implementation of the ranking after
   all."** It is one function that returns two facts, and §7.3's choice table
   pins every row of the pairing. The place to look for the defect this design
   is meant to prevent is any *other* read of `ISOCENTER_FORCE_PROCESSES` or
   `ISOCENTER_FORCE_THREADS` outside `_resolve_execution_choice` -- grep for it;
   there should be none in `session.py`.
4. **"The banner tests are substring assertions in disguise."** They are
   whole-line membership against a literal built from a pinned
   `ISOCENTER_MAX_WORKERS=3`, with a negative on the opposite parenthetical, for
   the measured reason in §7.0 -- one real line of `redact()` output contains
   `threads` and `processes` and both variable names at once. Check that no test
   in §7 was written as `in captured.out`; that is the one shape that would make
   this whole plan green for the wrong reason.
5. **"The 3.14t test skips instead of failing."** Check the `skipif` predicate.
   It must be `sysconfig.get_config_var("Py_GIL_DISABLED")`, a build property,
   with `sys._is_gil_enabled() is False` as a hard assertion inside. A `skipif`
   on `sys._is_gil_enabled()` turns an extension that re-enabled the GIL into a
   skip, and a green gate that proved nothing -- which is precisely the failure
   the gate exists to catch.
6. **"`run_parallel(strategy=...)` lets a caller hand in a strategy that
   disagrees with its own keywords."** True, and the mitigation is that the one
   caller passes `strategy=` and *nothing else*. If a second caller ever passes
   both, the keywords are silently ignored. Worth a `TypeError` if the reviewer
   wants one; it is not in this design because there is exactly one caller and a
   guard for a caller that does not exist is a branch no test can reach honestly.
7. **"You lost coverage in `test_memory_store_redaction_strategy.py`."** Yes,
   measurably, on 3.14t only -- §8.1 says which test, why the technique it used
   is now impossible, and what replaces it. Do not accept a rewrite of that file
   that quietly keeps its module docstring's claim about selecting the processes
   path.
8. **"The refusal is in the wrong place."** Check that it is above
   `persistence_manager.flush()` and outside the `try`. §7.2's
   `test_the_refusal_has_done_nothing` asserts the `caplog` clause that catches
   an in-`try` placement; a reviewer should confirm that clause exists, because
   without it a refusal inside the `try` passes every other assertion in that
   test while printing a sentence about work that never started.
9. **"The CHANGELOG says the old behaviour was luck."** It must not. §4.1
   measured that the `ISOCENTER_FORCE_PROCESSES` arm was always correct and the
   `ISOCENTER_MAX_TASKS_PER_CHILD` arm always failed, three of three. The ruling
   comment's "the previous success was luck" sentence is the one thing from it
   that §9.4 deliberately does not carry over.

---

## 12. Adjacent findings, for the owner to file

Not fixed here; named so they are not lost.

1. **The #185 warning names a lever the operator did not set.** On the
   `:memory:` recycling arm it reads `force_threads=True was set, but worker
   recycling ... was also asked for`. `force_threads=True` was set by `redact()`,
   not by the caller, so the message points at a knob the reader cannot find.
   Measured in probe arm C on both interpreters. Under **Q2** the `:memory:`
   instance of this becomes unreachable, but the same wording still reaches any
   caller-set `force_threads=True`.
2. **`ingest()` reads `ISOCENTER_FORCE_THREADS` and ignores it, silently**
   (#390, already documented in `docs/environment.md`). It is the same milestone
   shape as #400 -- a lever discarded without a word -- and it is the one path
   this bunch does not touch, because `ingest()` runs on the session's shared
   executor and there is no strategy to report.
3. **No `run_parallel` call site except `redact()` names its strategy anywhere.**
   `audit()`, `scan_pixel_content()`, `discover_redaction_zones()` and `export()`
   are silent about threads-versus-processes on every interpreter. A single INFO
   line inside `run_parallel`, once per call, keyed on `desc`, would close all of
   them at the cost of one line per pass; it was rejected for this bunch as scope
   (§3.4) and deserves its own issue and its own noise budget.
4. **A citation that is already wrong on `f544989`.**
   `tests/test_audit_read_barrier.py:233` reads "`_use_threads`
   (`parallel.py:133`)". `_use_threads` is at line 285; line 133 is inside
   `_Strategy`'s field list. `tests/test_source_citations.py` grades it under
   Rule 1 only -- 133 is in range of a 504-line file -- so the guard is green on
   a citation that points at the wrong function. Rule 2, the content pin, would
   have caught it, and the citation is not written in Rule 2's grammar. This is
   the class of defect that file's own docstring calls out ("an in-range check
   alone still passes after someone inserts a line above 201"), reached from the
   other direction: not drift, but a number that was never right. §6.3 corrects
   this one in passing; the general question -- whether the Rule 1 grammar should
   exist at all, or whether every citation should be a content pin -- is the
   owner's.
5. **`_redaction_worker_count()` and `_resolve_strategy` both read
   `ISOCENTER_MAX_WORKERS`.** They agree today only because `redact()` passes its
   number in explicitly, so `_resolve_strategy` never reaches its own read for
   that call. Two reads of one variable with two different defaults is the shape
   #335 and #341 were about; it is documented in the `ISOCENTER_MAX_WORKERS` row
   and is not a defect today, but after this change the printed number comes from
   `strategy.max_workers`, which makes the coupling load-bearing for the banner as
   well as for the pool.

---

## 13. Sequencing

1. `parallel.py`: `_Choice`, `_resolve_execution_choice`, `_Strategy.processes_requested_by`,
   `run_parallel(strategy=...)`. Migrate the `_use_threads` assertions in
   `tests/test_parallel_contract.py`; add §7.3's two tests. Suite green here, with
   no behaviour change visible from `session.py`.
2. `session.py` #384 only: resolve in `redact()`, thread `strategy` into
   `_apply_redaction_rules`, print and log from it, dispatch with `strategy=`.
   Add §7.1. Rewrite `test_memory_store_redaction_strategy.py`'s spy for the new
   keyword (§8.1) -- that file goes red at this step, before the refusal exists,
   and that is the expected red.
3. `session.py` #400: the refusal helper and its call site. Add §7.2. The two
   `test_memory_store_redaction_strategy.py` tests go red here and are rebuilt per
   §8.1.
4. Docs: `docs/environment.md`, `docs/api/stability.md` (pending Q3), the two
   `Superseded in part:` markers, both docstrings, CHANGELOG.
5. Full suite on **both** interpreters, `pytest -v`, unpiped, with
   `PYTHONDONTWRITEBYTECODE=1` and `PYTHONPATH=<worktree>`, after printing and
   reading `isocenter.__file__`. Baseline to beat: `1771 passed, 1 skipped` on the
   floor.

---

## 14. Amendments

Corrections made after the body above was written and before the brief was
handed to a developer. This is the log bunch A's brief carries as its §11; it is
numbered 14 here only because renumbering the body would be the silent rewrite
the convention forbids. **Where this section disagrees with §1-§13, this section
wins.**

### 14.1 (2026-09-09) Q1 ruled: `ISOCENTER_FORCE_PROCESSES` **warns**, it does not refuse

**The owner's ruling is the inverse of §10's recommendation, and it is right for
a reason this brief supplied.** The original "refuse, not warn" ruling of
2026-09-09 rested on *processes are never correct for a `:memory:` store, so
there is nothing to annotate*. §4.1 measured that this is **false for this
lever**: with `ISOCENTER_FORCE_PROCESSES=1` the redaction runs in threads,
returns 3, and zeroes every zone, on 3.12.14 and 3.14.7t alike. Nothing about
the run is incorrect. The premise that made refusal right does not hold here, so
neither does the conclusion.

That leaves this arm sitting squarely in the **#185 case**, which the original
ruling had itself named as the one where a warning is the right answer: a lever
that is ignored while the run stays correct. Refusing would turn a
currently-succeeding call into an error in order to report a condition that
harms nothing.

§4.3's framing survives the change and is in fact sharpened by it -- what it got
wrong was assuming both levers fell on the same side of its own line:

> A silence about what was done is worth a line. A silence about what can never
> be done is worth a refusal.

`ISOCENTER_FORCE_PROCESSES` on a `:memory:` store is the **first** kind: the
request was ignored, the pass ran, and the operator is owed the line.
`ISOCENTER_MAX_TASKS_PER_CHILD` is the **second**: the request is obeyed and
there is no run in which obeying it is correct.

**And the field that tells them apart is already in the design.**
`strategy.use_threads` is exactly the discriminator:

| `processes_requested_by` | `use_threads` | what happened | response |
| --- | --- | --- | --- |
| `None` | either | nobody asked; rank 4 is a default | silence |
| `"ISOCENTER_FORCE_PROCESSES"` | `True` | the request was discarded, the pass was correct | **warn** |
| `"ISOCENTER_MAX_TASKS_PER_CHILD"` | `False` | the request was obeyed and cannot work | **refuse** |

So §4.4's single call site stays a single call site; only its second half
changes. `_refuse_processes_on_a_memory_store` is renamed
**`_report_processes_lever_on_a_memory_store(db_path, strategy)`**, returns early
unless the store is `:memory:` and `strategy.processes_requested_by` is set, and
then **warns if `strategy.use_threads` and raises if not**. Everything §4.4 says
about placement is unchanged and now covers both outcomes: after the rules guard,
after the drain, before `RedactionService(...)`, the `try` and the pass-lock.

On the `:memory:` redaction path those two rows are the only reachable ones, and
that is a consequence of §2 rather than a coincidence: `redact()` passes
`force_threads=True` for a `:memory:` store, which sits at rank 2, so the only
thing that can make `use_threads` false is rank 1, the recycling lever. The
helper is nonetheless written on `use_threads` and not on the lever's name,
because `use_threads` is the fact that decides which of the two silences this is,
and a fifth lever added at some future rank would be classified correctly without
touching it.

### 14.2 The warning, and the mistake it must not repeat

The owner's constraint: the warning must be real speech, not a shrug -- it names
the store type, names the variable, says that `redact()` requested threads for a
`:memory:` store and that the request wins, and says plainly that the variable
had no effect on this run. And it must **not** become the #185 warning's twin.
§12 item 1 records why: #185's warning opens `force_threads=True was set`, and on
the redaction path `force_threads=True` was set by `redact()`, not by the reader,
so it points at a knob they cannot find and did not touch. That sentence is the
model of what not to write.

```
ISOCENTER_FORCE_PROCESSES=1 had no effect on this run. redact() requires threads
on a ":memory:" store and asks for them per call, and that request outranks the
variable, so this pass ran in threads and its result is correct. Processes cannot
redact a ":memory:" store at all: a spawned worker is handed _memory_conn=None by
SqliteStore.__setstate__ and opens a fresh, empty in-memory database with no
instance_blobs table. The variable still applies to every other parallel pass in
this process. If you set it expecting redaction in processes, that needs a
file-backed store -- Session("session.db").
```

Four properties to hold in review:

- **It names no knob the reader did not set.** `force_threads` appears nowhere.
  The subject of the second sentence is `redact()`, describing what the library
  does, not a parameter the reader could have passed. §14.4 has the test.
- **It says the run was correct**, in as many words. A warning in front of a
  correct result that does not say the result is correct is a warning that reads
  as a failure, and the reader's next hour goes into looking for damage that is
  not there.
- **It bounds itself.** "still applies to every other parallel pass in this
  process" is the fact an operator actually needs: their variable is working, at
  every step but this one.
- **It fires once per `redact()` call**, not once per `run_parallel`. #185's
  warning fires on every parallel pass in the process when both its levers are
  set, which its own comment concedes is "a lot of output on a long run". This
  one is emitted from `session.py`, once, beside the strategy banner.

`get_logger().warning(...)`, no `print()`. The console handler is WARNING and
above, so a logger warning is already user-visible -- measured, probe arm C's
captured stdout carries the #185 warning as a `WARNING:` line -- and a `print()`
beside it would be two spellings of one message.

### 14.3 Q2 ruled yes, with option (b): the resolver stops speaking

`ISOCENTER_MAX_TASKS_PER_CHILD` on a `:memory:` store **refuses**, exactly as
§4.4's second message says. §10's sub-problem -- the #185 warning printing "this
run uses processes" one line above a refusal of a run that never starts -- is
resolved by **option (b)**: warning emission moves from resolution to dispatch.

- `_resolve_execution_choice` **emits nothing**. It becomes a pure function of
  its arguments and the environment, which is what makes it safe to call ahead of
  a decision about whether to run at all -- the property §3.3's design needs and
  did not have.
- `_Choice` and `_Strategy` carry a second attribution field,
  **`threads_request_overridden_by: Optional[str]`** -- `"ISOCENTER_FORCE_THREADS"`
  or `"force_threads=True"`, the two spellings the #185 warning already chooses
  between -- set when and only when rank 1 beat a threads request. The
  `maxtasksperchild` value the message quotes is already on `_Strategy`.
- **`run_parallel` emits the #185 warning**, once, immediately before it picks
  among the three execution paths, whenever `strategy.threads_request_overridden_by`
  is set. Both entry points are covered: a strategy `run_parallel` resolved
  itself, and one handed to it as `strategy=`.
- **Frequency is unchanged for every path that exists today.** Resolution and
  dispatch are one-to-one in `run_parallel`, so every current caller warns exactly
  as often as before. The only behaviour that changes is the one the move is for:
  a strategy that is resolved and then refused never warns, because it was never
  run.

The message text itself does not change. Its `force_threads=True was set` opening
remains wrong in the way §12 item 1 describes -- that is a separate issue and is
**not** fixed here, because fixing it would edit a warning this bunch is only
relocating, and the relocation is already the part a reviewer has to check.

### 14.4 The test plan, as amended

§7.0's anchoring rules stand unchanged and apply to the warning too: a
`caplog` assertion on a strategy message is the same correct-by-accident shape as
a `capsys` assertion on the banner, and for the same measured reason -- one real
warning in this tree names `threads`, `processes` and two of the three variables
at once. **Every warning assertion below selects a single record first and then
asserts on that record's `getMessage()`**, never on `caplog.text`.

The selection is: the records whose `levelname` is `WARNING` **and** whose message
contains `ISOCENTER_FORCE_PROCESSES`, of which there must be **exactly one**.
Asserting the count before asserting the content is what stops a second, unrelated
warning from satisfying the test, and what makes "fires once" testable at all.

§7.2's table is replaced by the following. Rows marked *kept* are unchanged from
§7.2 except that their lever is now `ISOCENTER_MAX_TASKS_PER_CHILD`.

| Test | Asserts | The single production edit that reddens it |
| --- | --- | --- |
| `test_force_processes_on_a_memory_store_warns_and_still_redacts` | `redact() == 3`, every zone zeroed, every pixel outside still `FILL`, **no exception**; exactly one selected record; its message contains `":memory:"`, `ISOCENTER_FORCE_PROCESSES`, `had no effect`, `instance_blobs` and `Session("session.db")` as five separate assertions | the warning deleted |
| `test_the_warning_does_not_name_a_knob_the_operator_did_not_set` | the selected record's message does **not** contain `force_threads` | the warning written as a copy of #185's, which opens `force_threads=True was set`. Its own test, because it is the one property a reviewer cannot see by reading the other assertions pass |
| `test_the_warning_fires_once_for_a_whole_session_of_passes` | `:memory:`, `ISOCENTER_FORCE_PROCESSES=1`; run `audit()` **and then** `redact()`; still exactly one selected record | the warning emitted from `parallel.py` instead of `session.py`, which would warn on the audit pass too. `audit()` on a `:memory:` store under processes succeeds -- its worker reads and returns findings the parent persists -- and the developer should confirm that red-first rather than take it from here |
| `test_a_file_store_with_force_processes_says_nothing` | file store, lever set; `redact() == 3`; **zero** selected records | the `db_path == ":memory:"` half of the condition dropped |
| `test_a_memory_store_with_no_lever_says_nothing_and_says_threads` | no levers; `redact() == 3`; zero selected records; the whole `(threads)` banner line | attribution that treats rank 4 (the default) as a request -- the row that keeps `Session(":memory:")` quiet and working on 3.12, where processes are the default |
| `test_force_threads_beside_force_processes_says_nothing` | `:memory:`, both force levers; `redact() == 3`; zero selected records; the whole `(threads)` banner | attribution that reports `ISOCENTER_FORCE_PROCESSES` without the `ISOCENTER_FORCE_THREADS` supersession. Nothing was ignored here, so there is nothing to say |
| `test_max_tasks_per_child_on_a_memory_store_refuses` | *kept*; `pytest.raises(RuntimeError)`; the message names `ISOCENTER_MAX_TASKS_PER_CHILD`, `would have run in processes` and `no such table: instance_blobs` | the refusal deleted, or keyed on the lever's name rather than on `strategy.use_threads` |
| `test_the_refusal_is_not_a_redaction_error` | *kept*; `type(exc) is RuntimeError` **and** `not isinstance(exc, RedactionError)` | the refusal raised as `RedactionError` |
| `test_the_refusal_has_done_nothing` | *kept*, with the positive control and the `caplog` "no record beginning `Redaction failed.`" clause | the refusal moved below task preparation or inside the `try` |
| `test_the_refusal_is_not_preceded_by_the_recycling_warning` | `:memory:`, `ISOCENTER_MAX_TASKS_PER_CHILD=1`; `pytest.raises`; and **no** record anywhere in `caplog` contains `so this run uses processes` | option (b) not taken -- the #185 warning left in `_resolve_execution_choice`, where it fires during the resolution that precedes the refusal. This is the test that pins §14.3 |
| `test_the_recycling_warning_still_fires_when_the_pass_actually_runs` | file store, `ISOCENTER_FORCE_THREADS=1` **and** `ISOCENTER_MAX_TASKS_PER_CHILD=25`; `redact() == 3`; exactly one record containing `so this run uses processes`, and it quotes `maxtasksperchild=25` | **option (b) implemented as deleting the warning.** Without this test, every other test in this plan is green for a change that silently removed #185's speech, which would be this milestone undoing itself. It is the mandatory partner of the row above |
| `test_the_refusal_reaches_redact_by_machine` | *kept*, lever changed | -- |
| `test_the_refusal_fires_even_when_no_image_matches` | *kept*, lever changed | -- |
| `test_a_file_store_with_max_tasks_per_child_still_redacts` | file store, lever set; `redact() == 3`; no exception | the `:memory:` half of the refusal condition dropped |

§7.3's unit table gains one row and amends one:

- `test_the_choice_records_which_lever_asked_for_processes` now asserts the whole
  **three-field** `_Choice` per row, including `threads_request_overridden_by`,
  which must be `None` on every row except the two where rank 1 beat a threads
  request.
- **New:** `test_resolving_a_strategy_emits_nothing` -- `caplog` is empty of
  `WARNING` records across every row of the lever matrix passed to
  `_resolve_execution_choice`, including the recycling-beats-threads row. Killing
  edit: the logger call left in the resolver. This is the half of option (b) that
  §14.4's session-level test cannot see, because a session-level test cannot tell
  a warning that was never emitted from one emitted and then suppressed.
- The existing `_use_threads` warning assertions at
  `tests/test_parallel_contract.py:765-873` split in two: the **attribution**
  (`threads_request_overridden_by` is set, and to which of the two spellings)
  moves to `_resolve_execution_choice`; the **emission** (a record reaches the
  logger with the full text, quoting the number literally) moves to a
  `run_parallel` call with `caplog`. One assertion about one thing each, where
  there was one assertion about two.

The file name in §7.2's heading changes with its contents:
`tests/test_memory_store_refuses_processes.py` becomes
**`tests/test_memory_store_reports_its_processes_lever.py`**, because the file no
longer only refuses and a name that says it does would be the first false
sentence a reader met.

### 14.5 §8.1 restated: the coverage is not lost, and the file's premise survives

**§8.1 must not be carried forward as written.** It was reasoned from a refusal
that covered `ISOCENTER_FORCE_PROCESSES`, and under §14.1 that lever now warns.
The correct statement is smaller and different in kind.

`tests/test_memory_store_redaction_strategy.py`'s autouse fixture sets
`ISOCENTER_FORCE_PROCESSES=1` on a `:memory:` store. Under the ruling, that
combination still returns 3 and still redacts, so **neither test in that file
fails**, and the technique its module docstring defends is intact. That docstring
is worth reading precisely, because it is easy to misread as a claim about the
current run:

> **Every test here sets `ISOCENTER_FORCE_PROCESSES=1`** ... so the processes
> path is selected structurally on the free-threaded build too. Without that,
> 3.14t's default of threads would decide the test before the fix is consulted.

It is a claim about the **killing mutation**, not about the passing run. With
`force_threads=` deleted from the dispatch, `ISOCENTER_FORCE_PROCESSES=1` does
select processes and the test does go red -- on both interpreters, which is the
whole point of the lever. With the fix in place the lever loses, and that is the
behaviour under test. The claim was right when it was written and stays right
after this change, with one substitution: the mutation is now "the
`force_threads=` argument deleted from the `_resolve_strategy(` call in
`redact()`" rather than "from the `run_parallel(` call in
`_apply_redaction_rules`". Same one edit, fifty lines further up.

So what that file actually needs is three small things, and no rebuild:

1. The spy in `test_the_memory_store_asks_for_threads_per_call_and_a_file_store_does_not`
   reads `kwargs["strategy"].use_threads` instead of `kwargs.get("force_threads")`,
   because §6.2's dispatch now passes `strategy=`. Its two recorded expectations,
   `[True]` for the `:memory:` store and `[False]` for the file store, are
   unchanged in meaning.
2. `test_a_memory_store_redacts_through_the_front_door_under_processes` is
   **renamed** to `..._when_processes_are_requested`. The old name says the pass
   runs under processes; measured, it never has, on either interpreter, since
   #381 landed. The name was a misnomer at `f544989` and the new warning is
   about to say so out loud on every run of it.
3. The file's `:memory:` tests now emit §14.2's warning. They should **not**
   assert it -- that belongs to the new file -- and equally must not suppress it.
   A fixture that filtered it would hide the one message this bunch adds from the
   only existing tests that trigger it.

**And the coverage question §8.1 raised has a different answer than §8.1 gave.**
Nothing is lost, because nothing was there: `ISOCENTER_FORCE_PROCESSES=1` has
never selected the processes path for a `:memory:` `redact()` in the shipped
code, on either interpreter. What protects #381 on 3.14t is what always did --
the spy test's assertion that the argument is passed, plus the killing mutation
that makes the lever bite once the argument is gone. §14.4's warning and refusal
tests are additional coverage of a fact that had none, not a replacement for
coverage that existed.

### 14.6 In scope for this bunch: a citation that was never right

§12 item 4 is promoted from an adjacent finding to **a task in this bunch**, on
the owner's instruction, because the developer is editing its subject anyway.

`tests/test_audit_read_barrier.py:233` reads:

> - **On the free-threaded build (3.14t)** `_use_threads`
>   (`parallel.py:133`) returns True, `redact()` runs in threads, and
>   nothing is pickled.

`_use_threads` is at `parallel.py:285`. Line 133 is inside `_Strategy`'s field
list. The citation was wrong when it was written and is wrong at `f544989`;
`tests/test_source_citations.py` grades it under **Rule 1 only** -- 133 is in
range of a 504-line file -- so the guard has been green on a citation that points
at the wrong function for its whole life. This is the exact failure Rule 2, the
content pin, exists to catch, reached from the direction that file's docstring
does not consider: not a citation that drifted, but one that was never right, and
which no amount of insertion or deletion elsewhere will ever make wrong enough
for Rule 1 to notice.

The fix in this bunch: rewrite it in **Rule 2's grammar**, so it is content-pinned
from now on rather than merely in range. Rule 2's shape is
`` `<CODE>` at path.py line N ``, and the cited line's stripped text must equal
`<CODE>` exactly. The function is renamed by §6.1, so the citation is rewritten
against `_resolve_execution_choice`'s new `def` line and its new number, in that
grammar. A citation that is content-pinned goes red the moment an insertion moves
it, which is the property #310 asked for and this one never had.

What is **not** in scope, and stays §12's question for the owner: whether Rule 1
should exist at all, or whether every citation in the tree should be a content
pin. One citation is a fix; a sweep is a project.

### 14.7 Q4 settled: the banner wording is pinned

The owner returned this one, so it is decided here and the tests encode it.
`(threads)` and `(processes)`, exactly as §3.3 proposes:

```
Executing using 3 workers (threads)...
Executing using 3 workers (processes)...
```

Two words from `docs/environment.md`'s own vocabulary, one parenthetical, no
punctuation a test has to guess at. Rejected again on the way past: `(Process
Isolation)`, which names an implementation property rather than the choice and
leaves the recycling pool ambiguous; and any form carrying the *reason*, which is
the re-derivation the whole design exists to prevent -- the reason is what §14.2's
warning and §4.4's refusal are for, and they say it only when it matters.

Console `print` lines are tier 2, so this stays changeable later with a CHANGELOG
entry naming both spellings. It is pinned now because §14.4's tests assert whole
lines, and a whole-line assertion against an unsettled string is a test that will
be edited rather than a test that holds.

### 14.8 The CHANGELOG and the documentation, as amended

§9.4's **Breaking** entry is now about **one lever only**, and it is the lever
that actually changes what a released version claimed:

> **`redact()` on a `":memory:"` store now refuses `ISOCENTER_MAX_TASKS_PER_CHILD`
> instead of failing under it (#400).** 0.9.4's `docs/environment.md` states that
> with this variable set, `redact()` on a `:memory:` store "runs in processes and
> fails with `RedactionError` naming `no such table: instance_blobs`". That was
> accurate, and it was the wrong failure: the pass took the sidecar pass-lock,
> prepared every task, spawned workers, regenerated a SOP Instance UID per
> instance, wrote an `ERROR` audit row per failure and a `REDACTION` accounting
> row per rule-pass, printed a summary, and only then raised -- for a
> configuration that could have been rejected before any of it, because only
> `multiprocessing.Pool` implements worker recycling and a spawned worker cannot
> reach a `:memory:` store at all. It now raises **`RuntimeError`** from
> `redact()` before the pass-lock is taken and before any task is prepared,
> naming the store, the variable, why processes cannot work for this store, and
> two remedies. **The failure was never intermittent:** measured on 3.12.14 and
> 3.14.7t, all three of three instances failed every time, because
> `execute_redaction_task` ends in `persist_pixel_data` on every task. A caller
> who caught `RedactionError` around `redact()` to handle a partial pass will
> **not** catch this, deliberately -- nothing was attempted, so the exception
> meaning "these instances failed" would be the wrong report. `except
> RuntimeError` does catch it, since `RedactionError` is one. `redact_by_machine()`
> inherits it and restores the caller's configuration first.

A second entry, under **`### Changed`**, carries the `ISOCENTER_FORCE_PROCESSES`
half -- it is not breaking, because the call still succeeds and still returns the
same number:

> **`redact()` says so when `ISOCENTER_FORCE_PROCESSES` has no effect on it
> (#400).** On a `":memory:"` store `redact()` asks for threads per call, and
> that request outranks the variable, so the pass has always run in threads and
> the variable has always been ignored -- correctly, since a spawned worker
> cannot reach an in-memory database. Nothing said so. In a process where
> `audit()`, `scan_pixel_content()` and `export()` all obeyed the variable, one
> step did not, and no output mentioned it. That combination now logs one
> `WARNING` per `redact()` call naming the store, the variable, and the fact that
> the run was correct anyway. **Refusing was considered and rejected**: the
> result was never wrong, so there is a correct run to annotate, which is the
> `#185` case rather than its inverse. The recycling lever is the inverse and
> refuses -- see the breaking entry above.

§9.1's `docs/environment.md` edits change with them:

- **`ISOCENTER_FORCE_PROCESSES`** gains the warning, not an exception. The row's
  existing sentence -- "**Except `redact()` on a `:memory:` store**, which runs in
  threads on every interpreter, this variable notwithstanding" -- stays true and
  gains "and says so, with a `WARNING` naming this variable, once per call". The
  documented precedence order is unchanged, as the ruling required.
- **`ISOCENTER_MAX_TASKS_PER_CHILD`** loses its `RedactionError` sentence and
  gains the `RuntimeError` refusal.
- **`ISOCENTER_DB_PATH`**'s `:memory:` sentence gains "or refuses, if worker
  recycling is asked for" rather than the flat "or refuses" §9.1 proposed.

§9.2's `docs/api/stability.md` **Behaviours** clause narrows as Q3 allows, and to
the accurate wording: "`redact()` on a `:memory:` store runs in threads on every
interpreter, and refuses when worker recycling is asked for". §9.3's two
`Superseded in part:` markers are unchanged -- neither dated spec carries one for
these clauses today, checked at `f544989`.

### 14.9 What §11 should now say to a reviewer

§11's items 1, 2 and 7 were written against the refusal-for-both design and are
superseded:

- **Item 1** ("you refuse a configuration that works") is **the objection that
  won.** It is no longer a defence to mount; it is the ruling. What a reviewer
  should check instead is that the warning is not a shrug: §14.2's four
  properties, and §14.4's `test_the_warning_does_not_name_a_knob_the_operator_did_not_set`.
- **Item 2** ("Q1 is you overruling the owner") is closed. Q1 was ruled against
  this brief.
- **Item 7** ("you lost coverage") is closed and was wrong; §14.5 has the
  measurement.

Three items are added:

10. **"The move of the #185 warning deleted it."** The single most dangerous edit
    in this bunch. `test_the_recycling_warning_still_fires_when_the_pass_actually_runs`
    is the only thing standing between option (b) and a silent removal of
    #185's speech in a milestone about silences. Confirm that test exists and
    that it asserts the message text and the quoted number, not merely a record
    count.
11. **"The warning and the refusal are two spellings of one decision."** They are
    one helper reading one field, `strategy.use_threads`, per §14.1's table.
    Check there is no second condition anywhere -- in particular no `if lever ==
    "ISOCENTER_FORCE_PROCESSES"` in `session.py`, which would work today and be
    wrong the moment a fifth lever exists.
12. **"The warning fires more than once."** Once per `redact()` call is the
    promise; `test_the_warning_fires_once_for_a_whole_session_of_passes` pins it
    by running `audit()` first. A warning emitted from `parallel.py` passes every
    other test in §14.4.

### 14.10 Blast radius under the ruling, measured again

§8's measurement was of the design the ruling replaced, so it was taken again.
`probe_400_refusal_blast_radius.py`'s successor, `ruling_plugin.py`, wraps
`DicomSession.redact` with §14.1's table -- warn when threads won, refuse when
they did not, both only on a `:memory:` store -- and the whole suite runs under
it. Identical on both interpreters:

```
===== PROBE: warned 2 times, refused 0 times =====
  WARN   ISOCENTER_FORCE_PROCESSES  <-  tests/test_memory_store_redaction_strategy.py::test_a_memory_store_redacts_through_the_front_door_under_processes (call)
  WARN   ISOCENTER_FORCE_PROCESSES  <-  tests/test_memory_store_redaction_strategy.py::test_the_memory_store_asks_for_threads_per_call_and_a_file_store_does_not (call)

FAILED tests/test_frozen_surface.py::test_the_two_pass_behaviours_are_stated_as_contract
============ 1 failed, 1770 passed, 1 skipped in 324.99s (0:05:24) =============
```

3.14.7t: the same two firings, the same single failure,
`1 failed, 1770 passed, 1 skipped in 298.13s`.

**The refusal fires nowhere in the suite** and **the warning breaks nothing**.
The one failure is the same probe artefact §8 records -- the plugin's wrapper
carries no docstring, and `test_the_two_pass_behaviours_are_stated_as_contract`
asserts `"RuntimeError" in inspect.getdoc(DicomSession.redact)`. That test is
still a live guard on the `Raises:` clause §6.2 edits, and `redact()`'s real
docstring names `RuntimeError` before and after.

So the behavioural blast radius of this bunch, measured under the ruled design,
is **zero failing tests on both interpreters** -- against the three that §8
measured for the design the ruling replaced. §14.5's rewrite of §8.1 is what
that number means: the two tests that fired the warning did not fail, because
under the ruling their configuration still works.

Two further facts measured for §14.4's test plan, so the developer does not have
to take them on trust:

- **`audit()` on a `:memory:` store under `ISOCENTER_FORCE_PROCESSES=1`
  succeeds**, returning a `PhiReport` with findings, on 3.12.14. That is what
  makes `test_the_warning_fires_once_for_a_whole_session_of_passes` constructible
  -- the audit pass has to run for a `parallel.py` placement of the new warning to
  have something to wrongly warn about. Give that audit a configured
  `phi_tags` list rather than the default empty one, so the pass is unambiguous
  rather than merely warning that it found no tags.
- **`Py_GIL_DISABLED`** is `1` on the 3.14.7t venv and `None` on the 3.12.14
  venv, per §7.0.

### 14.11 The developer's scope, in order

1. `parallel.py`: `_Choice` with **three** fields, `_resolve_execution_choice`
   (pure -- no logger), `_Strategy` gains `processes_requested_by` and
   `threads_request_overridden_by`, `run_parallel(strategy=...)` **and** the #185
   warning emitted at dispatch. Migrate and split the `test_parallel_contract.py`
   assertions per §14.4; add `test_resolving_a_strategy_emits_nothing` and the
   dispatch-level emission test.
2. `session.py` #384: resolve once in `redact()`, thread `strategy` into
   `_apply_redaction_rules` (dropping `show_progress` and `max_workers`), print
   and log §14.7's banner from it, dispatch with `strategy=`. Add
   `tests/test_redaction_names_its_strategy.py` (§7.1). Update
   `test_memory_store_redaction_strategy.py`'s spy to
   `kwargs["strategy"].use_threads` and rename its first test (§14.5).
3. `session.py` #400: `_report_processes_lever_on_a_memory_store`, warning and
   refusal, placed after the drain and before the `try`. Add
   `tests/test_memory_store_reports_its_processes_lever.py` (§14.4).
4. `tests/test_audit_read_barrier.py:233`: rewrite the `parallel.py:133` citation
   in Rule 2's grammar against the renamed function (§14.6).
5. Docs and CHANGELOG per §14.8; `docs/api/stability.md` per Q3; the two
   `Superseded in part:` markers per §9.3; both docstrings' `Raises:` clauses.
6. Full suite on both interpreters, `pytest -v`, unpiped. Baseline: `1771 passed,
   1 skipped`, plus whatever §7.1 and §14.4 add.
