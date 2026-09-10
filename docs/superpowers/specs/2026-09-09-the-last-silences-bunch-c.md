# The Last Silences, bunch C: the pins that pin nothing

**Date:** 2026-09-09
**Milestone:** v0.9.5 -- The Last Silences
**Issues:** #396 (the Q9 vocabulary pin), #397 (the wfdb export options),
#401 (T-F4's table parser keeps the last duplicate), #394 (`_verify_worker`
at 14%), #383 (`persistence.py` has no `TARGETS` row)
**Base:** `main` at `861e1d1` (bunch B: #384, #400)
**Status:** design brief for a TDD developer. Two architect decisions are
recorded in §2 and §3; three owner questions are in §10.

Every issue in this bunch is the same shape: something in the repo claims a
fact is checked, and nothing checks it. A freeze test that greps for a word
anywhere in the package. A stability page that names two option names no test
reads. A table parser that silently prefers the wrong row. A frozen tier-1
method whose worker no test ever dispatches. A probe registry with no row for
the module that now carries the sidecar gate. Five claims, one story: **a pin
that cannot go red is documentation with a test's costume on.**

None of these is a live user-facing defect -- every issue says so, and every
one is right about that. What they are is the suite's ability to see a change,
and the milestone is about the difference between "we checked" and "we said we
checked."

---

## 1. How this was measured

Nothing below is inherited from the issue text. Three of the five issues turned
out to rest on a premise that has drifted or was never right; where an issue
disagrees with the code, the code is what is reported and the correction is
called out in the issue's own section.

- **Floor:** `/Users/kevin/Developer/Isocenter/.venv/bin/python` -- 3.12.14.
- **Free-threaded gate:** `/Users/kevin/Developer/Isocenter/.venv314t/bin/python` -- 3.14.7t.
- **Worktree:** `/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-a7be22b2681170766`.

Every invocation carried `PYTHONDONTWRITEBYTECODE=1` (#174) and
`PYTHONPATH=<worktree>`. The resolved import was checked first and read:

```
/Users/kevin/Developer/Isocenter/.claude/worktrees/agent-a7be22b2681170766/isocenter/__init__.py
```

so the editable install did not serve the main checkout. Every mutation below
was applied **in the worktree itself** and reverted with `git checkout`; a
scratchpad copy is not a mutation sandbox, because `PYTHONPATH` loses to the
process cwd and the copy measures the unmutated tree.

**One environment fact governs §6 and is easy to get wrong.** Neither local
interpreter has `pytesseract`:

```
pytesseract not installed. OCR features are disabled; install with `pip install isocenter[ocr]`.
```

while `.github/workflows/tests.yml` installs `tesseract-ocr` via apt **and**
`pip install -e ".[tests,ocr]"`. So `pixel_analysis.HAS_OCR` is `False` on both
gate interpreters here and `True` on every CI job. Any test whose result
depends on real OCR passes for different reasons in the two places, which is
the definition of an unreliable pin.

Five probes are committed beside this brief in
`docs/superpowers/specs/2026-09-09-the-last-silences-bunch-c/`, for bunch A and
B's reason -- a document whose whole claim is measured evidence cannot point at
a directory that exists for nobody but its author. They are kept off the docs
site by `mkdocs.yml`'s `exclude_docs` and cannot be collected by pytest
(`pytest.ini` sets `testpaths = tests`). Each derives the repo root from its own
`__file__`, so they run from any checkout:

- `probe_396_write_sites_by_callee.py` -- every `action_type=` string constant
  under `isocenter/`, grouped by the function it is passed to (§5.1).
- `probe_396_vocabulary_in_tests.py` -- which frozen words any test outside
  `test_frozen_surface.py` spells as a literal (§5.2).
- `probe_394_worker_crosses_a_pool.py` -- `scan_pixel_content()` under a real
  process pool, plus the pickle round-trip of a worker item (§6.1).
- `probe_394_entity_identity_by_mode.py` -- what a finding's `.entity` is in
  threads and in processes (§6.2).
- `probe_383_persistence_importers_and_sites.py` -- the 42 test files the
  `TARGETS` guard will demand, and every module's mutation-site count (§8.1).

---

## 2. Architect decision 1: what #383 should be

**Ruling: scope #383 to `scripts/mutation_probe.py`'s `TARGETS` alone. Retitle
the issue. The developer does not touch CLAUDE.md.**

`c925829` untracked `CLAUDE.md` and deleted the five tests that read it. The
file is now local-only: not in the repo, not in the sdist, not in any worktree
including this one. Half of #383's stated deliverable therefore has no home in
version control -- a PR cannot contain it, a reviewer cannot check it, and a
fresh clone has no copy to be stale.

Three reasons this is the right cut rather than a convenient one:

1. **The `TARGETS` half is the load-bearing half.** CLAUDE.md's own text calls
   `TARGETS` "the maintained version of this list", and
   `tests/test_mutation_probe_targets.py` enforces it in both directions. The
   table in CLAUDE.md was always the mirror, and it was the mirror that drifted
   in #106.
2. **The repo has already ruled on this class.** `c925829` is the decision that
   CLAUDE.md is not a tracked artifact and that no test reads it; the standing
   note is that facts a test needs live in Python. Re-opening that for one row
   would be re-litigating a decision two commits old.
3. **An untracked edit inside a PR is invisible work.** A developer told to edit
   `/Users/kevin/Developer/Isocenter/CLAUDE.md` from a worktree would be writing
   into another checkout, outside the diff, unreviewable, and lost on the next
   clone. That is worse than not doing it.

**What should happen to the issue text** (for the owner to apply; the architect
does not edit issues):

- **Title:** `Give persistence.py its own row in scripts/mutation_probe.py's TARGETS`
- **Body preamble to add at the top:**

  > **Scope narrowed 2026-09-09 (bunch C).** `c925829` untracked `CLAUDE.md` and
  > deleted the five tests that read it, so the "CLAUDE.md coverage table" half
  > of this issue no longer has a home in version control. `TARGETS` is the
  > maintained list and the one `tests/test_mutation_probe_targets.py` enforces;
  > this issue is now that row and nothing else.

- **Body correction to the "To do" paragraph:** the file list is measured, not
  guessed -- see §8.1. It is **42** test files, not the eight the issue names,
  because `_importers` demands every file whose *text* matches
  `isocenter\.persistence\b`. And the issue's "move the four bunch-1 files out
  of the `io_handlers.py` row if they do not import it" should be struck; §8.3
  says why.

Keeping the local CLAUDE.md's table in step is a real but separate courtesy,
and it is a one-line edit the main session can make outside this PR. It is not
in the developer's scope and must not appear in the branch.

---

## 3. Architect decision 2: bunch membership

**Ruling: the five-issue cut stands. Nothing moves in, nothing moves out.**

The theme is *pins that pin nothing*, and all five sit on it:

| Issue | The thing that claims to check | What it actually checks |
| --- | --- | --- |
| #396 | the frozen output vocabulary | that the word appears anywhere under `isocenter/`, docstrings included |
| #397 | the frozen wfdb export options | nothing; `patient_ids` is unreferenced by any test |
| #401 | the stability page's signature table | the *last* row per method, so a false row above the true one is invisible |
| #394 | `scan_pixel_content()`, a frozen tier-1 method | its `def` line |
| #383 | the probe's view of the sidecar gate | `io_handlers.py`, which does not contain it |

Two of them (#396, #397) are literally about the correct-by-accident class, one
(#401) is a parser that prefers the wrong row, and #383 is the tool that
measures kill signal being blind to a module. A developer holding "does this
pin have a kill signal?" can do all five sequentially, and the PR body is one
paragraph.

**#406 and #407 do not belong here.** #406 (a dtype-only `set_pixel_data()`
swallowed by frame dedup) and #407 (`imagecodecs_handler` joining the Basic
Offset Table into the codestream) are behavioural export-fidelity defects on
pixel-carriage and codec machinery. They share a mental model with each other
and with bunch A's #404/#386, and nothing with this bunch. Adding either would
turn the PR body into two paragraphs joined by "and also". **Recommend a bunch D
of #406 + #407**, which is a real theme of its own -- what the exported file
carries versus what the caller set -- and note that #407's last section says
#404's `(2, True)` refusal is *contingent on the bug*, so whoever takes it must
read both. #399 (`embed_identity_token` appending an item per call) is a
reversibility-semantics call with two defensible fixes; it stays where it was
planned, alone or ahead of D.

**#409 fits this theme but should not move.** A committed notebook output
quoting a line the library no longer emits is exactly "a claim nothing checks",
and its own "worth checking at the same time" section proposes the cheap guard
(grep saved outputs for strings the package no longer contains) that would live
happily next to this bunch's work. Two reasons to leave it: C is already five
issues, and #409 needs an owner call between re-executing the notebook and
stripping its outputs -- Q3 in §10. Its blocker is gone, though: #384 landed in
bunch B at `861e1d1`, so the banner has already changed and option 1 would not
need doing twice.

---

## 4. What "frozen" obliges, precisely

#397 and #401 both touch `docs/api/stability.md`, which
`tests/test_frozen_surface.py` treats as an artifact under pin rather than as
prose. Three distinctions the developer must hold, because two of the fixes
depend on them:

1. **Frozen is about spelling and shape, not about the page.** The promise is
   that `patient_ids` and `include_annotation_text` keep their names through
   1.x. The page is where the promise is *published*; the test is what makes it
   true. Fixing #397 does not change any promise -- it makes an existing one
   checkable.
2. **The page is data to T-F4, and T-F4 is where the page's own consistency is
   enforced.** #401 is a defect in the *reader*, not in the page. The page is
   correct today; the parser could not tell you if it stopped being.
3. **Adding a word to a frozen list is a new promise and is the owner's call.**
   §5.5 finds five user-visible strings the freeze does not name. This PR must
   not add them. It records them (§10 Q1).

Nothing in this bunch may change a frozen spelling, a parameter name, a default,
or a return shape. Two of the fixes edit `docs/api/stability.md`, and both edits
are re-*categorisations* of words already on the page plus a sentence naming a
test -- no word is added to a freeze and none is removed.

---

## 5. #396 -- the Q9 vocabulary pin

### 5.1 The defect, measured -- and the issue's evidence has drifted

**The core claim is true and was reproduced.** Applied in the worktree:

```
isocenter/io_handlers.py:1895   action_type="WARNING",   ->   action_type="WARN",
```

`pytest -v tests/test_frozen_surface.py` -> **`6 passed in 0.53s`**. The test
asks only that `"WARNING"` appear as a quoted literal somewhere under
`isocenter/`, and it still does -- `logger.py:33`'s level map, a SQL string in
`persistence.py:1449`, a comment in `io_handlers.py:44`, a comment in
`reporting.py:118`.

**Two things in the issue's evidence are no longer accurate at `861e1d1`.**

The issue names the site as `io_handlers.py:1826`; it is **1895**. And its list
of "the eight single-hit words" is wrong in both directions. Measured, the words
with exactly one quoted-literal occurrence under `isocenter/` -- and therefore
the ones the test as written does pin -- are:

```
RECONCILE_PRIVATE  REVERSIBLE_EXPORT  RISK  STANDARD  PRIVATE  SIGNAL  PASS  REVIEW_REQUIRED
```

The issue lists `SCAN_GAP`, `SHIFT_DATE`, `REMOVE_TAG`, `REPLACE_TAG` and
`COMPLIANCE_CHECK` as single-hit; they have 2, 6, 4, 8 and 2 occurrences. It
lists `RISK` and `PASS` as multi-hit; they have one each. The *shape* of the
complaint is unaffected -- ten of the eighteen words are unpinned by this test
-- but a developer working from the issue's list would pin the wrong five.

**The frozen words are not one vocabulary, they are three, and the page says
otherwise.** `probe_396_write_sites_by_callee.py` collects every `action_type=`
string constant under `isocenter/` and groups it by callee:

```
PhiRemediation: 5 sites, words=['REMOVE_TAG', 'REPLACE_TAG', 'SHIFT_DATE']
log_audit:     20 sites, words=['DATA_LOSS', 'ERROR', 'EXPORT', 'RECONCILE_PRIVATE',
                                'REDACTION', 'REVERSIBLE_EXPORT', 'RISK', 'SCAN_GAP', 'WARNING']
```

`docs/api/stability.md` calls all thirteen "the audit `action_type` strings".
Four of the thirteen are not:

- `REMOVE_TAG`, `REPLACE_TAG`, `SHIFT_DATE` are `PhiRemediation.action_type`
  values -- what a *proposal* says it will do. They reach a user through
  `PhiFinding.remediation_proposal`, which is frozen. They are never an audit
  row: `remediation.py` writes `REMEDIATION_REPLACE`,
  `REMEDIATION_SHIFT_DATE` and `REMEDIATION_REMOVE` when it acts on them.
- `COMPLIANCE_CHECK` is never written to the audit table at all. It is
  synthesised at report time into the `exceptions` list, at `session.py:2293`
  (BurnedInAnnotation) and `:2306` (pixel-geometry damage).

So the page's own category is wrong for four of thirteen words, which is the
same defect one level up: the page tells a reader where to look for a word, and
the place it names does not contain it.

### 5.2 What the current kill signal actually is

`probe_396_vocabulary_in_tests.py` counts, per word, the literal occurrences in
every test file *except* `test_frozen_surface.py`. Two results matter:

- `STANDARD` and `PRIVATE`: **zero**. Their only spelling anywhere in the repo
  is `io_handlers.py:783-784`, so `test_the_output_vocabularies_are_still_spelled_by_the_package`
  is genuinely their only pin -- and it works, because they are single-hit. That
  is luck, not design: one docstring mentioning `"STANDARD"` retires the pin
  silently.
- `WARNING` at the mutated site is killed elsewhere --
  `tests/test_reingest_after_redact.py:317` selects audit rows with
  `e[1] == "WARNING"`. So the word is not unpinned in the suite; the *freeze
  test* is what cannot see it. The fix's value is that the freeze becomes the
  pin rather than depending on whichever behavioural test happens to spell the
  word.

### 5.3 The probe: five collectors, five equalities

Replace `test_the_output_vocabularies_are_still_spelled_by_the_package` in
`tests/test_frozen_surface.py`. Do not add a second test that keeps the old
grep alongside the new pin -- a grep that passes on a docstring is not a
weaker check, it is a check that reports a different fact under the same name.

Each collector is a small AST walk over `(REPO / "isocenter").rglob("*.py")`,
narrow enough that its result is a short list a reader can verify by eye, and
each is compared for **set equality** against a literal in the test file.

**Pin A -- the audit vocabulary.** String constants reaching a call whose
callee name is `log_audit`, **both** ways it is called: as the `action_type`
keyword (20 sites), and as positional argument 0 (`persistence_manager.py:869`
writes `log_audit("ERROR", "SESSION", message)`). Collecting only the keyword
form leaves that one site respellable with Pin A green -- the M2 shape, at the
one place the collector would not be looking. Expected exactly:

```python
FROZEN_AUDIT_ACTION_TYPES = {
    "DATA_LOSS", "ERROR", "EXPORT", "RECONCILE_PRIVATE", "REDACTION",
    "REVERSIBLE_EXPORT", "RISK", "SCAN_GAP", "WARNING"}
```

`remediation.py` also writes audit rows, through a local variable and a module
constant rather than a literal argument, so its four words are invisible to
this collector by construction. They are unfrozen (§10 Q1); if Q1 rules them in,
the collector grows a fourth rule and not a fourth pin.

**Pin B -- the proposal vocabulary.** String constants passed as `action_type`
to a call whose callee name is `PhiRemediation`. Expected exactly
`{"REMOVE_TAG", "REPLACE_TAG", "SHIFT_DATE"}`.

**Pin C -- the loss scopes.** Module-level assignments in
`isocenter/io_handlers.py` whose target name starts `LOSS_SCOPE_` and whose
value is a string constant. Assert the **values** are exactly
`set(FROZEN_LOSS_SCOPES)`, and assert nothing about the names.

Renaming `LOSS_SCOPE_STANDARD` is a green mutation and should be. The
io\_handlers module is tier 3 wholesale on `docs/api/stability.md`; what is
frozen is the three strings a user reads, not the identifiers the package
spells them with, and a freeze test that reddened on a private rename would be
doing the opposite of what §4 says this file is for. The collector's dependence
on the `LOSS_SCOPE_` prefix is a red-and-update of the
`tests/test_source_citations.py` kind -- rename the prefix and this pin goes red
until someone updates it deliberately, which is the cheap price of not asserting
the names.

**Pin D -- the grade.** String constants anywhere inside the value of a
`validation_status=` keyword argument in `isocenter/session.py` (it is an
`IfExp`, so walk the subtree). Expected exactly `set(FROZEN_GRADES)`.

**Pin E -- the report exception categories.** String constants that are
**direct `ast.Tuple.elts` members** of a tuple passed to `exceptions.append(...)`
in `isocenter/session.py`. Expected exactly `{"COMPLIANCE_CHECK", "AUDIT_DROP"}`.

`ast.walk` is wrong here and would be the over-broad collector §5.3 warns
about: each of the three tuples at `session.py:2291/2304/2318` carries an
f-string whose `JoinedStr` parts are `Constant` nodes -- `" - "` among them --
so a walk collects punctuation and the expected set becomes soup nobody can
read. Take `elts` directly, and skip anything that is not `ast.Constant` of
`str`.
`AUDIT_DROP` is in the expected set because it *is* written there; it is not
thereby frozen, and the test carries a comment saying so and naming the issue
from §10 Q1.

**The collectors must read files by path, never by import.** Write
`(REPO / "isocenter").rglob("*.py")` and `ast.parse(p.read_text(...))`. This is
not a style preference: `tests/test_mutation_probe_targets._importers` matches
the *text* `isocenter\.io_handlers\b` anywhere in a test file, comments
included. One comment reading "collected from isocenter.io_handlers" drags
`test_frozen_surface.py` into that module's `TARGETS` row, re-runs it against
every `io_handlers.py` mutant for zero kill signal, and falsifies the file's own
docstring, which says it imports no probe target's module. The same trap fires
for `isocenter.persistence` the moment §8 lands. Spell module names in prose
("the io\_handlers module") or not at all.

### 5.4 The mutations that must kill it

The reviewer runs these in the worktree, one at a time, `git checkout` between:

| # | Edit | Test that must go red |
| --- | --- | --- |
| M1 | `isocenter/io_handlers.py:1895` `action_type="WARNING"` -> `"WARN"` | Pin A |
| M2 | `isocenter/io_handlers.py:2006` `action_type="DATA_LOSS"` -> `"DATALOSS"` (one of six sites) | Pin A |
| M3 | `isocenter/privacy.py:541` `action_type="SHIFT_DATE"` -> `"SHIFTDATE"` | Pin B |
| M4 | `isocenter/io_handlers.py:784` `LOSS_SCOPE_STANDARD = "STANDARD"` -> `"STD"` | Pin C |
| M5 | `isocenter/persistence_manager.py:869` `log_audit("ERROR", ...)` -> `log_audit("ERR", ...)` | Pin A (positional half) |
| M6 | `isocenter/session.py:2468` `"REVIEW_REQUIRED"` -> `"REVIEW"` | Pin D |
| M7 | `isocenter/session.py:2306` `"COMPLIANCE_CHECK"` -> `"COMPLIANCE"` (one of two sites) | Pin E |

M2, M5 and M7 are the ones that matter: each is a single-site respelling of a
word other sites still spell, and each is exactly what the current test cannot
see. Set equality catches them because the mutant word enters the collected set
even though the original stays in it. M5 is also the check that Pin A's
positional half was written and not skipped as an edge case.

There is no mutation for renaming a `LOSS_SCOPE_*` constant, deliberately: see
Pin C.

### 5.5 The boundary the reviewer must not file against this issue

**Set equality cannot see a write site deleted** when another site spells the
same word. Delete `action_type="DATA_LOSS"` at `io_handlers.py:2006` outright
and Pin A stays green, because the word survives at five other sites. That is
deliberate and it is written into the test's docstring:

> A set, not a census. A census (`word -> number of sites`) would also catch a
> deleted site, and would go red on every honest refactor that adds or merges
> one. Deleting a write site is a *behavioural* change -- an audit row that
> stops being written -- and belongs to the test that asserts the row exists,
> not to a pin on how the word is spelled. The probe's `visit_Expr` operator
> generates exactly that mutant; its kill belongs to `tests/test_data_loss_reporting.py`.

Also out of scope: adding any of the five unfrozen user-visible words to the
freeze (§10 Q1), and the stale `{'ANONYMIZE_METADATA': 1200, 'REDACT_PIXELS': 50}`
example in `reporting.py:114`, which names two action types the package has
never written. Worth a one-line fix if the developer is already in the file;
worth nothing if it means opening it.

### 5.6 The page edit

Rewrite the **Output vocabularies** paragraph of `docs/api/stability.md` to name
three vocabularies instead of one, moving no word off the freeze:

- the grade (`PASS`, `REVIEW_REQUIRED`; there is no `FAIL`);
- the audit `action_type` strings written by `log_audit` (the nine of Pin A);
- the remediation-proposal `action_type` strings carried on
  `PhiFinding.remediation_proposal` (the three of Pin B);
- the report exception category `COMPLIANCE_CHECK`;
- the `loss_scope` strings (`STANDARD`, `PRIVATE`, `SIGNAL`).

Keep the existing sentences that an existing string is never renamed or removed
in 1.x, that new strings may be added with a CHANGELOG entry, and that
`get_audit_losses()` is tier 2. Add one sentence naming
`tests/test_frozen_surface.py` as what makes each of the five checkable, since
the page already names it for the method table and the asymmetry was part of
what let this drift.

**What happens to `FROZEN_ACTION_TYPES`.** The existing thirteen-word list is
also read by T-F4's loop that asserts every vocabulary word is named on the
page. **Delete it** and replace that loop's input with
`FROZEN_AUDIT_ACTION_TYPES | FROZEN_PROPOSAL_ACTION_TYPES |
FROZEN_REPORT_EXCEPTIONS_FROZEN` (the last being `{"COMPLIANCE_CHECK"}`, not
Pin E's expected set, which includes the unfrozen `AUDIT_DROP`) plus
`FROZEN_LOSS_SCOPES` and `FROZEN_GRADES`. Two lists claiming the same thing is
how this page and this test got out of step in the first place; one name per
vocabulary, and T-F4 reads the union.

---

## 6. #394 -- nothing dispatches `scan_pixel_content()`'s worker

### 6.1 The defect, measured -- and three of the issue's premises are wrong

**The gap is real.** `grep -rn "scan_pixel_content\|_verify_worker" tests/*.py`
finds the names only in `test_frozen_surface.py`'s parameter table and in two
docstrings. No test calls the method; no test calls the worker.

**Three claims in the issue are not true of the code at `861e1d1`,** and a
developer who tests for them will write assertions with nothing behind them:

1. *"the lightweight-copy construction"* -- `scan_pixel_content` does not build
   one. `_make_lightweight_copy` is called only from `audit()`
   (`session.py:1878`). `scan_pixel_content` appends the **live** `Instance` to
   `worker_items` at `session.py:2012`.
2. *"`_rehydrate_findings` on the way back"* -- not called either. `audit()`
   calls it at `session.py:1888`; `scan_pixel_content` returns
   `PhiReport(all_findings)` untouched.
3. *"asserts the returned findings and the audit rows"* -- there are no audit
   rows. `scan_pixel_content` writes none. A test that flushes the audit queue
   and selects rows will find nothing, and `assert rows == []` is the
   `0 == 0` shape.

What is genuinely unexercised is: the method's four skip filters, the pickling
of `(Instance, Equipment, rules)` into a pool, `_verify_worker`'s body, and the
shape of what comes back.

**The issue's suggested test would assert `0 == 0`.** It proposes running under
`ISOCENTER_FORCE_PROCESSES=1`. Measured with
`probe_394_entity_identity_by_mode.py`, which patches
`isocenter.verification.analyze_pixels` to return one `TextRegion` outside the
configured zone:

```
RESULT[threads]   findings=1  uid=1.2.826.0.1.0 value='LEAKTEXT' entity_is_live=True
RESULT[processes] findings=0
```

`unittest.mock.patch` does not cross a process boundary, and neither local gate
interpreter has `pytesseract`, so the child's `analyze_pixels` returns `[]` at
`pixel_analysis.py:178` before it ever touches pixels. In CI, where the `ocr`
extra and the tesseract binary are both installed, the same test would run real
OCR over the fixture -- a different code path, a different reason to pass, and
one that a flat 16x16 frame makes `0` again by luck rather than by design.

**Therefore: no process-mode facade test.** The process boundary is covered
instead by an explicit pickle round-trip (T-394a), which is deterministic
everywhere. `probe_394_worker_crosses_a_pool.py` confirms the boundary itself is
sound, so there is no defect being papered over:

```
PICKLE OK: 1773 bytes
UNPICKLE OK; pixel data in copy: (16, 16)
SCAN OK (processes): 0 findings
```

and, with the array unloaded before pickling, the loader survives and rehydrates
in a fresh interpreter: `pickled bytes after unload: 1002` ->
`child-side get_pixel_data: ((16, 16), dtype('uint16'), 1000)`.

### 6.2 The probe: one new file, four tests

New file `tests/test_scan_pixel_content_dispatches_its_worker.py`. It imports
`isocenter.session`, `isocenter.entities` and `isocenter.pixel_analysis` --
none a probe target -- and reaches findings through the returned `PhiReport`
rather than importing `isocenter.privacy`, which is a target and would charge
this file to every `privacy.py` mutant for no kill signal.

An autouse fixture sets `ISOCENTER_MAX_WORKERS=3`, sets
`ISOCENTER_FORCE_THREADS=1`, and `delenv`s `ISOCENTER_FORCE_PROCESSES` and
`ISOCENTER_MAX_TASKS_PER_CHILD`. The threads lever is not decoration: 3.12's
default is processes, so without it the patch does not reach the worker and
every finding assertion silently becomes `0 == 0`. The fixture carries a comment
saying exactly that.

Every test patches `isocenter.verification.analyze_pixels` (measured working;
this is also what `tests/test_ocr_formal.py` patches). Zone space is
`(y1, y2, x1, x2)`; OCR box space is `(x, y, w, h)`; `_coverage` is the only
place that converts, and getting it backwards was #264.

**T-394a -- the worker, over the pickle the pool puts it through.**

```python
session.save(sync=True)
instance.unload_pixel_data()          # the pool's real case: the loader crosses
args = (instance, equipment, rules)
revived = pickle.loads(pickle.dumps(args))
with patch("isocenter.verification.analyze_pixels", return_value=[...]):
    findings = session_module._verify_worker(revived)
```

The `save(sync=True)` and the `unload_pixel_data()` are the substance of the
round-trip, not setup noise. A resident array pickles as bytes and proves only
that numpy pickles; unloaded, what crosses is the `SidecarPixelLoader`, which is
what a real pass sends. §6.1 measured the difference -- 1773 bytes resident
against 1002 unloaded, with `get_pixel_data()` on the far side returning the
right frame -- and the 1002-byte path is the one the pool takes. Assert
`revived[0].get_pixel_data()` equals the parent's array as part of this test, so
a loader that stopped surviving the pickle is red here rather than silently
scanning nothing in CI.

Two cases against one rule whose `redaction_zones` is `[[0, 100, 0, 100]]`:

- text at `(200, 200, 50, 50)` -- outside -- gives exactly one finding, whose
  `entity_uid` is the instance's SOP UID, whose `field_name` is
  `"PixelData[Frame=0]"`, and whose `value` is the region's text;
- text at `(10, 10, 50, 50)` -- covered -- gives **zero** findings.

Plus `_verify_worker((None, None, []))` returns `[]`, which is the only line of
the worker the two cases above miss.

The covered case is what makes `equipment` load-bearing. With only the
uncovered case, dropping `equipment` changes nothing -- coverage is `0.0`
either way -- and M9 below would survive.

**T-394b -- the facade dispatches, and its four filters.** Build one session
holding four series:

| series | equipment | rule | expected |
| --- | --- | --- | --- |
| S1 | serial `SN-A` | zones `[[0, 100, 0, 100]]` | **scanned** |
| S2 | no `device_serial_number` | -- | skipped |
| S3 | serial `SN-C` | no rule for `SN-C` | skipped |
| S4 | serial `SN-D` | rule present, `redaction_zones: []` | skipped |

With `analyze_pixels` patched to return one uncovered region for every
instance, assert `{f.entity_uid for f in report}` equals exactly S1's instance
UIDs. Set equality in both directions: a superset assertion is green when a
filter stops filtering.

**T-394c -- `serial_number=` narrows.** Two configured series, `SN-A` and
`SN-B`, both with zones. `scan_pixel_content(serial_number="SN-A")` returns
findings for S1's instances only; `scan_pixel_content()` returns both.
Asserting both calls is what stops the test being green on a filter that
rejects everything.

**T-394d -- the empty path.** A session whose only series has no equipment.
`scan_pixel_content()` returns an empty `PhiReport`, and `capsys` shows a line
naming the skipped count. Select the one line containing `"Skipped"` and assert
the count in it -- do not assert on the whole capture, which is the substring
shape this bunch exists to remove.

### 6.3 The mutations that must kill it

| # | Edit | Test that must go red |
| --- | --- | --- |
| M8 | `session.py:100` `if not instance:` -> `if instance:` | T-394a |
| M9 | `session.py:104` `verifier.verify_instance(instance, equipment)` -> `(instance, None)` | T-394a (covered case) |
| M10 | `session.py:1995` `if not matched_rule:` -> `if matched_rule:` | T-394b |
| M11 | `session.py:2001` delete the `if not matched_rule.get("redaction_zones")` arm | T-394b |
| M12 | `session.py:2008` `if serial_number and sn != serial_number` -> `sn == serial_number` | T-394c |
| M13 | `session.py:2021` `results = run_parallel(...)` -> `results = []` | T-394b, T-394c |
| M14 | `session.py:1981` `if not equip or not equip.device_serial_number` -> `if not equip` | T-394b |

M13 is the one the issue is really about: today it is a green mutation, and it
means "the pool is never dispatched" -- exactly the state the coverage run found.

Line numbers are `861e1d1`; the reviewer should re-locate by text if the
developer's own edits move them. Nothing in this bunch changes `session.py`, so
they should hold.

### 6.4 Scope boundaries

**Out: the child's pixel hydration.** With `HAS_OCR` false, `analyze_pixels`
returns before `instance.get_pixel_data()`, so no test that runs on both gate
interpreters can make a worker load pixels from the sidecar. The pickle
round-trip in T-394a plus the measurement in §6.1 is as close as this
environment gets, and the brief says so rather than dressing a smoke test as
one.

**Out, and to be filed: `PhiFinding.entity` means two different things
depending on an environment variable.** `verification.py:151` sets
`entity=instance` -- whichever object the worker was handed. In threads that is
the live graph object (**measured**: `entity_is_live=True`). In processes it is
an unpickled copy, so `finding.entity` is a dead object that mutating does
nothing to. `audit()` avoids exactly this: `scan_worker` strips
`f.entity = None` before returning (`session.py:84-85`) and
`_rehydrate_findings` resolves the path against the live graph on the far side,
and returning the enclosing instance rather than `None` was rejected in #57 for
the same class of reason. `scan_pixel_content` does neither.

The heavier half: under real OCR the child calls `get_pixel_data()`, so the
array is resident on the instance the finding points at, and **every finding
pickles the full pixel array back to the parent**. The measured sizes bracket
it -- 1773 bytes with the 16x16 array resident against 1002 with it unloaded --
and a real frame is megabytes, once per finding.

This is derived, not directly measured: the process arm cannot be observed here
because the patch does not cross the boundary and neither interpreter has OCR.
The filed issue should say that, and say that CI, which has the `ocr` extra, is
where it can be measured directly. `PhiFinding.entity` is tier-1 frozen, so the
fix is a design call (strip like `audit()` does, or carry an `entity_path` and
rehydrate) and not this PR's.

---

## 7. #397 and #401 -- the two stability-page pins

### 7.1 #397: the defect is `patient_ids`, not both options

**`include_annotation_text` is already pinned, behaviourally.** Applied in the
worktree:

```
isocenter/exporters/wfdb.py:327   options.get("include_annotation_text", False)
                              ->  options.get("include_annotation_txt", False)
```

`pytest -v tests/test_wfdb_privacy.py tests/test_murmur_annotations.py` ->
**`1 failed, 52 passed`**, the failure being
`test_annotation_text_is_present_when_opted_in_end_to_end`, which calls
`session.export(str(out), format="wfdb", include_annotation_text=True)` at
`tests/test_wfdb_privacy.py:655` and asserts the text reached the output.

**`patient_ids` is pinned by nothing, and the filter behind it is untested.**

```
isocenter/exporters/wfdb.py:322   options.get("patient_ids")  ->  options.get("patient_idz")
```

`pytest -v tests/test_wfdb_privacy.py tests/test_wfdb_conformance.py
tests/test_wfdb_writer.py tests/test_murmur_annotations.py
tests/test_frozen_surface.py` -> **`96 passed in 10.53s`**. No test in the repo
passes `patient_ids=` to the wfdb path at all -- every one of the 30
`format="wfdb"` call sites exports everything. So this is not only an unpinned
name: `patient_ids` on the wfdb path is a **subset filter no test exercises**,
and a filter that silently stopped filtering would export every patient's
waveforms to a caller who asked for one.

**The issue's suggested fix shape does not work.** It proposes "calling
`export(format='wfdb')` ... with an unknown one and asserting the `TypeError`".
`WfdbExporter.export(self, session, folder, **options)` reads its options with
`options.get(...)`, so an unknown option is silently ignored; there is no
`TypeError` and no signature to pin. That asymmetry with the dicom path is
itself a finding -- §10 Q2.

**The probe.** New tests in `tests/test_wfdb_privacy.py` (it already owns the
export-option behaviour, and adding a file would add a `TARGETS` question this
bunch does not need):

`test_the_wfdb_patient_ids_option_limits_the_export` -- two patients, each with
one waveform-bearing instance;
`session.export(folder, format="wfdb", patient_ids=[first])`; assert the set of
`.hea` record names written is exactly the first patient's, **and** that the
second patient's record name is absent. Both directions, because a filter that
rejects everything satisfies the first alone.

`test_the_wfdb_export_options_are_the_two_the_page_freezes` -- collect, by AST
from `isocenter/exporters/wfdb.py`, the string constants passed as argument 0 to
an `options.get(...)` call inside `WfdbExporter.export`, and assert the set
**equals** `{"patient_ids", "include_annotation_text"}`. Set equality, not
membership: the page says the wfdb options *are* these two, so a third read
without being frozen should be red for the same reason T-F1 reddens on a new
public `Session` method. Read by AST rather than by grepping the source text --
the method's own docstring names both options, which is exactly the accident
#396 is about. This is the cheap half; the behavioural test above is the one
with teeth.

**The page edit.** In `docs/api/stability.md`'s export-options paragraph, after
"Those option names are frozen with the method", name what pins each:
`tests/test_frozen_surface.py` for the dicom options through `_export_dicom`'s
signature, and `tests/test_wfdb_privacy.py` for the two wfdb options. One
sentence.

**The mutations that must kill it.**

| # | Edit | Test that must go red |
| --- | --- | --- |
| M15 | `wfdb.py:322` `options.get("patient_ids")` -> `"patient_idz"` | `test_the_wfdb_patient_ids_option_limits_the_export` (and the AST pin) |
| M16 | `wfdb.py:339` `if patient_ids and patient.patient_id not in patient_ids` -> `in patient_ids` | `test_the_wfdb_patient_ids_option_limits_the_export` |
| M17 | `wfdb.py:327` `options.get("include_annotation_text", ...)` -> `"include_annotation_txt"` | the existing `test_annotation_text_is_present_when_opted_in_end_to_end`, plus the AST pin |

M16 is the reason the behavioural test is not redundant with the AST pin: the
name can be right and the filter inverted.

### 7.2 #401: the parser keeps the last duplicate

**Confirmed at `861e1d1`.** A second `| \`save\` | \`sync=True\` |` row inserted
**above** the real one in `docs/api/stability.md` gives
`pytest -v tests/test_frozen_surface.py` -> **`6 passed in 0.55s`**. And
`test_frozen_surface.py` is the only test that parses that table --
`grep -rln stability tests/*.py` returns five files, and the other four mention
the page in prose.

**The fix has a correct-by-accident trap of its own, and it is the obvious
one.** The natural one-liner --
`assert len(rows) == len(FROZEN_SESSION_METHODS)` -- is **green with the
duplicate present**, because `dict()` has already collapsed the two rows into
one before it is counted. The assertion has to be on the `re.findall` **pairs
list**, before the dict is built.

**The probe.** The check must live *inside* a helper T-F4 calls, not inline in
T-F4. Inline, the only thing that can exercise it is T-F4 itself against the
real page, a unit test would have to carry its own copy of the assertion, and
reverting T-F4 to `dict(re.findall(...))` would leave that copy green -- a
duplicate-detection test that cannot detect the duplicate detection being
removed. One helper, three steps, in `tests/test_frozen_surface.py`:

```python
def _signature_rows(page: str) -> dict:
    """`docs/api/stability.md`'s Session table as `name -> params`.

    The duplicate check is here and not in the caller on purpose. `dict()`
    keeps the *last* match for a repeated key, so a false row placed above
    the true one leaves the page carrying a wrong signature with the pin
    green (#401, measured: `6 passed`). Asserting on the pairs before the
    dict exists is the only place the second row is still visible.
    """
    pairs = re.findall(r"^\| `(\w+)` \| (?:`([^`]*)`|—) \|$", page, re.MULTILINE)
    names = [name for name, _ in pairs]
    duplicated = sorted({n for n in names if names.count(n) > 1})
    assert not duplicated, (
        f"stability.md's Session table repeats {duplicated}; `dict()` keeps "
        f"the last, so a false row above the true one would be invisible")
    return dict(pairs)
```

T-F4 becomes `rows = _signature_rows(page)` followed by its existing
`assert rows == FROZEN_SESSION_METHODS`, unchanged otherwise.

Plus a durable unit test,
`test_a_duplicate_signature_row_is_not_silently_collapsed`, which builds a
two-line synthetic page holding two `save` rows with different parameters and
asserts `pytest.raises(AssertionError)` from `_signature_rows`. It must also
assert `_signature_rows` returns the right dict for a *clean* synthetic page,
so a helper that raised unconditionally would not satisfy it.

**The mutations that must kill it.** #401's fix is entirely in test code, and
the reviewer's edits are correspondingly not production edits. Say so plainly
rather than reporting "no killing mutation exists":

| # | Edit | Test that must go red |
| --- | --- | --- |
| M18 | insert a second `| \`save\` | \`sync=True\` |` row **above** the real one in `docs/api/stability.md` | T-F4 |
| M19 | in `tests/test_frozen_surface.py`, delete the duplicate assertion from `_signature_rows` so it is `return dict(re.findall(...))` | `test_a_duplicate_signature_row_is_not_silently_collapsed` **and** M18 |

M19 is the one to run carefully. It must kill the unit test *and* re-open M18 --
if it kills only the unit test, the check is not on the path T-F4 uses and the
fix is decorative.

**Out of scope, both named in the issue.** Row *order* stays unpinned: the
comparison is a dict and the page claims no order, so two swapped rows are
green and correctly so. And `_spell()` dropping `self` keeps a `def f(self, /,
...)` positional-only marker on the receiver invisible; nothing a caller can do
changes with it. Neither becomes a test here.

---

## 8. #383 -- `persistence.py` has no `TARGETS` row

### 8.1 The measurement

`probe_383_persistence_importers_and_sites.py`, at `861e1d1`:

```
persistence.py importers: 42
isocenter/persistence.py sites: 453
isocenter/io_handlers.py  sites: 519
isocenter/session.py      sites: 563
isocenter/privacy.py      sites:  71
isocenter/remediation.py  sites:  93
isocenter/parallel.py     sites:  73
```

The 42 are what `tests/test_mutation_probe_targets._importers` will demand the
moment the row exists -- every test file whose *text* matches
`isocenter\.persistence\b`. (The `\b` does not match before `_`, so
`isocenter.persistence_manager` is correctly not swept in.) The developer must
not hand-curate the list: run the probe and paste its output, sorted.

The issue names eight files (`test_persistence.py`, `test_save_redact_race.py`,
`test_memory_store_unlinks_its_temp_files.py`, `test_save_all_contract.py` and
the four bunch-1 files). All eight are in the 42; the other 34 are equally
demanded, and a row missing one is a red
`test_every_test_that_imports_a_target_module_is_listed`.

### 8.2 The row

```python
"isocenter/persistence.py": ([...the 42, sorted...], 30),
```

**Budget 30**, matching `io_handlers.py`. 453 sites at budget 30 is a stride of
15; `io_handlers.py`'s 519 at 30 is a stride of 17. Choosing the same number is
the point: the two largest modules should be sampled at comparable density, and
a different number here would need a reason this bunch does not have.

**Measured cost, so nobody is surprised.** Fourteen of the 42 files --
`test_async_persistence`, `test_audit_drop_accounting`, `test_blob_storage`,
`test_bytes_persistence`, `test_close_does_not_drop_an_orphaned_save`,
`test_compact_refuses_during_a_pass`, `test_compaction_races_a_concurrent_write`,
`test_compaction_reclaims_a_row_instances_does_not_carry`,
`test_concurrency_stress`, `test_persistence`, `test_persistence_concurrency`,
`test_save_redact_race`, `test_sidecar_gate_crosses_processes`,
`test_sidecar_gate_order` -- run in **19.26 s** (77 tests, 3.12.14). All 42 will
be well over a minute, so the persistence target alone is roughly 30-45 minutes
of a probe run at budget 30. That is the same order as `io_handlers.py` and it
is a by-hand tool, not CI, so it is affordable -- but it should be written in
the `TARGETS` comment, because an unexplained doubling of the run time is the
kind of thing someone later "fixes" by cutting the budget.

The comment above the row should say what the row buys, in the issue's own
terms: `persistence.py` carries `_hold_sidecar_gate`, `_hold_pass_lock`,
`_refuse_while_pass_open`, `_flock_within`, `_SIDECAR_GATE_TIMEOUT_S`, the
`:memory:` temp-file ownership flag and `compact_sidecar()`, and until this row
existed no mutant of any of them was ever generated.

### 8.3 What the issue asks for that should **not** be done

The issue says to "move the four bunch-1 files out of the `io_handlers.py` row
if they do not import it". Measured, the four (`test_compact_refuses_during_a_pass`,
`test_compaction_reclaims_a_row_instances_does_not_carry`,
`test_sidecar_gate_crosses_processes`, `test_sidecar_gate_order`) indeed do not
match `isocenter\.io_handlers\b` -- and neither do five others already in that
row (`test_empty_sequence_roundtrip`, `test_private_tag_arity_roundtrip`,
`test_private_tag_empty_value_roundtrip`, `test_redaction_export`,
`test_reversibility`).

**Leave all of them.** Import is a proxy for exercise, and the `TARGETS`
docstring says in its own words that the proxy is insufficient --
`test_remediation_actions.py` exercises `remediation.py` through `PhiInspector`
without importing it, "and no import scan can see that". The four call
`ingest()`, `redact()` and `compact()` through the facade, which is
`io_handlers.py` code on every one of those paths. Extras cost the guard
nothing; they cost run time, and whether they buy kill signal is a question a
probe run answers and a grep does not. Removing them without that run would be
trading a measured cost for an unmeasured risk, which is the trade this bunch
exists to stop making.

### 8.4 The probe and the mutation

There is no new test file. The pin already exists, and #383 is the data it
reads:

- **Probe:** add the row with an **empty** test list --
  `"isocenter/persistence.py": ([], 30)` -- and run
  `pytest -v tests/test_mutation_probe_targets.py`.
  `test_every_test_that_imports_a_target_module_is_listed` must fail and name
  all 42 files. That is the red step; then paste them in and watch it go green.
- **M20 (the reviewer's mutation):** delete any single file from the new list.
  `test_every_test_that_imports_a_target_module_is_listed` must go red naming
  it. Delete the whole row and it goes green -- which is the current state, and
  is why the row is the fix.
- **M21:** rename any listed file's entry to a non-existent path.
  `test_every_listed_test_file_exists` must go red.

Run `python -m scripts.mutation_probe 1` once after the row lands, purely to
confirm the new target is generated and the run completes; the survivors it
reports at budget 1 are not a verdict and should not go in the PR body.

---

## 9. Change list, file by file

| File | Change | Issue |
| --- | --- | --- |
| `tests/test_frozen_surface.py` | replace the Q9 grep with Pins A-E; extract `_parse_signature_rows`; add the duplicate-row assertion and its unit test | #396, #401 |
| `docs/api/stability.md` | Output vocabularies split into three vocabularies plus the grade and the loss scopes; one sentence naming the pinning tests for the wfdb options | #396, #397 |
| `tests/test_wfdb_privacy.py` | `test_the_wfdb_patient_ids_option_limits_the_export`, `test_the_wfdb_export_options_are_the_two_the_page_freezes` | #397 |
| `tests/test_scan_pixel_content_dispatches_its_worker.py` | new; T-394a-d | #394 |
| `scripts/mutation_probe.py` | the `isocenter/persistence.py` row, 42 files, budget 30, with the cost comment | #383 |
| `CHANGELOG.md` | one entry, §11 | all |

**No production file changes in this bunch.** That is unusual and it is worth
saying out loud in the PR body: five issues, and the only non-test edits are two
paragraphs of `docs/api/stability.md` and one dict entry in a script. If the
developer finds themselves editing `isocenter/`, something has gone outside the
brief -- with one exception, `reporting.py:114`'s stale comment (§5.5), which is
a comment and optional.

---

## 10. Owner questions

**Q1. Five user-visible strings the freeze does not name.** The report renders
`get_audit_summary()` as a count per `action_type`, and the audit table receives
four words that `docs/api/stability.md` does not list:
`REMEDIATION_REPLACE`, `REMEDIATION_SHIFT_DATE`, `REMEDIATION_REMOVE`
(`remediation.py:195/206/254/297/319/326`) and `REMEDIATION_DECLINED`
(`remediation.py:34`, read back by `persistence.py:1557`). A fifth,
`AUDIT_DROP` (`session.py:2320`), is a report exception category alongside the
frozen `COMPLIANCE_CHECK`. The page says "new strings may be added with a
CHANGELOG entry", so none of this is a broken promise -- but a user reading a
report sees all five, and four of them name a remediation that happened.
**Should they join the freeze at 1.0?** Adding them is a new promise and is not
this PR's to make; Pin A and Pin E are written so that adding one later is a
one-line change plus a page row. Recommend filing, not deciding.

**Q2. `export(format="wfdb", patient_id=[...])` exports everyone.** The dicom
path raises `TypeError` on an unknown option, because `_export_dicom` has a real
signature; the wfdb path reads `options.get(...)` and ignores what it does not
recognise. A typo'd option name is therefore a silent full-cohort export on one
format and an immediate error on the other. Making them agree is a behaviour
change with a CHANGELOG entry naming the new exception, and it is squarely this
milestone's theme -- but it is not a pin, and folding it into a
five-issue-no-production-code PR would change what the PR is. **File as its own
issue?**

**Q3. #409's option 1 or option 2.** Its stated blocker is gone: #384 landed at
`861e1d1`, so re-executing `Redaction Example.ipynb` would no longer need doing
twice. Option 1 (re-execute, commit the real output) is friendlier on GitHub;
option 2 (strip outputs, `nbstripout`) is the one that cannot go stale again.
The issue explicitly does not decide. **Which?** -- and if option 2, whether the
grep guard it also proposes (saved outputs versus strings the package no longer
contains) should extend `tests/test_documented_output_matches.py`.

---

## 11. CHANGELOG

One entry, under a v0.9.5 heading, in the project's register: what was true
before, what is true now, and why the old state was wrong. It has no breaking
sub-entry, because nothing a caller can do changes. Shape:

> **Tests and internal tooling.** Five pins that could not go red. The frozen
> output vocabulary was checked by grepping `isocenter/` for the word as a
> quoted literal, which a docstring or a SQL string satisfies: respelling
> `action_type="WARNING"` at its only write site left the freeze test green.
> It is now anchored on the write sites by AST, in the three vocabularies the
> code actually keeps -- audit `action_type`, remediation-proposal
> `action_type`, and the report exception categories -- which
> `docs/api/stability.md` had conflated into one. The `wfdb` export option
> `patient_ids` was declared frozen and referenced by no test, and the subset
> filter behind it was never exercised. `docs/api/stability.md`'s signature
> table was parsed with `dict(re.findall(...))`, which keeps the last duplicate,
> so a false row above a true one was invisible. `Session.scan_pixel_content()`,
> a frozen tier-1 method, had never dispatched its worker in any test --
> `session._verify_worker` was at 14% line coverage, its `def` line. And
> `persistence.py`, which since #368 carries the sidecar gate, the pass-lock and
> the orphan predicate, had no row in `scripts/mutation_probe.py`'s `TARGETS`,
> so no mutant of any of them was ever generated. (#396, #397, #401, #394, #383)

---

## 12. What the reviewer should attack

1. **Pin A-E for the shape they are meant to remove.** Does each collector
   return a non-empty set for the right reason? Feed each one a file with the
   word in a docstring and confirm it is *not* collected. Confirm each expected
   set is a literal in the test file and not derived from the code it checks --
   a pin derived from the code is green on any code, which is the reason
   `FROZEN_SESSION_METHODS` is transcribed by hand.
2. **M2, M5 and M7 specifically.** Single-site respellings of a multi-site word
   are the whole point; if any survives, the fix did not land. M5 is the
   positional `log_audit` call, which a keyword-only collector misses.
3. **§5.5's boundary, and Pin C's.** Deleting a write site is green and is meant
   to be; so is renaming a `LOSS_SCOPE_*` constant, because the io\_handlers
   module is tier 3 and only the three strings are frozen. Do not file either
   against #396; check instead that the docstrings say so.
4. **Pin E's collector.** Confirm it takes `Tuple.elts` directly. Point it at
   `session.py:2291` with `ast.walk` instead and watch `" - "` arrive in the
   collected set -- if it does not, the collector is not reading the tuples it
   claims to.
5. **T-394b's set equality in both directions**, and that its fixture actually
   enters each of the four arms -- a filter test whose S4 has no instances tests
   nothing. Count the instances per series in the fixture.
6. **The threads lever in `tests/test_scan_pixel_content_dispatches_its_worker.py`.**
   Delete `monkeypatch.setenv("ISOCENTER_FORCE_THREADS", "1")` and re-run on
   3.12: every finding assertion should collapse to zero findings and the file
   should go red. If it stays green, the tests are not asserting what they read
   as asserting.
7. **#401's assertion position, and where it lives.** Move the duplicate check
   to *after* `dict(pairs)` and confirm it stops working -- that is the trap and
   it should be demonstrably a trap. Then run M19: deleting the check from
   `_signature_rows` must redden the unit test **and** re-open M18. A check that
   only the unit test can see is not on T-F4's path.
8. **`tests/test_mutation_probe_targets.py` on the new row**, and the
   `_importers` text-match trap: `grep -n "isocenter\.\(io_handlers\|persistence\|privacy\|remediation\|crypto\|parallel\)" tests/test_frozen_surface.py tests/test_scan_pixel_content_dispatches_its_worker.py`
   must return nothing. A comment is enough to trip it.
9. **Both gate interpreters.** Nothing here is concurrency-sensitive, but
   `tests/test_scan_pixel_content_dispatches_its_worker.py` sets a threads lever
   and 3.14t's default is threads already -- confirm the file passes on
   `.venv314t` and that its assertions are not accidentally satisfied there by
   the default rather than by the lever.

---

## 13. Sequencing

`#401 -> #396 -> #397 -> #394 -> #383`.

#401 first because it is the smallest and it touches the same file #396
rewrites, so doing it second would mean rebasing a helper extraction under a
larger edit. #396 next, and its page edit is what #397's page sentence lands
beside. #394 is independent and can be written at any point; it is fourth
because it is the largest and should not block the three page-and-freeze
changes. #383 last, because its `TARGETS` row must list every file matching
`isocenter\.persistence\b` *at merge time*, and the new test file from #394 must
be checked against that pattern before the row is written.

One PR, five `Fixes` lines.
