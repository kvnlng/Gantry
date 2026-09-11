"""A log or print line that reports a caught exception names its type (#500).

#435 gave the package one spelling for an exception that becomes text --
`isocenter.logger.describe_exception`: `Type: message`, or `Type` alone
when the message is empty -- and swept the sites that turn an exception
into a *recorded* reason. #487 found one of the log-only lines that sweep
did not reach (`Failed to recover data from <uid>: ` under the wrong key),
and #500 is the rest of that class: a line that formats the exception
with `str()` renders `KeyError()`, `OSError()`, `StopIteration()`,
Fernet's `InvalidToken()` and every bare `raise` as nothing, so it ends
in a colon and says a step failed without saying how.

Four spellings call `str()`, and each is found here:

- `f"...: {e}"` and `f"...: {e!s}"`;
- `str(e)` anywhere inside a log or print argument;
- `e` passed to a `%s` placeholder (`logging` formats `%s` with `str()`).

`{e!r}` and a `%r` placeholder are not flagged: `repr()` already names the
type (`KeyError()`). `describe_exception(e)` is the spelling to use.

This is a source scan rather than a behavioural test per site because
the mutant is the same at every site -- the call unwrapped back to the
bare name -- and one scan kills it at all of them, including a site added
later. It reads the package's source and imports nothing from it, so no
row in `scripts/mutation_probe.py` demands it (#441's rule: a file joins
a row only with a measured kill, and the probe has no operator that
unwraps a call).

Only calls **inside the `except` block that binds the name** are
checked. That is where the exception is known to be one, and it is the
blind spot to know about, in two shapes:

- **A helper that receives the exception as a parameter.** Out of reach
  of a scan, and there is no such helper in the package today.
- **A handler that stores the exception and a line elsewhere formats
  what it stored.** `imagecodecs_handler.py`'s `except ImportError as e`
  assigns `IMPORT_ERROR = e` and returns; `is_available()`, a different
  function, printed `f"... Import Error: {IMPORT_ERROR}"`. That is the
  same defect as the 46 -- a bare `raise ImportError` in a broken
  install rendered as nothing after the colon -- and the scan
  structurally cannot see it, because the name it formats is a module
  global rather than a bound handler name. Found by hand in the review
  of #511 and converted there; no operator here catches the class, and a
  scan of every global that an `except` ever assigns would flag the
  `raise` sites this file deliberately passes.
"""
import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
PACKAGE = ROOT / "isocenter"

_LOG_METHODS = {"debug", "info", "warning", "warn", "error", "exception",
                "critical", "log"}

# A %-style placeholder, with its conversion character captured.
_PLACEHOLDER = re.compile(
    r"%(?:\([^)]*\))?[-#0 +]*(?:\*|\d+)?(?:\.(?:\*|\d+))?[hlL]?"
    r"([diouxXeEfFgGcrsa%])")

#: Sites that format a caught exception with `str()` on purpose, as
#: `{(relative path, enclosing function): reason}`. Keyed on the function
#: rather than the line number so an unrelated edit above does not
#: invalidate it; an entry that no longer matches anything is itself a
#: failure (the #333 convention), so this cannot quietly outlive its site.
#: One entry: 45 of the 46 sites #500 found were converted.
ALLOWED = {
    ("isocenter/utils/ctp_parser.py", "<module>"): (
        "the `__main__` block of a file that is run as a script path -- "
        "`python isocenter/utils/ctp_parser.py`, which is how "
        "tests/test_ctp_parser_extended.py drives it. There is no parent "
        "package there, so `from ..logger import describe_exception` at "
        "module scope raises ImportError before the block runs, and all "
        "three of those tests went red when it was added. A lazy absolute "
        "import inside the `except` would mask the parse error behind an "
        "ImportError on a checkout without isocenter installed. It is a "
        "terminal print in a one-shot converter, not a log line."),
}


def _is_log_call(node):
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr in _LOG_METHODS:
        return True
    return isinstance(func, ast.Name) and func.id == "print"


def _placeholders(fmt):
    return [m.group(1) for m in _PLACEHOLDER.finditer(fmt) if m.group(1) != "%"]


def _bare_uses(call, name):
    """How `call` formats the exception bound to `name` with `str()`, if at all."""
    found = []
    for arg in call.args:
        for sub in ast.walk(arg):
            if (isinstance(sub, ast.FormattedValue)
                    and isinstance(sub.value, ast.Name)
                    and sub.value.id == name
                    and sub.conversion in (-1, ord("s"))):
                found.append("f-string {%s}" % name)
            if (isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Name) and sub.func.id == "str"
                    and len(sub.args) == 1
                    and isinstance(sub.args[0], ast.Name)
                    and sub.args[0].id == name):
                found.append("str(%s)" % name)
    is_print = isinstance(call.func, ast.Name) and call.func.id == "print"
    if not is_print and call.args:
        fmt_node = call.args[0]
        specs = (_placeholders(fmt_node.value)
                 if isinstance(fmt_node, ast.Constant)
                 and isinstance(fmt_node.value, str) else None)
        for index, arg in enumerate(call.args[1:]):
            if isinstance(arg, ast.Name) and arg.id == name:
                spec = (specs[index] if specs is not None and index < len(specs)
                        else "s")
                if spec not in ("r", "a"):
                    found.append("%%%s <- %s" % (spec, name))
    return found


def _enclosing_functions(tree):
    """`{id(node): name of the innermost def enclosing it}`."""
    owner = {}

    def visit(node, current):
        for child in ast.iter_child_nodes(node):
            here = (child.name if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef)) else current)
            owner[id(child)] = here
            visit(child, here)

    visit(tree, "<module>")
    return owner


def scan_source(source, rel):
    """`[(rel, line, function, spelling)]` for every bare use in `source`."""
    tree = ast.parse(source)
    owner = _enclosing_functions(tree)
    hits = []
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler) or not handler.name:
            continue
        for stmt in handler.body:
            for node in ast.walk(stmt):
                if isinstance(node, ast.Call) and _is_log_call(node):
                    for spelling in _bare_uses(node, handler.name):
                        hits.append((rel, node.lineno,
                                     owner.get(id(handler), "<module>"),
                                     spelling))
    return hits


def _log_calls_in_handlers(source):
    tree = ast.parse(source)
    return sum(1 for handler in ast.walk(tree)
               if isinstance(handler, ast.ExceptHandler) and handler.name
               for stmt in handler.body for node in ast.walk(stmt)
               if isinstance(node, ast.Call) and _is_log_call(node))


def _package_sources():
    for path in sorted(PACKAGE.rglob("*.py")):
        yield path.relative_to(ROOT).as_posix(), path.read_text(encoding="utf-8")


def test_the_scan_finds_every_str_spelling_and_passes_the_others():
    """The detector itself: four spellings flagged, three passed.

    Pins the scan, so a regression in it cannot turn the package test
    below into a pass over nothing. Killing mutations in this file: the
    `conversion` test dropped (`{e!r}` flagged), the `str(e)` arm deleted,
    the `%r` exemption deleted, the `%s` arm deleted.
    """
    source = '''
import logging
log = logging.getLogger("x")
def f():
    try:
        pass
    except Exception as e:
        log.error(f"a: {e}")
        log.error(f"b: {e!s}")
        log.warning("c: " + str(e))
        log.warning("d %s: %s", "x", e)
        print(f"e: {e}")
        log.error(f"ok: {e!r}")
        log.error("ok %r", e)
        log.error(f"ok: {describe_exception(e)}")
'''
    hits = scan_source(source, "x.py")
    assert sorted((line, spelling) for _, line, _, spelling in hits) == [
        (8, "f-string {e}"),
        (9, "f-string {e}"),
        (10, "str(e)"),
        (11, "%s <- e"),
        (12, "f-string {e}"),
    ], hits
    assert {function for _, _, function, _ in hits} == {"f"}


def test_no_log_or_print_line_formats_a_caught_exception_with_str():
    """Every log and print line in `isocenter/` names the exception's type (#500).

    Red on 49e135a with 46 sites across 14 modules. 45 are converted to
    `describe_exception(...)`, by one token each, in this change, and one
    is in `ALLOWED` with its reason. Killing mutation: any one of those
    calls unwrapped back to the bare name.
    """
    hits = []
    for rel, source in _package_sources():
        hits.extend(scan_source(source, rel))
    offending = [hit for hit in hits if (hit[0], hit[2]) not in ALLOWED]
    assert not offending, (
        "these lines format a caught exception with str(), which renders a "
        "message-less one (KeyError(), OSError(), InvalidToken(), a bare "
        "raise) as nothing -- spell it describe_exception(e) (#435, #500):\n"
        + "\n".join(f"  {rel}:{line} in {function}(): {spelling}"
                    for rel, line, function, spelling in offending))


def test_every_allowed_site_still_exists_and_still_formats_with_str():
    """An allow-list entry that matches nothing is stale, and stale is red."""
    live = set()
    for rel, source in _package_sources():
        live.update((hit[0], hit[2]) for hit in scan_source(source, rel))
    stale = sorted(set(ALLOWED) - live)
    assert not stale, (
        f"ALLOWED names sites that no longer format with str(): {stale}; "
        "delete the entries")
    assert all(isinstance(reason, str) and reason.strip()
               for reason in ALLOWED.values()), "every entry needs a reason"


def test_the_scan_is_not_vacuous():
    """The package has log calls inside `except` blocks for the scan to read.

    If the walk stopped finding any -- a moved package, a parser change --
    the test above would pass over nothing. Measured on 49e135a: 54 such
    calls, a count the #500 conversion does not change (it wraps an
    argument, it adds or removes no call).
    """
    total = sum(_log_calls_in_handlers(source)
                for _, source in _package_sources())
    assert total >= 40, f"only {total} log calls inside except blocks found"
