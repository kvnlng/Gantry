import pathlib, re, sys
ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "scripts"))
import mutation_probe
from mutation_probe import TARGETS, count_ops


def importers(module_path):
    name = pathlib.Path(module_path).stem
    pat = re.compile(rf"isocenter\.{re.escape(name)}\b")
    return {f"tests/{p.name}" for p in sorted((ROOT / "tests").glob("test_*.py"))
            if pat.search(p.read_text(encoding="utf-8"))}


imps = importers("isocenter/persistence.py")
print("persistence.py importers:", len(imps))
for i in sorted(imps):
    print("   ", i)
io = set(TARGETS["isocenter/io_handlers.py"][0])
print()
print("already under io_handlers:", sorted(imps & io))
ioimps = importers("isocenter/io_handlers.py")
print()
print("io_handlers row files that do NOT import isocenter.io_handlers:")
for f in sorted(io - ioimps):
    print("   ", f)
print()
for mod in ["isocenter/persistence.py", "isocenter/io_handlers.py", "isocenter/session.py",
            "isocenter/privacy.py", "isocenter/remediation.py", "isocenter/parallel.py"]:
    src = (ROOT / mod).read_text(encoding="utf-8")
    print(mod, "sites:", count_ops(src))
