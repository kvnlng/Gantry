import pathlib
ROOT = pathlib.Path(__file__).resolve().parents[4]
words = ["DATA_LOSS", "ERROR", "EXPORT", "RECONCILE_PRIVATE", "REDACTION", "REMOVE_TAG",
         "REPLACE_TAG", "REVERSIBLE_EXPORT", "RISK", "SCAN_GAP", "SHIFT_DATE", "WARNING",
         "COMPLIANCE_CHECK", "STANDARD", "PRIVATE", "SIGNAL", "PASS", "REVIEW_REQUIRED"]
tests = [p for p in sorted((ROOT / "tests").glob("test_*.py"))
         if p.name != "test_frozen_surface.py"]
for w in words:
    hits = []
    for p in tests:
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if f'"{w}"' in line or f"'{w}'" in line:
                hits.append(f"{p.name}:{i}")
    print(f"{w:20s} {len(hits):3d}  {hits[:5]}")
