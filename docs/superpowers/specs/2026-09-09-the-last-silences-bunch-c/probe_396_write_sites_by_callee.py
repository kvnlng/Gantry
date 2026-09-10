import ast, pathlib, collections
ROOT = pathlib.Path(__file__).resolve().parents[4]
by = collections.defaultdict(list)
for p in sorted((ROOT / "isocenter").rglob("*.py")):
    tree = ast.parse(p.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        callee = fn.attr if isinstance(fn, ast.Attribute) else (fn.id if isinstance(fn, ast.Name) else "?")
        for k in node.keywords:
            if k.arg == "action_type" and isinstance(k.value, ast.Constant):
                by[callee].append((k.value.value, f"{p.relative_to(ROOT)}:{k.value.lineno}"))
for callee in sorted(by):
    words = sorted({w for w, _ in by[callee]})
    print(f"{callee}: {len(by[callee])} sites, words={words}")
    for w, s in sorted(by[callee]):
        print(f"    {w:20s} {s}")
