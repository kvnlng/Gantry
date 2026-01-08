---
description: How to release a new version of Gantry
---

# Release Process

This workflow documents the steps required to release a new version of Gantry to PyPI.

## 1. Verification

Ensure all tests pass and the codebase is stable.

```bash
# Run full test suite
// turbo
pytest
```

## 2. Prepare Documentation

1. **Update CHANGELOG.md**:
    * Move all items from `[Unreleased]` to a new section `[x.y.z] - YYYY-MM-DD`.
    * Ensure all major changes are documented.
2. **Update README.md** (if applicable):
    * Update feature lists or installation instructions if they have changed.

## 3. Bump Version

Update the version string in `setup.py`:

```python
# setup.py
setup(
    ...
    version="x.y.z",
    ...
)
```

## 4. Build Distribution

Clean previous builds and create new source/wheel distributions.

```bash
rm -rf dist/ build/ *.egg-info
python3 setup.py sdist bdist_wheel
```

## 5. Publish

Upload the package to PyPI using Twine.

```bash
twine upload dist/*
```

## 6. Git Tag

Tag the release in version control.

```bash
git tag -a v0.5.2 -m "Release v0.5.2"
git push origin v0.5.2
```
