# Changelog

All notable changes to the "Gantry" project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2025-12-18

### Added
- **Performance**:
    - **Split-Persistence**: Introduced a binary sidecar (`_pixels.bin`) for high-speed append-only pixel storage, reducing SQLite metadata size by 99%+.
    - **Database Indexing**: Added indexes to Foreign Keys (`patient_id_fk`, etc.) and `audit_log` for O(1) query performance.
    - **Multithreaded Redaction**: `redact_pixels` now uses `ThreadPoolExecutor` to process Machine Rules in parallel, achieving near-linear speedup on multi-core systems.
- **Optimization**:
    - **Inverted Redaction Loop**: Refactored logic to iterate images once per machine (O(M)) instead of applying every rule to every image (O(NM)).
    - **Empty Zone Skipping**: Automatically skips processing machines with no configured ROIs.
- **Benchmarks**:
    - Verified throughput of **140,000 metadata inserts/sec** and **580 MB/s pixel writes** in stress tests.
- **UX**:
    - Added realtime `tqdm` progress bars for redaction.

### Fixed
- **Multiprocessing**: Fixed "Pickling Error" on Windows/spawn start methods by creating lightweight copies of the object graph for worker communication.
- **Redaction**: Fixed crash when `get_pixel_data` returns `None` (missing file).
- **Redaction**: Fixed "Completely Outside" warning logic for RGB images (interpreting Channels as Columns).

## [0.4.1] - 2025-12-12

### Added
- **Configuration Actions**: Support for `REMOVE` and `EMPTY` actions in `privacy_config.json` for precise tag handling.
- **Ingest Summary**: `ingest` command now provides a detailed count of imported objects.

### Fixed
- **Persistence Priority**: Fixed "Split Brain" issue where remediated `Study`/`Series` metadata was overwritten by original file attributes during export.
- **Export Error**: Fixed validation strictness to allow export of files with stripped Command Set (Group 0000) tags.
- **API Consistency**: Unified `scan_for_phi` and `audit` methods.

## [0.4.0] - 2025-12-11

### Added
- **Features**:
    - **Safe Export**: New `export(safe=True)` mode ensuring no PHI leaves the system.
    - **Reversible Anonymization**: Securely embed encrypted original identities (`gantry.key`).
    - **Manual Persistence**: Changed default behavior to manual `.save()` for better user control.
    - **Background Persistence**: Non-blocking saves via `PersistenceManager`.
    - **PHI Analysis Reports**: `scan_for_phi` now returns a rich `PhiReport` object with Pandas DataFrame support.
    - **Parallel Processing**: Multi-process support for Import and PHI Scanning.
- **Improvements**:
    - **Console Output**: Suppressed noisy `pydicom` warnings and improved `tqdm` progress bars.
    - **Batch UX**: Better feedback during long-running operations.
    - **Test Coverage**: specific tests for `crypto`, `config`, and `safe_export`.

### Fixed
- **Regression**: Addressed silent failure in pixel export when source files are missing.
- **Bug**: Fixed `TypeError` in Remediation Date Shifting.
- **Bug**: Fixed `MultiValue` JSON serialization error in persistence.
- **Bug**: Fixed `ValueError` regarding Group 0000 elements during export.

## [0.3.0] - 2025-12-11

### Added
- **Robust Persistence (SQLite)**: Replaced `Pickle` with `SQLite` for session storage (`gantry.db`). Allows for scale and external querying.
- **Audit Trail**: Implemented a comprehensive audit system. Actions such as `Redaction` and `Remediation` are now logged to the `audit_log` table in the database.
- **Automated PHI Remediation**:
    - **Metadata Anonymization**: Automatically detects and anonymizes Patient Names and IDs.
    - **Deterministic Date Shifting**: Shifts study dates by a consistent offset (based on Patient ID hash) to preserve temporal relationships while obscuring actual dates.
- **`apply_remediation` API**: Added top-level API to `DicomSession` to easily apply fixes found by the privacy inspector.
- **Documentation**: Significant updates to `README.md` and architecture documentation.

### Changed
- **Breaking Change**: The internal persistence format has changed from `.pkl` to `.db`. Existing sessions from v0.2.0 cannot be loaded and must be re-imported.
- **Dependency Update**: Added `sqlite3` (stdlib) as a core dependency for the store backend.

## [0.2.0] - 2025-12-10

### Added
- **JSON Configuration Validation**: `ConfigLoader` now rejects rules with missing fields or invalid/illegal ROI definitions.
- **ROI Safety Checks**: Redaction operations now explicitly check image bounds, clipping ROIs to the image dimensions and warning if they are completely out of bounds.
- **File Deduplication**: `DicomImporter` now detects and skips files that have already been imported into the current session.

### Fixed
- **Recursive Sequence Import**: Nested sequences (e.g., in Structured Reports) are now correctly recursed and indexed.
- **Pixel Depth Export**: `DicomExporter` now correctly preserves 8-bit usage for relevant modalities (e.g., US, SC) instead of hardcoding 12/16-bit depth.

## [0.1.0] - 2025-12-09

### Added
- **Core Architecture**: Implemented the semantic object graph (`Patient` → `Study` → `Series` → `Instance`) to replace flat dictionary handling.
- **Facade Interface**: Added `gantry.Session` class as the primary entry point for user interaction, managing imports, persistence, and inventory.
- **Lazy Loading**: Implemented a Proxy Pattern for `Instance` objects. Metadata is loaded into memory during import, while heavy pixel data is read from disk only upon request.
- **De-Identification Service**: Added `RedactionService` to modify pixel data (burn-in removal) based on specific machine serial numbers.
- **Configuration Management**: Added support for `redaction_rules.json` to define Redaction Regions of Interest (ROIs) externally.
- **Machine Indexing**: Created `MachinePixelIndex` to efficiently group and retrieve instances by their Equipment attributes (Manufacturer, Model, Serial Number).
- **Builder Pattern**: Added `DicomBuilder` (and fluent sub-builders) to allow programmatic construction of complex DICOM hierarchies for testing and synthetic data generation.
- **IOD Validation**: Implemented `IODValidator` to enforce Type 1 and Type 2 attribute compliance for standard SOP Classes (e.g., CT Image Storage) before export.
- **Persistence**: Added `pickle`-based serialization to save and resume session state (`DicomStore`).
- **Import/Export**: Created `DicomImporter` for fast metadata scanning and `DicomExporter` for writing valid, standards-compliant `.dcm` files.

### Security
- Pixel data redaction is performed in-memory and committed to new files; original files are treated as read-only during the session to prevent accidental data loss.
