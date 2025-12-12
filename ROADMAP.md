# Project Roadmap

This document outlines the development plan for **Gantry**. We welcome contributions from the community to help us achieve these milestones!

## 📍 Current Status: v0.4.1
- [x] Core Object Model (`Patient` -> `Study` -> `Series` -> `Instance`)
- [x] Basic Facade (`DicomSession`)
- [x] Lazy Loading Pixel Data
- [x] Basic Redaction by Machine Serial Number
- [x] Configuration Validation & Safety Checks
- [x] PHI Detection (Privacy Inspector)
- [x] Consolidated Configuration Management
- [x] Facade-based PHI Scanning
- [x] Recursive Directory Import
- [x] Configuration Scaffolding (Auto-Inventory)
- [x] Standardized DICOM Derivation (UID Regeneration & Flags)
- [x] Improved Documentation & Docstrings
- [x] Comprehensive Logging System (File-based + Progress Bars)
- [x] Automated PHI Remediation (Metadata Anonymization & Date Shifting)
- [x] Robust Persistence Strategy (SQLite + Audit Trail)
- [x] Robust JSON Persistence (Support for Bytes/Private Tags)
- [x] Reversible Anonymization (Pseudonymization with Encrypted Private Tags)
- [x] Parallel Processing (Multiprocessing for Import & PHI Scan)
- [x] Optimized Batch UX (Deferred Persistence + Feedback)
- [x] Advanced Configuration Actions (REMOVE / EMPTY)

---

## 🚀 Upcoming Milestones

### v0.5.0 - Data Integrity & Advanced Redaction
Focus: Validating data integrity and expanding redaction capabilities.
- [ ] **Pixel Integrity Tests**: Verify that zeroed-out pixels are truly zero (not just concealed by LUTs).
- [ ] **Pixel Integrity Tests**: Add unit tests to verify `PhotometricInterpretation` and `SamplesPerPixel` are preserved after modification.
- [ ] **Memory Profiling**: Optimize `MachinePixelIndex` to handle massive datasets without excessive RAM usage.
- [ ] **Standard Privacy Profiles**: Built-in compliance profiles (e.g., DICOM PS3.15 Annex E) to simplify configuration.
- [ ] **Pixel Content Analysis (OCR)**: Detect burned-in text using OCR (Tesseract) / Cloud Vision to automatically flag sensitive images.

### v0.6.0 - Analytics & Reporting
Focus: Empowering users to understand their data through deep inspection on the object graph.
- [ ] **Object Graph to DataFrame**: Expose a method to flatten `Patient -> Study -> Series -> Instance` hierarchy into a comprehensive Pandas DataFrame.
- [ ] **Metadata Querying**: Enable SQL-like querying on the dataframe (e.g., "Find all scans with `SliceThickness < 1.0` acquired by 'GE' scanners").
- [ ] **Audit Reporting**: Export comprehensive CSV reports of the session inventory, including details on what was redacted or modified.
- [ ] **Structured Reporting (SR) Support**: Support for deep parsing and anonymization of DICOM Structured Reports.

### v0.7.0 - The Connector (Networking)
Focus: Integrating Gantry into clinical workflows via DIMSE services.
- [ ] **PACS Integration**: Implement C-STORE, C-FIND, C-MOVE using `pynetdicom` to query and pull studies directly.
- [ ] **Research Export Formats**: Native support for exporting to NIfTI and BIDS standards.

### v0.8.0 - Cloud Scale
Focus: Native support for cloud storage to handle massive datasets.
- [ ] **Persistence Abstraction**: Decouple storage logic to enable cloud backends and future plugins.
- [ ] **Cloud Storage Adapters**: Native ingestion/export for S3, Google Cloud Storage, and Azure Blob.

### v1.0.0 - Production Release
- [ ] **API Freeze**: Lock down the `DicomSession` interface.
- [ ] **Documentation**: Complete API reference and tutorials on ReadTheDocs or Wiki.
- [ ] **PyPI Release**: Publish package to the Python Package Index.

### v1.1.0 - Zero Code (CLI)
Focus: Making Gantry accessible to non-programmers and CI pipelines.
- [ ] **Gantry CLI**: A rich command-line interface for auditing and anonymizing datasets.

---

## 🔮 Future Ideas (Backlog)
- **3D Defacing**: Algorithmically remove facial features from 3D volumes (MRI/CT).
- **Plugin System**: Hooks for custom user scripts during ingest/audit/export loops.
- GUI Wrapper for `DicomSession`.
