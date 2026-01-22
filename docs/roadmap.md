# Project Roadmap

This document outlines the development plan for **Gantry**. We welcome contributions from the community to help us achieve these milestones!

## 📍 Current Status: v0.6.0 (Architecture & Analytics)

- [x] **Hybrid Persistence**: Split-storage (JSON + Table) for unlimited private tags.
- [x] **Unified Configuration**: API-driven config management (`GantryConfiguration`).
- [x] **Sidecar Offloading**: Eager binary extraction for fast session loading.
- [x] **Compliance Reporting**: Generate reports verifying dataset compliance against a selected privacy profile.
- [x] **Dataframe Export**: Expose a method to flatten `Patient -> Study -> Series -> Instance` hierarchy into a comprehensive parquet file.
- [x] **Query-based Export**: Export subsets of data based on metadata queries (e.g., "Modality == 'CT'").
- [x] **Export Manifest**: Automatic generation of visual (HTML) and machine-readable (CSV/JSON) manifests listing all exported files and their key metadata.
- [x] **Structured Reporting (SR) Support**: Support for deep parsing and anonymization of DICOM Structured Reports.
- [x] **Legacy Config Removal**: Streamlined codebase by removing list-based config support.

---

## 🚀 Upcoming Milestones

### v0.7.0 - The Connector (Networking & Refinement)

Focus: Integrating Gantry into clinical workflows and deepening analysis capabilities.

- [ ] **PACS Integration**: Implement C-STORE, C-FIND, C-MOVE using `pynetdicom` to query and pull studies directly.
- [ ] **Research Export Formats**: Native support for exporting to NIfTI and BIDS standards.
- [ ] **Pixel Content Analysis (OCR)**: Detect burned-in text using OCR (Tesseract) / Cloud Vision to automatically flag sensitive images.
- [ ] **Audit Reporting**: Export comprehensive CSV reports of the session inventory, including details on what was redacted or modified.
- [x] **Sidecar Compaction**: Tool to rewrite sidecar file and reclaim space from redacted/deleted instances.

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
- **GUI Wrapper for `DicomSession`**.
- **Official Docker Image**: Optimized container build for Gantry with pre-configured codecs and dependencies.
