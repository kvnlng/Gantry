# Project Roadmap

This document outlines the development plan for **Gantry**. We welcome contributions from the community to help us achieve these milestones!

## 📍 Current Status: v0.5.2 (Stability & Performance)

- [x] **Core Object Model**: `Patient` -> `Study` -> `Series` -> `Instance`
- [x] **Split-Persistence Architecture**: Binary sidecar (`_pixels.bin`) for high-speed pixel storage.
- [x] **Database Indexing**: O(1) lookups and scalable Joins via SQLite indexes.
- [x] **Multithreaded Redaction**: Parallelized pixel redaction using `ThreadPoolExecutor`.
- [x] **Free-Threaded Stability**: Full support for Python 3.14t (no-GIL) via versioned dirty tracking.
- [x] **Deep Memory Management**: Automatic pixel offloading (`Instance.unload_pixel_data()`) to handle datasets exceeding RAM.
- [x] **Async Audit Queue**: Non-blocking SQLite persistence for high-throughput auditing.
- [x] **Custom Privacy Profiles**: Support for external YAML profiles.
- [x] **Standard Privacy Profiles**: Built-in support for DICOM PS3.15 Annex E.
- [x] **Legacy Config Removal**: Streamlined codebase by removing list-based config support.

---

## 🚀 Upcoming Milestones

### v0.6.0 - Analytics & Reporting

Focus: Empowering users to understand their data through deep inspection on the object graph.

- [ ] **Dataframe Export**: Expose a method to flatten `Patient -> Study -> Series -> Instance` hierarchy into a comprehensive parquet file.
- [ ] **Sidecar Compaction**: Utility to vacuum/compact the `_pixels.bin` file to reclaim space from deleted or redacted images.
- [ ] **Pixel Content Analysis (OCR)**: Detect burned-in text using OCR (Tesseract) / Cloud Vision to automatically flag sensitive images.
- [ ] **Metadata Querying**: Enable SQL-like querying on the dataframe (e.g., "Find all scans with `SliceThickness < 1.0` acquired by 'GE' scanners").
- [ ] **Query-based Export**: Allow users to filter exports using criteria (e.g., `session.export(query="Modality=='CT' and SliceThickness > 5.0")`).
- [ ] **Compliance Reporting**: Generate reports verifying dataset compliance against a selected privacy profile.
- [ ] **Export Manifest**: Automatic generation of visual (HTML) and machine-readable (CSV/JSON) manifests listing all exported files and their key metadata.
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
- **GUI Wrapper for `DicomSession`**.
- **Official Docker Image**: Optimized container build for Gantry with pre-configured codecs and dependencies.
