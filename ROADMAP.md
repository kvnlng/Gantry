# Project Roadmap

This document outlines the development plan for **Gantry**. We welcome contributions from the community to help us achieve these milestones!

## 📍 Current Status: v0.4.0
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

---

## 🚀 Upcoming Milestones

### v0.5.0 - Data Integrity & Advanced Redaction
Focus: Validating data integrity and expanding redaction capabilities.
- [ ] **Pixel Integrity Tests**: Verify that zeroed-out pixels are truly zero (not just concealed by LUTs).
- [ ] **Pixel Integrity Tests**: Add unit tests to verify `PhotometricInterpretation` and `SamplesPerPixel` are preserved after modification.
- [ ] **UID Regeneration**: (Optional) Add a strategy to automatically regenerate SOP Instance UIDs for anonymized files to prevent ID conflicts with original data.
- [ ] **Memory Profiling**: Optimize `MachinePixelIndex` to handle massive datasets without excessive RAM usage.

### v1.0.0 - Production Release
- [ ] **API Freeze**: Lock down the `DicomSession` interface.
- [ ] **Documentation**: Complete API reference and tutorials on ReadTheDocs or Wiki.
- [ ] **PyPI Release**: Publish package to the Python Package Index.

---

## 🔮 Future Ideas (Backlog)
- Support for **DICOM Structured Reports (SR)**.
- **Auto-detection** of burned-in text using OCR (Tesseract) instead of fixed coordinates.
- GUI Wrapper for `DicomSession`.
