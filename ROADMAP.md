# Project Roadmap

This document outlines the development plan for **Gantry**. We welcome contributions from the community to help us achieve these milestones!

## 📍 Current Status: v0.2.0
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

---

## 🚀 Upcoming Milestones

### v0.3.0 - Data Integrity & Verification
Focus: Ensuring that modified files remain clinically valid and structurally sound.
- [ ] **Automated PHI Remediation**: Automatically suggest redaction zones based on PHI scan findings.
- [ ] **Robust Persistence Strategy**: Research and implement a more robust and transparent persistence layer (replacing `pickle`) to support large datasets and external querying.
- [ ] **Pixel Integrity Tests**: Verify that zeroed-out pixels are truly zero (not just concealed by LUTs).
- [ ] **Pixel Integrity Tests**: Add unit tests to verify `PhotometricInterpretation` and `SamplesPerPixel` are preserved after modification.
- [ ] **UID Regeneration**: (Optional) Add a strategy to automatically regenerate SOP Instance UIDs for anonymized files to prevent ID conflicts with original data.
- [ ] **Audit Trail**: Generate a side-car report (`audit_log.json`) listing exactly which files were modified and by which rule.

### v0.4.0 - Performance Optimization
Focus: Scaling Gantry to handle dataset sizes of 1,000+ images efficiently.
- [ ] **Parallel Processing**: Investigate using `concurrent.futures` in `RedactionService` to process multiple images simultaneously.
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
