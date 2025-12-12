# Gantry

**A Robust, Object-Oriented DICOM Management & Redaction Toolkit.**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Gantry** is a high-level framework for curating, anonymizing, and redacting medical imaging data. It transforms raw DICOM files into safe, research-ready datasets.

Instead of treating files as flat dictionaries, Gantry provides a semantic interface (`Patient` → `Study`) to automate critical tasks like **PHI Scanning**, **Reversible Anonymization**, **Pixel Redaction**, and **Compliance Auditing**.

Built for scale, its **Lazy-Loading** engine handles large cohorts with minimal memory overhead, allowing you to modify and export gigabytes of pixel data on demand.

---

## 🚀 Key Features

### 🛡️ Privacy & Security
*   **Automated PHI Remediation**: Detects and fixes PHI in metadata (Anonymization & Date Shifting).
*   **Reversible Anonymization**: Securely embeds encrypted original identities for authorized recovery (pseudonymization).
*   **Pixel Redaction**: Rules-based engine to redact burned-in PHI from pixel data.
*   **Safe Export**: Just-in-time scanning ensures no "dirty" data is ever written to disk.
*   **Audit Trail**: Logs all sensitive actions to an internal SQLite audit log for compliance.

### ⚡ Performance & Scale
*   **Parallel Processing**: Multi-process architecture for high-speed import and PHI scanning.
*   **Lazy Loading**: Minimal memory footprint; loads pixel data only when needed.
*   **Robust Persistence**: Powered by **SQLite** (`gantry.db`) to handle large-scale datasets with ACID guarantees.
*   **Optimized Batch Operations**: Efficiently processes thousands of files with deferred execution and non-blocking I/O.

### 🧠 Intelligent Data Management
*   **Hierarchical Object Model**: Work with semantic entities (`Patient` → `Study`) instead of raw tags.
*   **Machine-Centric Indexing**: Automatically groups images by equipment signature (Model & Serial).
*   **IOD Validation**: Built-in checks to ensure DICOM standard compliance (Type 1/2 tags).

### 🛠️ Developer Tools
*   **Fluent Builder API**: Programmatically construct valid DICOM datasets for testing.
*   **Comprehensive Logging**: Detailed file-based logs with environment-variable support.

---

## 📦 Installation
Clone the repository and install in editable mode:

```bash
git clone https://github.com/kvnlng/gantry.git
cd gantry
pip install -e .
```

## ⚡ Quick Start: End-to-End Workflow

This guide takes you through a complete de-identification pipeline. For a detailed breakdown of the **8 Safety Checkpoints** (Ingest, Examine, Target, Backup, etc.), see the [Gantry Safety Pipeline](docs/WORKFLOW.md).

The `gantry.Session` Facade is your primary entry point.

```python
import gantry

# 1. Ingest
session = gantry.Session("gantry.db")
session.ingest("./raw_dicom_data")

# 2. Examine (Inventory)
session.examine()
# Output: Inventory: 3 Devices...

# 3. Configure (Define Rules)
session.setup_config("privacy_config.json")
# [User edits json file...]
# Advanced: {"0010,0010": {"name": "PatientName", "action": "REMOVE"}}

# 4. Target (Audit for PHI)
risk_report = session.audit("privacy_config.json")

# 5. Backup (Identity Preservation)
session.enable_reversible_anonymization("gantry.key")
session.backup_identities(risk_report)

# 6. Anonymize (Metadata)
session.anonymize_metadata(risk_report)

# 7. Redact (Pixels)
session.load_config("privacy_config.json")
session.redact_pixels()

# 8. Verify
session.verify()

# 9. Export (Safe)
session.export_data("./clean_dicoms", safe=True)

# Save Session
session.save()
```

---

## 💾 Persistence & State Management

Gantry uses a robust SQLite backend (`gantry.db`) to handle large datasets. Persistence is **manual** to give you control over when to write to disk.

```python
# Save your session state to the database
app.save()  # Writes changes in the background
```

You must call `.save()` after operations like `import_folder`, `apply_remediation`, or `execute_config` if you want to keep the changes.


---

## 🕵️ Privacy Analysis & Audit

The **Target** checkpoint allows you to actively measure privacy risks before applying any remediation. Gantry generates a `risk_report` that you can analyze iteratively.

### 1. Generate Risk Report

```python
# Scan based on your "privacy_config.json"
risk_report = session.audit("privacy_config.json")
```

### 2. Analyze Findings

The `risk_report` is an iterable collection of `PhiFinding` objects, but its real power comes from integration with **Pandas**.

```python
# Convert to DataFrame
df = risk_report.to_dataframe()

# A. High-Level Summary
print(df["reason"].value_counts())
# Output:
# Dates are Safe Harbor restricted    120
# Names are PHI                        45
# Custom Tag Flagged (ProtocolName)    10

# B. Drill Down into Specific Risks
names = df[df["field"] == "PatientName"]
print(f"Found {len(names)} unique names exposed.")

# C. pivot analysis (e.g., Which modalities have the most issues?)
# (Assuming you joined with series metadata, or just check entity types)
print(df.groupby(["entity_type", "reason"]).size())
```

This analysis helps you refine your `privacy_config.json` (e.g., ignoring false positives) before you commit to anonymization.

---

## 🧩 Architecture

Gantry is modularized into the following components:

| Module | Description |
| :--- | :--- |
Gantry uses a **Layered Architecture** accessed via a central Facade.

```mermaid
graph TD
    User([User]) --> Session
    
    subgraph Core [Gantry Codebase]
        direction TB
        Session[DicomSession Facade]
        
        subgraph Logic [Services Layer]
            Privacy[PhiInspector]
            Remediation[RemediationService]
            Indexer[MachinePixelIndex]
        end

        subgraph Model [Object Graph]
            Patient --> Study
            Study --> Series
            Series --> Instance
        end

        subgraph Storage [Persistence Layer]
            Sqlite[(SqliteStore)]
        end
    end

    FS[File System]

    Session -->|Delegates| Logic
    Session -->|Manages| Model
    Session -->|Persists| Storage
    
    Logic -->|Scans/Modifies| Model
    Instance -.->|Lazy Load| FS
    Sqlite <-->|Hydrate/Save| Model
```

### Components

*   **Facade (`gantry.session`)**: The single entry point for all user interactions. It orchestrates the flow components.
*   **Object Graph (`gantry.entities`)**: A hierarchical, in-memory representation of DICOM data (`Patient` → `Study` → `Series` → `Instance`). It uses a **Proxy Pattern** to lazy-load pixel data from disk only when accessed, keeping memory usage low.
*   **Storage (`gantry.persistence`)**: A **SQLite** backend that handles metadata persistence, query optimization, and the **Audit Trail**.
*   **Services (`gantry.services`, `gantry.privacy`)**: encapsulation of business logic (Indexing, Redaction, PHI Scanning, Anonymization).
*   **I/O (`gantry.io_handlers`)**: Low-level wrappers around `pydicom` for robust file reading/writing.

---

## 🧪 Running Tests

Gantry uses `pytest` for comprehensive unit and integration testing.

```bash
# Run the full suite
pytest -v
```

---

## 🗺️ Roadmap

Interested in where Gantry is heading? Check out our [Roadmap](ROADMAP.md).

We welcome contributions! If you'd like to help with any items, please open a Pull Request.

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.
