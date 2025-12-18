# Gantry

**A Robust, Object-Oriented DICOM Management & Redaction Toolkit.**

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Gantry Tests](https://github.com/kvnlng/Gantry/actions/workflows/tests.yml/badge.svg)](https://github.com/kvnlng/Gantry/actions/workflows/tests.yml)

**Gantry** is a high-level framework for curating, anonymizing, and redacting medical imaging data. It transforms raw DICOM files into safe, research-ready datasets.

Instead of treating files as flat dictionaries, Gantry provides a semantic interface (`Patient` → `Study`) to automate critical tasks like **PHI Scanning**, **Reversible Anonymization**, **Pixel Redaction**, **Date Jittering**, and **Compliance Auditing**.

---

## 🚀 Key Features

### 🛡️ Privacy & Security
*   **Smart Remediation**: Detects and fixes PHI in metadata (Anonymization & Configurable Date Shifting).
*   **Research-Ready Configuration**: Built-in support for research datasets (keeping demographics, jittering dates).
*   **Strict De-Identification**: Optional "Nuclear" mode to remove all private tags except those required for reversibility.
*   **Reversible Anonymization**: Securely embeds encrypted original identities using **DICOM Standard Encrypted Attributes (0400,0500)** (Part 15 E.1.2 Compliant).
*   **Pixel Redaction**: Rules-based engine to redact burned-in PHI from pixel data, with auto-detection for known machine models.

### ⚡ Performance & Scale
*   **Parallel Processing**: Multi-process architecture for high-speed import and PHI scanning.
*   **Lazy Loading**: Minimal memory footprint; loads pixel data only when needed.
*   **Robust Persistence**: Powered by **SQLite** (`gantry.db`) to handle large-scale datasets with ACID guarantees.

### 🧠 Intelligent Data Management
*   **Hierarchical Object Model**: Work with semantic entities (`Patient` → `Study`) instead of raw tags.
*   **Machine-Centric Indexing**: Automatically groups images by equipment signature (Model & Serial).

### 🏆 Audit & Compliance
*   **Standard De-Identification**: Automatically stamps `(0012,0063)` and `(0012,0064)` with privacy profile details for compliance.
*   **Robust Auditing**: SQLite-backed audit logs with concurrency support for high-throughput remediation.

---

## 📦 Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/kvnlng/gantry.git
cd gantry
pip install -e .
```

---

## ⚡ Quick Start: Research Workflow

Gantry makes it easy to prepare data for research.

```python
import gantry

# 1. Ingest Data
session = gantry.Session("research_cohort.db")
session.ingest("./raw_dicom_data")

# 2. Inventory Equipment
session.examine() 
# Output: Found 3 Devices (GE Rev CT, Siemens Vida...)

# 3. Create Research Config
# Generates a YAML file with safe defaults for research:
# - Dates: JITTER (-365 to -1 days)
# - Demographics: KEEP (Age, Sex)
# - Private Tags: REMOVE (Strict)
session.scaffold_config("research_config.yaml")

# 4. (Optional) Customize Data Jitter
# Edit "research_config.yaml":
# date_jitter: { min_days: -10, max_days: -10 }

# 5. Backup Original Identities (Pseudonymization)
session.enable_reversible_anonymization("gantry.key")
session.backup_identities(session.store.patients)

# 6. Apply De-Identification
# This applies metadata anonymization, date shifting, and private tag removal
session.load_config("research_config.yaml")
risk_report = session.audit()
session.apply_remediation(risk_report)

# 7. Redact Pixels (if needed)
session.redact_pixels()

# 8. Safe Export
session.export_data("./clean_research_data", safe=True)

# Save Session
session.save()
```

---

## 🕵️ Advanced Configuration

### Date Jittering
Gantry supports deterministic date shifting based on a hash of the PatientID. You can configure the range in your config file:

```yaml
date_jitter:
  min_days: -365
  max_days: -1
```

### Private Tag Removal
By default, research configurations enable strict private tag removal. This removes *all* odd-group tags, ensuring no hidden PHI leaks, while automatically whitelisting Gantry's own security tags (`0400,0500`) used for reversible anonymization.

```yaml
remove_private_tags: true
```

### Standard Privacy Profiles
To simplify configuration, Gantry includes built-in privacy profiles based on industry standards.

**Basic Profile (`privacy_profile": "basic"`)**:
Based on the **DICOM PS3.15 Annex E (Basic Profile)**, this profile provides a safe baseline for de-identification by removing or cleaning 18+ common identifiers (e.g., `PatientName`, `PatientID`, `BirthDate`, `OperatorsName`).

Using a standard profile ensures you are compliant with best practices without manually specifying every tag. You can still override specific tags in your `phi_tags` config.

```json
{
    "version": "2.0",
    "privacy_profile": "basic",
    "phi_tags": {
        "0010,0010": { "action": "KEEP", "name": "Patient Name (Exception)" } 
    }
}
```

---

## 🧩 Architecture

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

---


### CTP Rule Import (Legacy Support)
If you are migrating from CTP (RSNA Clinical Trial Processor), Gantry can ingest your existing `DicomPixelAnonymizer.script` files to preserve your knowledge base of redaction zones.

**Step 1: Convert the Script**
Use the built-in utility to convert your `.script` file into a Gantry-compatible JSON file.

```bash
python -m gantry.utils.ctp_parser /path/to/DicomPixelAnonymizer.script gantry/resources/ctp_rules.yaml
```

**Step 2: Scaffolding**
Once the `ctp_rules.json` file is in `gantry/resources/`, the `scaffold_config` command will automatically use it to match machines and pre-fill redaction zones for your new inventory.

```python
# In your python session
session.scaffold_config("my_new_config.yaml") 
# Gantry will check resources/ctp_rules.yaml and apply matching zones!
```

## 🧪 Running Tests

```bash
pytest -v
```

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.
