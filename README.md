# Gantry

**A Robust, Object-Oriented DICOM Management & Redaction Toolkit.**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Gantry** is a high-level Python library designed to simplify the complexity of DICOM data management. Unlike low-level libraries that treat DICOM files as flat dictionaries of tags, Gantry constructs a **semantic object graph** (`Patient` → `Study` → `Series` → `Instance`).

It features a **Lazy-Loading Proxy** architecture, allowing you to index thousands of files with minimal memory footprint while retaining the ability to modify pixel data on demand.

---

## 🚀 Key Features

* **Hierarchical Object Model**: Work with logical entities, not just hex tags.
* **Machine-Centric Indexing**: Automatically group images by equipment (Scanner Model & Serial Number).
* **Lazy Loading**: Metadata is kept in memory; Pixel Data is loaded from disk only when requested.
* **De-Identification Engine**: Redact sensitive pixel data (burned-in PHI) based on machine specific rules.
* **Fluent Builder API**: Programmatically construct valid DICOM datasets for testing.
* **IOD Validation**: Built-in checks ensure your exported files comply with DICOM standards (Type 1/Type 2 attributes).
* **Session Persistence**: Save your workspace state to a lightweight `.pkl` file and resume later.

---

## 📦 Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/kvnlng/gantry.git
cd gantry
pip install -e .
```

---

## ⚡ Quick Start: The Gantry Session

The `gantry.Session` Facade is your primary entry point. It manages imports, persistence, and inventory.

```python
import gantry

# 1. Initialize a Session (Loads previous state if 'project.pkl' exists)
app = gantry.Session("my_project.pkl")

# 2. Ingest Data (Fast Metadata Scan)
app.import_folder("./raw_dicom_data")

# 3. Check Inventory
app.inventory()
# Output:
# Inventory: 3 Devices
#  - GE Revolution (S/N: SN-SCANNER-01)
#  - Siemens Prisma (S/N: SN-SCANNER-02)

# 4. Save State
# (Happens automatically on import/export operations)
```

---

## 🛡️ De-Identification Workflow

Gantry excels at redacting burned-in pixel data based on the specific machine that captured the image. You can define rules in a JSON configuration file.

### 1. Auto-Inventory (Scaffold Config)

Unsure which machines are in your dataset? Gantry can generate a config skeleton for you.

```python
# Identify unconfigured machines and write them to a JSON file
app.scaffold_config("my_redaction_rules.json")
```

Open `my_redaction_rules.json` and fill in the `redaction_zones` for each machine.

### 2. Define Rules (`redaction_rules.json`)

```json
{
    "version": "1.0",
    "machines": [
        {
            "serial_number": "SN-SCANNER-01",
            "model_name": "Revolution CT",
            "comment": "Redact Patient Name box in top-left",
            "redaction_zones": [
                {
                    "roi": [50, 100, 50, 200],
                    "note": "RowStart, RowEnd, ColStart, ColEnd"
                }
            ]
        }
    ]
}
```

### 2. Preview and Execute

```python
# Load the configuration
app.load_config("redaction_rules.json")

# Dry Run: See which images match the rules
app.preview_config()

# Execute: Lazy-load pixels, redact, and update the model
app.execute_config()

# Export: Write valid, redacted .dcm files to disk
app.export("./clean_dicoms")
```

---

## 🕵️ Privacy Inspector

Ensure your data is HIPAA Safe Harbor compliant by scanning for common PHI identifiers.

Ensure your data is HIPAA Safe Harbor compliant by scanning for common PHI identifiers directly from your session.

```python
# Scan all patients in the session
findings = app.scan_for_phi()
# Output:
# Scanning for PHI...
# Scan Complete. Found 2 potential PHI issues.
#  - [Patient] patient_name: John Doe (Names are PHI)

# Advanced: Use custom rules
app.scan_for_phi("my_custom_phi_rules.json")
```

---

## 🏗️ Advanced: The Builder Pattern

Need to generate synthetic test data? Use the Fluent Builder.

```python
from gantry import Builder
from datetime import date

patient = (
    Builder.start_patient("P123", "Test^Patient")
    .add_study("1.2.840.111.1", date(2023, 1, 1))
        .add_series("1.2.840.111.1.1", "CT", 1)
            .set_equipment("GE", "Revolution", "SN-999")
            .add_instance("1.2.840.111.1.1.1", "1.2.840.10008.5.1.4.1.1.2", 1)
                .set_pixel_data(my_numpy_array)
                .set_attribute("0020,0032", ["0","0","0"]) # Image Position
            .end_instance()
        .end_series()
    .end_study()
    .build()
)
```

---

## 🧩 Architecture

Gantry is modularized into the following components:

| Module | Description |
| :--- | :--- |
| **`gantry.session`** | The **Facade**. User-facing API for managing the workflow. |
| **`gantry.entities`** | The **Object Model**. Contains `Patient`, `Study`, `Instance`. Implements **Lazy Loading**. |
| **`gantry.io_handlers`** | Handles `pydicom` read/write, **Pickle persistence**, and the `DicomStore`. |
| **`gantry.services`** | Logic for **Indexing** (MachinePixelIndex) and **RedactionService**. |
| **`gantry.validation`** | Enforces **IOD Compliance** (Type 1/Type 2 tags) before export. |

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
