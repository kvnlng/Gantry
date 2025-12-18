# The Gantry Safety Pipeline: 8 Checkpoints for Secure Data Curation

This guide outlines the standard operating procedure for Curating, Anonymizing, and Redacting DICOM data using Gantry. It maps the user's intent ("Modes") to specific Gantry API capabilities ("Checkpoints").

---

## 🏗️ The 8 Checkpoints

| # | User Mode | Action | Gantry API / Feature |
| :--- | :--- | :--- | :--- |
| **1** | **Ingest** | Load & Index Data | `ingest` |
| **2** | **Examine** | Inventory & Query | `examine` |
| **3** | **Configure** | Define Rules | `setup_config` |
| **4** | **Target** | Identify Risks | `audit` |
| **5** | **Backup** | Secure Identity | `backup_identities` |
| **6** | **Anonymize** | Metadata Remediation | `anonymize_metadata` |
| **7** | **Redact** | Pixel Cleaning | `redact_pixels` |
| **8** | **Verify** | Safety Check | `verify` |
| **9** | **Export** | Release Data | `export_data` |

---

## 1. Checkpoint: Ingest
**Goal**: Bring raw data into the managed session.

```python
session.ingest("./raw_hospital_dump/2023_Q1")
```

## 2. Checkpoint: Examine
**Goal**: Understand what you have.

```python
session.examine()
```

## 3. Checkpoint: Configure (Define Rules)
**Goal**: Initialize your control files.
Gantry uses a **Unified Configuration** (v2.0) that defines both:
1. **PHI Tags**: Which metadata attributes to anonymize/hash (e.g., `PatientName`, `StudyDate`).
2. **Redaction Checkpoints**: Which machines need pixel-level redaction and where.

```python
# Generate a unified config skeleton based on your inventory
session.setup_config("privacy_config.yaml")

# USER ACTION: Edit 'privacy_config.yaml' in your text editor.
# - Add specific PHI tags to custom list
# - Define ROIs for detected machines
```

## 4. Checkpoint: Target (Audit)
**Goal**: Define and measure your privacy strategy.
This is an **active** checkpoint. You will iteratively refine your configuration tags and "measure" the accuracy of your definitions against the ingested metadata.

```python
# A. Audit Metadata (Measure Accuracy)
# Uses the 'phi_tags' defined in your unified config
risk_report = session.audit("privacy_config.yaml")
# ... Review report, edit config, repeat ...
```

## 4. Checkpoint: Backup (Identity preservation)
**Goal**: Secure the link between the Real World and the Research Data.
Before destroying identifiers, cryptographically seal them so authorized personnel can recover them later (Pseudonymization).

```python
# Initialize encryption key
session.enable_reversible_anonymization("master_key.key")

# "Backup" the identities of the targeted patients
session.preserve_identities(phi_findings)
```

### Mechanism: Standard-Compliant Reversibility
Gantry uses the **DICOM Standard Encrypted Attributes Sequence (0400,0500)** (per Part 15 E.1.2) to store original data.
- **Interoperability**: Unlike private tags, this standard method allows other compliant DICOM systems to recognize that encrypted data is present (even if they cannot decrypt it without the key).
- **Structure**:
    - `(0400,0500) EncryptedAttributesSequence`: Container
        - `(0400,0510) EncryptedContent`: The encrypted blob (AES-128-CBC)
        - `(0400,0520) EncryptedContentTransferSyntaxUID`: Signals the payload format.


## 5. Checkpoint: Anonymize (Metadata)
**Definition**: "Anonymize" refers strictly to **Tagging Data** (Metadata).
We clean the object graph attributes (PatientName, PatientID, etc.) using the **Target Findings** identified in the Audit (Checkpoint 3).

```python
# Use the findings from the Audit step
# risk_report = session.audit(...)
session.anonymize_metadata(risk_report)
```

## 6. Checkpoint: Redact (Pixels)
**Definition**: "Redact" refers strictly to **Imaging Data** (Pixels).
We remove burned-in text from the pixel matrix using the configured ROIs.

```python
# Load the plan
session.load_config("privacy_config.yaml")

# Execute: Process pixel data
session.redact_pixels()
```

## 7. Checkpoint: Verify (Double Check)
**Goal**: Ensure nothing was missed.
Re-run the scans on the *modified* in-memory session.

```python
final_check = session.audit()
if final_check:
    print("WARNING: Residual PHI detected!")
else:
    print("Verification Passed: Metadata is clean.")
```

## 8. Checkpoint: Export
**Goal**: Finalize and Release.
Write the clean objects to new DICOM files. The `safe=True` flag enforces a final "Gatekeeper" check.

```python
# Safe Export: Will fail/skip if any PHI remains
session.export("./clean_ dataset_v1", safe=True)
```

## 9. Saving Progress (Persistence)
**Goal**: Pause and Resume.
You can save the session state at any time. This persists all metadata to SQLite (`gantry.db`) and all modified pixel data to the **Sidecar** (`gantry_pixels.bin`).

```python
# Saves everything (including redacted pixels)
session.save()

# Resume later
new_session = gantry.Session("gantry.db")
# Pixels are lazy-loaded from the sidecar automatically
```
