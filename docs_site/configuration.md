# Configuration

Gantry uses a **Unified YAML Configuration** to control all aspects of de-identification.

## Example `config.yaml`

```yaml

# 1. Privacy Profile (Optional)
# Defines the baseline set of tags to remove/clean.
# Options:
#   - "basic": DICOM PS3.15 Annex E Basic Profile (Partial De-Id)
#   - "comprehensive": Full De-Identification (Most conservative)
#   - "/path/to/profile.yaml": Load a custom set of rules from an external file
privacy_profile: "basic"

# 2. Date Jitter
# Shift all dates by a random amount within this range (consistent per Patient).
date_jitter:
  min_days: -30
  max_days: -10

# 3. Private Tags
# Whether to remove all private dicom tags (odd groups).
remove_private_tags: true

# 4. Custom PHI Tags (Overrides Profile)
phi_tags:
  "0010,0010": { "action": "REMOVE", "name": "PatientName" }
  "0010,0020": { "action": "REPLACE", "name": "PatientID", "value": "ANON_{id}" }

# 5. Pixel Redaction Rules (Machine Specific)
machines:
  - serial_number: "DEV12345"
    model_name: "UltraSound Pro"
    redaction_zones:
      - [0, 50, 0, 800] # ROI: [row_start, row_end, col_start, col_end]
```

## Advanced Features

### Pixel Redaction

Gantry can scrub burned-in PHI from pixels based on matching the equipment's `DeviceSerialNumber`. Define `redaction_zones` in your config to automatically verify and scrub these regions during export/anonymization.

### Reversible Anonymization

To maintain a secure link back to the original identity:

```python
# Enable encryption (generates 'gantry.key')
session.enable_reversible_anonymization()

# Lock identities BEFORE anonymization to store encrypted original data
session.lock_identities("PATIENT_123")
```

Users can later recover the identity if they possess the correct key:

```python
session.recover_patient_identity("ANON_123")
```

### Strict Codec & Export Safety

Gantry performs strict validation during export. If a compressed image cannot be decompressed (e.g., due to missing codecs or corruption), the export **will fail** rather than passing through unverified data. This ensures 100% PHI safety.

Supported Transfer Syntaxes:

- JPEG Lossless (Process 14, SV1)
- JPEG 2000 (Lossless & Lossy)
- JPEG-LS
- RLE Lossless
- Standard JPEG Baseline/Extended
