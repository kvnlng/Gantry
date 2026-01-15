# Configuration Guide

Gantry uses a **Unified YAML Configuration** (v2.0) to control all aspects of de-identification, including PHI tag rules, date shifting, and pixel redaction.

This file allows you to define a reproducible privacy policy that can be shared across your team or version controlled.

## Quick Reference

| Section | Description |
| :--- | :--- |
| **[privacy_profile](#1-privacy-profile)** | Base set of rules (e.g., "basic", "comprehensive"). |
| **[date_jitter](#2-date-jitter)** | Randomly shifts dates to preserve intervals while hiding exact dates. |
| **[remove_private_tags](#3-private-tags)** | Removes vendor-specific private tags (odd groups). |
| **[phi_tags](#4-phi-tags)** | overrides or adds specific tag rules (e.g., `PatientName`). |
| **[machines](#5-pixel-redaction-machines)** | Defines burn-in redaction zones for specific equipment. |

---

## Complete Example

Save this as `gantry_config.yaml`:

```yaml
# 1. Privacy Profile (Base Rules)
# Options: "basic", "comprehensive", or path to external YAML
privacy_profile: "basic"

# 2. Date Jitter
# Shifts all dates by a random amount within this range.
# The shift is deterministic per-patient (consistent across studies).
date_jitter:
  min_days: -30
  max_days: -10

# 3. Private Tags
# Remove all odd-group tags (vendor specific) unless whitelisted?
remove_private_tags: true

# 4. Custom PHI Tags (Overrides Profile)
phi_tags:
  "0010,0010": 
    action: "REMOVE"
    name: "PatientName"
    
  "0010,0020": 
    action: "REPLACE"
    name: "PatientID"
    value: "ANONYMIZED" # Matches default if omitted
    
  "0008,0080":
    action: "KEEP" # Exception: Keep InstitutionName

# 5. Pixel Redaction Rules (Machine Specific)
machines:
  - serial_number: "US-12345"
    model_name: "Voluson E10"
    redaction_zones:
      # [row_start, row_end, col_start, col_end]
      - [0, 50, 0, 800]   # Top Banner
      - [900, 1024, 0, 400] # Bottom Left Details
```

---

## Detailed Options

### 1. Privacy Profile

Sets the baseline behavior for thousands of DICOM tags.

```yaml
privacy_profile: "comprehensive"
```

* **`basic`**: Implements the *DICOM PS3.15 Annex E Basic Profile*. Retains some descriptors but removes direct identifiers.
* **`comprehensive`**: Aggressive de-identification. Removes almost all non-structural text fields.
* **External File**: You can provide a path to another YAML file (e.g., `./profiles/my_hospital_standard.yaml`) to inherit its rules.

### 2. Date Jitter

Shifts all date attributes (`DA`, `DT`) by a random number of days.

* **Logic**: Gantry generates a secret random offset for each `PatientID`. This offset is consistent for that patient across all their studies and series, preserving temporal relationships (intervals) while hiding the absolute dates.
* **Config**:

    ```yaml
    date_jitter:
      min_days: -10
      max_days: 10
    ```

### 3. Private Tags

DICOM Private Tags (Odd Group Numbers, e.g., `0009,xxxx`) often contain hidden PHI strings dumped by the machine.

```yaml
remove_private_tags: true
```

* `true`: Removes **ALL** private tags. (Recommended for safety).
* `false`: Retains them (Use only if you are sure they are safe or strictly needed for analysis).

### 4. PHI Tags

Define specific rules for individual DICOM tags. Keys must be uppercase hex strings (e.g. `"0010,0010"`).

**Supported Actions:**

| Action | Logic | Example Config |
| :--- | :--- | :--- |
| **`REPLACE`** | Replaces value with "ANONYMIZED" (or custom string). | `action: "REPLACE", value: "Project-X"` |
| **`REMOVE`** | Completely deletes the tag from the dataset. | `action: "REMOVE"` |
| **`EMPTY`** | Sets the tag value to an empty string. | `action: "EMPTY"` |
| **`SHIFT`** | Applies the per-patient Date Jitter offset (Dates only). | `action: "SHIFT"` |
| **`KEEP`** | Explicitly retains the original value (Exception to profile). | `action: "KEEP"` |

**Example:**

```yaml
phi_tags:
  "0008,1030": { "action": "EMPTY", "name": "StudyDescription" }
  "0010,0030": { "action": "SHIFT", "name": "PatientBirthDate" }
```

### 5. Pixel Redaction (Machines)

Automatically scrubs burned-in text (pixels) for specific devices. Gantry identifies the machine using the `DeviceSerialNumber` (0018,1000) tag.

```yaml
machines:
  - serial_number: "SN-9999"
    model_name: "Documentation Only"
    redaction_zones:
      - [0, 100, 0, 500]
```

* **`serial_number`** (Required): Exact match for `0018,1000`.
* **`redaction_zones`**: List of regions to zero out.
  * Format: `[y1, y2, x1, x2]` (Row Start, Row End, Col Start, Col End).
  * Coordinates are 0-indexed.
