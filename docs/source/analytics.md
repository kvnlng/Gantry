# Analytics & Export

Gantry v0.6.0 introduces powerful tools for **Exploratory Data Analysis (EDA)**. Instead of blindly exporting entire cohorts, you can now interrogate your data using Pandas and perform targeted exports based on metadata criteria.

## Overview

The workflow consists of three steps:

1. **Index**: Ingest your DICOM data into a Gantry session.
2. **Analyze**: Generate a Pandas DataFrame of the entire cohort metadata.
3. **Export**: Filter the DataFrame and export only the matching instances (or specific subsets) to a new folder or Parquet file.

## 1. Generating a DataFrame

The `export_dataframe()` method streams metadata from the Gantry SQLite index into a Pandas DataFrame. This is extremely fast because it avoids loading pixel data.

```python
from gantry import Session

session = Session("my_project.db")
session.ingest("/path/to/dicom")

# Basic Inventory (Patient/Study/Series level info)
df = session.export_dataframe()
print(df.head())

# Comprehensive Metadata
# "expand_metadata=True" parses the full JSON attributes into columns
df_full = session.export_dataframe(expand_metadata=True)
print(df_full.columns)
# ['PatientID', 'Modality', 'SliceThickness', 'Manufacturer', ...]
```

## 2. Querying & Filtering

Once you have a DataFrame, you can use the full power of Pandas to explore your dataset.

### Example: Finding Thick Slice CTs

```python
# Filter for CT scans with slice thickness > 2.5mm
thick_cts = df_full[ 
    (df_full.Modality == 'CT') & 
    (df_full.SliceThickness > 2.5) 
]

print(f"Found {len(thick_cts)} thick slice instances.")
```

### Example: Finding specific scanners

```python
ge_scanners = df_full[ df_full.Manufacturer.str.contains('GE', case=False, na=False) ]
```

## 3. Targeted Export

You can export subsets of your data by passing a filter to the `export()` method. The `subset` argument accepts:

### A. Pandas DataFrame

Pass the filtered DataFrame directly. Gantry will export only the rows present in the DataFrame.

```python
# Export only the thick CTs identified above
session.export("export_thick_cts", subset=thick_cts)
```

### B. Query String

Pass a Pandas-style query string. Gantry will evaluate this against the full index.

```python
# Export all MRIs
session.export("export_mri", subset="Modality == 'MR'")

# Export specific Model
session.export("export_innovision", subset="ManufacturerModelName == 'InnoVision'")
```

### C. List of UIDs

Pass a list of `SOPInstanceUID` strings if you have matched them externally.

```python
uids = ["1.2.3.4", "1.2.3.5"]
session.export("export_manual", subset=uids)
```

## Parquet Export

For integration with external tools like Tableau, PowerBI, or Databricks, you can export the full inventory to a Parquet file.

```python
session.export_dataframe("cohort_inventory.parquet", expand_metadata=True)
```
