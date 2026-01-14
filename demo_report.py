from gantry import Session
import os

# 1. Initialize Session
if os.path.exists("demo.db"): os.remove("demo.db")
session = Session("demo.db")

# 2. Simulate Data (since we don't have files, we manually inject for the demo)
# In real usage, session.ingest() would create these.
session.store_backend.log_audit("INGEST", "BATCH_01", "Simulated Ingestion of 10 files")

# 3. Simulate Redaction/Anonymization Actions
# These usually happen inside session.anonymize() or session.redact()
# We manually log them to show how they appear in the report
session.store_backend.log_audit("ANONYMIZE", "PAT_12345", "Removed PatientName tag")
session.store_backend.log_audit("ANONYMIZE", "PAT_12345", "Shifted StudyDate by -10 days")
session.store_backend.log_audit("REDACT", "INST_98765", "Scrubbed pixel region [0,100,0,100]")

# 4. Generate Report
# This will flush the logs and create the markdown file
session.generate_report("compliance_report.md")

print("Report generated at compliance_report.md")
with open("compliance_report.md", "r") as f:
    print(f.read())
