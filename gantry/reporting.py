import datetime
from dataclasses import dataclass, field
from typing import Dict, Protocol

@dataclass
class ComplianceReport:
    """
    Data Transfer Object holding all information required for a compliance report.
    """
    generated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    gantry_version: str = "Unknown"
    project_name: str = "Gantry Session"
    
    # Configuration / Context
    privacy_profile: str = "Unknown"
    deid_method: str = "Safe Harbor (Basic Profile)" # Default, can be overridden
    
    # Cohort Statistics
    total_patients: int = 0
    total_studies: int = 0
    total_series: int = 0
    total_instances: int = 0
    
    # Audit / Processing Statistics
    # e.g., {'ANONYMIZE_METADATA': 1200, 'REDACT_PIXELS': 50, 'EXPORT': 1200}
    audit_summary: Dict[str, int] = field(default_factory=dict)
    
    # Exceptions & Errors
    # List of "ERROR" or "WARNING" logs: (timestamp, action, details)
    exceptions: list = field(default_factory=list)

    # Manifest Summary
    # e.g., "Top 10 Studies: ..." or a simple list/dict
    manifest_summary: str = ""

    # Validation
    validation_status: str = "PENDING" # PASS, FAIL, PENDING
    validation_issues: int = 0
    verification_details: str = ""


class ReportRenderer(Protocol):
    """Protocol for a report renderer."""
    def render(self, report: ComplianceReport, output_path: str) -> None:
        ...


class MarkdownRenderer:
    """Renders the ComplianceReport as a formatted Markdown document."""

    def render(self, report: ComplianceReport, output_path: str) -> None:
        md_content = f"""# Compliance Report

**Generated At:** {report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}
**Project:** {report.project_name}
**System Version:** Gantry v{report.gantry_version}

## 1. Executive Summary

| Metric | Value |
| :--- | :--- |
| **Validation Status** | **{report.validation_status}** |
| Total Patients | {report.total_patients} |
| Total Instances | {report.total_instances} |
| Privacy Profile | {report.privacy_profile} |
| De-ID Method | {report.deid_method} |

## 2. Processing Audit

The following actions were recorded in the secure audit trail:

| Action Type | Count |
| :--- | :--- |
"""
        # Add audit rows
        if report.audit_summary:
            for action, count in sorted(report.audit_summary.items()):
                md_content += f"| {action} | {count} |\n"
        else:
            md_content += "| *No audit logs found* | 0 |\n"

        # Exceptions Section
        if report.exceptions:
            md_content += f"\n## 3. Exceptions & Errors\n\n> [!WARNING]\n> The following issues were encountered during processing:\n\n"
            md_content += "| Timestamp | Action | Details |\n| :--- | :--- | :--- |\n"
            for exc in report.exceptions:
                # exc is expected to be (timestamp, action, details)
                # truncate details if too long?
                md_content += f"| {exc[0]} | {exc[1]} | {exc[2]} |\n"
        else:
            md_content += f"\n## 3. Exceptions & Errors\n\n*No exceptions or errors were recorded.*\n"

        # Cohort Manifest Section
        md_content += f"""
## 4. Cohort Manifest

{report.manifest_summary if report.manifest_summary else "*No manifest data available.*"}
"""

        md_content += f"""
## 5. Validation & Verification

*   **Identified Issues:** {report.validation_issues}
*   **Methodology:** The dataset was processed using the Gantry Safe Harbor pipeline. Pixel data was scanned against machine-specific redaction zones. Metadata was remediated according to DICOM PS3.15 {report.privacy_profile} profile.
*   **Verification Details:** {report.verification_details if report.verification_details else "Standard automated checks performed."}

---
**Data Protection Officer Signature:**

__________________________________________________
*(Date)*
"""
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(md_content)

def get_renderer(format_type: str) -> ReportRenderer:
    if format_type.lower() in ["md", "markdown"]:
        return MarkdownRenderer()
    raise ValueError(f"Unsupported report format: {format_type}")
